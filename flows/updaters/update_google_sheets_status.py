"""Fetch project status data from Google Sheets.

Replaces the legacy R script (get_googlesheets.R) and Python pipeline
(import_status.py + import_status_lib.py). Fetches three categories:

1. Project status sheets — a private TSV index pointing to ~26 project
   spreadsheets that follow the GoaT schema 2.5 format.
2. DTOL Plant Genome Size Estimates — Kew genome size data.
3. DTOL assembly informatics status — tolqc kmer draft sizes.
4. CNGB project status.

Outputs are per-project expanded TSV files matching legacy format.
"""

import csv
import io
import os
import re

import numpy as np
import pandas as pd

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    MIN_RECORDS,
    OUTPUT_PATH,
    S3_PATH,
    default,
    parse_args,
    required,
)
from flows.lib.utils import is_safe_path, safe_get, upload_to_s3

# Google Sheets URLs for non-project-status data
DTOL_PLANT_GENOME_SIZE_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSt0R1T3MpoOM6UFNMaT_Q9gR5TYyUZC1wgLqW_6_cH9zzII8ehadrbHX8bpktjTv2_yt_KHaj3x_e1"
    "/pub?output=tsv"
)
DTOL_TOLQC_STATUS_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vTU-En_URbYPtfyjBueQhnz7wYHt-OHVxvRyv9tNvCUPCTX9EEzxOL41QCUh6hgVNv-Vv_gLSAMJXv-"
    "/pub?gid=1442224132&single=true&output=tsv"
)
CNGB_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vQeTqi-qnoNgNl58gWDBT4CcR8nF9SmFOkC82KC6pkH42CoEi94yInhBE25SfxBqNeMBeVbpeEVs9GI"
    "/pub?gid=1726876704&single=true&output=tsv"
)


# ---------------------------------------------------------------------------
# Project status processing (port of import_status_lib.py)
# ---------------------------------------------------------------------------


def _open_google_spreadsheet(
    acronym: str, url: str, header_index: int
) -> pd.DataFrame:
    """Download a published Google Sheet as TSV and return a DataFrame."""
    encodings = ["utf-8", "ISO-8859-1", "latin1"]
    response = safe_get(url, timeout=120)
    response.raise_for_status()

    df = None
    for enc in encodings:
        try:
            content = response.content.decode(enc)
            df = pd.read_csv(
                io.StringIO(content),
                delimiter="\t",
                header=header_index,
                dtype=object,
                quoting=csv.QUOTE_NONE,
            )
            break
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    if df is None:
        raise ValueError(f"Failed to decode sheet for {acronym}")

    df.rename(columns={"#NCBI_taxon_id": "NCBI_taxon_id"}, inplace=True)
    df["project"] = acronym.upper()
    return df


def _general_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Replace whitespace-only cells with NaN, drop empty rows/cols."""
    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.replace("publication_available", "published", regex=False)
    df.dropna(how="all", axis=1, inplace=True)
    df.dropna(how="all", axis=0, inplace=True)
    df.rename(columns={"#NCBI_taxon_id": "NCBI_taxon_id"}, inplace=True)
    return df


def _cleanup_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise column headers: lowercase, underscored, no parens."""
    df.columns = (
        df.columns.str.replace(" ", "_")
        .str.replace(r"\(", "", regex=True)
        .str.replace(r"\)", "", regex=True)
        .str.lower()
    )
    return df


def _create_mandatory_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure mandatory columns exist."""
    for col in [
        "ncbi_taxon_id", "species", "family", "synonym",
        "publication_id", "contributing_project_lab",
        "target_list_status", "sequencing_status",
    ]:
        if col not in df.columns:
            df[col] = np.nan
    return df


def _expand_target_status(df: pd.DataFrame, acronym: str) -> pd.DataFrame:
    """Populate long_list, family_representative, other_priority columns."""
    for col in ["long_list", "family_representative", "other_priority"]:
        if col not in df.columns:
            df[col] = np.nan
    df["long_list"] = acronym

    lower = acronym.lower()
    fr_mask = df["target_list_status"].isin(
        [f"{lower}_family_representative", "family_representative"]
    )
    df.loc[fr_mask, "family_representative"] = acronym

    op_mask = df["target_list_status"].isin(
        [f"{lower}_other_priority", "other_priority"]
    )
    df.loc[op_mask, "other_priority"] = acronym
    return df


def _reduce_sequencing_status(df: pd.DataFrame, acronym: str) -> pd.DataFrame:
    """Map project-prefixed statuses to simple GoaT statuses."""
    status_map = {
        f"{acronym}_published": "published",
        f"{acronym}_insdc_open": "insdc_open",
        f"{acronym}_open": "open",
        f"{acronym}_insdc_submitted": "in_progress",
        f"{acronym}_in_assembly": "in_progress",
        f"{acronym}_data_generation": "in_progress",
        f"{acronym}_in_progress": "in_progress",
        f"{acronym}_sample_acquired": "sample_acquired",
        f"{acronym}_sample_collected": "sample_collected",
    }
    df["sequencing_status"] = df["sequencing_status"].replace(status_map)
    return df


def _create_status_columns(df: pd.DataFrame, acronym: str) -> pd.DataFrame:
    """Create and populate per-status columns."""
    statuses = [
        "sample_collected", "sample_acquired", "in_progress",
        "data_generation", "in_assembly", "insdc_submitted",
        "open", "insdc_open", "published",
    ]
    for s in statuses:
        if s not in df.columns:
            df[s] = np.nan
        df.loc[df["sequencing_status"] == s, s] = acronym
    return df


def _expand_sequencing_status(df: pd.DataFrame, acronym: str) -> pd.DataFrame:
    """Cascade statuses upward: published implies insdc_open, etc."""
    df.loc[df["published"] == acronym, "insdc_open"] = acronym
    df.loc[df["insdc_open"] == acronym, "open"] = acronym
    df.loc[df["open"] == acronym, "in_progress"] = acronym
    df.loc[df["data_generation"] == acronym, "in_progress"] = acronym
    df.loc[df["in_assembly"] == acronym, "in_progress"] = acronym
    df.loc[df["in_progress"] == acronym, "sample_acquired"] = acronym
    df.loc[df["sample_acquired"] == acronym, "sample_collected"] = acronym
    return df


def _process_project(acronym: str, url: str, header_row: int) -> pd.DataFrame:
    """Full processing pipeline for one project status sheet."""
    df = _open_google_spreadsheet(acronym, url, header_row)
    df = _general_cleanup(df)
    df = _cleanup_headers(df)
    df = _create_mandatory_columns(df)
    df = _expand_target_status(df, acronym)
    df = _reduce_sequencing_status(df, acronym)
    df = _create_status_columns(df, acronym)
    df = _expand_sequencing_status(df, acronym)
    return df


# ---------------------------------------------------------------------------
# Dedicated sheet fetchers (port of get_googlesheets.R)
# ---------------------------------------------------------------------------


def _fetch_dtol_plant_genome_sizes(output_path: str) -> int:
    """Fetch DTOL Plant Genome Size Estimates from Kew."""
    response = safe_get(DTOL_PLANT_GENOME_SIZE_URL, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text), delimiter="\t", dtype=str)
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"\(", "", regex=True)
        .str.replace(r"\)", "", regex=True)
        .str.lower()
    )
    df.dropna(how="all", axis=0, inplace=True)
    df = df[df["genus"].notna() & (df.get("project", pd.Series()) == "DTOL")]
    df["primary"] = "1"
    df.to_csv(output_path, sep="\t", index=False)
    return len(df)


def _fetch_dtol_tolqc_status(output_path: str) -> int:
    """Fetch DTOL assembly informatics status (kmer draft)."""
    response = safe_get(DTOL_TOLQC_STATUS_URL, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(
        io.StringIO(response.text),
        delimiter="\t",
        dtype=str,
        na_values=["NA", "missing", "", "NULL"],
    )
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"\(", "", regex=True)
        .str.replace(r"\)", "", regex=True)
        .str.lower()
    )
    df.dropna(how="all", axis=0, inplace=True)
    df = df[df["taxon"].notna()]
    df = df[df["accession"].isna() | ~df["accession"].str.startswith("GCA_", na=False)]
    df = df[~df["statussummary"].str.startswith("9", na=False)]
    df = df[~df["statussummary"].str.startswith("5", na=False)]
    df = df[["taxon", "est_size_mb", "length_mb"]].copy()
    for col in ["est_size_mb", "length_mb"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[df["est_size_mb"].notna() | df["length_mb"].notna()]
    df.to_csv(output_path, sep="\t", index=False)
    return len(df)


def _fetch_cngb(output_path: str) -> int:
    """Fetch CNGB project status sheet."""
    response = safe_get(CNGB_URL, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(
        io.StringIO(response.text),
        delimiter="\t",
        dtype=str,
        na_values=["NA", "missing", "", "NULL"],
    )
    df.dropna(how="all", axis=0, inplace=True)
    df.to_csv(output_path, sep="\t", index=False)
    return len(df)


# ---------------------------------------------------------------------------
# Prefect tasks and flow
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def fetch_project_status_sheets(
    index_url: str, output_dir: str
) -> dict:
    """Fetch all project status sheets listed in the private index TSV.

    Args:
        index_url (str): URL (or path) to the index TSV with columns
            project_acronym, published_url, start_header_line.
        output_dir (str): Directory to write per-project expanded TSVs.

    Returns:
        dict: Mapping of project acronym to row count.
    """
    response = safe_get(index_url, timeout=60)
    response.raise_for_status()

    index_df = pd.read_csv(
        io.StringIO(response.text),
        delimiter="\t",
        usecols=["project_acronym", "published_url", "start_header_line"],
        dtype={"project_acronym": str, "published_url": str, "start_header_line": int},
    )

    results = {}
    for _, row in index_df.iterrows():
        acronym = row["project_acronym"]
        url = row["published_url"]
        header_row = int(row["start_header_line"])
        print(f"Processing {acronym} (header row {header_row})")
        try:
            df = _process_project(acronym, url, header_row)
            out_file = os.path.join(output_dir, f"{acronym}_expanded.tsv")
            df.to_csv(out_file, sep="\t", index=False)
            results[acronym] = len(df)
            print(f"  {acronym}: {len(df)} rows")
        except Exception as exc:
            print(f"  {acronym}: FAILED — {exc}")
            failed_path = os.path.join(output_dir, f"{acronym}_expanded.tsv.failed")
            open(failed_path, "w").close()  # noqa: SIM115 — legacy compat
            results[acronym] = 0
    return results


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def fetch_other_sheets(output_dir: str) -> dict:
    """Fetch the three non-project Google Sheets (DTOL plant, tolqc, CNGB).

    Args:
        output_dir (str): Directory to write TSV files.

    Returns:
        dict: Mapping of filename to row count.
    """
    results = {}

    plant_path = os.path.join(output_dir, "DTOL_Plant_Genome_Size_Estimates.tsv")
    try:
        results["DTOL_Plant_Genome_Size_Estimates"] = _fetch_dtol_plant_genome_sizes(
            plant_path
        )
        print(f"Plant genome sizes: {results['DTOL_Plant_Genome_Size_Estimates']} rows")
    except Exception as exc:
        print(f"Plant genome sizes: FAILED — {exc}")
        results["DTOL_Plant_Genome_Size_Estimates"] = 0

    tolqc_path = os.path.join(
        output_dir, "DTOL_assembly_informatics_status_kmer_draft.tsv"
    )
    try:
        results["DTOL_tolqc_status"] = _fetch_dtol_tolqc_status(tolqc_path)
        print(f"DTOL tolqc status: {results['DTOL_tolqc_status']} rows")
    except Exception as exc:
        print(f"DTOL tolqc status: FAILED — {exc}")
        results["DTOL_tolqc_status"] = 0

    cngb_path = os.path.join(output_dir, "cngb.tsv")
    try:
        results["cngb"] = _fetch_cngb(cngb_path)
        print(f"CNGB: {results['cngb']} rows")
    except Exception as exc:
        print(f"CNGB: FAILED — {exc}")
        results["cngb"] = 0

    return results


@task(log_prints=True)
def upload_s3_dir(local_dir: str, s3_path: str) -> None:
    """Upload all TSV files in a directory to S3."""
    for fname in sorted(os.listdir(local_dir)):
        if fname.endswith(".tsv") or fname.endswith(".tsv.gz"):
            local_path = os.path.join(local_dir, fname)
            remote_path = f"{s3_path.rstrip('/')}/{fname}"
            print(f"Uploading {fname} to {remote_path}")
            upload_to_s3(local_path, remote_path)


@flow()
def update_google_sheets_status(
    output_path: str,
    index_url: str = None,
    s3_path: str = None,
    min_records: int = 0,
) -> bool:
    """Fetch all Google Sheets project status and supplementary data.

    Args:
        output_path (str): Directory to write output TSVs.
        index_url (str): URL to the private index TSV (from env
            GOAT_SHEETS_INDEX_URL if not provided).
        s3_path (str): Optional S3 path to upload results.
        min_records (int): Minimum total records to accept.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    os.makedirs(output_path, exist_ok=True)

    if index_url is None:
        index_url = os.environ.get("GOAT_SHEETS_INDEX_URL", "")

    total = 0
    project_results = {}
    if index_url:
        project_results = fetch_project_status_sheets(index_url, output_path)
        total += sum(project_results.values())
    else:
        print("No index URL provided — skipping project status sheets")

    other_results = fetch_other_sheets(output_path)
    total += sum(other_results.values())

    if total < min_records:
        raise RuntimeError(
            f"Google Sheets: fewer than {min_records} total records: {total}"
        )

    if s3_path:
        upload_s3_dir(output_path, s3_path)

    emit_event(
        event="update.google.sheets.status.finished",
        resource={
            "prefect.resource.id": f"update.google.sheets.status.{output_path}",
            "prefect.resource.type": "google.sheets.status",
        },
        payload={
            "total_records": total,
            "projects": len(project_results),
            "other_sheets": len(other_results),
        },
    )
    return True


if __name__ == "__main__":
    INDEX_URL = {
        "flags": ["--index_url"],
        "keys": {
            "help": "URL to the private index TSV listing project sheets.",
            "type": str,
        },
    }
    args = parse_args(
        [required(OUTPUT_PATH), INDEX_URL, S3_PATH, MIN_RECORDS],
        "Fetch project status data from Google Sheets.",
    )
    update_google_sheets_status(**vars(args))
