"""Fetch VGP Ordinal Phase1+ status from the live Google Sheet.

This updater replaces the legacy ``vgp_live_sheet_curation.py`` script
from goat-data. It downloads the live VGP spreadsheet, cleans headers,
translates project names to canonical acronyms, expands sequencing
status columns following the GoaT status hierarchy, and writes a TSV
matching the ``FILE_VGP_Ordinal_Phase1.types.yaml`` schema.

The companion ``update_vgp_original_status.py`` fetches the less
frequently updated VGP GitHub YAML tracker source.
"""

import io
import os

import numpy as np
import pandas as pd

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import MIN_RECORDS, OUTPUT_PATH, S3_PATH, parse_args, required
from flows.lib.utils import is_safe_path, safe_get, upload_to_s3

# Published (export) link to the VGP Ordinal Phase1+ Google Sheet
VGP_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1Jwjv6Kwc6VIn1UMMhnG6kvFCxjwGdC5b7p_HtbDOMOs"
    "/export?format=tsv"
    "&id=1Jwjv6Kwc6VIn1UMMhnG6kvFCxjwGdC5b7p_HtbDOMOs"
    "&gid=1380659438"
)

# Columns to import from the spreadsheet
SOURCE_COLUMNS = [
    "Order",
    "Lineage",
    "Superorder",
    "Family Scientific Name",
    "Scientific Name",
    "English Name",
    "NCBI taxon ID",
    "Status",
    "QV",
    "IUCN (2016-2024)",
    "CITES",
    "Main project",
    "Second project",
]

# Map free-text project names to canonical EBP acronyms
PROJECT_ACRONYMS = {
    "Sanger 25G": "25GP",
    "Sanger 25G project": "25GP",
    "AfricaBP": "AFRICABP",
    "Cetacean GP": "CGP",
    "DToL": "DTOL",
    "DToL?": "DTOL",
    "Yggdrasil": "YGG",
    "CatalanBP": "CBP",
    "Canadian Biogenome Project": "CANBP",
    "Canada Biogenome Project": "CANBP",
    "Threatened Species Initiative (TSI)": "TSI",
    "Minderoo OceanOmics": "OG",
    "DToL, ERGA": "DTOL,ERGA",
    "Amazoomics : Genomics of Brazilian Biodiversity": "AMAZOOMICS,GBB",
    "AmaZoomics : Genomics of Brazilian Biodiversity": "AMAZOOMICS,GBB",
    "Individual, Google": "Individual,Google",
}

# Numeric status code → GoaT sequencing status
STATUS_MAP = {
    "0": "",
    "1": "sample_collected",
    "2": "",
    "3": "in_progress",
    "4": "open",
    "5": "open",
}

# Full ordered list of GoaT sequencing status columns
SEQUENCING_STATUSES = [
    "sample_collected",
    "sample_acquired",
    "in_progress",
    "data_generation",
    "in_assembly",
    "insdc_submitted",
    "open",
    "insdc_open",
    "published",
]


# ---------------------------------------------------------------------------
# Processing helpers
# ---------------------------------------------------------------------------


def _cleanup_table(df: pd.DataFrame) -> pd.DataFrame:
    """Replace whitespace-only cells with NaN, drop empty rows/cols."""
    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.replace(r"^ +| +$", "", regex=True)
    df.dropna(how="all", axis=1, inplace=True)
    df.dropna(how="all", axis=0, inplace=True)
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


def _get_acronym(project_name: str) -> str:
    """Map a free-text project name to a canonical acronym."""
    return PROJECT_ACRONYMS.get(project_name, project_name)


def _translate_projects(df: pd.DataFrame) -> pd.DataFrame:
    """Map free-text project names to canonical acronyms."""
    for col in ["main_project", "second_project", "project"]:
        if col in df.columns:
            df[col] = df[col].map(lambda v: _get_acronym(str(v)) if pd.notna(v) else v)
    return df


def _build_all_projects(df: pd.DataFrame) -> pd.DataFrame:
    """Create 'all_projects' column from project + main + second."""
    df["all_projects"] = df.apply(
        lambda row: ",".join(
            sorted(
                {
                    x
                    for x in [
                        row.get("project"),
                        row.get("main_project"),
                        row.get("second_project"),
                    ]
                    if pd.notna(x)
                }
            )
        ),
        axis=1,
    )
    return df


def _expand_sequencing_status(df: pd.DataFrame) -> pd.DataFrame:
    """Map numeric status codes and cascade the GoaT status hierarchy."""
    # Ensure all status columns exist
    for col in SEQUENCING_STATUSES:
        if col not in df.columns:
            df[col] = None

    # Map numeric codes to status names
    df["sequencing_status"] = df["status"].map(STATUS_MAP)

    # Populate status columns with all_projects for matching rows
    for status in SEQUENCING_STATUSES:
        df.loc[df["sequencing_status"] == status, status] = df["all_projects"]

    # Cascade status hierarchy upward
    df.loc[df["published"] == df["all_projects"], "insdc_open"] = df["all_projects"]
    df.loc[df["insdc_open"] == df["all_projects"], "open"] = df["all_projects"]
    df.loc[df["open"] == df["all_projects"], "in_progress"] = df["all_projects"]
    df.loc[df["data_generation"] == df["all_projects"], "in_progress"] = df["all_projects"]
    df.loc[df["in_assembly"] == df["all_projects"], "in_progress"] = df["all_projects"]
    df.loc[df["in_progress"] == df["all_projects"], "data_generation"] = df["all_projects"]
    df.loc[df["in_progress"] == df["all_projects"], "sample_acquired"] = df["all_projects"]
    df.loc[df["sample_acquired"] == df["all_projects"], "sample_collected"] = df["all_projects"]
    return df


def _process_vgp_sheet(raw_tsv: str) -> pd.DataFrame:
    """Full processing pipeline for the VGP live sheet.

    Args:
        raw_tsv (str): Raw TSV text content from Google Sheets.

    Returns:
        pd.DataFrame: Cleaned, expanded DataFrame ready for export.
    """
    df = pd.read_csv(
        io.StringIO(raw_tsv),
        sep="\t",
        dtype=object,
        engine="python",
        on_bad_lines="warn",
        usecols=SOURCE_COLUMNS,
    )
    df = _cleanup_table(df)
    df = _cleanup_headers(df)
    df["project"] = "VGP"
    df = _translate_projects(df)
    df = _build_all_projects(df)
    df = _expand_sequencing_status(df)
    return df


# ---------------------------------------------------------------------------
# Prefect tasks and flow
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def fetch_vgp_live_sheet(output_path: str, min_records: int = 0) -> int:
    """Download the VGP Ordinal Phase1+ Google Sheet and write a TSV.

    Args:
        output_path (str): Path to the output TSV file.
        min_records (int): Minimum number of rows expected.

    Returns:
        int: Number of data rows written.
    """
    response = safe_get(VGP_SHEET_URL, timeout=120)
    if response is None:
        raise RuntimeError("Failed to fetch VGP live sheet: no response received")
    response.raise_for_status()

    df = _process_vgp_sheet(response.text)
    row_count = len(df)
    print(f"VGP live sheet: {row_count} rows after processing")

    if row_count < min_records:
        raise RuntimeError(f"VGP live sheet has fewer than {min_records} rows: {row_count}")

    df.to_csv(output_path, sep="\t", index=False)
    print(f"Wrote {output_path}")
    return row_count


@task(log_prints=True)
def upload_s3_tsv(local_path: str, s3_path: str) -> None:
    """Upload VGP TSV to S3."""
    print(f"Uploading {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_vgp_status(output_path: str, s3_path: str = "", min_records: int = 0) -> bool:
    """Fetch the VGP Ordinal Phase1+ live sheet and optionally upload to S3.

    Args:
        output_path (str): Path to the output TSV file.
        s3_path (str): Optional S3 path to upload the result.
        min_records (int): Minimum record count to accept the output.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")

    resolved_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(resolved_path), exist_ok=True)

    row_count = fetch_vgp_live_sheet(resolved_path, min_records)

    if row_count > min_records and s3_path:
        upload_s3_tsv(resolved_path, s3_path)

    emit_event(
        event="update.vgp.status.finished",
        resource={
            "prefect.resource.id": f"update.vgp.{resolved_path}",
            "prefect.resource.type": "vgp.status",
        },
        payload={"row_count": row_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, MIN_RECORDS],
        "Fetch VGP Ordinal Phase1+ status from the live Google Sheet.",
    )
    update_vgp_status(**vars(args))
