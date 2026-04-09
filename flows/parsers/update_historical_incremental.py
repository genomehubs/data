"""Daily incremental updates to historical assembly records.

Identifies assembly versions newly superseded since the last run and appends them to assembly_historical.tsv.  No NCBI fetches are required — data is copied directly from the previous assembly_current.tsv parse output.

Usage:
    python -m flows.parsers.update_historical_incremental \\
        --input_path assembly_data_report.jsonl \\
        --previous_tsv assembly_current.tsv.previous \\
        --historical_tsv outputs/assembly_historical.tsv
"""

import csv
import json
import os
from glob import glob
from pathlib import Path
from typing import Optional

from flows.lib.conditional_import import flow
from flows.lib.shared_args import INPUT_PATH
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required
from flows.lib.utils import Parser
from flows.parsers.parse_backfill_historical_versions import parse_accession

PREVIOUS_TSV = {
    "flags": ["-p", "--previous_tsv"],
    "keys": {
        "help": "Path to assembly_current.tsv from the previous run.",
        "type": str,
    },
}

HISTORICAL_TSV = {
    "flags": ["-H", "--historical_tsv"],
    "keys": {
        "help": "Path to assembly_historical.tsv to update.",
        "type": str,
    },
}


def load_previous_parsed_by_base(previous_tsv: str) -> dict[str, dict[int, dict]]:
    """Load previous parsed results indexed by base accession and version.

    Args:
        previous_tsv (str): Path to assembly_current.tsv from the previous run.

    Returns:
        dict: Nested mapping of base_accession -> version -> row data.
            Returns an empty dict if the file is not found, which is expected
            on the first run after the Phase 0 backfill.
    """
    previous_by_base: dict[str, dict[int, dict]] = {}

    try:
        with open(previous_tsv, encoding="utf-8") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                accession = row["accession"]
                base_acc, version = parse_accession(accession)
                if base_acc not in previous_by_base:
                    previous_by_base[base_acc] = {}
                previous_by_base[base_acc][version] = dict(row)
    except FileNotFoundError:
        print(f"Warning: Previous TSV not found: {previous_tsv}")
        print("  This is expected for the first run after the Phase 0 backfill.")
        return {}

    total = sum(len(v) for v in previous_by_base.values())
    print(f"Loaded {total} assemblies from previous parsed results.")
    print(f"  Unique base accessions: {len(previous_by_base)}")

    return previous_by_base


def build_superseded_row(
    previous_row: dict,
    previous_version: int,
    new_accession: str,
    new_version: int,
    release_date: str,
) -> dict:
    """Build a superseded row from a previous row with updated metadata.

    Args:
        previous_row (dict): Row data copied from the previous parsed TSV.
        previous_version (int): Version number of the assembly being superseded.
        new_accession (str): Accession of the assembly that supersedes this one.
        new_version (int): Version number of the superseding assembly.
        release_date (str): Release date of the superseding assembly.

    Returns:
        dict: Updated row with version_status, assembly_id, and superseded_by fields.
    """
    base_acc, _ = parse_accession(previous_row["accession"])
    row = previous_row.copy()
    row["version_status"] = "superseded"
    row["assembly_id"] = f"{base_acc}_{previous_version}"
    row["superseded_by"] = new_accession
    row["superseded_by_version"] = new_version
    row["superseded_date"] = release_date
    return row


def build_missing_version_record(
    base_acc: str,
    missing_version: int,
    new_version: int,
    new_accession: str,
    is_new_series: bool = False,
) -> dict:
    """Build a record describing a version missing from the previous parsed TSV.

    Args:
        base_acc (str): Base accession without version suffix.
        missing_version (int): The version number that could not be found.
        new_version (int): The new version number that triggered this check.
        new_accession (str): Full accession of the new assembly.
        is_new_series (bool): True if this is a new assembly series with no
            prior history at all.

    Returns:
        dict: Record suitable for writing to a missing-versions JSON file.
    """
    record = {
        "base_accession": base_acc,
        "missing_version": missing_version,
        "new_version": new_version,
        "new_accession": new_accession,
    }
    if is_new_series:
        record["note"] = "New assembly series — prior versions may need backfill"
    return record


def identify_newly_superseded(
    new_jsonl: str,
    previous_by_base: dict[str, dict[int, dict]],
) -> tuple[list[dict], list[dict]]:
    """Identify versions that became superseded in the current JSONL update.

    For each assembly with version > 1 in the new JSONL, checks whether the
    immediately preceding version exists in the previous parsed TSV.  Assemblies
    whose predecessor is found are added to the superseded list; those missing a
    predecessor are recorded for optional backfill.

    Args:
        new_jsonl (str): Path to the current assembly_data_report.jsonl.
        previous_by_base (dict): Indexed previous parsed results from
            load_previous_parsed_by_base.

    Returns:
        tuple: (newly_superseded, missing_versions) where each is a list of dicts.
    """
    newly_superseded: list[dict] = []
    missing_versions: list[dict] = []

    with open(new_jsonl) as f:
        for line in f:
            assembly = json.loads(line)
            accession = assembly["accession"]
            base_acc, new_version = parse_accession(accession)

            if new_version <= 1:
                continue

            previous_version = new_version - 1

            if base_acc not in previous_by_base:
                missing_versions.append(build_missing_version_record(
                    base_acc, previous_version, new_version, accession,
                    is_new_series=True,
                ))
                continue

            if previous_version not in previous_by_base[base_acc]:
                missing_versions.append(build_missing_version_record(
                    base_acc, previous_version, new_version, accession,
                ))
                continue

            previous_row = previous_by_base[base_acc][previous_version]
            release_date = assembly.get("releaseDate") or ""
            newly_superseded.append(build_superseded_row(
                previous_row, previous_version, accession, new_version, release_date,
            ))

    return newly_superseded, missing_versions


def append_superseded_to_tsv(
    newly_superseded: list[dict], historical_tsv: str
) -> None:
    """Append newly superseded rows to the historical TSV, deduplicating by assembly_id.

    Reads the existing file if present, merges new rows (new rows take
    precedence on duplicate assembly_id), and writes the combined result back.

    Args:
        newly_superseded (list): Row dicts from identify_newly_superseded.
        historical_tsv (str): Path to assembly_historical.tsv.
    """
    if not newly_superseded:
        print("  No newly superseded versions to add.")
        return

    existing: dict[str, dict] = {}
    historical_path = Path(historical_tsv)

    if historical_path.exists():
        with open(historical_tsv, encoding="utf-8") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                existing[row["assembly_id"]] = dict(row)

    for row in newly_superseded:
        existing[row["assembly_id"]] = row

    fieldnames = list(next(iter(existing.values())).keys())
    with open(historical_tsv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(existing.values())

    print(f"  Added {len(newly_superseded)} newly superseded versions.")
    print(f"  Total records in {historical_tsv}: {len(existing)}")


def print_superseded_summary(newly_superseded: list[dict]) -> None:
    """Print a short summary of the newly superseded versions.

    Args:
        newly_superseded (list): Row dicts from identify_newly_superseded.
    """
    if not newly_superseded:
        print("  Found: 0 newly superseded versions.")
        return

    print(f"  Found: {len(newly_superseded)} newly superseded versions.")
    print("  Examples:")
    for row in newly_superseded[:5]:
        print(f"    {row['accession']} -> superseded by v{row['superseded_by_version']}")
    if len(newly_superseded) > 5:
        print(f"    ... and {len(newly_superseded) - 5} more")


def print_missing_versions_warning(missing: list[dict]) -> None:
    """Print a warning listing versions absent from the previous parsed TSV.

    Args:
        missing (list): Missing-version records from identify_newly_superseded.
    """
    if not missing:
        return

    print(f"\n  Warning: {len(missing)} assemblies have missing previous versions.")
    print("  These may need manual backfill:")
    for m in missing[:5]:
        print(
            f"    {m['base_accession']}: "
            f"need v{m['missing_version']}, have v{m['new_version']}"
        )
    if len(missing) > 5:
        print(f"    ... and {len(missing) - 5} more")
    print("\n  To backfill missing versions, run:")
    print("    python -m flows.parsers.backfill_missing_versions")


@flow(log_prints=True)
def run_incremental_historical_update(
    new_jsonl: str,
    previous_tsv: str,
    historical_tsv: str,
) -> dict:
    """Daily incremental update of the historical assembly TSV.

    Called after parse_ncbi_assemblies completes.  Uses the previous parsed
    TSV — no NCBI fetches are made.

    Args:
        new_jsonl (str): Path to the current assembly_data_report.jsonl.
        previous_tsv (str): Path to assembly_current.tsv from the previous run.
        historical_tsv (str): Path to assembly_historical.tsv to update.

    Returns:
        dict: Summary with keys newly_superseded_count, missing_versions_count,
            and missing_versions (list of records).
    """
    separator = "=" * 80
    print(f"\n{separator}")
    print("INCREMENTAL HISTORICAL UPDATE")
    print(f"{separator}\n")

    print("[1/3] Loading previous parsed results...")
    previous_by_base = load_previous_parsed_by_base(previous_tsv)

    if not previous_by_base:
        print("  No previous parsed data available — skipping incremental update.")
        print("  This is expected for the first run after the Phase 0 backfill.\n")
        return {
            "newly_superseded_count": 0,
            "missing_versions_count": 0,
            "missing_versions": [],
        }

    print("\n[2/3] Identifying newly superseded versions...")
    newly_superseded, missing = identify_newly_superseded(new_jsonl, previous_by_base)
    print_superseded_summary(newly_superseded)
    print_missing_versions_warning(missing)

    print("\n[3/3] Updating historical TSV...")
    append_superseded_to_tsv(newly_superseded, historical_tsv)

    print(f"\n{separator}")
    print(
        f"INCREMENTAL UPDATE COMPLETE  "
        f"Superseded: {len(newly_superseded)}  "
        f"Missing: {len(missing)}"
    )
    print(f"{separator}\n")

    return {
        "newly_superseded_count": len(newly_superseded),
        "missing_versions_count": len(missing),
        "missing_versions": missing,
    }


def incremental_update_wrapper(
    working_yaml: str,
    work_dir: str,
    append: bool,
    data_freeze_path: Optional[str] = None,
    **kwargs,
) -> None:
    """Wrapper matching the fetch_parse_validate parser signature.

    Derives the previous TSV and historical TSV paths from work_dir and
    delegates to run_incremental_historical_update.

    Args:
        working_yaml (str): Path to the working YAML file (unused; accepted
            for pipeline compatibility).
        work_dir (str): Path to the working directory containing the JSONL,
            the previous TSV, and the historical TSV.
        append (bool): Unused; accepted for pipeline compatibility.
        data_freeze_path (str, optional): Unused; accepted for pipeline
            compatibility.
        **kwargs: Additional keyword arguments passed by the pipeline.
    """
    glob_path = os.path.join(work_dir, "*.jsonl")
    paths = glob(glob_path)
    if not paths:
        raise FileNotFoundError(f"No jsonl file found in {work_dir}")
    if len(paths) > 1:
        raise ValueError(f"More than one jsonl file found in {work_dir}")

    results = run_incremental_historical_update(
        new_jsonl=paths[0],
        previous_tsv=os.path.join(work_dir, "assembly_current.tsv.previous"),
        historical_tsv=os.path.join(work_dir, "assembly_historical.tsv"),
    )

    if results["missing_versions_count"] > 0:
        missing_json_path = os.path.join(work_dir, "missing_versions.json")
        with open(missing_json_path, "w", encoding="utf-8") as f:
            json.dump(results["missing_versions"], f, indent=2)
        print(f"  Missing versions written to: {missing_json_path}")


def plugin() -> Parser:
    """Register the flow."""
    return Parser(
        name="UPDATE_HISTORICAL_INCREMENTAL",
        func=incremental_update_wrapper,
        description="Daily incremental update of historical assembly records.",
    )


if __name__ == "__main__":
    args = _parse_args(
        [required(INPUT_PATH), required(PREVIOUS_TSV), required(HISTORICAL_TSV)],
        description="Daily incremental update of historical assembly records",
    )
    results = run_incremental_historical_update(
        new_jsonl=args.input_path,
        previous_tsv=args.previous_tsv,
        historical_tsv=args.historical_tsv,
    )
    print(f"Summary: superseded={results['newly_superseded_count']}, "
          f"missing={results['missing_versions_count']}")
    if results["missing_versions_count"] > 0:
        missing_json_path = Path(args.historical_tsv).parent / "missing_versions.json"
        with open(missing_json_path, "w", encoding="utf-8") as f:
            json.dump(results["missing_versions"], f, indent=2)
        print(
            f"  Action needed: {results['missing_versions_count']} missing versions."
        )
        print(f"  Written to: {missing_json_path}")
        print("  Run: python -m flows.parsers.backfill_missing_versions")
