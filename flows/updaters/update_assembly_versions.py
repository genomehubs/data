"""Fetch assembly versions missing from historical records.

Run this when parse_assembly_versions reports assemblies whose previous
version was absent from the previous parsed TSV.  Fetches only the specified
missing versions from NCBI FTP, parses them, and merges the result into the
existing assembly_historical.tsv.

Usage:
    python -m flows.updaters.update_assembly_versions \\
        --missing_json tmp/missing_versions.json \\
        --yaml_path configs/assembly_historical.types.yaml \\
        --work_dir tmp
"""

import csv
import json
import os
from pathlib import Path
from typing import Optional

from flows.lib import utils
from flows.lib.conditional_import import emit_event, flow
from flows.lib.shared_args import WORK_DIR, YAML_PATH
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required
from flows.parsers.parse_backfill_historical_versions import (
    find_all_assembly_versions,
    parse_historical_version,
    parse_version,
    setup_cache_directories,
)
from flows.parsers.parse_ncbi_assemblies import write_to_tsv

MISSING_JSON = {
    "flags": ["-m", "--missing_json"],
    "keys": {
        "help": "Path to the missing_versions.json produced by parse_assembly_versions.",
        "type": str,
    },
}


def load_missing_versions(missing_json: str) -> list[dict]:
    """Load the list of missing versions from a JSON file.

    The file is written by parse_assembly_versions when it encounters
    assemblies with no matching previous version in the parsed TSV.

    Args:
        missing_json (str): Path to missing_versions.json.

    Returns:
        list: Missing-version records, each with base_accession, missing_version,
            new_version, and new_accession keys.
    """
    with open(missing_json, encoding="utf-8") as f:
        return json.load(f)


def load_existing_historical(historical_tsv: str) -> dict[str, dict]:
    """Load an existing assembly_historical.tsv keyed by genbankAccession.

    Args:
        historical_tsv (str): Path to the existing historical TSV file.

    Returns:
        dict: Rows keyed by genbankAccession, or empty dict if the file is absent.
    """
    existing: dict[str, dict] = {}
    if not Path(historical_tsv).exists():
        return existing

    with open(historical_tsv, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            acc = row.get("genbankAccession", "")
            if acc:
                existing[acc] = dict(row)

    return existing


@flow(log_prints=True)
def update_assembly_versions(
    missing_json: str,
    yaml_path: str,
    work_dir: str = ".",
) -> None:
    """Fetch and parse assembly versions missing from the historical TSV.

    For each entry in missing_json, discovers all versions of that assembly
    via NCBI FTP, fetches metadata for the specific missing version, parses it
    through the standard GenomeHubs pipeline, and merges the result into the
    existing assembly_historical.tsv.  Emits a completion event on finish.

    Args:
        missing_json (str): Path to missing_versions.json from parse_assembly_versions.
        yaml_path (str): Path to assembly_historical.types.yaml.
        work_dir (str): Working directory for caches and output.
    """
    setup_cache_directories(work_dir)
    config = utils.load_config(config_file=yaml_path)

    missing = load_missing_versions(missing_json)
    if not missing:
        print("No missing versions to backfill.")
        emit_event(
            event="update.assembly_versions.completed",
            resource={
                "prefect.resource.id": f"update.assembly_versions.{work_dir}",
                "prefect.resource.type": "assembly.versions",
            },
            payload={"succeeded": 0, "failed": 0, "status": "no_op"},
        )
        return

    historical_tsv = config.meta["file_name"]
    existing = load_existing_historical(historical_tsv)
    parsed = dict(existing)

    total = len(missing)
    succeeded = 0
    failed = 0

    separator = "=" * 80
    print(f"\n{separator}")
    print("ASSEMBLY VERSION UPDATE")
    print(f"{separator}")
    print(f"  Missing entries to process: {total}")
    print(f"  Merging into: {historical_tsv}")
    print(f"  Existing records: {len(existing)}")
    print(f"{separator}\n")

    for i, entry in enumerate(missing):
        base_acc = entry["base_accession"]
        missing_version = entry["missing_version"]
        new_accession = entry["new_accession"]
        target_acc = f"{base_acc}.{missing_version}"

        print(f"[{i + 1}/{total}] {target_acc}")

        all_versions = find_all_assembly_versions(new_accession, work_dir)
        if not all_versions:
            print("  Warning: No versions found via FTP — skipping.")
            failed += 1
            continue

        version_data = next(
            (v for v in all_versions if parse_version(v.get("accession", "")) == missing_version),
            None,
        )
        if version_data is None:
            print(f"  Warning: v{missing_version} not found in FTP listing — skipping.")
            failed += 1
            continue

        try:
            print(f"  Parsing v{missing_version}...", end=" ", flush=True)
            row = parse_historical_version(
                version_data=version_data,
                config=config,
                base_accession=base_acc,
                version_num=missing_version,
                current_accession=new_accession,
            )
            genbank_acc = row.get("genbankAccession", target_acc)
            parsed[genbank_acc] = row
            succeeded += 1
            print("done")
        except Exception as e:
            print(f"failed ({e})")
            failed += 1
            continue

    if succeeded > 0:
        print(f"\nWriting {len(parsed)} records to {historical_tsv}...")
        write_to_tsv(parsed, config)

    print(f"\n{separator}")
    print("ASSEMBLY VERSION UPDATE COMPLETE")
    print(f"{separator}")
    print(f"  Succeeded: {succeeded}/{total}")
    if failed > 0:
        print(f"  Failed:    {failed}/{total}")
    print(f"  Total records in {historical_tsv}: {len(parsed)}")
    print(f"{separator}\n")

    emit_event(
        event="update.assembly_versions.completed",
        resource={
            "prefect.resource.id": f"update.assembly_versions.{work_dir}",
            "prefect.resource.type": "assembly.versions",
        },
        payload={"succeeded": succeeded, "failed": failed, "status": "success"},
    )


if __name__ == "__main__":
    args = _parse_args(
        [required(MISSING_JSON), required(YAML_PATH), WORK_DIR],
        description="Fetch assembly versions missing from historical records",
    )
    update_assembly_versions(
        missing_json=args.missing_json,
        yaml_path=args.yaml_path,
        work_dir=args.work_dir,
    )
