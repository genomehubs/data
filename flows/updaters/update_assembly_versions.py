"""Fetch metadata for assembly versions missing from historical records.

Run this when parse_assembly_versions reports assemblies whose previous
version was absent from the previous parsed TSV.  Fetches raw NCBI metadata
for each missing accession and writes it to a JSONL file for downstream
parsing by parse_backfill_historical_versions.

Usage:
    python -m flows.updaters.update_assembly_versions \\
        --missing_json tmp/missing_versions.json \\
        --work_dir tmp
"""

import json
import os

from flows.lib.assembly_versions_utils import (
    fetch_version_metadata,
    setup_cache_directories,
)
from flows.lib.conditional_import import emit_event, flow
from flows.lib.shared_args import MISSING_JSON, WORK_DIR
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required

OUTPUT_JSONL = "missing_assembly_versions.jsonl"


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


@flow(log_prints=True)
def update_assembly_versions(
    missing_json: str,
    work_dir: str = ".",
) -> None:
    """Fetch metadata for missing assembly versions and write to JSONL.

    For each entry in missing_json, fetches the new accession's raw metadata
    from NCBI datasets and writes the results to a JSONL file.  The JSONL is
    consumed by parse_backfill_historical_versions.  Emits a completion event
    on finish.

    Args:
        missing_json (str): Path to missing_versions.json from parse_assembly_versions.
        work_dir (str): Working directory for cache and JSONL output.
    """
    setup_cache_directories(work_dir)
    missing = load_missing_versions(missing_json)

    if not missing:
        print("No missing versions to fetch.")
        emit_event(
            event="update.assembly_versions.completed",
            resource={
                "prefect.resource.id": f"update.assembly_versions.{work_dir}",
                "prefect.resource.type": "assembly.versions",
            },
            payload={"fetched": 0, "failed": 0, "status": "no_op"},
        )
        return

    output_jsonl = os.path.join(work_dir, OUTPUT_JSONL)
    total = len(missing)
    fetched = 0
    failed = 0

    separator = "=" * 80
    print(f"\n{separator}")
    print("ASSEMBLY VERSION UPDATE")
    print(f"{separator}")
    print(f"  Entries to fetch: {total}")
    print(f"  Output JSONL:     {output_jsonl}")
    print(f"{separator}\n")

    with open(output_jsonl, "w", encoding="utf-8") as f:
        for i, entry in enumerate(missing):
            new_accession = entry["new_accession"]
            print(f"[{i + 1}/{total}] Fetching {new_accession}...", end=" ", flush=True)
            metadata = fetch_version_metadata(new_accession, work_dir)
            if metadata:
                f.write(json.dumps(metadata) + "\n")
                fetched += 1
                print("done")
            else:
                failed += 1
                print("failed (no metadata returned)")

    print(f"\n{separator}")
    print("ASSEMBLY VERSION UPDATE COMPLETE")
    print(f"{separator}")
    print(f"  Fetched: {fetched}/{total}")
    if failed > 0:
        print(f"  Failed:  {failed}/{total}")
    print(f"  Output:  {output_jsonl}")
    print(f"{separator}\n")

    emit_event(
        event="update.assembly_versions.completed",
        resource={
            "prefect.resource.id": f"update.assembly_versions.{work_dir}",
            "prefect.resource.type": "assembly.versions",
        },
        payload={"fetched": fetched, "failed": failed, "status": "success"},
    )


if __name__ == "__main__":
    args = _parse_args(
        [required(MISSING_JSON), WORK_DIR],
        description="Fetch assembly versions missing from historical records",
    )
    update_assembly_versions(
        missing_json=args.missing_json,
        work_dir=args.work_dir,
    )
