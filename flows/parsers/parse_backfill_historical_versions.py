"""One-time historical backfill of superseded assembly versions from NCBI.

Discovers and parses all superseded versions for assemblies with version > 1.
Run once before starting the daily incremental pipeline.

Usage:
    python -m flows.parsers.parse_backfill_historical_versions \\
        --input_path data/assembly_data_report.jsonl \\
        --yaml_path configs/assembly_historical.types.yaml \\
        --work_dir tmp
"""

import json
import os
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Optional

from genomehubs import utils as gh_utils

from flows.lib import utils
from flows.lib.assembly_versions_utils import (
    find_all_assembly_versions,
    parse_accession,
    parse_version,
    setup_cache_directories,
)
from flows.lib.conditional_import import flow
from flows.lib.shared_args import INPUT_PATH, WORK_DIR, YAML_PATH
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required
from flows.lib.utils import Config, Parser
from flows.parsers.parse_ncbi_assemblies import (
    fetch_and_parse_sequence_report,
    process_assembly_report,
    write_to_tsv,
)


def parse_historical_version(
    version_data: dict,
    config: Config,
    base_accession: str,
    version_num: int,
    current_accession: str,
) -> dict:
    """Parse a single historical version using GenomeHubs parser logic.

    Ensures consistency with current assemblies by reusing
    process_assembly_report with version_status="superseded" and
    fetch_and_parse_sequence_report.

    Args:
        version_data (dict): Raw NCBI metadata from the datasets CLI.
        config (Config): Config object loaded from the YAML file.
        base_accession (str): Base accession (e.g. GCA_000002035).
        version_num (int): Integer version (1, 2, 3, ...).
        current_accession (str): The latest accession that superseded this one.

    Returns:
        dict: Parsed row dict ready for TSV output.
    """
    version_data = utils.convert_keys_to_camel_case(version_data)

    processed_report = process_assembly_report(
        report=version_data,
        previous_report=None,
        config=config,
        parsed={},
        version_status="superseded",
    )

    fetch_and_parse_sequence_report(processed_report)

    processed_report["processedAssemblyInfo"]["assemblyID"] = (
        f"{base_accession}_{version_num}"
    )

    return gh_utils.parse_report_values(config.parse_fns, processed_report)


def derive_checkpoint_path(
    input_path: str, yaml_path: str, work_dir: str
) -> str:
    """Derive a stable checkpoint path from parser inputs.

    Places the checkpoint alongside the data in work_dir so its location
    can be determined without extra CLI arguments.

    Args:
        input_path (str): Path to the assembly report JSONL file.
        yaml_path (str): Path to the parser YAML configuration file.
        work_dir (str): Working directory.

    Returns:
        str: Path to the checkpoint JSON file.
    """
    input_stem = Path(input_path).stem
    config_stem = Path(yaml_path).stem
    checkpoint_dir = Path(work_dir) / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    name = f"backfill__{config_stem}__{input_stem}.json"
    return str(checkpoint_dir / name)


def load_checkpoint(checkpoint_file: str) -> dict:
    """Load checkpoint data if the file exists.

    Args:
        checkpoint_file (str): Path to the checkpoint JSON file.

    Returns:
        dict: Checkpoint dict, or empty dict if absent.
    """
    if Path(checkpoint_file).exists():
        with open(checkpoint_file) as f:
            return json.load(f)
    return {}


def save_checkpoint(
    checkpoint_file: str, processed_count: int, completed: bool = False
):
    """Persist current progress to the checkpoint file.

    Args:
        checkpoint_file (str): Path to the checkpoint JSON file.
        processed_count (int): Number of assemblies processed so far.
        completed (bool): True when the full run finished successfully.
            A completed checkpoint resets start_index on the next run so
            all entries are re-collected (using cached network data).
    """
    Path(checkpoint_file).parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, "w") as f:
        json.dump({
            "processed_count": processed_count,
            "completed": completed,
            "timestamp": datetime.now().isoformat(),
        }, f, indent=2)


def identify_assemblies_needing_backfill(input_path: str) -> list[dict]:
    """Identify assemblies with version > 1 that need historical backfill.

    Args:
        input_path (str): Path to assembly_data_report.jsonl.

    Returns:
        list: Assembly info dicts describing what needs backfilling.
    """
    assemblies = []
    with open(input_path) as f:
        for line in f:
            record = json.loads(line)
            accession = record["accession"]
            base_acc, version = parse_accession(accession)

            if version > 1:
                assemblies.append({
                    "base_accession": base_acc,
                    "current_version": version,
                    "current_accession": accession,
                    "historical_versions_needed": list(range(1, version)),
                })
    return assemblies


@flow(log_prints=True)
def backfill_historical_versions(
    input_path: str,
    yaml_path: str,
    work_dir: str = ".",
    checkpoint_file: Optional[str] = None,
):
    """One-time backfill of all historical assembly versions.

    Accumulates all parsed rows in memory and writes the output TSV once at
    the end.  Checkpoints are saved periodically so the run can be resumed
    after interruption but do not trigger intermediate TSV writes.

    Args:
        input_path (str): Path to assembly_data_report.jsonl.
        yaml_path (str): Path to assembly_historical.types.yaml.
        work_dir (str): Working directory for caches, checkpoints, and output.
        checkpoint_file (str, optional): Explicit checkpoint path. Derived
            from inputs when omitted.
    """
    setup_cache_directories(work_dir)
    config = utils.load_config(config_file=yaml_path)
    checkpoint_file = checkpoint_file or derive_checkpoint_path(
        input_path, yaml_path, work_dir,
    )

    print("Scanning for assemblies needing historical backfill...")
    assemblies = identify_assemblies_needing_backfill(input_path)

    if not assemblies:
        print("No assemblies with version > 1 found. Nothing to backfill.")
        return

    checkpoint = load_checkpoint(checkpoint_file)
    # A completed checkpoint means the previous run finished successfully.
    # Reset to 0 so all entries are collected again (network fetches still use
    # the on-disk cache, so the re-run is fast).
    if checkpoint.get("completed", False):
        start_index = 0
    else:
        start_index = checkpoint.get("processed_count", 0)

    total_assemblies = len(assemblies)
    total_versions = sum(
        len(a["historical_versions_needed"]) for a in assemblies
    )

    print(f"\n{'=' * 80}")
    print("ONE-TIME HISTORICAL BACKFILL")
    print(f"{'=' * 80}")
    print(f"  Assemblies to process: {total_assemblies}")
    print(f"  Total historical versions: {total_versions}")
    if start_index > 0:
        print(f"  Resuming from checkpoint: {start_index}/{total_assemblies}")
    print(f"{'=' * 80}\n")

    parsed = {}
    processed = start_index

    for assembly_info in assemblies[start_index:]:
        base_acc = assembly_info["base_accession"]
        current_version = assembly_info["current_version"]
        current_accession = assembly_info["current_accession"]

        print(
            f"[{processed + 1}/{total_assemblies}] "
            f"{base_acc} (current: v{current_version})"
        )

        all_versions = find_all_assembly_versions(current_accession, work_dir)
        if not all_versions:
            print("  Warning: No versions found via FTP")
            processed += 1
            continue

        for version_data in all_versions:
            version_acc = version_data.get("accession", "")
            version_num = parse_version(version_acc)

            if version_num >= current_version:
                continue

            try:
                print(f"  Parsing v{version_num}...", end=" ", flush=True)
                row = parse_historical_version(
                    version_data=version_data,
                    config=config,
                    base_accession=base_acc,
                    version_num=version_num,
                    current_accession=current_accession,
                )
                genbank_acc = row.get("genbankAccession", version_acc)
                parsed[genbank_acc] = row
                print("done")
            except Exception as e:
                print(f"failed ({e})")
                continue

        processed += 1

        if processed % 100 == 0:
            save_checkpoint(checkpoint_file, processed)
            pct = processed / total_assemblies * 100
            print(
                f"\n  Checkpoint saved: "
                f"{processed}/{total_assemblies} ({pct:.1f}%)\n"
            )

    if parsed:
        print(f"\nWriting {len(parsed)} records to TSV...")
        write_to_tsv(parsed, config)

    save_checkpoint(checkpoint_file, processed, completed=True)

    print(f"\n{'=' * 80}")
    print("BACKFILL COMPLETE")
    print(f"{'=' * 80}")
    print(f"  Processed: {processed}/{total_assemblies} assemblies")
    print(f"  Records written: {len(parsed)}")
    print(f"  Output: {config.meta['file_name']}")
    print("\n  Next step: Run daily incremental pipeline")
    print(f"{'=' * 80}\n")


def backfill_historical_versions_wrapper(
    working_yaml: str,
    work_dir: str,
    append: bool,
    data_freeze_path: Optional[str] = None,
    **kwargs,
):
    """Wrapper matching the fetch_parse_validate parser signature.

    Locates the *.jsonl input in work_dir and delegates to
    backfill_historical_versions.

    Args:
        working_yaml (str): Path to the working YAML file.
        work_dir (str): Path to the working directory.
        append (bool): Whether to append (unused, accepted for compatibility).
        data_freeze_path (str, optional): Ignored; accepted for compatibility.
        **kwargs: Additional keyword arguments.
    """
    glob_path = os.path.join(work_dir, "*.jsonl")
    paths = glob(glob_path)
    if not paths:
        raise FileNotFoundError(f"No jsonl file found in {work_dir}")
    if len(paths) > 1:
        raise ValueError(f"More than one jsonl file found in {work_dir}")

    backfill_historical_versions(
        input_path=paths[0],
        yaml_path=working_yaml,
        work_dir=work_dir,
    )


def plugin():
    """Register the flow."""
    return Parser(
        name="BACKFILL_HISTORICAL_VERSIONS",
        func=backfill_historical_versions_wrapper,
        description="One-time backfill of historical assembly versions.",
    )


if __name__ == "__main__":
    """Run the flow."""
    args = _parse_args(
        [required(INPUT_PATH), required(YAML_PATH), WORK_DIR],
        description="One-time historical backfill for assembly versions",
    )
    backfill_historical_versions(**vars(args))
