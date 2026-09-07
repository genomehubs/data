"""Historical backfill of superseded assembly versions from NCBI.

Discovers and parses all superseded versions for assemblies with version > 1.
Run once before starting the daily incremental pipeline, and re-invoked by the
daily gap-fill path whenever parse_assembly_versions reports a missing version
— so the write into assembly_historical.tsv preserves the rows already there.

Usage:
    python -m flows.parsers.parse_backfill_historical_versions \\
        --input_path data/assembly_data_report.jsonl \\
        --yaml_path configs/assembly_historical.types.yaml \\
        --work_dir tmp
"""

import csv
import json
import os
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Optional

from genomehubs import utils as gh_utils

from flows.lib import utils
from flows.lib.assembly_versions_utils import (
    COL_ACCESSION,
    find_all_assembly_versions,
    get_accession,
    open_tsv,
    parse_accession,
    parse_version,
    setup_cache_directories,
)
from flows.lib.conditional_import import flow
from flows.lib.shared_args import INPUT_PATH, WORK_DIR, YAML_PATH
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required
from flows.lib.utils import Config, Parser, append_to_tsv
from flows.parsers.parse_ncbi_assemblies import (
    fetch_and_parse_sequence_report,
    process_assembly_report,
    write_to_tsv,
)

DELIMITER = "\t"


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


def resolve_output_path(config: Config, work_dir: str) -> str:
    """Point the config's output at work_dir instead of the YAML's directory.

    ``config.meta["file_name"]`` is resolved relative to the YAML file, so an
    out-of-tree config would write the historical TSV next to itself.  Rewrite
    it onto work_dir, mirroring what run_generic_tsv_parser does.

    Args:
        config (Config): Loaded YAML configuration.
        work_dir (str): Working directory the run was given.

    Returns:
        str: The resolved output path, also stored back on the config.
    """
    output_path = os.path.join(
        work_dir, os.path.basename(config.meta["file_name"])
    )
    config.meta["file_name"] = output_path
    return output_path


def read_existing_output(output_path: str) -> tuple[list, set]:
    """Read the header and the accessions already in the historical TSV.

    Only the header line and the accession column are kept, so this stays
    cheap on a file holding tens of thousands of rows.

    The header doubles as the "this file already holds data" signal: it is
    empty only when the file is missing or zero-length, whereas the accession
    set is also empty for a file whose rows all lack an accession.

    The header matters as much as the accessions: the Phase 1 daily parser
    rewrites this file with the union of the columns across every row, so the
    order on disk is its output, not this config's.  Phase 1 now bounds that
    union by the declared schema where it can resolve the YAML, but a file
    widened by an earlier run — or by a run that could not — still carries
    columns the config never declared.  Appending against the config header
    would then write rows a few columns short.

    Args:
        output_path (str): Path to assembly_historical.tsv.

    Returns:
        tuple: (header, accessions), both empty when the file does not exist
            or holds nothing yet.
    """
    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        return [], set()

    accessions = set()
    with open_tsv(output_path) as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        for row in reader:
            accession = get_accession(row)
            if accession:
                accessions.add(accession)
        header = list(reader.fieldnames or [])
    return header, accessions


def write_or_append_parsed(parsed: dict, config: Config) -> int:
    """Write parsed rows without destroying rows already in the output.

    ``write_to_tsv`` opens the output with mode "w", which is correct for the
    one-time backfill against an empty directory but destructive once the
    gap-fill path re-invokes this parser daily against a populated
    assembly_historical.tsv.  Write the whole file only when it does not exist
    yet — that branch produces the header line — and otherwise append the rows
    that are not already present.  The rewrite branch keys off the header
    rather than the accession set, so a file whose rows all lack an accession
    is appended to rather than overwritten.

    Deduplication is required because appending is not idempotent:
    find_all_assembly_versions re-parses every version below the current one,
    and a completed checkpoint deliberately restarts from index 0.

    Rows are appended under the header already on disk rather than the config
    header, so a file the Phase 1 parser has widened keeps its columns lined
    up.  A row missing one of those columns is written empty for it.

    Args:
        parsed (dict): Mapping of accession -> parsed row.
        config (Config): Loaded YAML configuration.

    Returns:
        int: Number of rows actually written.
    """
    output_path = config.meta["file_name"]
    header, existing = read_existing_output(output_path)

    if not header:
        write_to_tsv(parsed, config)
        return len(parsed)

    new_rows = [
        row for accession, row in parsed.items() if accession not in existing
    ]
    skipped = len(parsed) - len(new_rows)
    if skipped:
        print(f"  Skipping {skipped} versions already in {output_path}")
    if new_rows:
        append_to_tsv(header, new_rows, config.meta)
    return len(new_rows)


@flow(log_prints=True)
def backfill_historical_versions(
    input_path: str,
    yaml_path: str,
    work_dir: str = ".",
    checkpoint_file: Optional[str] = None,
):
    """Backfill historical assembly versions into assembly_historical.tsv.

    Accumulates all parsed rows in memory and writes the output TSV once at
    the end.  Checkpoints are saved periodically so the run can be resumed
    after interruption but do not trigger intermediate TSV writes.

    The output is written into work_dir, and rows already in the output are
    preserved — the daily gap-fill path re-invokes this flow, so a truncating
    write would destroy the backfill it is meant to extend.

    Args:
        input_path (str): Path to assembly_data_report.jsonl.
        yaml_path (str): Path to assembly_historical.types.yaml.
        work_dir (str): Working directory for caches, checkpoints, and output.
        checkpoint_file (str, optional): Explicit checkpoint path. Derived
            from inputs when omitted.
    """
    setup_cache_directories(work_dir)
    config = utils.load_config(config_file=yaml_path)
    output_path = resolve_output_path(config, work_dir)
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
    written = 0

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
                genbank_acc = row.get(COL_ACCESSION) or version_acc
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
        written = write_or_append_parsed(parsed, config)

    save_checkpoint(checkpoint_file, processed, completed=True)

    print(f"\n{'=' * 80}")
    print("BACKFILL COMPLETE")
    print(f"{'=' * 80}")
    print(f"  Processed: {processed}/{total_assemblies} assemblies")
    print(f"  Records parsed: {len(parsed)}")
    print(f"  Records written: {written}")
    print(f"  Output: {output_path}")
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
