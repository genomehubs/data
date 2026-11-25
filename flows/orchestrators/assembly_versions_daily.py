"""Daily historical assembly version chain for the NCBI assembly TSV.

Triggered by the validate.file.pair.finished event for the NCBI YAML once the
TSV has validated. Runs in order against files in work_dir:

1. parse_assembly_versions: diffs the new JSONL against the previous parse and
   appends newly superseded versions to assembly_historical.tsv.
2. Gap-fill, only when gaps were found: update_assembly_versions fetches the
   missing versions into gap_fill_dir, then the backfill appends them to
   assembly_historical.tsv.
3. generate_assembly_summary: writes assembly_version_summary.tsv.
4. compute_taxon_milestones: writes taxon_milestone_summary.tsv.

gap_fill_dir is kept outside work_dir because the glob-based wrappers reject a
second JSONL there.
"""

import json
import os

from flows.lib.compute_taxon_milestones import compute_taxon_milestones
from flows.lib.conditional_import import flow
from flows.lib.generate_assembly_summary import generate_assembly_summary
from flows.parsers.parse_assembly_versions import (
    derive_assembly_version_paths,
    load_assembly_versions_config,
    load_historical_headers,
    parse_assembly_versions,
)
from flows.parsers.parse_backfill_historical_versions import backfill_historical_versions
from flows.updaters.update_assembly_versions import OUTPUT_JSONL, update_assembly_versions

MISSING_VERSIONS_JSON = "missing_versions.json"
HISTORICAL_YAML = "configs/assembly_historical.types.yaml"


@flow(log_prints=True)
def assembly_versions_daily(
    work_dir: str,
    jsonl_path: str,
    yaml_path: str,
    taxdump_path: str,
    gap_fill_dir: str,
    historical_yaml_path: str = HISTORICAL_YAML,
) -> dict:
    """Run the daily historical diff, summary and milestone stages.

    Args:
        work_dir (str): Directory holding the JSONL, the parsed TSV and its
            ".previous" snapshot, assembly_historical.tsv and the working YAML.
        jsonl_path (str): Path to the current assembly_data_report JSONL.
        yaml_path (str): Path to the working YAML copy inside work_dir. Its
            file_name must resolve to files in work_dir.
        taxdump_path (str): NCBI taxdump directory used for taxon milestones.
        gap_fill_dir (str): Scratch directory for the gap-fill JSONL, outside work_dir.
        historical_yaml_path (str): Schema for assembly_historical.tsv, used by the backfill.

    Returns:
        dict: newly_superseded_count and missing_versions_count from the diff.
    """
    config = load_assembly_versions_config(yaml_path)
    previous_tsv, historical_tsv = derive_assembly_version_paths(jsonl_path, config=config)

    results = parse_assembly_versions(
        new_jsonl=jsonl_path,
        previous_tsv=previous_tsv,
        historical_tsv=historical_tsv,
        historical_headers=load_historical_headers(work_dir),
    )

    # Written on every run, so an empty list replaces a previous day's gaps.
    missing_json_path = os.path.join(work_dir, MISSING_VERSIONS_JSON)
    with open(missing_json_path, "w", encoding="utf-8") as f:
        json.dump(results["missing_versions"], f, indent=2)

    if results["missing_versions_count"] > 0:
        update_assembly_versions(missing_json=missing_json_path, work_dir=gap_fill_dir)
        backfill_historical_versions(
            input_path=os.path.join(gap_fill_dir, OUTPUT_JSONL),
            yaml_path=historical_yaml_path,
            work_dir=work_dir,
        )

    generate_assembly_summary(work_dir=work_dir, yaml_path=yaml_path)
    compute_taxon_milestones(work_dir=work_dir, taxdump_path=taxdump_path, yaml_path=yaml_path)

    return {
        "newly_superseded_count": results["newly_superseded_count"],
        "missing_versions_count": results["missing_versions_count"],
    }
