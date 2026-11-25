"""Daily historical assembly version chain for the NCBI assembly TSV.

Triggered by the validate.file.pair.finished event for the NCBI YAML once the
TSV has validated. Runs in order against files in work_dir:

1. parse_assembly_versions: diffs the new JSONL against the previous parse and
   appends newly superseded versions to assembly_historical.tsv.
2. generate_assembly_summary: writes assembly_version_summary.tsv.
3. compute_taxon_milestones: writes taxon_milestone_summary.tsv.

Gap-filling (update_assembly_versions and the backfill) is not run here. It
writes missing_assembly_versions.jsonl into work_dir, which the glob-based
wrappers reject on the next run. missing_versions.json is written when gaps are
found so the gap-fill can be run by hand.
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

MISSING_VERSIONS_JSON = "missing_versions.json"


@flow(log_prints=True)
def assembly_versions_daily(
    work_dir: str,
    jsonl_path: str,
    yaml_path: str,
    taxdump_path: str,
) -> dict:
    """Run the daily historical diff, summary and milestone stages.

    Args:
        work_dir (str): Directory holding the JSONL, the parsed TSV and its
            ".previous" snapshot, assembly_historical.tsv and the working YAML.
        jsonl_path (str): Path to the current assembly_data_report JSONL.
        yaml_path (str): Path to the working YAML copy inside work_dir. Its
            file_name must resolve to files in work_dir.
        taxdump_path (str): NCBI taxdump directory used for taxon milestones.

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

    if results["missing_versions_count"] > 0:
        missing_json_path = os.path.join(work_dir, MISSING_VERSIONS_JSON)
        with open(missing_json_path, "w", encoding="utf-8") as f:
            json.dump(results["missing_versions"], f, indent=2)
        print(f"Missing versions written to: {missing_json_path}")

    generate_assembly_summary(work_dir=work_dir, yaml_path=yaml_path)
    compute_taxon_milestones(work_dir=work_dir, taxdump_path=taxdump_path, yaml_path=yaml_path)

    return {
        "newly_superseded_count": results["newly_superseded_count"],
        "missing_versions_count": results["missing_versions_count"],
    }
