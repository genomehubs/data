"""Batch fetch-parse-validate for all Google Sheets status list TSVs.

Triggered by update.google.sheets.status.finished, this flow iterates
through all FILE_*.types.yaml in the status-lists directory and runs
the full fetch-parse-validate pipeline (with SKIP_PARSING) for each one
whose corresponding TSV is present in work_dir.
"""

import os
import sys
from glob import glob
from typing import Optional

from flows.lib.conditional_import import flow
from flows.lib.utils import load_config
from flows.lib.wrapper_fetch_parse_validate import Parser, fetch_parse_validate


@flow(log_prints=True)
def batch_validate_status_lists(
    yaml_dir: str,
    work_dir: str,
    s3_path: str = "s3://goat/sources/status-lists/",
    taxdump_path: Optional[str] = None,
    dry_run: bool = False,
    min_valid: int = 0,
    min_assigned: int = 0,
) -> bool:
    """Run fetch-parse-validate (SKIP_PARSING) for all status list TSVs in work_dir.

    For each FILE_*.types.yaml in yaml_dir, checks if the corresponding
    TSV exists in work_dir. If present, invokes the standard
    fetch_parse_validate flow with SKIP_PARSING.

    Args:
        yaml_dir: Directory containing FILE_*.types.yaml files.
        work_dir: Directory containing TSVs output by the updater.
        s3_path: S3 path prefix for validated files.
        taxdump_path: Optional NCBI taxdump path.
        dry_run: If True, skip S3 upload.
        min_valid: Minimum valid row count per file.
        min_assigned: Minimum assigned taxa per file.

    Returns:
        bool: True if all validations passed.
    """
    yaml_files = sorted(glob(os.path.join(yaml_dir, "FILE_*.types.yaml")))
    if not yaml_files:
        print(f"No FILE_*.types.yaml found in {yaml_dir}")
        return False

    results = {}

    for yaml_path in yaml_files:
        yaml_name = os.path.basename(yaml_path)
        try:
            config = load_config(yaml_path)
        except Exception as e:
            print(f"  SKIP {yaml_name}: failed to load config — {e}")
            results[yaml_name] = "skip-config-error"
            continue

        tsv_name = os.path.basename(config.config["file"]["name"])
        tsv_path = os.path.join(work_dir, tsv_name)

        if not os.path.exists(tsv_path):
            # TSV not present — updater may not have produced it this run
            results[yaml_name] = "skip-no-tsv"
            continue

        try:
            fetch_parse_validate(
                parser=Parser.SKIP_PARSING,
                yaml_path=yaml_path,
                s3_path=s3_path,
                work_dir=work_dir,
                taxdump_path=taxdump_path,
                dry_run=dry_run,
                min_valid=min_valid,
                min_assigned=min_assigned,
            )
            results[yaml_name] = "pass"
            print(f"  ✓ {yaml_name}")
        except Exception as e:
            print(f"  ✗ {yaml_name}: {e}")
            results[yaml_name] = "fail"

    # Summary
    passed = sum(1 for v in results.values() if v == "pass")
    failed = sum(1 for v in results.values() if v == "fail")
    skipped = sum(1 for v in results.values() if v.startswith("skip"))
    print(f"\nBatch fetch-parse-validate: {passed} passed, {failed} failed, {skipped} skipped")

    return failed == 0


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Batch validate status list TSVs.")
    p.add_argument("--yaml-dir", required=True, help="Directory with FILE_*.types.yaml")
    p.add_argument("--work-dir", required=True, help="Directory with updater TSVs")
    p.add_argument("--taxdump-path", default=None)
    p.add_argument("--s3-path", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--min-valid", type=int, default=0)
    p.add_argument("--min-assigned", type=int, default=0)
    args = p.parse_args()

    success = batch_validate_status_lists(**vars(args))
    sys.exit(0 if success else 1)
