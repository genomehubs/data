"""Batch validate all Google Sheets status list TSVs.

Triggered by update.google.sheets.status.finished, this flow iterates
through all FILE_*.types.yaml in the status-lists directory and runs
SKIP_PARSING + validate for each one whose corresponding TSV is present
in work_dir.
"""

import os
import sys
from glob import glob
from typing import Optional

from flows.lib.conditional_import import flow
from flows.lib.utils import load_config
from flows.parsers.register import register_plugins
from flows.validators.validate_file_pair import validate_file_pair

PARSERS = register_plugins()


def _copy_yaml_to_workdir(yaml_path: str, work_dir: str) -> str:
    """Copy YAML (and dependencies) to work_dir."""
    import shutil

    os.makedirs(work_dir, exist_ok=True)
    dest = os.path.join(work_dir, os.path.basename(yaml_path))
    shutil.copy(yaml_path, dest)
    config = load_config(yaml_path)
    if "needs" in config.config.get("file", {}):
        source_dir = os.path.dirname(yaml_path)
        needs = config.config["file"]["needs"]
        if isinstance(needs, str):
            needs = [needs]
        for dep in needs:
            dep_path = os.path.join(source_dir, dep)
            if os.path.exists(dep_path):
                shutil.copy(dep_path, work_dir)
    return dest


@flow(log_prints=True)
def batch_validate_status_lists(
    yaml_dir: str,
    work_dir: str,
    taxdump_path: Optional[str] = None,
    dry_run: bool = False,
    s3_path: Optional[str] = None,
    min_valid: int = 0,
    min_assigned: int = 0,
) -> bool:
    """Validate all status list TSVs present in work_dir.

    For each FILE_*.types.yaml in yaml_dir, checks if the corresponding
    TSV exists in work_dir. If present, runs SKIP_PARSING (file existence
    check) then validate_file_pair.

    Args:
        yaml_dir: Directory containing FILE_*.types.yaml files.
        work_dir: Directory containing TSVs output by the updater.
        taxdump_path: Optional NCBI taxdump path.
        dry_run: If True, skip S3 upload.
        s3_path: S3 path for validated files (None = local only).
        min_valid: Minimum valid row count per file.
        min_assigned: Minimum assigned taxa per file.

    Returns:
        bool: True if all validations passed.
    """
    yaml_files = sorted(glob(os.path.join(yaml_dir, "FILE_*.types.yaml")))
    if not yaml_files:
        print(f"No FILE_*.types.yaml found in {yaml_dir}")
        return False

    skip_parser = PARSERS.parsers["SKIP_PARSING"]
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

        # Copy YAML to work_dir for validator
        working_yaml = _copy_yaml_to_workdir(yaml_path, work_dir)

        # Run SKIP_PARSING (verifies file exists)
        try:
            skip_parser.func(
                working_yaml=working_yaml,
                work_dir=work_dir,
                append=False,
            )
        except FileNotFoundError as e:
            print(f"  FAIL {yaml_name}: TSV not found after copy — {e}")
            results[yaml_name] = "fail-parser"
            continue

        # Run validation
        effective_s3 = None if dry_run else s3_path
        try:
            status = validate_file_pair(
                yaml_path=yaml_path,
                work_dir=work_dir,
                taxdump_path=taxdump_path,
                s3_path=effective_s3,
                min_valid=min_valid,
                min_assigned=min_assigned,
            )
            results[yaml_name] = "pass" if status else "fail-validation"
            if status:
                print(f"  ✓ {yaml_name}")
            else:
                print(f"  ✗ {yaml_name}")
        except Exception as e:
            print(f"  ✗ {yaml_name}: {e}")
            results[yaml_name] = "fail-exception"

    # Summary
    passed = sum(1 for v in results.values() if v == "pass")
    failed = sum(1 for v in results.values() if v.startswith("fail"))
    skipped = sum(1 for v in results.values() if v.startswith("skip"))
    print(f"\nBatch validation: {passed} passed, {failed} failed, {skipped} skipped")

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
