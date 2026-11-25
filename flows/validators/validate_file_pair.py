"""Validate data quality and schema compliance."""

import json
import os
import subprocess
import sys
from typing import Optional

from flows.lib import utils
from flows.lib.conditional_import import emit_event, flow, task
from flows.validators.args import parse_args


@task(log_prints=True)
def validate_yaml_file(
    yaml_path: str,
    taxdump_path: Optional[str] = None,
    min_valid: int = 0,
    min_assigned: int = 0,
) -> tuple:
    """
    Validate the YAML file using blobtk validate.

    Args:
        yaml_path (str): Path to the YAML file.
        taxdump_path (str): Path to an NCBI format taxdump.
        min_valid (int): Minimum expected number of valid rows.
        min_assigned (int): Minimum expected number of assigned taxa.

    Returns:
        tuple: (status, validation_report, taxonomy_report).
    """
    if not utils.is_safe_path(yaml_path):
        raise ValueError(f"Unsafe YAML path: {yaml_path}")

    # Validate the YAML file using blobtk validate
    cmd = ["blobtk", "validate", "-g", yaml_path]
    if taxdump_path is not None:
        if not utils.is_safe_path(taxdump_path):
            raise ValueError(f"Unsafe taxdump path: {taxdump_path}")
        cmd.extend(
            [
                "-t",
                taxdump_path,
                "-n",
                "scientific name",
            ]
        )

    # Run the command with subprocess run and capture stdout
    result = utils.run_quoted(cmd, stdout=subprocess.PIPE, text=True)
    status = result.returncode == 0
    output = result.stdout
    sys.stdout.write(output)
    validation_report = None
    taxonomy_report = None
    for line in output.splitlines():
        if line.startswith("{") and line.endswith("}"):
            if validation_report is None:
                validation_report = json.loads(line)
            elif taxonomy_report is None:
                taxonomy_report = json.loads(line)

    return (status, validation_report, taxonomy_report)


@task()
def check_min_counts(validation_report, taxonomy_report, min_valid, min_assigned):
    """
    Check if the validation and taxonomy reports meet the minimum counts.

    Args:
        validation_report (dict): The validation report.
        taxonomy_report (dict): The taxonomy report.
        min_valid (int): Minimum expected number of valid rows.
        min_assigned (int): Minimum expected number of assigned taxa.

    Returns:
        bool: True if all checks pass.

    Raises:
        ValueError: If counts are below minimum thresholds.
    """
    if validation_report is not None:
        valid_count = int(validation_report.get("valid", 0))
        if valid_count < min_valid:
            raise ValueError(f"Expected at least {min_valid} valid rows, found {valid_count}")

    if taxonomy_report is not None:
        assigned_count = int(taxonomy_report.get("assigned", 0))
        if assigned_count < min_assigned:
            raise ValueError(f"Expected at least {min_assigned} assigned taxa, " f"found {assigned_count}")

    return True


@flow()
def validate_file_pair(
    yaml_path: str,
    work_dir: str,
    taxdump_path: Optional[str] = None,
    min_valid: int = 1,
    min_assigned: int = 1,
) -> bool:
    """
    Validate the YAML/TSV file pair against schema and taxonomy.

    Checks that the TSV file conforms to the schema defined in the YAML
    configuration file, and optionally validates taxonomy assignments.

    Args:
        yaml_path (str): Path to the source YAML file.
        work_dir (str): Path to the working directory.
        taxdump_path (str): Path to an NCBI format taxdump.
        min_valid (int): Minimum expected number of valid rows.
        min_assigned (int): Minimum expected number of assigned taxa.

    Returns:
        bool: True if validation passed.
    """
    yaml_file_name = os.path.basename(yaml_path)
    local_yaml_path = os.path.join(work_dir, yaml_file_name)
    status, validation_report, taxonomy_report = validate_yaml_file(
        local_yaml_path, taxdump_path, min_valid, min_assigned
    )

    if status:
        check_min_counts(validation_report, taxonomy_report, min_valid, min_assigned)

    emit_event(
        event="validate.file.pair.finished",
        resource={
            "prefect.resource.id": f"validate.file.{yaml_path}",
            "prefect.resource.type": "validate.file",
            "prefect.resource.is.valid": "yes" if status else "no",
        },
        payload={"matches_previous": status},
    )
    return status


@flow()
def validate_and_publish(
    yaml_path: str,
    work_dir: str,
    taxdump_path: Optional[str] = None,
    s3_path: Optional[str] = None,
    min_valid: int = 1,
    min_assigned: int = 1,
    per_file_subdir: bool = False,
    dry_run: bool = False,
) -> bool:
    """
    Validate a YAML/TSV pair in the work directory and publish it to S3 if it passes.

    Args:
        yaml_path (str): Path to the source YAML file.
        work_dir (str): Path to the working directory.
        taxdump_path (str): Path to an NCBI format taxdump.
        s3_path (str): Path to the TSV directory on S3. Publishing is skipped if None.
        min_valid (int): Minimum expected number of valid rows.
        min_assigned (int): Minimum expected number of assigned taxa.
        per_file_subdir (bool): Publish each pair to its own subdirectory.
        dry_run (bool): Log the publish destinations without uploading.

    Returns:
        bool: True if the pair validated.
    """
    status = validate_file_pair(yaml_path, work_dir, taxdump_path, min_valid, min_assigned)
    if status and s3_path is not None:
        # blobtk writes the validated pair to a validated/ directory beside the working YAML
        validated_yaml_path = os.path.join(work_dir, "validated", os.path.basename(yaml_path))
        utils.publish_validated_pair(
            validated_yaml_path,
            theme=utils.theme_from_s3_path(s3_path),
            per_file_subdir=per_file_subdir,
            dry_run=dry_run,
        )
    return status


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args("Validate a YAML/TSV file pair.")
    validate_file_pair(**vars(args))
