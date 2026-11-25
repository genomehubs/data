"""Validate and publish an existing YAML/TSV pair in a work directory, without fetching or parsing."""

from flows.lib.shared_args import (
    DRY_RUN,
    MIN_ASSIGNED,
    MIN_VALID,
    PER_FILE_SUBDIR,
    S3_PATH,
    TAXDUMP_PATH,
    WORK_DIR,
    YAML_PATH,
    parse_args,
    required,
)
from flows.validators.validate_file_pair import validate_and_publish

if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [
            required(YAML_PATH),
            WORK_DIR,
            TAXDUMP_PATH,
            MIN_VALID,
            MIN_ASSIGNED,
            S3_PATH,
            DRY_RUN,
            PER_FILE_SUBDIR,
        ],
        "Validate and publish a YAML/TSV file pair.",
    )
    validate_and_publish(**vars(args))
