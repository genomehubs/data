"""Standard arguments for validator flows."""

import argparse

from flows.lib.shared_args import MIN_ASSIGNED, MIN_VALID, S3_PATH, TAXDUMP_PATH, WORK_DIR, YAML_PATH, default
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required


def parse_args(description: str = "Validate data quality") -> argparse.Namespace:
    """Parse command-line arguments for validators.

    Args:
        description (str): Description of the validator flow.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    return _parse_args(
        [
            required(YAML_PATH),
            WORK_DIR,
            default(S3_PATH, None),
            TAXDUMP_PATH,
            MIN_VALID,
            MIN_ASSIGNED,
        ],
        description,
    )
