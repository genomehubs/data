#!/usr/bin/env python3

import sys
from os.path import abspath, dirname

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    # sourcery skip: avoid-builtin-shadow
    __package__ = "flows"

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import MIN_RECORDS, OUTPUT_PATH, parse_args, required
from flows.updaters.api import api_config as cfg
from flows.updaters.api import api_tools as at


@task(retries=2, retry_delay_seconds=2, log_prints=True)
def fetch_nhm_tsv(
    file_path: str,
    min_lines: int = 1,
) -> int:
    """
    Fetch NHM tsv file.

    Args:
        file_path (str): Path to the output file.
        min_lines (int): Minimum number of lines in the output file.

    Returns:
        int: Number of lines written to the output file.
    """
    # fetch the nhm tsv file
    at.get_from_source(
        cfg.nhm_url_opener,
        cfg.nhm_api_count_handler,
        cfg.nhm_row_handler,
        cfg.nhm_fieldnames,
        file_path,
    )

    # Count the number of lines in the file
    with open(file_path, "r") as f:
        line_count = sum(1 for _ in f)

    # If the file has less than min_records lines, raise an error
    if line_count < min_lines:
        raise RuntimeError(
            f"File {file_path} has less than {min_lines} lines: {line_count}"
        )
    # Return the line count
    return line_count


@flow()
def update_nhm(output_path: str, min_records: int) -> None:
    line_count = fetch_nhm_tsv(output_path, min_records)
    emit_event(
        event="update.nhm.tsv.finished",
        resource={
            "prefect.resource.id": f"update.nhm.{output_path}",
            "prefect.resource.type": "nhm.tsv",
        },
        payload={"line_count": line_count},
    )
    return True


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [required(OUTPUT_PATH), MIN_RECORDS],
        "Fetch species data from NHM.",
    )

    update_nhm(**vars(args))
    update_nhm(**vars(args))
