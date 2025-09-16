#!/usr/bin/env python3

import os
import subprocess
import sys
from os.path import abspath, dirname

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    # sourcery skip: avoid-builtin-shadow
    __package__ = "flows"

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import OUTPUT_PATH, parse_args, required
from flows.lib.utils import is_local_file_current_http, is_safe_path


@task(retries=2, retry_delay_seconds=2, log_prints=True)
def fetch_tolid_prefixes(
    local_path: str,
    http_path: str = (
        "https://gitlab.com/wtsi-grit/darwin-tree-of-life-sample-naming/"
        "-/raw/master/tolids.txt?ref_type=heads"
    ),
    min_lines: int = 400000,
) -> bool:
    """
    Fetch the ToLID prefix file and filter by root taxon if specified.

    Args:
        http_path (str): URL to fetch the ToLID prefix file from.
        local_path (str): Path to save the ToLID prefix file.

    Returns:
        bool: True if the fetched file matches the remote version, False otherwise.
    """

    if not is_safe_path(local_path):
        raise ValueError(f"Unsafe local path: {local_path}")
    if not is_safe_path(http_path):
        raise ValueError(f"Unsafe HTTP path: {http_path}")

    # create local_path if it doesn't exist
    os.makedirs(local_path, exist_ok=True)
    local_file = f"{local_path}/tolids.txt"
    # Fetch the remote file
    cmd = ["curl", "-sSL", http_path, "-o", local_file]
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # check the number of lines in the file
    with open(local_file, "r") as f:
        num_lines = sum(1 for _ in f)
    if num_lines < min_lines:
        print(f"File has too few lines: {num_lines} < {min_lines}")
        return False, num_lines

    return True, num_lines


@task(log_prints=True)
def tolid_file_is_up_to_date(local_path: str, http_path: str) -> bool:
    """
    Check if the local ToLID prefixes file is up-to-date with the remote file.

    Args:
        local_path (str): Path to the local file.
        http_path (str): Path to the remote file on HTTP.

    Returns:
        bool: True if the local file is up-to-date, False otherwise.
    """
    return is_local_file_current_http(f"{local_path}/tolids.txt", http_path)


@flow()
def update_tolid_prefixes(output_path: str) -> None:
    """Fetch the ToLID prefixes file.

    Args:
        output_path (str): Path to save the taxonomy dump.
    """
    http_path = (
        "https://gitlab.com/wtsi-grit/darwin-tree-of-life-sample-naming/"
        "-/raw/master/tolids.txt?ref_type=heads"
    )
    status = None
    complete = False
    if tolid_file_is_up_to_date(output_path, http_path):
        status = True
        complete = True
        line_count = 0
        with open(f"{output_path}/tolids.txt", "r") as f:
            line_count = sum(1 for _ in f)
    else:
        status = False
        complete, line_count = fetch_tolid_prefixes(
            local_path=output_path, http_path=http_path
        )
    print(f"TolID file matches previous: {status}")

    if complete:
        emit_event(
            event="update.tolid.prefixes.finished",
            resource={
                "prefect.resource.id": f"fetch.tolid.prefixes.{output_path}",
                "prefect.resource.type": "tolid.prefixes",
                "prefect.resource.matches.previous": "yes" if status else "no",
            },
            payload={"matches_previous": status, "line_count": line_count},
        )
    return status


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [required(OUTPUT_PATH)],
        "Fetch ToLID prefixes.",
    )

    update_tolid_prefixes(**vars(args))
