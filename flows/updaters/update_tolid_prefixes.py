#!/usr/bin/env python3

import os
import subprocess
import sys
from os.path import abspath, dirname

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    __package__ = "flows"

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    OUTPUT_PATH,
    ROOT_TAXID,
    S3_PATH,
    default,
    parse_args,
    required,
)
from flows.lib.utils import is_local_file_current_http


@task(retries=2, retry_delay_seconds=2, log_prints=True)
def fetch_tolid_prefixes(
    root_taxid: str,
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
        root_taxid (str): Root taxon ID to filter by.
        http_path (str): URL to fetch the ToLID prefix file from.
        local_path (str): Path to save the ToLID prefix file.

    Returns:
        bool: True if the fetched file matches the remote version, False otherwise.
    """
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
def update_tolid_prefixes(root_taxid: str, output_path: str, s3_path: str) -> None:
    """Fetch and optionally update the ToLID prefixes file.

    Args:
        root_taxid (str): Root taxon ID to filter by.
        output_path (str): Path to save the taxonomy dump.
        s3_path (str): S3 path to compare with.
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
            local_path=output_path, http_path=http_path, root_taxid=root_taxid
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
        [default(ROOT_TAXID, "taxon"), required(OUTPUT_PATH), S3_PATH],
        "Fetch ToLID prefixes and optionally filter by root taxon.",
    )

    update_tolid_prefixes(**vars(args))
