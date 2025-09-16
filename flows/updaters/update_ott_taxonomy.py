#!/usr/bin/env python3

import os
import subprocess
import sys
from os.path import abspath, dirname

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    # sourcery skip: avoid-builtin-shadow
    __package__ = "flows"

import json

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import OUTPUT_PATH, parse_args, required
from flows.lib.utils import is_local_file_current_http, is_safe_path


@task(retries=2, retry_delay_seconds=2, log_prints=True)
def fetch_ott_taxonomy(
    local_path: str,
    http_path: str,
) -> bool:
    """
    Fetch the OTT taxonomy  and filter by root taxon if specified.

    Args:
        http_path (str): URL to fetch the taxonomy from.
        local_path (str): Path to save the taxonomy.

    Returns:
        bool: True if the fetched file matches the remote version, False otherwise.
    """
    if not is_safe_path(local_path):
        raise ValueError(f"Unsafe local path: {local_path}")
    if not is_safe_path(http_path):
        raise ValueError(f"Unsafe HTTP path: {http_path}")
    # create local_path if it doesn't exist
    os.makedirs(local_path, exist_ok=True)
    local_gz_file = f"{local_path}/ott.tar.gz"
    # Fetch the remote file
    cmd = ["curl", "-sSL", http_path, "-o", local_gz_file]
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # extract the tar.gz file
    cmd = ["tar", "-xzf", local_gz_file, "-C", local_path]
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # Find the extracted subdirectory (should start with 'ott')
    extracted_dirs = [
        d
        for d in os.listdir(local_path)
        if os.path.isdir(os.path.join(local_path, d)) and d.startswith("ott")
    ]
    if not extracted_dirs:
        raise RuntimeError("No extracted ott directory found.")
    ott_dir = os.path.join(local_path, extracted_dirs[0])

    # Move all files from the ott subdirectory to local_path
    for fname in os.listdir(ott_dir):
        src = os.path.join(ott_dir, fname)
        dst = os.path.join(local_path, fname)
        if os.path.exists(dst):
            os.remove(dst)
        os.rename(src, dst)

    # Remove the now-empty ott subdirectory
    os.rmdir(ott_dir)

    # set the timestamp of extracted files to match the tar.gz file
    gz_mtime = os.path.getmtime(local_gz_file)
    for fname in os.listdir(local_path):
        fpath = os.path.join(local_path, fname)
        if os.path.isfile(fpath):
            os.utime(fpath, (gz_mtime, gz_mtime))

    # remove the tar.gz file
    os.remove(local_gz_file)

    return True


@task(log_prints=True)
def ott_taxonomy_is_up_to_date(local_path: str, http_path: str) -> bool:
    """
    Check if the local OTT taxonomy file is up-to-date with the remote file.

    Args:
        local_path (str): Path to the local file.
        http_path (str): Path to the remote file on HTTP.

    Returns:
        bool: True if the local file is up-to-date, False otherwise.
    """
    return is_local_file_current_http(f"{local_path}/taxonomy.tsv", http_path)


def set_ott_url() -> str:
    """Set the OTT URL.

    Returns:
        str: The OTT URL.
    """

    # Fetch OTT taxonomy info as JSON
    cmd = [
        "curl",
        "-X",
        "POST",
        "-s",
        "https://api.opentreeoflife.org/v3/taxonomy/about",
    ]
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    ott_json = json.loads(result.stdout)

    # Extract required fields
    source = ott_json.get("source", "")
    name = ott_json.get("name", "")
    version = ott_json.get("version", "")

    # Replace "draft" with "." in source to get OTT_VERSION
    ott_version = source.replace("draft", ".")
    ott_major_version = f"{name}{version}"

    return (
        f"https://files.opentreeoflife.org/ott/"
        f"{ott_major_version}/{ott_version}.tgz"
    )


@flow()
def update_ott_taxonomy(output_path: str) -> None:
    """Fetch the OTT taxonomy file.

    Args:
        output_path (str): Path to save the taxonomy dump.
    """
    http_path = set_ott_url()
    status = None
    complete = False
    if ott_taxonomy_is_up_to_date(output_path, http_path):
        status = True
        complete = True
    else:
        status = False
        complete = fetch_ott_taxonomy(local_path=output_path, http_path=http_path)
    print(f"OTT taxonomy file matches previous: {status}")

    if complete:
        emit_event(
            event="update.ott.taxonomy.finished",
            resource={
                "prefect.resource.id": f"fetch.ott.taxonomy.{output_path}",
                "prefect.resource.type": "ott.taxonomy",
                "prefect.resource.matches.previous": "yes" if status else "no",
            },
            payload={"matches_previous": status},
        )
    return status


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [required(OUTPUT_PATH)],
        "Fetch OTT taxonomy file.",
    )

    update_ott_taxonomy(**vars(args))
