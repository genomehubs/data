#!/usr/bin/env python3

import os
import subprocess
import sys
from collections import defaultdict
from os.path import abspath, dirname

import yaml

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    # sourcery skip: avoid-builtin-shadow
    __package__ = "flows"

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    INPUT_PATH,
    OUTPUT_PATH,
    ROOT_TAXID,
    S3_PATH,
    default,
    parse_args,
    required,
)
from flows.lib.utils import fetch_from_s3, is_safe_path, upload_to_s3


def get_file_paths_from_config(config: dict, file_paths: dict) -> dict:
    key = config.get("xref_label")
    input_path = config.get("path")
    output_path = config.get("out")
    if key is not None and input_path is not None:
        file_paths[key] = {
            "input": input_path,
        }
    return output_path


@task(log_prints=True)
def read_input_config(input_path: str) -> dict:
    print(f"Reading input config from {input_path}")
    file_paths = defaultdict(dict)
    try:
        with open(input_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error reading {input_path}: {e}")
        exit()
    try:
        output_path = get_file_paths_from_config(config, file_paths)
        if output_path is not None:
            file_paths["out"] = output_path
        for taxonomy in config.get("taxonomies", []):
            get_file_paths_from_config(taxonomy, file_paths)
    except Exception as e:
        print(f"Error parsing {input_path}: {e}")
        exit()
    return file_paths


@task(log_prints=True)
def run_blobtk_taxonomy(root_taxid: str, input_path: str, output_path: str) -> None:
    print(f"Running blobtk taxonomy with root taxid {root_taxid}")
    if not is_safe_path(root_taxid):
        raise ValueError(f"Unsafe root taxid: {root_taxid}")
    if not is_safe_path(input_path):
        raise ValueError(f"Unsafe input path: {input_path}")
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    cmd = [
        "blobtk",
        "taxonomy",
        "-c",
        input_path,
        "-O",
        output_path,
        "-r",
        root_taxid,
    ]
    print(f"Running command: {' '.join(cmd)}")
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in process.stdout:
            print(line, end="")
        process.wait()
        if process.returncode != 0:
            print(f"Command failed with exit code {process.returncode}")
            exit()
    except Exception as e:
        print(f"Error running blobtk taxonomy: {e}")
        exit()


@task(log_prints=True)
def fetch_s3_file(s3_path: str, local_path: str) -> None:
    print(f"Fetching file from {s3_path} to {local_path}")
    fetch_from_s3(s3_path, local_path)


@task(log_prints=True)
def upload_s3_file(local_path: str, s3_path: str) -> None:
    print(f"Uploading file from {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_genomehubs_taxonomy(
    root_taxid: str, input_path: str, output_path: str, s3_path: str
) -> None:
    """Update the GenomeHubs taxonomy JSONL file.

    Args:
        root_taxid (str): Root taxon ID to filter by.
        input_path (str): Path to the input config file.
        output_path (str): Path to save the taxonomy dump.
        s3_path (str): S3 path to upload the taxonomy dump.
    """

    # 1. parse input config yaml
    file_paths = read_input_config(input_path)

    # 2. check files exist locally (file or directory)

    for key, paths in file_paths.items():
        if "input" in paths:
            taxonomy_path = paths["input"]
            if not os.path.exists(taxonomy_path):
                print(f"Error: {taxonomy_path} not found")
                exit()
            if not (os.path.isfile(taxonomy_path) or os.path.isdir(taxonomy_path)):
                print(f"Error: {taxonomy_path} is not a file or directory")
                exit()
    # 3. run blobtk to collate and filter taxonomies
    run_blobtk_taxonomy(root_taxid, input_path, output_path)

    # 4. upload updated JSONL file to s3 if s3_path is provided
    if s3_path:
        upload_s3_file(f"{output_path}", s3_path)

    # 5. count lines in output file
    line_count = 0
    try:
        with open(f"{output_path}", "r") as f:
            line_count = sum(1 for _ in f)
        print(f"Output file has {line_count} lines")
    except Exception as e:
        print(f"Error reading {output_path}: {e}")
        exit()

    emit_event(
        event="update.genomehubs.taxonomy.finished",
        resource={
            "prefect.resource.id": f"fetch.taxonomy.{output_path}",
            "prefect.resource.type": "genomehubs.taxonomy",
            # "prefect.resource.matches.previous": ("yes" if status else "no"),
        },
        payload={"line_count": line_count},
    )


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [
            default(ROOT_TAXID, "2759"),
            required(INPUT_PATH),
            required(OUTPUT_PATH),
            S3_PATH,
        ],
        "Collate source taxonomies and names into GenomeHubs JSONL taxonomy format.",
    )

    update_genomehubs_taxonomy(**vars(args))
