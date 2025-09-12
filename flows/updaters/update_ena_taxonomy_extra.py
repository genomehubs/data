#!/usr/bin/env python3

import json
import os
import sys
from os.path import abspath, dirname
from urllib.request import urlopen

from tqdm import tqdm

from flows.lib.utils import fetch_from_s3, upload_to_s3

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    __package__ = "flows"

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    APPEND,
    OUTPUT_PATH,
    ROOT_TAXID,
    S3_PATH,
    TAXDUMP_PATH,
    default,
    parse_args,
    required,
)


@task()
def read_ncbi_tax_ids(taxdump_path: str) -> set[str]:
    """Read NCBI tax IDs from the taxdump nodes file."""
    print(f"Reading NCBI taxids from {taxdump_path}")
    tax_ids = set()
    nodes_file = os.path.join(taxdump_path, "nodes.dmp")
    with open(nodes_file, "r") as f:
        for line in f:
            fields = line.strip().split("\t")
            if len(fields) > 1:
                tax_ids.add(fields[0])
    return tax_ids


@task()
def add_jsonl_tax_ids(jsonl_path: str, tax_ids: set[str]) -> None:
    print(f"Reading previously fetched ENA taxids from {jsonl_path}")
    filtered_path = f"{jsonl_path}.filtered"
    try:
        with open(jsonl_path, "r") as f, open(filtered_path, "w") as f_out:
            for line in f:
                data = json.loads(line)
                tax_id = data["taxId"]
                if tax_id not in tax_ids:
                    f_out.write(line)
                    tax_ids.add(tax_id)
        os.replace(filtered_path, jsonl_path)
    except Exception as e:
        print(f"Error reading {jsonl_path}: {e}")
        exit()


@task(log_prints=True)
def get_ena_api_new_taxids(root_taxid: str, existing_tax_ids: set[str]) -> set[str]:
    print(f"Fetching new taxids for tax_tree({root_taxid}) from ENA API")

    limit = 10000000
    url = (
        f"https://www.ebi.ac.uk/ena/portal/api/search?result=taxon"
        f"&query=tax_tree({root_taxid})&limit={limit}"
    )

    # Stream the content of the URL
    column_index = None
    new_tax_ids = set()
    with urlopen(url) as response:
        for line in response:
            columns = line.decode("utf-8").strip().split("\t")
            if column_index is None:
                column_index = 0 if columns[0] == "tax_id" else 1
            else:
                tax_id = columns[column_index]
                if tax_id not in existing_tax_ids:
                    new_tax_ids.add(tax_id)
    return new_tax_ids


def fetch_ena_jsonl(tax_id, f_out):
    print("Fetching new tax_ids from ENA API")
    url = "https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/"
    with urlopen(url + tax_id) as response:
        for line in response:
            f_out.write(line.decode("utf-8").strip())
        f_out.write("\n")


@task(log_prints=True)
def update_ena_jsonl(new_tax_ids: set[str], output_path: str, append: bool) -> None:
    print(f"Updating ENA JSONL file at {output_path} with new tax IDs")
    url = "https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/"
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "a" if append else "w") as f:
            for tax_id in tqdm(new_tax_ids, desc="Fetching ENA tax IDs"):
                try:
                    with urlopen(url + tax_id) as response:
                        for line in response:
                            f.write(line.decode("utf-8").strip())
                        f.write("\n")
                except Exception as e:
                    print(f"Error fetching {tax_id}: {e}")
    except Exception as e:
        print(f"Error updating {output_path}: {e}")


def fetch_s3_jsonl(s3_path: str, local_path: str) -> None:
    print(f"Fetching existing ENA JSONL file from {s3_path} to {local_path}")
    fetch_from_s3(s3_path, local_path)


def upload_s3_jsonl(local_path: str, s3_path: str) -> None:
    print(f"Uploading updated ENA JSONL file from {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_ena_taxonomy_extra(
    root_taxid: str, taxdump_path: str, output_path: str, s3_path: str, append: bool
) -> None:
    """Update the ENA taxonomy JSONL file.

    Args:
        root_taxid (str): Root taxon ID to filter by.
        taxdump_path (str): Path to the NCBI taxdump files.
        output_path (str): Path to save the taxonomy dump.
        append (bool): Flag to append entries to an existing JSONL file.
    """

    # 1. read IDs from ncbi nodes file
    existing_tax_ids = read_ncbi_tax_ids(taxdump_path)
    # 2. fetch jsonl file from s3 if s3_path is provided
    if s3_path:
        fetch_s3_jsonl(s3_path, output_path)
    # 3. read existing IDs from local JSONL file
    if append:
        add_jsonl_tax_ids(output_path, existing_tax_ids)
    # 4. fetch list of new IDs from ENA API
    new_tax_ids = get_ena_api_new_taxids(root_taxid, existing_tax_ids)
    # 5. fetch details for new IDs from ENA API and save to JSONL file
    update_ena_jsonl(new_tax_ids, output_path, append)
    # 6. upload updated JSONL file to s3 if s3_path is provided
    if s3_path:
        upload_s3_jsonl(output_path, s3_path)

    status = len(new_tax_ids) == 0

    emit_event(
        event="update.ena.taxonomy.finished",
        resource={
            "prefect.resource.id": f"fetch.taxonomy.{output_path}",
            "prefect.resource.type": "ena.taxonomy",
            "prefect.resource.matches.previous": ("yes" if status else "no"),
        },
        payload={"matches_previous": status},
    )
    return status


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [
            default(ROOT_TAXID, "2759"),
            required(TAXDUMP_PATH),
            required(OUTPUT_PATH),
            S3_PATH,
            APPEND,
        ],
        "Fetch extra taxa from ENA taxonomy API and optionally filter by root taxon.",
    )

    update_ena_taxonomy_extra(**vars(args))
