#!/usr/bin/env python3

import os
from urllib.parse import urlencode

import requests
from conditional_import import flow, task
from shared_args import (
    ASSEMBLY_ID,
    HTTP_PATH,
    S3_PATH,
    TAXON_ID,
    WORK_DIR,
    multi,
    parse_args,
    required,
)
from utils import find_http_file, find_s3_file, get_genomehubs_attribute_value


@task()
def snapshot_exists(s3_path: list, record_id: str, index_type: str = "feature") -> str:
    """
    Check if a snapshot exists for the record ID.

    Args:
        s3_path (list): List of paths to the S3 buckets.
        record_id (str): Record ID.
        index_type (str): Type of index to fetch.

    Returns:
        str: Path to the snapshot file.
    """
    return find_s3_file(s3_path, f"{record_id}_{index_type}.tsv")


@task()
def list_busco_lineages(assembly_id: str, work_dir: str) -> list:
    """
    Use GoaT to BUSCO lineages for the assembly ID.

    Args:
        assembly_id (str): Assembly ID.
        work_dir (str): Path to the working directory.

    Returns:
        list: List of BUSCO lineagess.
    """
    goat_api = "https://goat.genomehubs.org/api/v2"
    queryString = urlencode(
        {
            "query": "tax_lineage(queryA.taxon_id)",
            "queryA": f"assembly--assembly_id={assembly_id}",
            "fields": "odb10_lineage",
        }
    )
    url = f"{goat_api}/search?{queryString}"
    # Fetch the list of BUSCO lineages
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    return [
        get_genomehubs_attribute_value(result, "odb10_lineage")
        for result in response.json()["results"]
    ]


@task()
def find_busco_files(assembly_id, busco_lineages, work_dir, http_path):
    busco_http_path = [f"{path}/{assembly_id}" for path in http_path]
    busco_work_dir = f"{work_dir}/{assembly_id}/busco"
    # create busco directory
    os.makedirs(busco_work_dir, exist_ok=True)
    busco_files = []
    for lineage in busco_lineages:
        for path in busco_http_path:
            if busco_file := find_http_file(path, f"{lineage}/full_table.tsv"):
                local_file = f"{busco_work_dir}/{lineage}_full_table.tsv"
                requests.get(busco_file, timeout=300).content
                with open(local_file, "wb") as file:
                    file.write(requests.get(busco_file, timeout=300).content)
                busco_files.append(local_file)
                break
    return busco_files


@task()
def find_blobtoolkit_files(assembly_id, work_dir, http_path):
    blobtoolkit_api_url = "https://blobtoolkit.genomehubs.org/api/v1"
    blobtoolkit_search_url = f"{blobtoolkit_api_url}/search/{assembly_id}"
    response = requests.get(blobtoolkit_search_url, timeout=300)
    response.raise_for_status()
    results = response.json()
    if not results:
        print(f"No results found for {assembly_id}")
        return []
    # sort the results by the revision number and keep the highest as result
    results.sort(key=lambda x: x["revision"], reverse=True)
    result = results[0]
    # find the dataset id from the returned json
    dataset_id = result.get("id", "")
    if not dataset_id:
        print(f"No dataset id found for {assembly_id}")
        return []
    # check there is at least one key in summaryStats.readMapping
    if not result.get("summaryStats", {}).get("readMapping"):
        print(f"No readMapping found for {assembly_id}")
        return []
    # fetch the full dataset metadata
    blobtoolkit_metadata_url = f"{blobtoolkit_api_url}/dataset/id/{dataset_id}"
    response = requests.get(blobtoolkit_metadata_url, timeout=300)
    response.raise_for_status()
    metadata = response.json()
    print(metadata)

    # blobtoolkit_work_dir = f"{work_dir}/{assembly_id}/blobtoolkit"
    # # create blobtoolkit directory
    # os.makedirs(blobtoolkit_work_dir, exist_ok=True)
    # blobtoolkit_files = []
    # for path in blobtoolkit_http_path:
    #     if blobtoolkit_file := find_http_file(path, "blobplot.json"):
    #         local_file = f"{blobtoolkit_work_dir}/blobplot.json"
    #         with open(local_file, "wb") as file:
    #             file.write(requests.get(blobtoolkit_file).content)
    #         blobtoolkit_files.append(local_file)
    # return blobtoolkit_files


@flow()
def index_assembly_features(
    assembly_id: str, taxon_id: str, work_dir: str, s3_path: list, http_path: list
) -> None:
    # if snapshot_exists(s3_path, assembly_id, "feature"):
    #     return taxon_id
    # find_files(assembly_id, work_dir, s3_path)
    # busco_lineages = list_busco_lineages(assembly_id, work_dir)
    # busco_files = find_busco_files(assembly_id, busco_lineages, work_dir, http_path)
    blobtoolkit_files = find_blobtoolkit_files(assembly_id, work_dir, http_path)
    print(blobtoolkit_files)


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [
            required(ASSEMBLY_ID),
            required(TAXON_ID),
            WORK_DIR,
            multi(S3_PATH),
            multi(HTTP_PATH),
        ],
        "Find, index and snapshot assembly features.",
    )

    # Run the flow
    index_assembly_features(**vars(args))
