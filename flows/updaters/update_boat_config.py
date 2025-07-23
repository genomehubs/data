#!/usr/bin/env python3

# sourcery skip: avoid-builtin-shadow
import hashlib
import os
import subprocess
import sys
from os.path import abspath, dirname

import boto3
import requests
from botocore.exceptions import ClientError

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    __package__ = "flows"

# from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.conditional_import import flow, task
from flows.lib.shared_args import (
    APPEND,
    OUTPUT_PATH,
    ROOT_TAXID,
    S3_PATH,
    default,
    parse_args,
    required,
)
from flows.lib.utils import parse_tsv


def taxon_id_to_ssh_path(ssh_host, taxon_id, assembly_name):
    command = [
        "ssh",
        ssh_host,
        "bash",
        "-c",
        (
            f"'. /etc/profile && module load speciesops && "
            f"speciesops getdir --taxon_id {taxon_id}'"
        ),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(
            (
                f"WARNING: Error fetching directory for taxon_id {taxon_id}: "
                f"{result.stderr}"
            )
        )
        return
    # Filter the result to get the lustre path
    lustre_path = [line for line in result.stdout.splitlines() if "/lustre" in line]
    if not lustre_path:
        print(
            (
                f"WARNING: No lustre path found for taxon_id {taxon_id} in result: "
                f"{result.stdout}"
            )
        )
        return
    # Use the first lustre path
    lustre_path = lustre_path[0].strip()
    return f"{lustre_path}/analysis/{assembly_name}/busco"


def lookup_buscos(ssh_host, file_path):
    if "lustre" in file_path:

        command = [
            "ssh",
            ssh_host,
            "bash",
            "-c",
            (f"'ls -d {file_path}/*_odb*/'"),
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return []
        busco_dirs = [
            os.path.basename(os.path.normpath(line))
            for line in result.stdout.splitlines()
            if "/busco" in line
        ]
    return busco_dirs


def assembly_id_to_busco_sets(alt_host, assembly_id):
    """
    Fetch the alternative path for an assembly ID from the alt host.
    This function uses SSH to run a command on the alt host to get the path.
    """
    # find file on alt_host
    command = [
        "ssh",
        alt_host,
        "bash",
        "-c",
        f"'ls /volumes/data/by_accession/{assembly_id}'",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        return f"/volumes/data/by_accession/{assembly_id}", result.stdout.splitlines()

    # find file at busco.cog.sanger.ac.uk
    lineages = [
        "lepidoptera_odb10",
        "endopterygota_odb10",
        "insecta_odb10",
        "arthropoda_odb10",
        "eukaryota_odb10",
    ]
    busco_sets = []
    for lineage in lineages:
        busco_url = (
            f"https://busco.cog.sanger.ac.uk/{assembly_id}/{lineage}/full_table.tsv"
        )
        response = requests.get(busco_url)
        if response.status_code == 200:
            busco_sets.append(lineage)
    return f"https://busco.cog.sanger.ac.uk/{assembly_id}", busco_sets


def prepare_output_files(file_path, visited_file_path, append):
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    config_header = [
        "taxon_id",
        "assembly_id",
        "assembly_name",
        "assembly_span",
        "chromosome_count",
        "assembly_level",
        "lustre_path",
        "busco_sets",
    ]

    line_count = 0

    # If append is false, open the file for writing and truncate it
    if not append and os.path.exists(file_path):
        with open(file_path, "w") as f:
            f.truncate(0)
            f.write("\t".join(config_header) + "\n")
    elif not os.path.exists(file_path):
        # If the file does not exist, create it
        with open(file_path, "w") as f:
            f.write("\t".join(config_header) + "\n")
    else:
        # count the lines in the file
        with open(file_path, "r") as f:
            for _ in f:
                line_count += 1
        line_count = max(line_count - 1, 0)  # Exclude header line

    visited_assembly_ids = set()
    # If the visited file exists, read it and store the visited assembly IDs
    if os.path.exists(visited_file_path):
        if append:
            with open(visited_file_path, "r") as f:
                visited_assembly_ids = {line.strip() for line in f}
        else:
            # truncate the file if not appending
            with open(visited_file_path, "w") as f:
                f.truncate(0)
    return visited_assembly_ids, line_count


def fetch_goat_results(root_taxid):
    # GoaT results for the root taxID
    query = (
        f"tax_tree%28{root_taxid}%29%20AND%20assembly_level%20%3D%20chromosome%2C"
        "complete%20genome%20AND%20biosample_representative%20%3D%20primary"
    )
    fields = "assembly_span%2Cchromosome_count%2Cassembly_level"
    names = "assembly_name"
    # Generate a dynamic query_id using a hash of the query and fields
    query_id = hashlib.md5(f"{query}{fields}{names}".encode("utf-8")).hexdigest()[:10]
    query_url = (
        f"https://goat.genomehubs.org/api/v2/search?query={query}"
        f"&result=assembly&includeEstimates=true&taxonomy=ncbi"
        f"&fields={fields}&names={names}"
        f"&size=100000&filename=download.tsv&queryId={query_id}&persist=once"
    )

    # fetch query_url with accept header tsv. use python module requests
    headers = {"Accept": "text/tab-separated-values"}
    response = requests.get(query_url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(
            f"Error fetching BoaT config info: {response.status_code} {response.text}"
        )

    # Parse the TSV response
    if tsv_data := parse_tsv(response.text):
        return tsv_data
    else:
        raise RuntimeError("No data found in BoaT config info response")


@task(retries=2, retry_delay_seconds=2, log_prints=True)
def fetch_boat_config_info(
    root_taxid: str,
    file_path: str,
    min_lines: int = 1,
    append: bool = False,
    ssh_host: str = "farm",
    alt_host: str = "btkdev",
) -> int:
    """
    Fetch BoaT config info for a given root taxID.

    Args:
        root_taxid (str): Root taxonomic ID for fetching datasets.
        file_path (str): Path to the output file.
        min_lines (int): Minimum number of lines expected in the output file.
        append (bool): Flag to append data to previous results.
        ssh_host (str): SSH host to connect to for fetching directories.

    Returns:
        int: Number of lines written to the output file.
    """

    tsv_data = fetch_goat_results(root_taxid)

    # Prepare output files and get visited assembly IDs
    visited_file_path = f"{os.path.splitext(file_path)[0]}.visited"
    visited_assembly_ids, line_count = prepare_output_files(
        file_path, visited_file_path, append
    )

    for row in tsv_data:
        taxon_id = row["taxon_id"]
        assembly_id = row["assembly_id"]
        # Skip if the assembly_id has already been visited
        if assembly_id in visited_assembly_ids:
            print(
                (
                    f"Skipping already visited assembly_id {assembly_id} "
                    f"for taxon_id {taxon_id}."
                )
            )
            continue
        print(
            f"Processing taxon_id {taxon_id}, assembly_id {assembly_id} "
            f"for assembly_name {row['assembly_name']}."
        )
        # Add the assembly_id to the new visited list
        with open(visited_file_path, "a") as f:
            f.write(f"{assembly_id}\n")

        assembly_name = row["assembly_name"]
        # Run speciesops command on the farm via ssh using subprocess
        # Use a single shell command string so that module load and
        # speciesops run in the same shell
        lustre_path = taxon_id_to_ssh_path(ssh_host, taxon_id, assembly_name)
        busco_sets = []
        if lustre_path:
            busco_sets = lookup_buscos(ssh_host, lustre_path)

        if not busco_sets:
            lustre_path, busco_sets = assembly_id_to_busco_sets(alt_host, assembly_id)

        if not busco_sets:
            print(
                f"Warning: No BUSCO sets found for taxon_id {taxon_id} "
                f"and assembly_name {assembly_name}. Skipping."
            )
            continue

        config_row = [
            taxon_id,
            assembly_id,
            assembly_name,
            row["assembly_span"],
            row["chromosome_count"],
            row["assembly_level"],
            lustre_path,
            ",".join(busco_sets),
        ]

        with open(file_path, "a") as f:
            f.write("\t".join(map(str, config_row)) + "\n")

        line_count += 1

    if line_count < min_lines:
        print(
            f"WARNING: File {file_path} has less than {min_lines} lines: {line_count}"
        )

    # Return the number of lines written to the file
    return line_count


@task(retries=2, retry_delay_seconds=2)
def compare_datasets_summary(local_path: str, s3_path: str) -> bool:
    """
    Compare local and remote NCBI datasets summary files.

    Args:
        local_path (str): Path to the local file.
        s3_path (str): Path to the remote file on s3.

    Returns:
        bool: True if the files are the same, False otherwise.
    """

    # Return error if the local file does not exist
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file {local_path} does not exist")

    s3 = boto3.client("s3")

    # Extract bucket name and key from the S3 path
    def parse_s3_path(s3_path):
        bucket, key = s3_path.removeprefix("s3://").split("/", 1)
        return bucket, key

    bucket, key = parse_s3_path(s3_path)

    # Return false if the remote file does not exist
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False

    # Generate md5sum of the local file
    def generate_md5(file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    local_md5 = generate_md5(local_path)

    # Generate md5sum of the remote file
    remote_obj = s3.get_object(Bucket=bucket, Key=key)
    remote_md5 = remote_obj["ETag"].strip('"')

    # Return True if the md5sums are the same
    return local_md5 == remote_md5


@flow()
def update_boat_config(
    root_taxid: str, output_path: str, append: bool, s3_path: str
) -> None:
    line_count = fetch_boat_config_info(
        root_taxid, file_path=output_path, min_lines=1, append=append
    )
    print(
        f"Fetched BoaT config info for taxID {root_taxid}. Lines written: {line_count}"
    )
    # if s3_path:
    #     status = compare_datasets_summary(output_path, s3_path)
    #     emit_event(
    #         event="update.ncbi.datasets.finished",
    #         resource={
    #             "prefect.resource.id": f"fetch.datasets.{output_path}",
    #             "prefect.resource.type": "ncbi.datasets",
    #             "prefect.resource.matches.previous": "yes" if status else "no",
    #         },
    #         payload={"line_count": line_count, "matches_previous": status},
    #     )
    #     return status
    # return False


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [default(ROOT_TAXID, "2759"), required(OUTPUT_PATH), APPEND, S3_PATH],
        "Collate available data for BoaT import.",
    )

    update_boat_config(**vars(args))
