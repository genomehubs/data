import hashlib
import os
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

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
from flows.lib.utils import is_safe_path, parse_tsv, run_quoted, safe_get


def taxon_id_to_ssh_path(ssh_host, taxon_id, assembly_name):

    if not is_safe_path(ssh_host):
        raise ValueError(f"Unsafe ssh host: {ssh_host}")
    if not is_safe_path(taxon_id):
        raise ValueError(f"Unsafe taxon_id: {taxon_id}")

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
    result = run_quoted(command, capture_output=True, text=True)
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
        if not is_safe_path(ssh_host):
            raise ValueError(f"Unsafe ssh host: {ssh_host}")
        if not is_safe_path(file_path):
            raise ValueError(f"Unsafe file path: {file_path}")

        command = [
            "ssh",
            ssh_host,
            "bash",
            "-c",
            (f"'ls -d {file_path}/*_odb*/'"),
        ]
        result = run_quoted(command, capture_output=True, text=True)
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

    if not is_safe_path(alt_host):
        raise ValueError(f"Unsafe alt host: {alt_host}")
    if not is_safe_path(assembly_id):
        raise ValueError(f"Unsafe assembly_id: {assembly_id}")

    # find file on alt_host
    command = [
        "ssh",
        alt_host,
        "bash",
        "-c",
        f"'ls /volumes/data/by_accession/{assembly_id}'",
    ]
    result = run_quoted(command, capture_output=True, text=True)
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
        response = safe_get(busco_url)
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


def fetch_goat_results(root_taxid: str, output_path: str) -> list[dict]:
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
    response = safe_get(query_url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(
            f"Error fetching BoaT config info: {response.status_code} {response.text}"
        )

    # Parse the TSV response
    if tsv_data := parse_tsv(response.text):
        with open(output_path, "w") as f:
            f.write(response.text)
        return tsv_data
    else:
        raise RuntimeError("No data found in BoaT config info response")


def trawl_farm_data(goat_results_path: str, farm_results_path: str) -> None:
    """Trawl farm data to find additional assemblies.

    Args:
        goat_results_path (str): Path to the GoaT results TSV file.
        farm_results_path (str): Path to save the farm results TSV file.
    """
    import csv
    import subprocess
    import time

    if not os.path.exists(goat_results_path):
        raise FileNotFoundError(f"GoaT results file not found: {goat_results_path}")

    # Read the GoaT results to get taxon IDs and assembly names
    taxon_assembly = []
    with open(goat_results_path, "r") as f:
        header = f.readline().strip().split("\t")
        taxon_id_idx = header.index("taxon_id")
        assembly_name_idx = header.index("assembly_name")
        for line in f:
            fields = line.strip().split("\t")
            taxon_id = fields[taxon_id_idx]
            assembly_name = fields[assembly_name_idx]
            taxon_assembly.append((taxon_id, assembly_name))

    # Write input CSV for trawler.sh
    input_csv = "farm_trawl_input.csv"
    with open(input_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for taxon_id, assembly_name in taxon_assembly:
            writer.writerow([taxon_id, assembly_name])

    # Copy trawler.sh and input CSV to farm
    trawler_script = "trawler.sh"
    remote_script = "trawler.sh"
    remote_input = "farm_trawl_input.csv"
    remote_output = "farm_trawl_output.csv"
    subprocess.run(["scp", trawler_script, f"farm:~/{remote_script}"], check=True)
    subprocess.run(["scp", input_csv, f"farm:~/{remote_input}"], check=True)

    # Submit job via bsub
    bsub_cmd = f". /etc/profile && rm -f $HOME/{remote_output}.finished && bsub -o trawler.log bash $HOME/{remote_script} $HOME/{remote_input} $HOME/{remote_output}"
    subprocess.run(["ssh", "farm", bsub_cmd], check=True)

    # Wait for output file to appear
    while True:
        result = subprocess.run(
            [
                "ssh",
                "farm",
                f"test -f ~/{remote_output}.finished && echo READY || echo WAIT",
            ],
            capture_output=True,
            text=True,
        )
        if "READY" in result.stdout:
            break
        print("Waiting for farm trawler job to finish...")
        time.sleep(30)

    # Copy output file back
    subprocess.run(["scp", f"farm:~/{remote_output}", farm_results_path], check=True)


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


def filter_buscos(buscos):
    # Exclude bacteria_odb and archaea_odb
    buscos = [
        b
        for b in buscos
        if not (
            b.startswith("bacteria_odb")
            or b.startswith("archaea_odb")
            or b.startswith("mm49_")
        )
    ]
    # Group by prefix before _odb
    prefix_map = defaultdict(list)
    for b in buscos:
        prefix = b.split("_odb")[0]
        prefix_map[prefix].append(b)
    filtered = []
    for prefix, items in prefix_map.items():
        if len(items) > 1:
            if metaeuk := [x for x in items if x.endswith("_metaeuk")]:
                filtered.extend(metaeuk)
            else:
                filtered.append(items[0])
                print(f"Warning: Multiple BUSCOs with prefix {prefix}: {items}")
        else:
            filtered.extend(items)
    return filtered


@task(log_prints=True)
def filter_farm_data(
    farm_results_path: str, goat_results_path: str, output_path: str
) -> None:
    """Filter farm results to include only assemblies with lepidoptera BUSCOs.

    Combine with GoaT results to add additional fields.

    Args:
        farm_results_path (str): Path to the farm results TSV file.
        goat_results_path (str): Path to the GoaT results TSV file.
        output_path (str): Path to save the filtered results TSV file.
    """
    if not os.path.exists(farm_results_path):
        raise FileNotFoundError(f"Farm results file not found: {farm_results_path}")
    if not os.path.exists(goat_results_path):
        raise FileNotFoundError(f"GoaT results file not found: {goat_results_path}")

    # Read assembly IDs from GoaT results
    goat_data = {}
    with open(goat_results_path, "r") as f:
        goat_header = f.readline().strip().split("\t")
        assembly_name_idx = goat_header.index("assembly_name")
        for line in f:
            fields = line.strip().split("\t")
            assembly_name = fields[assembly_name_idx]
            goat_data[assembly_name] = fields

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
    # Filter farm results
    with open(farm_results_path, "r") as infile, open(output_path, "w") as outfile:
        infile.readline()
        outfile.write("\t".join(config_header) + "\n")
        for line in infile:
            fields = line.strip().split(",")
            if len(fields) < 4 or not fields[2] or not fields[3]:
                continue
            buscos = fields[3].split(";")
            buscos = [b for b in buscos if b]
            filtered_buscos = filter_buscos(buscos)
            # if not any(
            #     busco.startswith("lepidoptera_odb") for busco in filtered_buscos
            # ):
            #     continue
            assembly_name = fields[1]
            if assembly_name in goat_data:
                # Optionally merge with goat_data fields if needed
                # Skip second and third columns from goat_data
                # Keep first column, then columns from index 3 onward
                merged = (
                    [goat_data[assembly_name][0]]
                    + goat_data[assembly_name][3:]
                    + [fields[2]]
                    + [",".join(filtered_buscos)]
                )
                outfile.write("\t".join(merged) + "\n")


@flow()
def update_boat_config(
    root_taxid: str, output_path: str, append: bool, s3_path: str
) -> None:
    # fetch_goat_results(root_taxid, f"{output_path}/goat_results.tsv")

    # trawl_farm_data(
    #     f"{output_path}/goat_results.tsv", f"{output_path}/farm_results.csv"
    # )

    filter_farm_data(
        f"{output_path}/farm_results.csv",
        f"{output_path}/goat_results.tsv",
        f"{output_path}/filtered_farm_results.tsv",
    )
    # line_count = fetch_boat_config_info(
    #     root_taxid, file_path=output_path, min_lines=1, append=append
    # )
    # print(
    #     f"Fetched BoaT config info for taxID {root_taxid}. Lines written: {line_count}"
    # )
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
