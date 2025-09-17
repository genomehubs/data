import os
import subprocess

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import OUTPUT_PATH, parse_args, required
from flows.lib.utils import generate_md5, is_local_file_current_http, is_safe_path


@task(retries=2, retry_delay_seconds=2, log_prints=True)
def fetch_ncbi_taxonomy(
    local_path: str,
    http_path: str = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz",
) -> bool:
    """
    Fetch the NCBI taxonomy dump.

    Args:
        http_path (str): URL to fetch the taxonomy dump from.
        local_path (str): Path to save the taxonomy dump.

    Returns:
        bool: True if the fetched file matches the remote version, False otherwise.
    """
    # create local_path if it doesn't exist
    if not is_safe_path(local_path):
        raise ValueError(f"Unsafe local path: {local_path}")
    if not is_safe_path(http_path):
        raise ValueError(f"Unsafe HTTP path: {http_path}")
    os.makedirs(local_path, exist_ok=True)
    local_gz_file = f"{local_path}/taxdump.tar.gz"
    # Fetch the remote file
    cmd = ["curl", "-sSL", http_path, "-o", local_gz_file]
    print(f"Running command: {' '.join(cmd)}")
    # Inputs have been validated by is_safe_path; safe to use in subprocess
    subprocess.run(cmd, check=True)

    remote_md5_path = f"{http_path}.md5"
    # Fetch the remote MD5 checksum
    cmd = ["curl", "-sSL", remote_md5_path]
    print(f"Running command: {' '.join(cmd)}")
    # Inputs have been validated by is_safe_path; safe to use in subprocess
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    remote_md5 = result.stdout.split()[0]

    # Calculate the local MD5 checksum
    local_md5 = generate_md5(local_gz_file)
    print(f"Local MD5: {local_md5}, Remote MD5: {remote_md5}")

    if local_md5 != remote_md5:
        print("MD5 checksums do not match. The file may be corrupted.")
        return False

    # extract the tar.gz file
    cmd = ["tar", "-xzf", local_gz_file, "-C", local_path]
    print(f"Running command: {' '.join(cmd)}")
    # Inputs have been validated by is_safe_path; safe to use in subprocess
    subprocess.run(cmd, check=True)

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
def taxonomy_is_up_to_date(local_path: str, http_path: str) -> bool:
    """
    Check if the local NCBI taxonomy file is up-to-date with the remote file.

    Args:
        local_path (str): Path to the local file.
        http_path (str): Path to the remote file on HTTP.

    Returns:
        bool: True if the local file is up-to-date, False otherwise.
    """
    return is_local_file_current_http(f"{local_path}/nodes.dmp", http_path)


@flow()
def update_ncbi_taxonomy(output_path: str) -> None:
    """Fetch and the NCBI taxonomy dump.

    Args:
        output_path (str): Path to save the taxonomy dump.
    """
    http_path = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz"
    status = None
    if taxonomy_is_up_to_date(output_path, http_path):
        status = True
    else:
        status = False
        fetch_ncbi_taxonomy(local_path=output_path, http_path=http_path)
    print(f"Taxonomy update status: {status}")

    emit_event(
        event="update.ncbi.taxonomy.finished",
        resource={
            "prefect.resource.id": f"fetch.taxonomy.{output_path}",
            "prefect.resource.type": "ncbi.taxonomy",
            "prefect.resource.matches.previous": "yes" if status else "no",
        },
        payload={"matches_previous": status},
    )
    return status


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [required(OUTPUT_PATH)],
        "Fetch NCBI taxdump.",
    )

    update_ncbi_taxonomy(**vars(args))
    update_ncbi_taxonomy(**vars(args))
