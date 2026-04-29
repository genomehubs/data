import gzip
import os

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import OUTPUT_PATH, S3_PATH, parse_args, required
from flows.lib.utils import is_safe_path, safe_get, upload_to_s3


UCSC_URL = "https://hgdownload.soe.ucsc.edu/hubs/UCSC_GI.assemblyHubList.txt"
OUTPUT_FILENAME = "UCSC_GI.assemblyHubList.tsv.gz"


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def fetch_ucsc_hub_list(output_dir: str) -> tuple[str, int]:
    """Fetch the UCSC assembly hub accession list and write as gzipped TSV.

    The source file is a tab-separated text file served with ISO-8859-1
    encoding. We decode to UTF-8 for consistency.

    Args:
        output_dir (str): Directory to write the output file.

    Returns:
        tuple[str, int]: Path to the output file and number of data lines.
    """
    output_path = os.path.join(output_dir, OUTPUT_FILENAME)
    tsv_path = output_path.removesuffix(".gz")

    print(f"Fetching UCSC hub list from {UCSC_URL}")
    response = safe_get(UCSC_URL, timeout=60)
    response.raise_for_status()
    response.encoding = "iso-8859-1"
    text = response.text

    with open(tsv_path, "w") as f:
        f.write(text)
    line_count = text.count("\n")

    with open(tsv_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
        f_out.write(f_in.read())
    os.remove(tsv_path)

    print(f"Wrote {line_count} lines to {output_path}")
    return output_path, line_count


@task(log_prints=True)
def upload_s3_file(local_path: str, s3_path: str) -> None:
    """Upload file to S3."""
    print(f"Uploading {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_ucsc_assemblies(
    output_path: str,
    s3_path: str = None,
) -> bool:
    """Fetch the UCSC assembly hub list and optionally upload to S3.

    Args:
        output_path (str): Directory to write the output file.
        s3_path (str): Optional S3 directory path to upload the result.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    os.makedirs(output_path, exist_ok=True)

    local_file, line_count = fetch_ucsc_hub_list(output_path)

    if s3_path:
        remote_path = f"{s3_path.rstrip('/')}/{OUTPUT_FILENAME}"
        upload_s3_file(local_file, remote_path)

    emit_event(
        event="update.ucsc.assemblies.finished",
        resource={
            "prefect.resource.id": f"update.ucsc.{output_path}",
            "prefect.resource.type": "ucsc.assemblies",
        },
        payload={"line_count": line_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH],
        "Fetch UCSC assembly hub accession list.",
    )
    update_ucsc_assemblies(**vars(args))
