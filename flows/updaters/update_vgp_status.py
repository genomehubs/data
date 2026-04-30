import os

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import MIN_RECORDS, OUTPUT_PATH, S3_PATH, parse_args, required
from flows.lib.utils import upload_to_s3
from flows.updaters.api import api_config as cfg
from flows.updaters.api import api_tools as at


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def fetch_vgp_tsv(
    file_path: str,
    min_lines: int = 1,
) -> int:
    """Fetch VGP status list from the Vertebrate Genomes Project GitHub tracker.

    Downloads the VGP genome portal YAML tracker, extracts species records,
    and writes a TSV with per-species status fields.

    Args:
        file_path (str): Path to the output TSV file.
        min_lines (int): Minimum number of rows expected.

    Returns:
        int: Number of lines written to the output file.
    """
    at.get_from_source(
        cfg.vgl_url_opener,
        cfg.vgl_hub_count_handler,
        cfg.vgl_row_handler,
        cfg.vgl_fieldnames,
        file_path,
    )

    with open(file_path, "r") as f:
        line_count = sum(1 for _ in f)

    if line_count < min_lines:
        raise RuntimeError(
            f"VGP file {file_path} has fewer than {min_lines} lines: {line_count}"
        )
    print(f"Wrote {line_count} lines to {file_path}")
    return line_count


@task(log_prints=True)
def upload_s3_tsv(local_path: str, s3_path: str) -> None:
    """Upload VGP TSV to S3."""
    print(f"Uploading VGP TSV from {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_vgp_status(
    output_path: str, s3_path: str = None, min_records: int = 0
) -> bool:
    """Fetch the VGP status list and optionally upload to S3.

    Args:
        output_path (str): Path to the output TSV file.
        s3_path (str): Optional S3 path to upload the result.
        min_records (int): Minimum record count to accept the output.

    Returns:
        bool: True on success.
    """

    resolved_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(resolved_path), exist_ok=True)
    line_count = fetch_vgp_tsv(resolved_path, min_records)
    if line_count > min_records and s3_path:
        upload_s3_tsv(resolved_path, s3_path)
    emit_event(
        event="update.vgp.status.finished",
        resource={
            "prefect.resource.id": f"update.vgp.{resolved_path}",
            "prefect.resource.type": "vgp.status",
        },
        payload={"line_count": line_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, MIN_RECORDS],
        "Fetch VGP status list from the Vertebrate Genomes Project.",
    )
    update_vgp_status(**vars(args))
