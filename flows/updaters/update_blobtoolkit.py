import contextlib
import csv
import gzip
import json
import os
import time

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import MIN_RECORDS, OUTPUT_PATH, S3_PATH, parse_args, required
from flows.lib.utils import _build_session, is_safe_path, upload_to_s3

BTK_API = "https://blobtoolkit.genomehubs.org/api/v1"
BTK_VIEW = "https://blobtoolkit.genomehubs.org/view"

TSV_FIELDNAMES = [
    "accession",
    "taxid",
    "species",
    "taxon_name",
    "subspecies",
    "id",
    "source",
    "sourceSlug",
    "sourceStub",
    "busco_lineage",
    "busco_string",
    "busco_complete",
    "nohit",
    "target",
    "at_percent",
    "gc_percent",
    "n_percent",
]


def _stream_datasets(root: str, session) -> list:
    """Stream BlobToolKit dataset entries for a taxon root.

    Args:
        root (str): Taxonomic root to query (e.g., "Eukaryota").
        session: A requests.Session with retry support.

    Returns:
        list: List of dataset metadata dicts.
    """
    url = f"{BTK_API}/search/{root}"
    response = session.get(url, timeout=300)
    response.raise_for_status()
    return response.json()


def _extract_stats(meta: dict) -> dict:
    """Extract BlobToolKit summary stats into a flat dict row.

    Args:
        meta (dict): Raw BTK dataset metadata.

    Returns:
        dict: Flat row dict matching TSV_FIELDNAMES.
    """
    summary = meta.get("summaryStats", {})
    row = {
        "accession": meta.get("accession", ""),
        "taxid": str(meta.get("taxid", "")),
        "species": meta.get("species", meta.get("taxon_name", "")),
        "taxon_name": meta.get("taxon_name", ""),
        "subspecies": "",
        "id": meta.get("id", ""),
        "source": "BlobToolKit",
        "sourceSlug": meta.get("id", ""),
        "sourceStub": "https://blobtoolkit.genomehubs.org/view/dataset/",
        "busco_lineage": "",
        "busco_string": "",
        "busco_complete": "",
        "nohit": "",
        "target": "",
        "at_percent": "",
        "gc_percent": "",
        "n_percent": "",
    }

    with contextlib.suppress(KeyError):
        taxon_name = meta.get("taxon_name", "")
        species = meta.get("species", "")
        if species and taxon_name and len(taxon_name) > len(species):
            row["subspecies"] = taxon_name

    if "busco" in summary:
        for lineage, stats in summary["busco"].items():
            row["busco_lineage"] = lineage
            row["busco_string"] = stats.get("string", "")
            total = stats.get("t", 0)
            if total > 0:
                row["busco_complete"] = f"{stats.get('c', 0) / total * 100:.2f}"
            break

    if "stats" in summary:
        row["nohit"] = f"{summary['stats'].get('noHit', 0) * 100:.2f}"
        with contextlib.suppress(KeyError):
            row["target"] = f"{summary['stats']['target'] * 100:.2f}"

    if "baseComposition" in summary:
        bc = summary["baseComposition"]
        row["at_percent"] = f"{bc.get('at', 0) * 100:.2f}"
        row["gc_percent"] = f"{bc.get('gc', 0) * 100:.2f}"
        row["n_percent"] = f"{bc.get('n', 0) * 100:.2f}"

    return row


def _describe_files(meta: dict) -> list:
    """Generate analysis file descriptors for a BlobToolKit dataset.

    Args:
        meta (dict): Raw BTK dataset metadata.

    Returns:
        list: List of file descriptor dicts.
    """
    plots = ["cumulative", "snail"]
    summary = meta.get("summaryStats", {})
    if summary.get("readMapping"):
        plots.append("blob")

    files = []
    dataset_id = meta.get("id", "")
    accession = meta.get("accession", "")
    taxid = str(meta.get("taxid", ""))

    for plot in plots:
        if plot == "blob":
            url = f"{BTK_API}/image/{dataset_id}/{plot}/circle?format=png"
        else:
            url = f"{BTK_API}/image/{dataset_id}/{plot}?format=png"
        files.append(
            {
                "name": f"{plot}.png",
                "url": url,
                "source_url": f"{BTK_VIEW}/{dataset_id}/dataset/{dataset_id}/{plot}",
                "analysis_id": f"btk-{dataset_id}",
                "description": f"a {plot} plot from BlobToolKit analysis {dataset_id}",
                "title": f"{plot} plot {dataset_id}",
                "command": "blobtoolkit pipeline",
                "assembly_id": accession,
                "taxon_id": taxid,
                "analysis": {
                    "name": "BlobToolKit",
                    "title": f"BlobToolKit analysis of {accession}",
                    "description": (
                        f"Analysis of public assembly {accession} "
                        f"using BlobToolKit"
                    ),
                    "source": "BlobToolKit",
                    "source_url": (
                        f"https://blobtoolkit.genomehubs.org/view/dataset/{dataset_id}"
                    ),
                },
            }
        )
    return files


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def fetch_blobtoolkit(
    output_dir: str,
    root: str = "Eukaryota",
    min_records: int = 1,
) -> tuple[int, int]:
    """Fetch BlobToolKit data and write TSV + files YAML.

    Uses a persistent session with connection pooling for the many API calls.

    Args:
        output_dir (str): Directory to write btk.tsv.gz and btk.files.yaml.
        root (str): Taxonomic root to query.
        min_records (int): Minimum dataset count to accept.

    Returns:
        tuple[int, int]: Number of dataset rows and file entries written.
    """
    session = _build_session()
    print(f"Fetching BlobToolKit datasets for {root}")
    datasets = _stream_datasets(root, session)
    print(f"Found {len(datasets)} datasets")

    if len(datasets) < min_records:
        raise RuntimeError(
            f"BlobToolKit returned fewer than {min_records} datasets: {len(datasets)}"
        )

    tsv_path = os.path.join(output_dir, "btk.tsv")
    gz_path = os.path.join(output_dir, "btk.tsv.gz")
    files_path = os.path.join(output_dir, "btk.files.yaml")

    all_rows = []
    all_files = []
    for dataset in datasets:
        meta = dataset if isinstance(dataset, dict) else {}
        row = _extract_stats(meta)
        all_rows.append(row)
        files = _describe_files(meta)
        all_files.extend(files)

    with open(tsv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=TSV_FIELDNAMES, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    with open(tsv_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        f_out.write(f_in.read())
    os.remove(tsv_path)

    import yaml

    with open(files_path, "w") as f:
        yaml.dump(all_files, f, default_flow_style=False)

    print(f"Wrote {len(all_rows)} rows to {gz_path}")
    print(f"Wrote {len(all_files)} file entries to {files_path}")
    return len(all_rows), len(all_files)


@task(log_prints=True)
def upload_s3_files(output_dir: str, s3_path: str) -> None:
    """Upload BTK output files to S3."""
    for filename in ("btk.tsv.gz", "btk.files.yaml"):
        local = os.path.join(output_dir, filename)
        remote = f"{s3_path.rstrip('/')}/{filename}"
        if os.path.exists(local):
            print(f"Uploading {local} to {remote}")
            upload_to_s3(local, remote)


@flow()
def update_blobtoolkit(
    output_path: str,
    s3_path: str = None,
    min_records: int = 0,
) -> bool:
    """Fetch BlobToolKit analysis data and optionally upload to S3.

    Args:
        output_path (str): Directory to write output files.
        s3_path (str): Optional S3 directory to upload results.
        min_records (int): Minimum dataset count to accept.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    os.makedirs(output_path, exist_ok=True)

    row_count, file_count = fetch_blobtoolkit(
        output_path, min_records=min_records
    )

    if s3_path:
        upload_s3_files(output_path, s3_path)

    emit_event(
        event="update.blobtoolkit.finished",
        resource={
            "prefect.resource.id": f"update.btk.{output_path}",
            "prefect.resource.type": "blobtoolkit",
        },
        payload={"row_count": row_count, "file_count": file_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, MIN_RECORDS],
        "Fetch BlobToolKit analysis data.",
    )
    update_blobtoolkit(**vars(args))
