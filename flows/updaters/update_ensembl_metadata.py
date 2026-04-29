import csv
import gzip
import json
import os
from enum import Enum

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import OUTPUT_PATH, S3_PATH, parse_args, required
from flows.lib.utils import is_safe_path, safe_get, upload_to_s3


class EnsemblDivision(Enum):
    """Supported Ensembl genome database divisions."""

    FUNGI = "fungi"
    METAZOA = "metazoa"
    PLANTS = "plants"
    PROTISTS = "protists"
    RAPID = "rapid"
    VERTEBRATES = "vertebrates"


DIVISION_URLS = {
    EnsemblDivision.FUNGI: (
        "http://ftp.ensemblgenomes.org/pub/current/fungi/"
        "species_metadata_EnsemblFungi.json"
    ),
    EnsemblDivision.METAZOA: (
        "http://ftp.ensemblgenomes.org/pub/current/metazoa/"
        "species_metadata_EnsemblMetazoa.json"
    ),
    EnsemblDivision.PLANTS: (
        "http://ftp.ensemblgenomes.org/pub/current/plants/"
        "species_metadata_EnsemblPlants.json"
    ),
    EnsemblDivision.PROTISTS: (
        "http://ftp.ensemblgenomes.org/pub/current/protists/"
        "species_metadata_EnsemblProtists.json"
    ),
    EnsemblDivision.RAPID: (
        "https://ftp.ensembl.org/pub/rapid-release/"
        "species_metadata.json"
    ),
    EnsemblDivision.VERTEBRATES: (
        "https://ftp.ensembl.org/pub/current/"
        "species_metadata_EnsemblVertebrates.json"
    ),
}

DIVISION_OUTPUT_NAMES = {
    EnsemblDivision.FUNGI: "species_metadata_EnsemblFungi.tsv.gz",
    EnsemblDivision.METAZOA: "species_metadata_EnsemblMetazoa.tsv.gz",
    EnsemblDivision.PLANTS: "species_metadata_EnsemblPlants.tsv.gz",
    EnsemblDivision.PROTISTS: "species_metadata_EnsemblProtists.tsv.gz",
    EnsemblDivision.RAPID: "species_metadata_EnsemblRapid.tsv.gz",
    EnsemblDivision.VERTEBRATES: "species_metadata_EnsemblVertebrates.tsv.gz",
}


def _extract_fields(record: dict, division: EnsemblDivision) -> list:
    """Extract TSV fields from a single Ensembl metadata JSON record.

    Different divisions use slightly different JSON structures for the
    same conceptual fields. This normalises them to a common 5-column
    format: assembly_accession, name, release_date, strain, taxonomy_id.

    Args:
        record (dict): A single species metadata JSON object.
        division (EnsemblDivision): The Ensembl division.

    Returns:
        list: A list of 5 string values, or None if the record is invalid.
    """
    if division == EnsemblDivision.RAPID:
        accession = record.get("assembly_accession", "")
        name = record.get("ensembl_production_name", "")
        release_date = record.get("release_date", "")
        strain = record.get("strain", "")
        taxonomy_id = str(record.get("taxonomy_id", ""))
    elif division == EnsemblDivision.VERTEBRATES:
        assembly = record.get("assembly", {})
        organism = record.get("organism", {})
        accession = assembly.get("assembly_accession", "")
        name = organism.get("url_name", "")
        release_date = record.get("release_date", "")
        strain = organism.get("strain", "")
        taxonomy_id = str(record.get("taxonomy_id", ""))
    else:
        organism = record.get("organism", {})
        accession = record.get("assembly_accession", "")
        name = organism.get("url_name", "")
        release_date = record.get("release_date", "")
        strain = organism.get("strain", "")
        taxonomy_id = str(record.get("taxonomy_id", ""))
    if not accession:
        return None
    return [accession, name, release_date, strain, taxonomy_id]


TSV_HEADERS = [
    "assembly_accession",
    "name",
    "release_date",
    "strain",
    "taxonomy_id",
]


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def fetch_ensembl_division(
    division: EnsemblDivision,
    output_dir: str,
) -> tuple[str, int]:
    """Fetch Ensembl species metadata JSON and convert to gzipped TSV.

    Args:
        division (EnsemblDivision): Ensembl division to fetch.
        output_dir (str): Directory to write the output file.

    Returns:
        tuple[str, int]: Path to the output file and number of records written.
    """
    url = DIVISION_URLS[division]
    output_name = DIVISION_OUTPUT_NAMES[division]
    output_path = os.path.join(output_dir, output_name)

    print(f"Fetching Ensembl {division.value} from {url}")
    response = safe_get(url, timeout=600)
    response.raise_for_status()

    records = response.json()
    if not isinstance(records, list):
        raise ValueError(
            f"Expected JSON array from {url}, got {type(records).__name__}"
        )

    tsv_path = output_path.removesuffix(".gz")
    row_count = 0
    with open(tsv_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(TSV_HEADERS)
        for record in records:
            row = _extract_fields(record, division)
            if row is not None:
                writer.writerow(row)
                row_count += 1

    with open(tsv_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
        f_out.write(f_in.read())
    os.remove(tsv_path)

    print(f"Wrote {row_count} records to {output_path}")
    return output_path, row_count


@task(log_prints=True)
def upload_s3_file(local_path: str, s3_path: str) -> None:
    """Upload file to S3."""
    print(f"Uploading {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_ensembl_metadata(
    output_path: str,
    division: str = "vertebrates",
    s3_path: str = None,
) -> bool:
    """Fetch Ensembl species metadata for a given division.

    Args:
        output_path (str): Directory to write output files.
        division (str): Ensembl division name (fungi, metazoa, plants,
            protists, rapid, vertebrates).
        s3_path (str): Optional S3 directory to upload the result.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    os.makedirs(output_path, exist_ok=True)

    div = EnsemblDivision(division.lower())
    local_file, row_count = fetch_ensembl_division(div, output_path)

    if s3_path:
        output_name = DIVISION_OUTPUT_NAMES[div]
        remote_path = f"{s3_path.rstrip('/')}/{output_name}"
        upload_s3_file(local_file, remote_path)

    emit_event(
        event="update.ensembl.metadata.finished",
        resource={
            "prefect.resource.id": f"update.ensembl.{division}.{output_path}",
            "prefect.resource.type": "ensembl.metadata",
            "prefect.resource.division": division,
        },
        payload={"division": division, "row_count": row_count},
    )
    return True


if __name__ == "__main__":
    DIVISION = {
        "flags": ["--division"],
        "keys": {
            "help": "Ensembl division (fungi, metazoa, plants, protists, rapid, vertebrates).",
            "type": str,
            "default": "vertebrates",
        },
    }
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, DIVISION],
        "Fetch Ensembl species metadata for a given division.",
    )
    update_ensembl_metadata(**vars(args))
