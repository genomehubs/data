import csv
import os

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import MIN_RECORDS, OUTPUT_PATH, S3_PATH, parse_args, required
from flows.lib.utils import is_safe_path, safe_get, upload_to_s3

JGI_BASE_URL = "https://gold-ws.jgi.doe.gov"
JGI_STUDY_ID = "Gs0000001"

FIELDNAMES = [
    "projectGoldId",
    "projectName",
    "legacyGoldId",
    "studyGoldId",
    "biosampleGoldId",
    "organismGoldId",
    "itsProposalId",
    "itsSpid",
    "itsSampleId",
    "pmoProjectId",
    "gptsProposalId",
    "ncbiBioProjectAccession",
    "ncbiBioSampleAccession",
    "projectStatus",
    "sequencingStatus",
    "jgiFundingProgram",
    "jgiFundingYear",
    "hmpId",
    "modDate",
    "addDate",
    "sequencingStrategy",
    "sequencingCenters",
    "seqMethod",
    "genomePublications",
    "otherPublications",
    "sraExperimentIds",
    "ncbiTaxId",
]


def _exchange_token(offline_token: str) -> str:
    """Exchange a JGI offline token for an access token.

    Args:
        offline_token (str): The JGI offline (API) token.

    Returns:
        str: A valid access token.

    Raises:
        RuntimeError: If the token exchange fails.
    """
    url = f"{JGI_BASE_URL}/exchange?offlineToken={offline_token}"
    response = safe_get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"JGI token exchange failed: HTTP {response.status_code} — "
            f"check that JGI_OFFLINE_TOKEN is valid"
        )
    token = response.content.decode().strip()
    if not token:
        raise RuntimeError("JGI token exchange returned empty access token")
    return token


def _fetch_organisms(access_token: str) -> dict:
    """Fetch organism-to-taxid mapping from JGI GOLD API.

    Args:
        access_token (str): Valid JGI access token.

    Returns:
        dict: Mapping of organismGoldId to ncbiTaxId.
    """
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = f"{JGI_BASE_URL}/api/v1/organisms?studyGoldId={JGI_STUDY_ID}"
    response = safe_get(url, headers=headers, timeout=120)
    response.raise_for_status()
    organisms = response.json()
    return {org["organismGoldId"]: org.get("ncbiTaxId", "") for org in organisms}


def _fetch_projects(access_token: str) -> list:
    """Fetch project records from JGI GOLD API.

    Args:
        access_token (str): Valid JGI access token.

    Returns:
        list: List of project dictionaries.
    """
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = f"{JGI_BASE_URL}/api/v1/projects?studyGoldId={JGI_STUDY_ID}"
    response = safe_get(url, headers=headers, timeout=120)
    response.raise_for_status()
    return response.json()


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def fetch_jgi_tsv(file_path: str, min_lines: int = 1) -> int:
    """Fetch JGI 1KFG project data and write to TSV.

    Exchanges the offline token for an access token, fetches organism-taxid
    mapping and project records, filters for whole genome sequencing projects,
    and writes a TSV.

    Args:
        file_path (str): Path to the output TSV file.
        min_lines (int): Minimum number of data rows expected.

    Returns:
        int: Number of lines written (including header).
    """
    offline_token = os.environ.get("JGI_OFFLINE_TOKEN")
    if not offline_token:
        raise RuntimeError(
            "JGI_OFFLINE_TOKEN environment variable is not set — "
            "cannot authenticate with JGI GOLD API"
        )

    print("Exchanging JGI offline token for access token")
    access_token = _exchange_token(offline_token)

    print(f"Fetching organisms for study {JGI_STUDY_ID}")
    org_to_taxid = _fetch_organisms(access_token)
    print(f"Found {len(org_to_taxid)} organisms")

    print(f"Fetching projects for study {JGI_STUDY_ID}")
    projects = _fetch_projects(access_token)
    print(f"Found {len(projects)} total projects")

    source_fields = [f for f in FIELDNAMES if f != "ncbiTaxId"]
    row_count = 0
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(FIELDNAMES)
        for project in projects:
            if project.get("sequencingStrategy") != "Whole Genome Sequencing":
                continue
            organism_id = project.get("organismGoldId", "")
            taxid = org_to_taxid.get(organism_id, "")
            row = [project.get(field, "") for field in source_fields] + [taxid]
            writer.writerow(row)
            row_count += 1

    line_count = row_count + 1  # include header
    if row_count < min_lines:
        raise RuntimeError(
            f"JGI file has fewer than {min_lines} data rows: {row_count}"
        )
    print(f"Wrote {row_count} WGS projects to {file_path}")
    return line_count


@task(log_prints=True)
def upload_s3_tsv(local_path: str, s3_path: str) -> None:
    """Upload JGI TSV to S3."""
    print(f"Uploading JGI TSV from {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_jgi_status(
    output_path: str,
    s3_path: str = None,
    min_records: int = 0,
) -> bool:
    """Fetch JGI 1KFG status list and optionally upload to S3.

    Args:
        output_path (str): Path to the output TSV file.
        s3_path (str): Optional S3 path to upload the result.
        min_records (int): Minimum record count to accept the output.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    resolved_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(resolved_path), exist_ok=True)

    line_count = fetch_jgi_tsv(resolved_path, min_records)

    if line_count > min_records and s3_path:
        upload_s3_tsv(resolved_path, s3_path)

    emit_event(
        event="update.jgi.status.finished",
        resource={
            "prefect.resource.id": f"update.jgi.{output_path}",
            "prefect.resource.type": "jgi.status",
        },
        payload={"line_count": line_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, MIN_RECORDS],
        "Fetch JGI 1KFG status list.",
    )
    update_jgi_status(**vars(args))
