import os

from tol.core import DataSourceFilter
from tol.sources.portal import portal

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    MIN_RECORDS,
    OUTPUT_PATH,
    S3_PATH,
    parse_args,
    required,
)
from flows.lib.utils import upload_to_s3

PROJECT_LIST = ["AEGIS", "ASG", "BAT1K", "DTOL", "ERGA", "ERGAPI", "PSYCHE", "VGP"]

ORDERED_MILESTONES = [
    "sample_collected",
    "sample_acquired",
    "data_generation",
    "in_assembly",
    "submitted",
    "published",
]


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def connect_to_portal():
    """Connect to the ToL portal and retrieve the filtered set of species approved for shipping.
    Get the subset of species that have a value for sts_sample_sts_accept_date_min,
    which is the date of the earliest accepted sample for that species.
    This is a proxy for whether there is any sample data for that species in the portal (= sample_collected),
    and therefore whether it is likely to be included in the declared status lists.
    This is a proxy for whether a species has been physically collected and the manifest is approved.
    """
    print("Connecting to ToL portal...")
    prtl = portal()
    data_filter = DataSourceFilter()
    data_filter.and_ = {"sts_sample_sts_accept_date_min": {"exists": {}}}
    return prtl.get_list("species", object_filters=data_filter)


# -----------------------------
# Helper functions for complex field extraction and status mapping
# -----------------------------


def get_projects(species, *args):
    """Return comma-separated list of project acronyms associated with the species."""
    return ",".join(species.sts_sample_sts_project_union or [])


def get_gals(species, *args):
    """Return comma-separated list of GAL names associated with the species."""
    return ",".join(species.sts_sample_sts_gal_name_union or [])


def get_collected_status(species, *args):
    """Return 'sample_collected' if species has an ID, otherwise empty string."""
    return "sample_collected" if species.sts_sample_sts_accept_date_min else ""


def get_acquired_status(species, *args):
    """Return 'sample_acquired' if species has a received date, otherwise empty string."""
    return "sample_acquired" if species.sts_sample_sts_receive_date_min else ""


def get_in_the_lab_status(species, *args):
    """Return 'data_generation' if species has a  date of active lab work, otherwise empty string."""
    return (
        "data_generation"
        if species.benchling_tissue_prep_benchling_sampleprep_date_min
        else ""
    )


def map_tola_status(species, *args):
    """Map detailed TOLA status to simplified GoaT sequencing status."""
    status_to_map = {
        "1 submitted": "submitted",
        "2 curated": "in_assembly",
        "3 curation": "in_assembly",
        "4 data complete": "in_assembly",
        "5 data issue": "data_generation",
        "6 data generation": "data_generation",
        "7 ignore": "data_generation",
    }

    return status_to_map.get(
        species.informatics_tolid_informatics_status_summary_min, ""
    )


def get_resample_status(species, *args):
    """Return project acronyms if species is flagged for recollection, otherwise empty string."""
    return (
        get_projects(species)
        if species.sts_sequencing_material_status == "RECOLLECTION_REQUIRED"
        else ""
    )


def pick_latest_status(species, *args):
    """Determine the latest sequencing status for a species based on multiple fields."""

    possible_status_rank = {
        status: rank for rank, status in enumerate(ORDERED_MILESTONES, start=1)
    }

    statuses = [
        get_collected_status(species),
        get_acquired_status(species),
        get_in_the_lab_status(species),
        map_tola_status(species),
    ]

    latest_status = ""
    highest_rank = 0

    for status in statuses:
        rank = possible_status_rank.get(status, 0)
        if rank > highest_rank:
            highest_rank = rank
            latest_status = status

    return latest_status


def get_project_and_milestones(species):
    """Return a dictionary of project milestones and per-project sequencing status for a species."""

    possible_projects = PROJECT_LIST

    projects = [p.upper() for p in (species.sts_sample_sts_project_union or [])]
    project_string = ",".join(projects)

    latest_status = pick_latest_status(species)

    status_order = ORDERED_MILESTONES

    # --- Milestones ---
    projects_in_milestone = {}

    if latest_status in status_order:
        max_index = status_order.index(latest_status)
        for index, status in enumerate(status_order):
            projects_in_milestone[status] = project_string if index <= max_index else ""
    else:
        projects_in_milestone = {status: "" for status in status_order}

    # --- Per-project sequencing status ---
    project_latest_status = {}

    for project in possible_projects:
        field = f"sequencing_status_{project.lower()}"
        project_latest_status[field] = latest_status if project in projects else ""

    return projects_in_milestone, project_latest_status


def get_field_value(obj, field_spec):
    if callable(field_spec):
        return field_spec(obj)
    value = obj
    for attr in field_spec.split("."):
        value = getattr(value, attr)
    return value


# -----------------------------
# Main task
# -----------------------------


@task(log_prints=True)
def fetch_tol_portal_status(file_path: str, min_lines: int) -> int:

    fields = [
        {"name": "ncbi_taxon_id", "spec": "id"},
        {"name": "scientific_name", "spec": "sts_scientific_name"},
        {"name": "family", "spec": "sts_family"},
        {"name": "tolid", "spec": "tolid_prefix"},
        {"name": "long_list", "spec": get_projects},
        {"name": "other_priority", "spec": get_projects},
        {
            "name": "tola_summary_status",
            "spec": "informatics_tolid_informatics_status_summary_min",
        },
        {
            "name": "tola_detailed_status",
            "spec": "informatics_tolid_informatics_status_min",
        },
        {"name": "tola_translated_status", "spec": map_tola_status},
        {"name": "recollection_needed", "spec": get_resample_status},
        {"name": "sample_collected_by", "spec": get_gals},
        {"name": "latest_sequencing_status", "spec": pick_latest_status},
    ]

    milestone_headers = ORDERED_MILESTONES

    status_project_headers = [
        f"sequencing_status_{project.lower()}" for project in PROJECT_LIST
    ]

    header = (
        [field["name"] for field in fields] + milestone_headers + status_project_headers
    )

    filtered_set = connect_to_portal()

    line_count = 0
    print("Writing ToL Portal project data to file...")

    with open(file_path, "w") as tsv_file:
        tsv_file.write("\t".join(header) + "\n")

        for species in filtered_set:
            sts_values = [
                str(get_field_value(species, field["spec"]) or "") for field in fields
            ]

            milestones, project_statuses = get_project_and_milestones(species)

            milestone_values = [
                milestones.get(milestone, "") for milestone in milestone_headers
            ]
            status_project_values = [
                project_statuses.get(project_status, "")
                for project_status in status_project_headers
            ]

            tsv_file.write(
                "\t".join(sts_values)
                + "\t"
                + "\t".join(milestone_values)
                + "\t"
                + "\t".join(status_project_values)
                + "\n"
            )

            line_count += 1

    if line_count < min_lines:
        raise RuntimeError(
            f"File {file_path} has less than {min_lines} lines: {line_count}"
        )

    print(f"Finished writing TSV file. Line count: {line_count}")
    return line_count


@task(log_prints=True)
def upload_s3_tsv(local_path: str, s3_path: str) -> None:
    print(f"Uploading file from {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_tol_portal_status(output_path: str, s3_path: str, min_records: int) -> None:
    """Update the ToL Portal Project Status TSV file."""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    line_count = fetch_tol_portal_status(output_path, min_records)

    if line_count >= min_records and s3_path:
        upload_s3_tsv(output_path, s3_path)

    emit_event(
        event="update.tol_portal_project.tsv.finished",
        resource={
            "prefect.resource.id": f"update.tol_portal_project.{output_path}",
            "prefect.resource.type": "tol.portal.project",
        },
        payload={"line_count": line_count},
    )


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, MIN_RECORDS],
        "Fetch ToL Portal Project Status data.",
    )

    update_tol_portal_status(**vars(args))
