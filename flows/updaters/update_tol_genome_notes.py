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


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def connect_to_portal():
    """Connect to the ToL portal and retrieve the filtered set of genome notes.Get the subset of species that have a value for gn_date_published.
    This is a proxy for whether there is any genome note for a species"""
    print("Connecting to ToL portal...")
    prtl = portal()
    data_filter = DataSourceFilter()
    data_filter.and_ = {"gn_date_published": {"exists": {}}}
    return prtl.get_list("genome_note", object_filters=data_filter)


# Helper functions for complex field extraction
def get_published_status(genome_note, *args):
    """Return 'published' if genome note has an ID, otherwise empty string."""
    return "published" if genome_note.id else ""


def get_projects(genome_note, *args):
    """Return comma-separated list of project IDs associated with the genome note."""
    return ",".join(genome_note.gn_tolid.sts_sample_sts_project_union or [])


def get_published_status_project(genome_note, field_name=None):
    """
    Return 'published' if the given project acronym (derived from the suffix of the field name) is present
    in the genome note's associated projects.
    """
    if not field_name:
        return ""

    # Extract acronym from field name
    # e.g. sequencing_status_asg → asg
    project_acronym = field_name.split("_")[-1].lower()

    projects = genome_note.gn_tolid.sts_sample_sts_project_union or []
    projects = [p.lower() for p in projects]

    return "published" if project_acronym in projects else ""


def get_field_value(obj, field_spec, field_name=None):
    """Get value from object using string path or callable."""
    if not isinstance(field_spec, str):
        return field_spec(obj, field_name) if field_name else field_spec(obj)
    value = obj
    for attr in field_spec.split("."):
        value = getattr(value, attr)
    return value


@task(log_prints=True)
def fetch_tol_genome_notes(file_path: str, min_lines: int) -> int:

    fields = [
        {"name": "ncbi_taxon_id", "spec": "gn_species.id"},
        {"name": "doi", "spec": "id"},
        {"name": "gn_species_name", "spec": "gn_species_name"},
        {"name": "gn_assembly.id", "spec": "gn_assembly.id"},
        {"name": "gn_pmid", "spec": "gn_pmid"},
        {"name": "gn_date_published", "spec": "gn_date_published"},
        {"name": "gn_passed_pr", "spec": "gn_passed_pr"},
        {"name": "sequencing_status", "spec": get_published_status},
        {"name": "published", "spec": get_projects},
        {"name": "sequencing_status_aegis", "spec": get_published_status_project},
        {"name": "sequencing_status_asg", "spec": get_published_status_project},
        {"name": "sequencing_status_dtol", "spec": get_published_status_project},
        {"name": "sequencing_status_ergapi", "spec": get_published_status_project},
        {"name": "sequencing_status_psyche", "spec": get_published_status_project},
        {"name": "sequencing_status_vgp", "spec": get_published_status_project},
    ]

    header = [field["name"] for field in fields]
    filtered_set = connect_to_portal()

    line_count = 0
    print("Writing ToL genome notes to file...")
    with open(file_path, "w") as tsv_file:
        tsv_file.write("\t".join(header) + "\n")

        for genome_note in filtered_set:
            row = [
                str(get_field_value(genome_note, field["spec"], field["name"]) or "")
                for field in fields
            ]
            tsv_file.write("\t".join(row) + "\n")
            line_count += 1

    # If the file has less than min_lines lines, raise an error
    if line_count < min_lines:
        raise RuntimeError(
            f"File {file_path} has less than {min_lines} lines: {line_count}"
        )
    print(f"Finished writing ToL genome notes to file. Line count: {line_count}")
    # Return the line counts
    return line_count


@task(log_prints=True)
def upload_s3_tsv(local_path: str, s3_path: str) -> None:
    print(f"Uploading updated ToL genome notes TSV file from {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_tol_genome_notes(output_path: str, s3_path: str, min_records: int) -> None:
    """Update the ToL genome notes TSV file."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    line_count = fetch_tol_genome_notes(output_path, min_records)
    if line_count >= min_records and s3_path:
        upload_s3_tsv(output_path, s3_path)
    emit_event(
        event="update.tol_genome_notes_expanded.tsv.finished",
        resource={
            "prefect.resource.id": f"update.tol_genome_notes.{output_path}",
            "prefect.resource.type": "tol.genome.notes",
        },
        payload={"line_count": line_count},
    )
    return True


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [required(OUTPUT_PATH), S3_PATH, MIN_RECORDS],
        "Fetch ToL genome notes data.",
    )

    update_tol_genome_notes(**vars(args))
