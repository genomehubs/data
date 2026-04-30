import csv
import gzip
import os
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, timedelta
from itertools import groupby

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    INPUT_PATH,
    MIN_RECORDS,
    OUTPUT_PATH,
    ROOT_TAXID,
    S3_PATH,
    default,
    parse_args,
    required,
)
from flows.lib.utils import is_safe_path, run_quoted, upload_to_s3


SRA_FIELDNAMES = [
    "taxon_id",
    "sra_accession",
    "run_accession",
    "library_source",
    "platform",
    "reads",
    "total_reads",
    "total_runs",
]


def _split_chunks(values, split_val):
    """Split an iterable into chunks at occurrences of split_val.

    Args:
        values: Iterable to split.
        split_val: Value at which to split.

    Yields:
        (int, group) pairs.
    """
    index = 0

    def chunk_index(val):
        nonlocal index
        if val == split_val:
            index += 1
        return index

    return groupby(values, chunk_index)


def _open_file(file_path, **kwargs):
    """Open a file, decompressing gzip if needed."""
    if file_path.endswith(".gz"):
        return gzip.open(file_path, "rt", encoding="utf8", **kwargs)
    return open(file_path, "r", encoding="utf8", **kwargs)


def _read_exp_xml(node, obj):
    """Extract fields from an ExpXml element."""
    for child in node:
        tag = child.tag
        if tag == "Bioproject":
            obj["bioproject"] = child.text
        elif tag == "Biosample":
            obj["biosample"] = child.text
        elif tag == "Organism":
            obj["taxon_id"] = child.get("taxid")
        elif tag == "Experiment":
            obj["sra_accession"] = child.get("acc")
        elif tag == "Summary":
            obj["platform"] = child.findtext("Platform") or ""
        elif tag == "Library_descriptor":
            source = child.findtext("LIBRARY_SOURCE")
            obj["library_source"] = source.lower() if source else ""


def _read_runs(node, obj):
    """Extract run accessions and read counts from a Runs element."""
    if "runs" not in obj:
        obj["runs"] = []
    for child in node:
        obj["runs"].append(
            {"accession": child.get("acc"), "reads": child.get("total_spots", "0")}
        )


def parse_sra_xml(xml_file: str) -> list:
    """Parse an SRA efetch docsum XML file into row dicts.

    Args:
        xml_file (str): Path to the XML (or .xml.gz) file.

    Returns:
        list: List of dicts with taxon_id, sra_accession, runs, etc.
    """
    rows = []
    xml_header = '<?xml version="1.0" encoding="UTF-8" ?>\n'
    with _open_file(xml_file) as f:
        for _, doc in _split_chunks(f, xml_header):
            lines = list(doc)
            try:
                root = ET.fromstringlist(lines)
            except ET.ParseError:
                continue
            for doc_summary in root.iter("DocumentSummary"):
                obj = {"date": "", "runs": []}
                for child in doc_summary:
                    tag = child.tag
                    if tag == "CreateDate":
                        obj["date"] = child.text or ""
                    elif tag == "ExpXml":
                        _read_exp_xml(child, obj)
                    elif tag == "Runs":
                        _read_runs(child, obj)
                if "taxon_id" in obj and obj["runs"]:
                    rows.append(obj)
    return rows


def group_by_taxon(rows: list, grouped: dict = None) -> list:
    """Group SRA runs by taxon, keeping the 10 most recent per taxon.

    Args:
        rows (list): Parsed SRA row dicts with runs.
        grouped (dict): Optional existing grouped data to merge into.

    Returns:
        list: One dict per taxon with aggregated fields.
    """
    if not grouped:
        grouped = defaultdict(lambda: {"count": 0, "reads": 0, "runs": []})
    for obj in sorted(rows, key=lambda r: r.get("date", "")):
        taxon_id = obj.get("taxon_id")
        if not taxon_id:
            continue
        for run in obj.get("runs", []):
            try:
                reads = int(run["reads"])
            except (ValueError, TypeError):
                reads = 0
            row = {
                "sra_accession": obj.get("sra_accession", ""),
                "run_accession": run["accession"],
                "library_source": obj.get("library_source", ""),
                "platform": obj.get("platform", ""),
                "reads": reads,
            }
            grouped[taxon_id]["runs"].insert(0, row)
            grouped[taxon_id]["count"] += 1
            grouped[taxon_id]["reads"] += reads
            if len(grouped[taxon_id]["runs"]) > 10:
                grouped[taxon_id]["runs"].pop()

    return [
        {
            "taxon_id": taxon_id,
            "sra_accession": ";".join(r["sra_accession"] for r in grp["runs"]),
            "run_accession": ";".join(r["run_accession"] for r in grp["runs"]),
            "library_source": ";".join(r["library_source"] for r in grp["runs"]),
            "platform": ";".join(r["platform"] for r in grp["runs"]),
            "reads": ";".join(str(r["reads"]) for r in grp["runs"]),
            "total_reads": grp["reads"],
            "total_runs": grp["count"],
        }
        for taxon_id, grp in grouped.items()
    ]


def load_previous_tsv(file_path: str) -> dict:
    """Load previously grouped SRA data from a TSV for incremental updates.

    Args:
        file_path (str): Path to the existing TSV (or .tsv.gz).

    Returns:
        dict: Grouped data keyed by taxon_id, or empty dict if file missing.
    """
    if not os.path.isfile(file_path):
        return {}
    grouped = defaultdict(lambda: {"count": 0, "reads": 0, "runs": []})
    with _open_file(file_path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            taxon_id = row["taxon_id"]
            grouped[taxon_id]["count"] = int(row["total_runs"])
            grouped[taxon_id]["reads"] = int(row["total_reads"])
            run_accs = row["run_accession"].split(";")
            sra_accs = row["sra_accession"].split(";")
            lib_srcs = row["library_source"].split(";")
            platforms = row["platform"].split(";")
            reads_list = row["reads"].split(";")
            for i, run_acc in enumerate(run_accs):
                grouped[taxon_id]["runs"].append(
                    {
                        "run_accession": run_acc,
                        "sra_accession": sra_accs[i] if i < len(sra_accs) else "",
                        "library_source": lib_srcs[i] if i < len(lib_srcs) else "",
                        "platform": platforms[i] if i < len(platforms) else "",
                        "reads": int(reads_list[i]) if i < len(reads_list) else 0,
                    }
                )
    return grouped


def _get_yesterday() -> str:
    """Return yesterday's date as YYYY/MM/DD."""
    return (date.today() - timedelta(days=1)).strftime("%Y/%m/%d")


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def fetch_sra_xml(
    output_xml: str,
    root_taxid: str = "2759",
    min_date: str = "2024/01/01",
) -> str:
    """Fetch SRA docsum XML from NCBI using esearch/efetch.

    Requires the NCBI Entrez Direct (edirect) tools and NCBI_API_KEY
    environment variable.

    Args:
        output_xml (str): Path to write the XML output.
        root_taxid (str): Root taxon ID to query.
        min_date (str): Start date for the query (YYYY/MM/DD).

    Returns:
        str: Path to the written XML file.
    """
    api_key = os.environ.get("NCBI_API_KEY", "")
    max_date = _get_yesterday()

    query = f"(txid{root_taxid}[organism:exp])"
    esearch_cmd = [
        "esearch", "-db", "sra", "-query", query,
    ]
    if api_key:
        esearch_cmd.extend(["-api_key", api_key])
    esearch_cmd.extend(["-mindate", min_date, "-maxdate", max_date])

    efetch_cmd = ["efetch", "-db", "sra", "-format", "docsum"]
    if api_key:
        efetch_cmd.extend(["-api_key", api_key])

    print(f"Running esearch | efetch for taxid {root_taxid} ({min_date} to {max_date})")
    esearch = run_quoted(esearch_cmd, capture_output=True, text=True, timeout=300)
    if esearch.returncode != 0:
        raise RuntimeError(f"esearch failed: {esearch.stderr}")

    with open(output_xml, "w") as f:
        efetch = run_quoted(
            efetch_cmd, input=esearch.stdout, capture_output=True, text=True, timeout=600
        )
        if efetch.returncode != 0:
            raise RuntimeError(f"efetch failed: {efetch.stderr}")
        f.write(efetch.stdout)

    print(f"Wrote SRA XML to {output_xml}")
    return output_xml


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def parse_and_write_sra(
    xml_path: str,
    output_path: str,
    previous_path: str = None,
) -> int:
    """Parse SRA XML and write grouped TSV.

    Args:
        xml_path (str): Path to the SRA docsum XML.
        output_path (str): Path to write the output TSV.
        previous_path (str): Optional path to previous TSV for incremental merge.

    Returns:
        int: Number of taxon rows written.
    """
    previous = load_previous_tsv(previous_path) if previous_path else {}
    rows = parse_sra_xml(xml_path)
    print(f"Parsed {len(rows)} records from XML")

    grouped_rows = group_by_taxon(rows, grouped=previous)
    print(f"Grouped into {len(grouped_rows)} taxa")

    tsv_path = output_path.removesuffix(".gz")
    with open(tsv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=SRA_FIELDNAMES, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for row in grouped_rows:
            writer.writerow(row)

    if output_path.endswith(".gz"):
        with open(tsv_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
            f_out.write(f_in.read())
        os.remove(tsv_path)

    print(f"Wrote {len(grouped_rows)} taxon rows to {output_path}")
    return len(grouped_rows)


@task(log_prints=True)
def upload_s3_file(local_path: str, s3_path: str) -> None:
    """Upload file to S3."""
    print(f"Uploading {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_sra_data(
    output_path: str,
    input_path: str = None,
    root_taxid: str = "9612",
    s3_path: str = None,
    min_records: int = 0,
) -> bool:
    """Fetch and parse SRA data, writing grouped TSV output.

    If input_path is provided, parses that XML file directly. Otherwise
    fetches fresh data from NCBI using esearch/efetch.

    Args:
        output_path (str): Path to write the output TSV (or .tsv.gz).
        input_path (str): Optional path to an existing SRA XML file.
        root_taxid (str): Root taxon ID for the NCBI query.
        s3_path (str): Optional S3 path to upload the result.
        min_records (int): Minimum taxon count to accept the output.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    resolved_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(resolved_path), exist_ok=True)

    if input_path and os.path.isfile(input_path):
        xml_path = input_path
    else:
        xml_path = f"{resolved_path}.xml"
        fetch_sra_xml(xml_path, root_taxid=root_taxid)

    row_count = parse_and_write_sra(xml_path, resolved_path)

    if row_count < min_records:
        raise RuntimeError(
            f"SRA output has fewer than {min_records} taxa: {row_count}"
        )

    if s3_path:
        upload_s3_file(output_path, s3_path)

    emit_event(
        event="update.sra.data.finished",
        resource={
            "prefect.resource.id": f"update.sra.{output_path}",
            "prefect.resource.type": "sra.data",
        },
        payload={"row_count": row_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [
            required(OUTPUT_PATH),
            INPUT_PATH,
            default(ROOT_TAXID, "9612"),
            S3_PATH,
            MIN_RECORDS,
        ],
        "Fetch and parse SRA data into grouped TSV.",
    )
    update_sra_data(**vars(args))
