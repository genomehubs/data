import contextlib
import csv
import gzip
import os
import re
import tempfile
from collections import Counter

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import (
    MIN_RECORDS,
    OUTPUT_PATH,
    ROOT_TAXID,
    S3_PATH,
    default,
    parse_args,
    required,
)
from flows.lib.utils import is_safe_path, safe_get, upload_to_s3

REFSEQ_FTP = "https://ftp.ncbi.nlm.nih.gov/refseq/release"

ORGANELLE_FIELDNAMES = [
    "id",
    "organelle",
    "taxonId",
    "genbankAccession",
    "assemblySpan",
    "gcPercent",
    "nPercent",
    "releaseDate",
    "sourceAuthor",
    "sourceYear",
    "sourceTitle",
    "pubmedId",
    "bioproject",
    "biosample",
    "sampleLocation",
]

MONTHS = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _reformat_date(date_str: str) -> str:
    """Convert DD-MMM-YYYY to YYYY-MM-DD."""
    parts = re.split(r"[:\-]", date_str)
    if len(parts) < 3:
        return date_str
    return f"{parts[2]}-{MONTHS.get(parts[1].upper(), '00')}-{parts[0].zfill(2)}"


def _refseq_listing(collection: str) -> list:
    """Fetch directory listing of GenBank files for a RefSeq collection.

    Args:
        collection (str): Collection name (e.g., "mitochondrion").

    Returns:
        list: URLs of .genomic.gbff.gz files.
    """
    pattern = re.compile(r"(\w+\.\d+\.genomic\.gbff\.gz)")
    url = f"{REFSEQ_FTP}/{collection}"
    response = safe_get(url, timeout=120)
    response.raise_for_status()
    return [
        f"{url}/{match[1]}"
        for line in response.text.split("\n")
        if (match := pattern.search(line))
    ]


def _parse_features(entry, fields: dict) -> None:
    """Extract taxonId and sample location from SeqRecord features."""
    qualifiers = entry.features[0].qualifiers
    if "db_xref" in qualifiers:
        for xref in qualifiers["db_xref"]:
            key, value = xref.split(":", 1)
            if key == "taxon":
                fields["taxonId"] = value
    if "lat_lon" in qualifiers:
        fields["sampleLocation"] = qualifiers["lat_lon"][0]


def _parse_references(entry, fields: dict) -> None:
    """Extract reference metadata from SeqRecord annotations."""
    submitted_re = re.compile(r"Submitted\s\(\d{2}-\w{3}-(\d{4})\)")
    published_re = re.compile(r"\s\((\d{4})\)[^(]*$")
    for ref in entry.annotations.get("references", []):
        if ref.journal == "Unpublished":
            continue
        if ref.journal.startswith("Submitted"):
            if "sourceAuthor" in fields:
                continue
            match = submitted_re.search(ref.journal)
            if match:
                fields["sourceYear"] = match[1]
        elif "sourceAuthor" in fields:
            continue
        else:
            match = published_re.search(ref.journal)
            if match:
                fields["sourceYear"] = match[1]
            if ref.title:
                fields["sourceTitle"] = ref.title
            if ref.pubmed_id:
                fields["pubmedId"] = ref.pubmed_id
        if ref.authors:
            fields["sourceAuthor"] = ref.authors
        elif ref.consrtm:
            fields["sourceAuthor"] = ref.consrtm


def _parse_xrefs(entry, fields: dict) -> None:
    """Extract BioProject/BioSample cross-references."""
    if not entry.dbxrefs:
        return
    bioprojects = []
    biosamples = []
    for dbxref in entry.dbxrefs:
        with contextlib.suppress(ValueError):
            key, value = dbxref.split(":", 1)
            if key == "BioProject":
                bioprojects.append(value)
            elif key == "BioSample":
                biosamples.append(value)
    if bioprojects:
        fields["bioproject"] = ";".join(bioprojects)
    if biosamples:
        fields["biosample"] = ";".join(biosamples)


def _parse_sequence(entry, fields: dict) -> bool:
    """Compute sequence stats (GC%, N%, span). Returns False if all Ns."""
    seqstr = str(entry.seq.upper())
    counter = Counter(seqstr)
    length = len(seqstr)
    n_pct = counter["N"] / length * 100 if length > 0 else 100
    fields["nPercent"] = f"{n_pct:.2f}"
    if n_pct == 100:
        return False
    gc = counter["G"] + counter["C"]
    at = counter["A"] + counter["T"]
    fields["gcPercent"] = f"{gc / (gc + at) * 100:.2f}" if (gc + at) > 0 else "0.00"
    fields["assemblySpan"] = str(length)
    return True


def _parse_flatfile(flatfile_path: str, organelle: str, root_taxon: str = None) -> list:
    """Parse a single GenBank flatfile for organelle sequences.

    Args:
        flatfile_path (str): Path to a gzipped GenBank file.
        organelle (str): Organelle type ("mitochondrion" or "plastid").
        root_taxon (str): Optional taxonomic root to filter by.

    Returns:
        list: List of row dicts.
    """
    from Bio import SeqIO

    comment_re = re.compile(
        r"(?:derived|identical)\s(?:from|to)\s([\w\d]+).*COMPLETENESS: full length",
        re.DOTALL,
    )
    rows = []
    with gzip.open(flatfile_path, "rt") as fh:
        for entry in SeqIO.parse(fh, "gb"):
            if root_taxon and root_taxon not in entry.annotations.get("taxonomy", []):
                continue
            fields = {"id": entry.id, "organelle": organelle}
            comment = entry.annotations.get("comment", "")
            if comment:
                match = comment_re.search(comment)
                if match:
                    fields["genbankAccession"] = match[1]
                else:
                    continue
            _parse_features(entry, fields)
            _parse_references(entry, fields)
            fields["releaseDate"] = _reformat_date(entry.annotations.get("date", ""))
            _parse_xrefs(entry, fields)
            try:
                if not _parse_sequence(entry, fields):
                    continue
            except Exception:
                continue
            rows.append(fields)
    return rows


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def fetch_and_parse_organelles(
    output_path: str,
    organelles: list = None,
    root_taxon: str = None,
) -> int:
    """Fetch RefSeq organelle data and parse to gzipped TSV.

    Downloads GenBank flatfiles from NCBI FTP for each organelle type,
    parses sequence records, and writes a combined TSV.

    Args:
        output_path (str): Path to write the output TSV (or .tsv.gz).
        organelles (list): List of organelle types to parse.
        root_taxon (str): Optional taxonomic root filter.

    Returns:
        int: Number of rows written.
    """
    if organelles is None:
        organelles = ["mitochondrion", "plastid"]

    all_rows = []
    for organelle in organelles:
        print(f"Fetching listing for {organelle}")
        listing = _refseq_listing(organelle)
        print(f"Found {len(listing)} files for {organelle}")

        for url in listing:
            print(f"Downloading {url}")
            response = safe_get(url, timeout=600)
            response.raise_for_status()

            with tempfile.NamedTemporaryFile(suffix=".gbff.gz", delete=False) as tmp:
                tmp.write(response.content)
                tmp_path = tmp.name

            try:
                rows = _parse_flatfile(tmp_path, organelle, root_taxon)
                all_rows.extend(rows)
                print(f"Parsed {len(rows)} records from {os.path.basename(url)}")
            finally:
                os.unlink(tmp_path)

    tsv_path = output_path.removesuffix(".gz")
    with open(tsv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=ORGANELLE_FIELDNAMES,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    if output_path.endswith(".gz"):
        with open(tsv_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
            f_out.write(f_in.read())
        os.remove(tsv_path)

    print(f"Wrote {len(all_rows)} total organelle records to {output_path}")
    return len(all_rows)


@task(log_prints=True)
def upload_s3_file(local_path: str, s3_path: str) -> None:
    """Upload file to S3."""
    print(f"Uploading {local_path} to {s3_path}")
    upload_to_s3(local_path, s3_path)


@flow()
def update_refseq_organelles(
    output_path: str,
    root_taxid: str = None,
    s3_path: str = None,
    min_records: int = 0,
) -> bool:
    """Fetch and parse RefSeq organelle data.

    Args:
        output_path (str): Path to write the output TSV.
        root_taxid (str): Optional root taxon filter.
        s3_path (str): Optional S3 path to upload the result.
        min_records (int): Minimum record count to accept.

    Returns:
        bool: True on success.
    """
    if not is_safe_path(output_path):
        raise ValueError(f"Unsafe output path: {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    row_count = fetch_and_parse_organelles(
        output_path, root_taxon=root_taxid
    )

    if row_count < min_records:
        raise RuntimeError(
            f"RefSeq organelles: fewer than {min_records} records: {row_count}"
        )

    if s3_path:
        upload_s3_file(output_path, s3_path)

    emit_event(
        event="update.refseq.organelles.finished",
        resource={
            "prefect.resource.id": f"update.refseq.organelles.{output_path}",
            "prefect.resource.type": "refseq.organelles",
        },
        payload={"row_count": row_count},
    )
    return True


if __name__ == "__main__":
    args = parse_args(
        [required(OUTPUT_PATH), ROOT_TAXID, S3_PATH, MIN_RECORDS],
        "Fetch and parse RefSeq organelle data.",
    )
    update_refseq_organelles(**vars(args))
