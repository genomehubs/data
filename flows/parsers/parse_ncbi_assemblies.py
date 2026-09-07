import json
import os
import re
import shutil
import subprocess
from collections import defaultdict
from glob import glob
from typing import Generator, Optional

from genomehubs import utils as gh_utils

from flows.lib import utils  # noqa: E402
from flows.lib.conditional_import import flow, run_count, task  # noqa: E402
from flows.lib.utils import Config, Parser, parse_s3_file  # noqa: E402
from flows.parsers.args import parse_args  # noqa: E402

# Roots that contain expensive sequence-derived data
SEQUENCE_DERIVED_ROOTS = {
    "processedAssemblyStats",
    "processedOrganelleInfo",
    "chromosomes",
    "organelles",
}

CANONICAL_RANKS = ["species", "genus", "family", "order", "class", "phylum", "kingdom"]


def _normalise_taxid(value) -> str | None:
    """Turn a taxid-like value into a stable string key."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return str(value)


def locate_taxonomy_lookup(work_dir: str) -> str:
    """Locate the blobtk taxonomy nodes.jsonl by convention.

    Search order:
      1. work_dir/nodes.jsonl
      2. work_dir/taxonomy/nodes.jsonl
      3. sibling taxonomy dirs under the parent (for example ../genomehubs-taxonomy/*/nodes.jsonl)

    Rules:
      - no file -> raise FileNotFoundError
      - one file -> return it
      - multiple files -> raise ValueError
    """
    work_dir = os.path.abspath(work_dir)
    candidates = []

    # Same directory
    candidates.extend(glob(os.path.join(work_dir, "nodes.jsonl")))

    # Same directory under taxonomy/
    candidates.extend(glob(os.path.join(work_dir, "taxonomy", "nodes.jsonl")))

    # Sibling taxonomy area, e.g. ../genomehubs-taxonomy/eukaryota/nodes.jsonl
    parent = os.path.dirname(work_dir)
    sibling_candidates = glob(
        os.path.join(parent, "genomehubs-taxonomy", "*", "nodes.jsonl"),
        recursive=True,
    )
    candidates.extend(sibling_candidates)

    # Generic sibling taxonomy dir
    candidates.extend(glob(os.path.join(parent, "taxonomy", "nodes.jsonl")))
    candidates.extend(glob(os.path.join(parent, "taxonomy", "*", "nodes.jsonl")))

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for path in candidates:
        real = os.path.abspath(path)
        if real not in seen:
            seen.add(real)
            unique.append(real)

    if not unique:
        raise FileNotFoundError(
            f"No taxonomy nodes.jsonl found near {work_dir}. "
            "Expected one of: work_dir/nodes.jsonl, work_dir/taxonomy/nodes.jsonl, "
            "or a sibling taxonomy dir such as ../genomehubs-taxonomy/*/nodes.jsonl"
        )

    if len(unique) > 1:
        raise ValueError("Multiple taxonomy nodes.jsonl candidates found: " + ", ".join(unique))

    return unique[0]


def load_taxonomy_lookup(nodes_jsonl_path: str) -> dict[str, dict[str, str]]:
    """Load blobtk taxonomy output into a taxid -> lineage lookup."""
    lookup: dict[str, dict[str, str]] = {}

    with open(nodes_jsonl_path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in taxonomy lookup file at line {line_no}: {exc}") from exc

            taxid = rec.get("taxon_id")
            if taxid is None:
                continue

            taxid_key = _normalise_taxid(taxid)
            if taxid_key is None:
                continue

            simple_lineage: dict[str, str] = {}

            lineage = rec.get("lineage", [])
            for ancestor in lineage:
                if not isinstance(ancestor, dict):
                    continue
                rank = ancestor.get("taxon_rank")
                if rank in CANONICAL_RANKS:
                    rank_value = ancestor.get("taxon_id")
                    simple_lineage[rank] = _normalise_taxid(rank_value) or "None"

            lookup[taxid_key] = simple_lineage

    return lookup


def enrich_assembly_row_with_taxonomy(row: dict, taxonomy_lookup: dict[str, dict[str, str]]) -> dict:
    """Attach genus/family/order/class/phylum/kingdom taxids to a parsed row.

    Expected row keys:
      - taxId / tax_id / taxon_id
    Output keys:
      - genusTaxId
      - familyTaxId
      - orderTaxId
      - classTaxId
      - phylumTaxId
      - kingdomTaxId
    """
    taxid_value = row.get("taxId") or row.get("tax_id") or row.get("taxon_id") or row.get("taxonId")
    if taxid_value is None:
        return row

    taxid_key = _normalise_taxid(taxid_value)
    if taxid_key is None:
        return row

    lineage = taxonomy_lookup.get(taxid_key) or {}
    for rank in CANONICAL_RANKS:
        row[f"{rank}TaxId"] = lineage.get(rank) or ""

    return row


def enrich_parsed_assemblies(parsed: dict, work_dir: str) -> dict:
    """Load the taxonomy lookup from a sibling convention and enrich the parsed assembly rows."""
    try:
        taxonomy_path = locate_taxonomy_lookup(work_dir)
    except (FileNotFoundError, ValueError) as e:
        print(f"Warning: {e}. Skipping taxonomy enrichment.")
        return parsed
    taxonomy_lookup = load_taxonomy_lookup(taxonomy_path)

    for row in parsed.values():
        enrich_assembly_row_with_taxonomy(row, taxonomy_lookup)

    return parsed


def parse_assembly_report(jsonl_path: str) -> Generator[dict, None, None]:
    """
    Parses an NCBI datasets JSONL file and yields each assembly report.

    Args:
        jsonl_path (str): The path to the JSONL file.

    Yields:
        dict: The assembly report data as a dictionary.
    """
    try:
        with open(jsonl_path, "r") as f:
            for line in f:
                yield utils.convert_keys_to_camel_case(json.loads(line))
    except Exception as e:
        raise RuntimeError(f"Error reading JSONL file: {e}") from e


def fetch_ncbi_datasets_sequences(accession: str, timeout: int = 30) -> Generator[dict, None, None]:
    """
    Fetches a sequence report from NCBI datasets for the given accession.

    Args:
        accession (str): The accession number to fetch the sequence report for.
        timeout (int): The number of seconds to wait for the command to complete.

    Yields:
        dict: The sequence report data as a JSON object, one line at a time.
    """
    if not utils.is_safe_path(accession):
        raise ValueError(f"Unsafe accession: {accession}")
    result = utils.run_quoted(
        [
            "datasets",
            "summary",
            "genome",
            "accession",
            accession,
            "--report",
            "sequence",
            "--as-json-lines",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Error fetching sequences report: {result.stderr}")
    for line in result.stdout.split("\n"):
        if not line:
            continue
        yield json.loads(line)


def is_atypical_assembly(report: dict, parsed: dict) -> bool:
    """
    Check if an assembly is atypical.

    Args:
        report (dict): A dictionary containing the assembly information.
        parsed (dict): A dictionary containing parsed data.

    Returns:
        bool: True if the assembly is atypical, False otherwise.
    """
    if "assemblyInfo" not in report:
        return True
    if report["assemblyInfo"].get("atypical", {}).get("isAtypical", False):
        # delete from parsed if present
        accession = report["accession"]
        if accession in parsed:
            del parsed[accession]
        return True
    return False


def process_assembly_report(
    report: dict,
    previous_report: Optional[dict],
    config: Config,
    parsed: dict,
    version_status: str = "current",
) -> dict:
    """Process assembly level information.

    This function takes a data dictionary and an optional previous_report dictionary,
    and updates the 'processedAssemblyInfo' field in the data dictionary with
    information about the assembly's RefSeq and GenBank accessions. It also checks if
    the current assembly is the primary assembly based on the 'refseqCategory' field in
    the 'assemblyInfo' dictionary.

    Args:
        report (dict): A dictionary containing the assembly information.
        previous_report (Optional[dict]): A dictionary containing previous assembly
        information, used to determine if the current assembly is the same as the
        previous one.
        config (Config): A Config object containing the configuration data.
        parsed (dict): A dictionary containing parsed data.
        version_status (str): Version status - "current" (default) or "superseded"
            for historical versions. Defaults to "current" to maintain backward
            compatibility with existing code.

    Returns:
        dict: The updated report dictionary.
    """
    # Uncomment to filter atypical assemblies
    # if is_atypical_assembly(report, parsed):
    #     return None
    processed_report = {**report, "processedAssemblyInfo": {"organelle": "nucleus", "versionStatus": version_status}}
    if "pairedAccession" in report:
        if processed_report["pairedAccession"].startswith("GCF_"):
            processed_report["processedAssemblyInfo"]["refseqAccession"] = report["pairedAccession"]
            processed_report["processedAssemblyInfo"]["genbankAccession"] = report["accession"]
        else:
            processed_report["processedAssemblyInfo"]["refseqAccession"] = report["accession"]
            processed_report["processedAssemblyInfo"]["genbankAccession"] = report["pairedAccession"]
    else:
        processed_report["processedAssemblyInfo"]["genbankAccession"] = report["accession"]
    if (
        previous_report
        and processed_report["processedAssemblyInfo"]["genbankAccession"]
        == previous_report["processedAssemblyInfo"]["genbankAccession"]
    ):
        processed_report = previous_report | processed_report
    if "refseqCategory" in processed_report.get("assemblyInfo", {}) or "refseqAccession" in processed_report.get(
        "processedAssemblyInfo", {}
    ):
        processed_report["processedAssemblyInfo"]["primaryValue"] = 1

    # Initialize assemblyId from genbankAccession (will be updated later if data_freeze is provided)
    processed_report["processedAssemblyInfo"]["assemblyId"] = processed_report["processedAssemblyInfo"][
        "genbankAccession"
    ]

    return processed_report


def snapshot_previous_output(config: Config) -> None:
    """Copy the prior-run output TSV to a `.previous` sibling before it is overwritten.

    parse_assembly_versions needs yesterday's assembly_current.tsv to find each newly
    superseded predecessor (v_n-1), but write_to_tsv overwrites the output file in
    place. fetch_previous_file_pair has already placed yesterday's published copy at
    config.meta["file_name"], so snapshotting it here preserves it across the overwrite,
    at the exact path the historical parser reads. No-op on the first run, when no prior
    output exists.

    Args:
        config (Config): Config whose meta["file_name"] is the output TSV path.
    """
    output_path = config.meta.get("file_name")
    if output_path and os.path.exists(output_path):
        previous_path = f"{output_path}.previous"
        shutil.copy2(output_path, previous_path)
        print(f"  Snapshotted previous output -> {previous_path}")


@task()
def write_to_tsv(parsed: dict, config: Config):
    """Write parsed data to a TSV file.

    Args:
        parsed (dict): A dictionary containing parsed data.
        config (Config): A Config object containing the configuration data.
    """
    if config.meta["file_name"].endswith(".gz"):
        config.meta["file_name"] = config.meta["file_name"][:-3]
        gh_utils.write_tsv(parsed, config.headers, config.meta)
        os.system(f"gzip -f {config.meta['file_name']}")
    else:
        gh_utils.write_tsv(parsed, config.headers, config.meta)


@task(log_prints=True, retries=2, retry_delay_seconds=2)
def fetch_and_parse_sequence_report(data: dict):
    """
    Processes the sequence report for an NCBI dataset, adding date fields for assemblies
    that meet certain metrics.

    Args:
        data (dict): A dictionary containing assembly statistics and information.

    Returns:
        None: This function modifies the `data` dictionary in-place to add the processed
            assembly statistics.
    """
    accession = data["accession"]
    span = int(data["assemblyStats"]["totalSequenceLength"])
    level = data["assemblyInfo"]["assemblyLevel"]
    if level in ["Contig", "Scaffold"]:
        return
    organelles: defaultdict[str, list] = defaultdict(list)
    try:
        chromosomes: list = []
        assigned_span = 0
        for seq in fetch_ncbi_datasets_sequences(accession, timeout=120 * (run_count + 1)):
            if utils.is_non_nuclear(seq):
                organelles[seq["chr_name"]].append(seq)
            elif utils.is_assigned_to_chromosome(seq):
                assigned_span += seq["length"]
                if utils.is_chromosome(seq):
                    chromosomes.append(seq)
    except subprocess.TimeoutExpired:
        print(f"ERROR: Timeout fetching sequence report for {accession}")
        return
    utils.add_organelle_entries(data, organelles)
    utils.check_ebp_criteria(data, span, chromosomes, assigned_span)
    utils.add_chromosome_entries(data, chromosomes)


def add_report_to_parsed_reports(parsed: dict, report: dict, config: Config, biosamples: dict):
    """
    Add the report to the parsed reports.

    Args:
        parsed (dict): A dictionary containing parsed data.
        report (dict): A dictionary containing the assembly report.
        config (Config): A Config object containing the configuration data.
        biosamples (dict): A dictionary containing biosample information.
    """
    accession = report["processedAssemblyInfo"]["genbankAccession"]
    row = gh_utils.parse_report_values(config.parse_fns, report)
    if accession not in parsed:
        utils.update_organelle_info(report, row)
    if "linkedAssembly" not in row or row["linkedAssembly"] is None:
        row["linkedAssembly"] = []
    biosample = row.get("biosampleAccession", [])
    if biosample not in biosamples:
        biosamples[biosample] = []
    if biosample:
        linked_assemblies = biosamples[biosample]
        for acc in linked_assemblies:
            if acc == accession:
                continue
            linked_row = parsed[acc]
            if accession not in linked_row["linkedAssembly"]:
                if not isinstance(linked_row["linkedAssembly"], list):
                    linked_row["linkedAssembly"] = []
                linked_row["linkedAssembly"].append(accession)
            if acc not in row["linkedAssembly"]:
                row["linkedAssembly"].append(acc)
        linked_assemblies.append(accession)
    parsed[accession] = row
    return parsed


def use_previous_report(processed_report: dict, parsed: dict, config: Config):
    """
    Reuse previous sequence-derived data when the assembly release date is unchanged.

    The raw releaseDate string is the authoritative guard in the current pipeline.
    If the assembly is the same release, we should not re-fetch sequence metadata
    just because the previous TSV was loaded under a different header set or because
    the previous row is missing some sequence-derived keys from the current schema.

    Args:
        processed_report (dict): A dictionary containing processed assembly data.
        parsed (dict): A dictionary containing parsed data.
        config (Config): A Config object containing the configuration data.

    Returns:
        bool: True if the accession is known and the releaseDate strings match,
              False otherwise.
    """
    accession = processed_report["processedAssemblyInfo"]["genbankAccession"]
    if accession not in config.previous_parsed:
        return False

    previous_report = config.previous_parsed[accession]
    current_release = processed_report.get("assemblyInfo", {}).get("releaseDate")
    previous_release = previous_report.get("releaseDate")
    return current_release == previous_release


@task()
def set_up_feature_file(config: Config):
    """
    Set up the feature file.

    Args:
        config (Config): A Config object containing the configuration data.
    """
    gh_utils.write_tsv({}, config.feature_headers, {"file_name": config.feature_file})


def append_features(processed_report: dict, config: Config):
    """
    Append features to the feature file.

    Args:
        processed_report (dict): A dictionary containing processed assembly data.
        config (Config): A Config object containing the configuration data.
    """
    if config.feature_file is not None and "chromosomes" in processed_report:
        utils.append_to_tsv(
            processed_report["chromosomes"],
            config.feature_headers,
            {"file_name": config.feature_file},
        )


@task(log_prints=True)
def set_representative_assemblies(parsed: dict, biosamples: dict):
    """
    Set the representative assembly for each biosample.

    Args:
        parsed (dict): A dictionary containing parsed data.
        biosamples (dict): A dictionary containing biosample information.
    """
    for accessions in biosamples.values():
        most_recent = None
        primary_assembly = None
        latest_date = None
        for accession in accessions:
            if accession not in parsed:
                continue
            row = parsed[accession]
            if most_recent is None or row["releaseDate"] > latest_date:
                most_recent = accession
                latest_date = row["releaseDate"]
            if row["refseqCategory"] is not None:
                primary_assembly = accession
        if primary_assembly is not None:
            parsed[primary_assembly]["biosampleRepresentative"] = 1
        elif most_recent is not None:
            parsed[most_recent]["biosampleRepresentative"] = 1


def _iter_config_paths(config: Config) -> Generator[str, None, None]:
    """Yield all 'path' values from the types config."""
    types_cfg = config.config or {}
    for section in ("attributes", "identifiers", "metadata", "taxonomy", "taxon_names"):
        for item in types_cfg.get(section, {}).values():
            if path := item.get("path"):
                yield path


def _is_sequence_derived(path: str) -> bool:
    """Check if path starts with a sequence-derived root."""
    root = path.split(".")[0].split("==")[0]
    return root in SEQUENCE_DERIVED_ROOTS


def get_cached_sequence_fields(processed_report: dict, config: Config) -> Optional[dict]:
    """Return cached sequence-derived field values when the releaseDate matches."""
    accession = processed_report["processedAssemblyInfo"]["genbankAccession"]
    if accession not in config.previous_parsed:
        return None

    previous_row = config.previous_parsed[accession]

    current_release = processed_report.get("assemblyInfo", {}).get("releaseDate")
    previous_release = previous_row.get("releaseDate")
    if current_release != previous_release:
        return None

    # Previous rows can be loaded under a different YAML header set. The cache
    # should still be considered valid for the same release date even when the
    # previous row has fewer sequence-derived keys than the current schema.
    types_cfg = config.config or {}
    keep_headers = set()

    for section in ("attributes", "identifiers", "metadata"):
        for item in types_cfg.get(section, {}).values():
            path = item.get("path", "")
            header = item.get("header", "")
            if header and any(path.startswith(f"{root}.") or path == root for root in SEQUENCE_DERIVED_ROOTS):
                keep_headers.add(header)

    cached = {}
    for header in keep_headers:
        value = previous_row.get(header)
        if value not in (None, ""):
            cached[header] = value
    return cached


@task()
def process_assembly_reports(
    jsonl_path: str,
    config: Config,
    biosamples: dict,
    parsed: dict,
    previous_report: Optional[dict] = None,
):
    """
    Process assembly reports and fetch sequence reports.

    Args:
        jsonl_path (str): Path to the NCBI datasets JSONL file.
        config (Config): A Config object containing the configuration data.
        biosamples (dict): A dictionary containing biosample information.
        parsed (dict): A dictionary containing parsed data.
        previous_report (Optional[dict]): A dictionary containing the previous
        assembly report.

    Returns:
        None
    """
    for report in parse_assembly_report(jsonl_path=jsonl_path):
        try:
            print(f"Processing report for {report.get('accession', 'unknown')}")
            processed_report = process_assembly_report(report, previous_report, config, parsed)
            if processed_report is None:
                continue

            # Check if we can reuse cached sequence fields (release date unchanged)
            can_reuse_cached = use_previous_report(processed_report, parsed, config)

            # Always parse the new data to get fresh field values
            # (don't copy the old row wholesale)

            if can_reuse_cached:
                # Release date is unchanged: skip re-fetching sequence metadata even if
                # the previous row lacks a subset of cached sequence-derived values.
                cached_fields = get_cached_sequence_fields(processed_report, config)
            else:
                # Release date changed, fetch new sequence data
                fetch_and_parse_sequence_report(processed_report)
                cached_fields = {}

            append_features(processed_report, config)
            add_report_to_parsed_reports(parsed, processed_report, config, biosamples)

            # If we have cached fields, overlay them on the parsed row
            if can_reuse_cached and (cached_fields := get_cached_sequence_fields(processed_report, config)):
                accession = processed_report["processedAssemblyInfo"]["genbankAccession"]
                if accession in parsed:
                    # Overlay cached sequence-derived field values onto the newly parsed row
                    for header, value in cached_fields.items():
                        parsed[accession][header] = value

                    # Also restore feature file entries if they exist
                    if config.feature_file is not None and accession in config.previous_features:
                        utils.append_to_tsv(
                            config.previous_features[accession],
                            config.feature_headers,
                            {"file_name": config.feature_file},
                        )

            if previous_report is not None:
                previous_report = processed_report
        except Exception as e:
            print(
                (
                    f"Error processing report for "
                    f"{report.get('accession', 'unknown')}: "
                    f"{e} (line {e.__traceback__.tb_lineno})"
                )
            )
            continue


@task(log_prints=True)
def parse_data_freeze_file(data_freeze_path: str) -> dict:
    """
    Fetch and parse a 2-column TSV with the data freeze list of assemblies and their
    respective status from the given S3 path.

    Args:
        data_freeze_path (str): The S3 path to the data freeze list TSV file.
    Returns:
        dict: A dictionary mapping assembly accessions to their freeze subsets.

    """
    # from s3 to temporary file
    print(f"Fetching data freeze file from {data_freeze_path}")
    data_freeze = parse_s3_file(data_freeze_path)
    print(f"Parsed {len(data_freeze)} entries from data freeze file")
    # Debug: print first few entries to verify structure
    for i, (k, v) in enumerate(list(data_freeze.items())[:5]):
        print(f"  Sample entry {i}: {k} -> {v}")
    return data_freeze


@task()
def set_data_freeze_default(parsed: dict, data_freeze_name: str):
    """
    Set the default data freeze information for all assemblies.

    Args:
        parsed (dict): A dictionary containing parsed data.
        data_freeze_name (str): The name of the default data freeze.
    """
    for line in parsed.values():
        line["dataFreeze"] = [data_freeze_name]
        line["assemblyId"] = line["genbankAccession"]


@task(log_prints=True)
def process_datafreeze_info(processed_report: dict, data_freeze: dict, config: Config):
    """
    Process the data freeze information for a given assembly report.
    Rename the assembly

    Args:
        processed_report (dict): A dictionary containing processed assembly data.
        data_freeze (dict): A dictionary containing data freeze information.
    """
    data_freeze_name = (
        re.sub(r"\.tsv(\.gz)?$", "", os.path.basename(config.meta["file_name"]))
        if config.meta["file_name"]
        else "data_freeze"
    )
    print(f"Processing data freeze info for {data_freeze_name}")
    for accession, line in processed_report.items():
        genbank = line.get("genbankAccession", "N/A")
        print(f"Processing data freeze info for {genbank}")

        # Only look up based on genbankAccession (the key of this record)
        # GCF-specific records will be created separately by create_paired_accession_records
        status = data_freeze.get(genbank)

        if not status:
            print(f"  No match found in data_freeze for {genbank}")
            continue

        print(f"  Found in data_freeze: {status}")
        # Handle both comma-separated strings and lists
        if isinstance(status, str) and "," in status:
            line["dataFreeze"] = status.split(",")
        else:
            line["dataFreeze"] = status

        print(f"Renaming assemblyId for {genbank} to {genbank}_{data_freeze_name}")
        line["assemblyId"] = f"{genbank}_{data_freeze_name}"


@task(log_prints=True)
def create_paired_accession_records(parsed: dict, data_freeze: dict, config: Config):
    """
    Create separate records for paired GCF accessions that have different data freeze values.

    If both GCA and GCF are in the data_freeze file with different values, we need separate
    output records for each. This function identifies paired accessions and clones records.

    Args:
        parsed (dict): A dictionary containing parsed data (keyed by genbankAccession/GCA).
        data_freeze (dict): A dictionary containing data freeze information.
        config (Config): A Config object containing the configuration data.
    """
    data_freeze_name = (
        re.sub(r"\.tsv(\.gz)?$", "", os.path.basename(config.meta["file_name"]))
        if config.meta["file_name"]
        else "data_freeze"
    )

    # Find all GCF accessions in data_freeze and create records for them
    new_records = {}

    for accession, freeze_value in data_freeze.items():
        if not accession.startswith("GCF_"):
            continue

        # Extract the numeric part: GCF_003369695.1 -> 003369695
        gcf_number = accession.split("_")[1].split(".")[0]

        # Find corresponding GCA in parsed by matching the numeric part
        gca_accession = None
        for parsed_key in parsed:
            if parsed_key.startswith("GCA_"):
                gca_number = parsed_key.split("_")[1].split(".")[0]
                if gca_number == gcf_number:
                    gca_accession = parsed_key
                    break

        if not gca_accession:
            print(f"No GCA found for GCF {accession}, skipping")
            continue

        # Clone the GCA record for the GCF accession with its own freeze values
        gca_row = parsed[gca_accession]
        gcf_row = {
            **gca_row,
            "dataFreeze": (
                freeze_value.split(",") if isinstance(freeze_value, str) and "," in freeze_value else freeze_value
            ),
        }

        # Set the assemblyId with GCF accession
        gcf_row["assemblyId"] = f"{accession}_{data_freeze_name}"

        # Update the refseqAccession to match the GCF since this is now the primary key
        gcf_row["refseqAccession"] = accession
        gcf_row["genbankAccession"] = accession  # Also update genbank to GCF for consistency

        new_records[accession] = gcf_row
        print(f"Created paired record for {accession} from {gca_accession} with dataFreeze: {gcf_row['dataFreeze']}")

    # Add new records to parsed
    parsed.update(new_records)
    print(f"Added {len(new_records)} paired GCF records")


def build_data_freeze_output(parsed, data_freeze, config):
    """
    Build output records that only include entries from the data_freeze file.

    For each accession in data_freeze:
    - If GCA: use the existing record from parsed
    - If GCF: create from the matching GCA record

    Args:
        parsed (dict): Dictionary of parsed records keyed by accession.
        data_freeze (dict): Dictionary of data_freeze entries.
        config (Config): Configuration object.

    Returns:
        dict: Filtered dictionary containing only records from data_freeze.
    """
    data_freeze_name = (
        re.sub(r"\.tsv(\.gz)?$", "", os.path.basename(config.meta["file_name"]))
        if config.meta["file_name"]
        else "data_freeze"
    )

    output = {}

    for accession, freeze_value in data_freeze.items():
        if accession.startswith("GCA_"):
            # GCA entry: use existing record from parsed
            if accession in parsed:
                row = parsed[accession]
                row["dataFreeze"] = (
                    freeze_value.split(",") if isinstance(freeze_value, str) and "," in freeze_value else freeze_value
                )
                row["assemblyId"] = f"{accession}_{data_freeze_name}"
                output[accession] = row
                print(f"Added GCA record {accession} with dataFreeze: {row['dataFreeze']}")
            else:
                print(f"GCA {accession} in data_freeze but not found in parsed, skipping")

        elif accession.startswith("GCF_"):
            # GCF entry: find matching GCA and create record
            gcf_number = accession.split("_")[1].split(".")[0]
            gca_accession = None

            for parsed_key in parsed.keys():
                if parsed_key.startswith("GCA_"):
                    gca_number = parsed_key.split("_")[1].split(".")[0]
                    if gca_number == gcf_number:
                        gca_accession = parsed_key
                        break

            if not gca_accession:
                print(f"GCF {accession} in data_freeze but no matching GCA found, skipping")
                continue

            # Clone the GCA record for the GCF accession
            gca_row = parsed[gca_accession]
            gcf_row = {
                **gca_row,
                "dataFreeze": (
                    freeze_value.split(",") if isinstance(freeze_value, str) and "," in freeze_value else freeze_value
                ),
            }

            gcf_row["assemblyId"] = f"{accession}_{data_freeze_name}"
            gcf_row["refseqAccession"] = accession
            gcf_row["genbankAccession"] = accession

            output[accession] = gcf_row
            print(f"Created GCF record {accession} from {gca_accession} with dataFreeze: {gcf_row['dataFreeze']}")

    print(f"Built data_freeze output with {len(output)} records")
    return output


@flow(log_prints=True)
def parse_ncbi_assemblies(
    input_path: str,
    yaml_path: str,
    append: bool,
    feature_file: Optional[str] = None,
    data_freeze_path: Optional[str] = None,
    **kwargs,
):
    """
    Parse NCBI datasets assembly data.

    Args:
        input_path (str): Path to the NCBI datasets JSONL file.
        yaml_path (str): Path to the YAML configuration file.
        append (bool): Flag to append values to an existing TSV file(s).
        feature_file (str): Path to the feature file.
        data_freeze_path (str): Path to data freeze list TSV on S3.
        **kwargs: Additional keyword arguments.
    """
    config = utils.load_config(
        config_file=yaml_path,
        feature_file=feature_file,
        load_previous=append,
    )
    if feature_file is not None:
        set_up_feature_file(config)

    biosamples = {}
    parsed = {}
    previous_report = {} if append else None
    process_assembly_reports(input_path, config, biosamples, parsed, previous_report)
    set_representative_assemblies(parsed, biosamples)

    if data_freeze_path is None:
        set_data_freeze_default(parsed, data_freeze_name="latest")
    else:
        data_freeze = parse_data_freeze_file(data_freeze_path)  # This returns the data freeze dictionary
        # Only include records that are in the data_freeze file
        parsed = build_data_freeze_output(parsed, data_freeze, config)
    snapshot_previous_output(config)
    parsed = enrich_parsed_assemblies(parsed, os.path.dirname(input_path))
    write_to_tsv(parsed, config)


def parse_ncbi_assemblies_wrapper(
    working_yaml: str,
    work_dir: str,
    append: bool,
    data_freeze_path: Optional[str] = None,
    **kwargs,
) -> None:
    """
    Wrapper function to parse the NCBI assemblies JSONL file.

    Args:
        working_yaml (str): Path to the working YAML file.
        work_dir (str): Path to the working directory.
        append (bool): Whether to append to the existing TSV file.
        data_freeze_path (str, optional): Path to a data freeze list TSV on S3.
        **kwargs: Additional keyword arguments.
    """
    # use glob to find the jsonl file in the working directory
    glob_path = os.path.join(work_dir, "*.jsonl")
    paths = glob(glob_path)
    # raise error if no jsonl file is found
    if not paths:
        raise FileNotFoundError(f"No jsonl file found in {work_dir}")
    # rais error if more than one jsonl file is found
    if len(paths) > 1:
        raise ValueError(f"More than one jsonl file found in {work_dir}")
    parse_ncbi_assemblies(
        input_path=paths[0],
        yaml_path=working_yaml,
        append=append,
        data_freeze_path=data_freeze_path,
    )


def plugin():
    """Register the flow."""
    return Parser(
        name="NCBI_ASSEMBLIES",
        func=parse_ncbi_assemblies_wrapper,
        description="Parse NCBI assemblies from a datasets JSONL file.",
    )


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args("Parse NCBI assemblies from a datasets JSONL file.")
    parse_ncbi_assemblies(**vars(args))
