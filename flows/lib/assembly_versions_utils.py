"""Shared utilities for assembly version discovery, schema and paths.

Used by the backfill parser, the incremental parser, the summary generator and
the milestone flow to discover assembly versions via NCBI FTP, fetch
per-version metadata, read rows written by any phase, and resolve the
current-assembly TSV paths from a loaded config rather than a hardcoded name.
"""

import csv
import glob as globmod
import gzip
import json
import os
import re
import time
from typing import Callable, Optional

from flows.lib import utils

ACCESSION_PATTERN = re.compile(r"^GC[AF]_\d{9}\.\d+$")

# Cell values that mean "no value".  Upstream writes the literal string
# "None" wherever a field was absent when the row was formatted --
# genomehubs' format_entry stringifies a bare None -- so every phase has to
# read it as empty or it becomes a date, a taxid or a metric that is not
# there.
ABSENT_VALUES = frozenset({"", "None"})

# Leading bytes of a gzip member, used to detect compression by content.
GZIP_MAGIC = b"\x1f\x8b"

# Canonical column names, as declared in assembly_historical.types.yaml.  Every
# phase writes these; the ``*_ALIASES`` tuples additionally tolerate the
# snake_case spellings written by earlier revisions of the Phase 1 parser.
COL_ACCESSION = "genbankAccession"
COL_ASSEMBLY_ID = "assemblyID"
COL_VERSION_STATUS = "versionStatus"
COL_SUPERSEDED_BY = "superseded_by"
COL_SUPERSEDED_BY_VERSION = "superseded_by_version"
COL_SUPERSEDED_DATE = "superseded_date"

ACCESSION_ALIASES = (COL_ACCESSION, "accession")
# "assemblyId" is the spelling parse_ncbi_assemblies has written since
# 5180090 (2026-07-27); assembly_historical.types.yaml and Phase 0 still use
# "assemblyID".  Read both until the case is settled upstream — the write side
# stays on COL_ASSEMBLY_ID, which is what the YAML declares.
ASSEMBLY_ID_ALIASES = (COL_ASSEMBLY_ID, "assemblyId", "assembly_id")
VERSION_STATUS_ALIASES = (COL_VERSION_STATUS, "version_status")

# Every aliased field, canonical spelling first.  canonicalize_columns walks
# this to collapse a row onto the spellings the YAML declares.
ALIASES_BY_CANONICAL = (
    ACCESSION_ALIASES,
    ASSEMBLY_ID_ALIASES,
    VERSION_STATUS_ALIASES,
)

# Filename of the historical TSV, and the outputs derived from it.  Used to
# exclude the pipeline's own products when discovering the current TSV.
HISTORICAL_TSV_NAME = "assembly_historical.tsv"
HISTORICAL_YAML_NAME = "assembly_historical.types.yaml"
# configs/ sits beside flows/ in the repo, and holds the historical schema.
REPO_CONFIG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "configs",
)
CURRENT_TSV_DEFAULT = "assembly_current.tsv"
DERIVED_TSV_NAMES = frozenset({
    HISTORICAL_TSV_NAME,
    "assembly_version_summary.tsv",
    "taxon_milestone_summary.tsv",
})


def cell(row: dict, *keys: str) -> str:
    """Return the first populated cell in ``row`` among ``keys``.

    Values in ABSENT_VALUES read as empty, so a column holding the literal
    string "None" is treated the same as a column holding nothing.

    Args:
        row (dict): A TSV row.
        *keys (str): Column names to try, in precedence order.

    Returns:
        str: The first populated value, stripped, or "" when there is none.
    """
    for key in keys:
        value = str(row.get(key) or "").strip()
        if value not in ABSENT_VALUES:
            return value
    return ""


def _first_present(row: dict, keys: tuple) -> str:
    """Return the first populated value in ``row`` for any of ``keys``."""
    return cell(row, *keys)


def get_accession(row: dict) -> str:
    """Return a row's versioned accession, whichever column holds it.

    Real assembly TSVs use ``genbankAccession``; ``accession`` is accepted for
    rows that originated from an NCBI JSONL record or an older parse.

    Args:
        row (dict): A TSV row.

    Returns:
        str: The versioned accession, or "" when absent.
    """
    return _first_present(row, ACCESSION_ALIASES)


def get_assembly_id(row: dict) -> str:
    """Return a row's assembly ID across all three spellings in the pipeline.

    ``assemblyID`` is canonical (declared in assembly_historical.types.yaml and
    written by Phase 0); ``assemblyId`` is what the current-assembly parser
    writes upstream; ``assembly_id`` is the legacy snake_case form.

    Args:
        row (dict): A TSV row.

    Returns:
        str: The assembly ID, or "" when absent.
    """
    return _first_present(row, ASSEMBLY_ID_ALIASES)


def get_version_status(row: dict) -> str:
    """Return a row's version status, tolerating either naming convention.

    Args:
        row (dict): A TSV row.

    Returns:
        str: The version status, or "" when absent.
    """
    return _first_present(row, VERSION_STATUS_ALIASES)


def canonicalize_columns(row: dict) -> dict:
    """Return a copy of ``row`` with each alias spelling folded onto its column.

    Reads tolerate the alias spellings, but a row about to be *written* to the
    historical TSV must carry one column per field.  A row copied from the
    current TSV otherwise arrives holding upstream's ``assemblyId`` alongside
    the ``assemblyID`` the caller sets, and the merged output then has two
    assembly-ID columns disagreeing about the same assembly — only one of
    which the historical YAML declares.

    Only fields the row actually has are touched, so this never invents an
    empty column for a field the row never carried.

    Args:
        row (dict): A TSV row, possibly using alias spellings.

    Returns:
        dict: A copy holding only the canonical spelling of each aliased field.
    """
    canonical_row = dict(row)
    for aliases in ALIASES_BY_CANONICAL:
        if not any(alias in row for alias in aliases):
            continue
        value = _first_present(row, aliases)
        for alias in aliases[1:]:
            canonical_row.pop(alias, None)
        canonical_row[aliases[0]] = value
    return canonical_row


def is_gzipped(path: str) -> bool:
    """Report whether a TSV is gzip-compressed.

    The name alone is not enough: ``snapshot_previous_output`` copies a
    gzipped current TSV verbatim to ``<name>.gz.previous``, so the compressed
    file does not end ``.gz``.  Sniff the magic bytes, and fall back to the
    extension when the file cannot be read.

    Args:
        path (str): Path to the TSV.

    Returns:
        bool: True when the file is gzip-compressed.
    """
    name = str(path)
    try:
        with open(name, "rb") as f:
            return f.read(2) == GZIP_MAGIC
    except OSError:
        return name.endswith(".gz")


def open_tsv(path: str, mode: str = "rt"):
    """Open a TSV transparently, decompressing gzipped files.

    Args:
        path (str): Path to the TSV.
        mode (str): Text mode to open with.

    Returns:
        IO: An open text-mode file object.
    """
    if is_gzipped(path):
        return gzip.open(path, mode, encoding="utf-8")
    return open(path, mode, encoding="utf-8")


def discover_current_tsv_name(work_dir: str) -> str:
    """Find the current-assembly TSV filename in ``work_dir``.

    Fallback for callers that have no config to resolve from.  Considers every
    ``*.tsv`` / ``*.tsv.gz`` in ``work_dir`` except the pipeline's own derived
    outputs.

    Args:
        work_dir (str): Directory holding the pipeline files.

    Returns:
        str: Basename of the current TSV.  Falls back to
            ``assembly_current.tsv`` when nothing is on disk yet, so callers
            still produce a meaningful not-found message.

    Raises:
        ValueError: If several candidates are present and none of them is the
            conventional ``assembly_current.tsv``.
    """
    candidates = sorted(
        globmod.glob(os.path.join(work_dir, "*.tsv"))
        + globmod.glob(os.path.join(work_dir, "*.tsv.gz"))
    )
    names = [
        os.path.basename(c)
        for c in candidates
        if os.path.basename(c) not in DERIVED_TSV_NAMES
    ]
    if not names:
        return CURRENT_TSV_DEFAULT
    if len(names) == 1:
        return names[0]
    if CURRENT_TSV_DEFAULT in names:
        return CURRENT_TSV_DEFAULT
    raise ValueError(
        f"Cannot identify the current assembly TSV in {work_dir}: {names}. "
        "Pass a config so the filename can be resolved from meta['file_name']."
    )


def resolve_current_tsv_paths(
    work_dir: str, config: Optional[object] = None
) -> tuple[str, str, Callable]:
    """Resolve the current-assembly TSV paths and the matching open function.

    The filename comes from ``config.meta["file_name"]`` when a config is
    supplied, so nothing hardcodes ``assembly_current.tsv``.  The previous-run
    snapshot is ``<current>.previous``, written verbatim by
    ``snapshot_previous_output`` — so it carries the same compression as the
    current file, which is why the open function is returned alongside.

    Args:
        work_dir (str): Directory holding the pipeline files.
        config (Config, optional): Loaded YAML config whose
            ``meta["file_name"]`` names the current TSV.

    Returns:
        tuple: ``(current_tsv, previous_tsv, open_fn)``.
    """
    if config is not None:
        name = os.path.basename(config.meta["file_name"])
    else:
        name = discover_current_tsv_name(work_dir)
    current_tsv = os.path.join(work_dir, name)
    return current_tsv, f"{current_tsv}.previous", open_tsv


def resolve_historical_tsv_path(work_dir: str) -> str:
    """Return the historical TSV path inside ``work_dir``.

    Args:
        work_dir (str): Directory holding the pipeline files.

    Returns:
        str: Path to assembly_historical.tsv.
    """
    return os.path.join(work_dir, HISTORICAL_TSV_NAME)


def resolve_historical_yaml_path(work_dir: str) -> Optional[str]:
    """Locate assembly_historical.types.yaml, or None when it is not found.

    The pipeline copies a parser's YAML into work_dir, so look there first and
    fall back to the copy in the repo's configs/ directory.  Returning None
    rather than raising keeps the schema an optimisation: the caller falls
    back to the column order already on disk.

    Args:
        work_dir (str): Directory holding the pipeline files.

    Returns:
        str or None: Path to the historical types YAML.
    """
    candidates = [
        os.path.join(work_dir, HISTORICAL_YAML_NAME),
        os.path.join(REPO_CONFIG_DIR, HISTORICAL_YAML_NAME),
    ]
    return next((path for path in candidates if os.path.exists(path)), None)


def parse_version(accession: str) -> int:
    """Extract the version number from a dotted accession string.

    Args:
        accession (str): e.g. GCA_000002035.3

    Returns:
        int: Version number (defaults to 1 if no dot-suffix).
    """
    parts = accession.split(".")
    return int(parts[1]) if len(parts) > 1 else 1


def parse_accession(accession: str) -> tuple[str, int]:
    """Split an accession into its base and version components.

    Args:
        accession (str): e.g. GCA_000002035.3

    Returns:
        tuple: (base_accession, version_number).
    """
    parts = accession.split(".")
    return parts[0], int(parts[1]) if len(parts) > 1 else 1


def load_versions_by_base(tsv_path: str) -> dict[str, set[int]]:
    """Index the assembly versions already present in a TSV by base accession.

    Used against assembly_historical.tsv so the daily diff can tell a genuine
    gap from one that a previous run — or the Phase 0 backfill — has already
    parsed.  Only the accession column is read, so this stays cheap on a file
    holding tens of thousands of rows, and it opens no network connections.

    Args:
        tsv_path (str): Path to a TSV written by any phase.

    Returns:
        dict: Mapping of base_accession -> set of version numbers present.
            Empty when the file is absent or empty, which is the expected
            state before the first backfill.
    """
    versions_by_base: dict[str, set[int]] = {}

    if not os.path.exists(tsv_path) or os.path.getsize(tsv_path) == 0:
        return versions_by_base

    with open_tsv(tsv_path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            accession = get_accession(row)
            if not accession:
                continue
            base_acc, version = parse_accession(accession)
            versions_by_base.setdefault(base_acc, set()).add(version)

    return versions_by_base


def setup_cache_directories(work_dir: str) -> None:
    """Create cache directory structure under work_dir.

    Args:
        work_dir (str): Path to the working directory.
    """
    for subdir in ("version_discovery", "metadata"):
        os.makedirs(
            os.path.join(work_dir, "backfill_cache", subdir), exist_ok=True
        )


def get_cache_path(work_dir: str, cache_type: str, identifier: str) -> str:
    """Generate a human-readable cache file path.

    Args:
        work_dir (str): Path to the working directory.
        cache_type (str): Cache category (version_discovery or metadata).
        identifier (str): Accession string used as the filename stem.

    Returns:
        str: Path to the JSON cache file.
    """
    safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", identifier)
    return os.path.join(work_dir, "backfill_cache", cache_type, f"{safe_id}.json")


def load_from_cache(cache_path: str, max_age_days: int = 30) -> dict:
    """Load data from cache if it exists and is recent enough.

    Args:
        cache_path (str): Path to the cache JSON file.
        max_age_days (int): Maximum acceptable age in days.

    Returns:
        dict: Cached data, or empty dict on miss/expiry.
    """
    try:
        if os.path.exists(cache_path):
            cache_age = time.time() - os.path.getmtime(cache_path)
            if cache_age < (max_age_days * 24 * 3600):
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
    except Exception as e:
        print(f"  Warning: Could not load cache from {cache_path}: {e}")
    return {}


def save_to_cache(cache_path: str, data: dict) -> None:
    """Save data to a cache file, creating parent dirs as needed.

    Args:
        cache_path (str): Path to the cache JSON file.
        data (dict): Data to persist.
    """
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"  Warning: Could not save cache to {cache_path}: {e}")


def discover_version_accessions(base_accession: str, work_dir: str) -> list[str]:
    """Discover all versioned accessions for a base assembly via NCBI FTP.

    Args:
        base_accession (str): Full accession (e.g. GCA_000002035.3).
        work_dir (str): Working directory for cache storage.

    Returns:
        list: Sorted list of versioned accession strings.
    """
    import requests

    base_match = re.match(r"(GC[AF]_\d+)", base_accession)
    if not base_match:
        return []

    base = base_match.group(1)
    setup_cache_directories(work_dir)
    cache_path = get_cache_path(work_dir, "version_discovery", base)
    cached = load_from_cache(cache_path, max_age_days=7)

    if cached and "accessions" in cached:
        print(f"  Using cached version list for {base}")
        return cached["accessions"]

    print(f"  Discovering versions for {base} via FTP")
    ftp_url = (
        f"https://ftp.ncbi.nlm.nih.gov/genomes/all/"
        f"{base[:3]}/{base[4:7]}/{base[7:10]}/{base[10:13]}/"
    )

    try:
        response = requests.get(ftp_url, timeout=30)
        if response.status_code != 200:
            print(f"  Warning: FTP query failed for {base}")
            return []
    except Exception as e:
        print(f"  Error querying FTP for {base}: {e}")
        return []

    version_pattern = rf"{re.escape(base)}\.\d+"
    accessions = sorted(set(re.findall(version_pattern, response.text)))

    save_to_cache(cache_path, {
        "accessions": accessions,
        "base_accession": base,
        "ftp_url": ftp_url,
    })
    return accessions


def fetch_version_metadata(version_acc: str, work_dir: str) -> dict:
    """Fetch NCBI datasets metadata for a single assembly version.

    Uses utils.run_quoted to safely invoke the datasets CLI.  Results are
    cached for 30 days.

    Args:
        version_acc (str): Versioned accession (e.g. GCA_000002035.1).
        work_dir (str): Working directory for cache storage.

    Returns:
        dict: Metadata dict, or empty dict on failure.
    """
    cache_path = get_cache_path(work_dir, "metadata", version_acc)
    cached = load_from_cache(cache_path, max_age_days=30)

    if cached and "metadata" in cached:
        return cached["metadata"]

    if not ACCESSION_PATTERN.match(version_acc):
        print(f"    Skipping unexpected accession format: {version_acc}")
        return {}

    cmd = [
        "datasets", "summary", "genome", "accession",
        version_acc, "--as-json-lines",
    ]
    try:
        result = utils.run_quoted(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=60,
        )
        if result.returncode == 0 and result.stdout and result.stdout.strip():
            version_data = json.loads(result.stdout.strip())
            save_to_cache(cache_path, {
                "metadata": version_data,
                "cached_at": time.time(),
            })
            return version_data

        print(f"    Warning: No metadata for {version_acc}")
    except Exception as e:
        print(f"    Warning: Error fetching {version_acc}: {e}")

    return {}


def find_all_assembly_versions(base_accession: str, work_dir: str) -> list[dict]:
    """Discover all versions and fetch metadata for each.

    Delegates to discover_version_accessions for FTP discovery and
    fetch_version_metadata for per-version metadata retrieval.  Both layers
    use independent caches.

    Args:
        base_accession (str): Full accession (e.g. GCA_000002035.3).
        work_dir (str): Working directory for cache storage.

    Returns:
        list: List of metadata dicts, one per version found.
    """
    accessions = discover_version_accessions(base_accession, work_dir)
    versions = []
    for version_acc in accessions:
        metadata = fetch_version_metadata(version_acc, work_dir)
        if metadata:
            versions.append(metadata)
    return versions
