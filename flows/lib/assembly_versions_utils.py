"""Shared utilities for assembly version discovery and fetching.

Used by both the backfill parser and the incremental updater to discover
assembly versions via NCBI FTP and fetch per-version metadata.
"""

import json
import os
import re
import time

from flows.lib import utils

ACCESSION_PATTERN = re.compile(r"^GC[AF]_\d{9}\.\d+$")


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
