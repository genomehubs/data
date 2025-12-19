#!/usr/bin/env python3
"""
One-time historical backfill process for assembly versions.

This script discovers and parses ALL superseded versions from NCBI for assemblies
that currently have version > 1. It should be run ONCE before starting the daily
incremental pipeline.

Process:
1. Scan input JSONL for assemblies with version > 1
2. Discover all versions via NCBI FTP directory listing (with caching)
3. Fetch metadata for each version via datasets command (with caching)
4. Parse using Rich's existing parser (includes sequence reports + metrics)
5. Write to assembly_historical.tsv with version_status="superseded"

Caching:
- Version discovery cached 7 days (FTP queries)
- Individual metadata cached 30 days (datasets queries)
- On re-run: only fetches NEW assemblies

Usage:
    python -m flows.parsers.backfill_historical_versions \\
        --input flows/parsers/eukaryota/ncbi_dataset/data/assembly_data_report.jsonl \\
        --config configs/assembly_historical.yaml \\
        --checkpoint tmp/backfill_checkpoint.json
"""

import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from genomehubs import utils as gh_utils

from flows.lib import utils
from flows.lib.conditional_import import task


# =============================================================================
# Cache Management (from DToL prototype - proven to work)
# =============================================================================

def setup_cache_directories():
    """Create cache directory structure."""
    cache_dirs = [
        "tmp/backfill_cache/version_discovery",
        "tmp/backfill_cache/metadata"
    ]
    for cache_dir in cache_dirs:
        os.makedirs(cache_dir, exist_ok=True)


def get_cache_path(cache_type: str, identifier: str) -> str:
    """Generate cache file path for given type and identifier."""
    return f"tmp/backfill_cache/{cache_type}/{identifier}.json"


def load_from_cache(cache_path: str, max_age_days: int = 30) -> Dict:
    """Load data from cache if it exists and is recent enough."""
    try:
        if os.path.exists(cache_path):
            cache_age = time.time() - os.path.getmtime(cache_path)
            if cache_age < (max_age_days * 24 * 3600):
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
    except Exception as e:
        print(f"  Warning: Could not load cache from {cache_path}: {e}")
    return {}


def save_to_cache(cache_path: str, data: Dict):
    """Save data to cache file."""
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"  Warning: Could not save cache to {cache_path}: {e}")


# =============================================================================
# Version Discovery via FTP (KEY DIFFERENCE from Rich's parser)
# =============================================================================

def find_all_assembly_versions(base_accession: str) -> List[Dict]:
    """
    Find all versions of an assembly by examining NCBI FTP structure.

    This is the KEY difference from Rich's parser:
    - Rich's parser: Gets latest versions from input JSONL
    - This function: Discovers ALL versions (including historical) via FTP

    Args:
        base_accession: Full accession (e.g., GCA_000002035.3)

    Returns:
        List of dicts with full NCBI metadata for each version
    """
    # Extract base (e.g., GCA_000002035 from GCA_000002035.3)
    base_match = re.match(r'(GC[AF]_\d+)', base_accession)
    if not base_match:
        return []

    base = base_match.group(1)

    # Check version discovery cache first
    setup_cache_directories()
    version_cache_path = get_cache_path("version_discovery", base)
    cached_data = load_from_cache(version_cache_path, max_age_days=7)

    if cached_data and 'versions' in cached_data:
        print(f"  Using cached version data for {base}")
        return cached_data['versions']

    print(f"  Discovering versions for {base} via FTP")
    versions = []

    try:
        # Construct FTP URL
        # Example: https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/002/035/
        ftp_url = f"https://ftp.ncbi.nlm.nih.gov/genomes/all/{base[:3]}/{base[4:7]}/{base[7:10]}/{base[10:13]}/"

        # Get directory listing
        response = requests.get(ftp_url, timeout=30)
        if response.status_code != 200:
            print(f"  Warning: FTP query failed for {base}")
            return []

        # Parse HTML for version directories (e.g., GCA_000002035.1, GCA_000002035.2, etc.)
        version_pattern = rf"{base}\.\d+"
        found_versions = re.findall(version_pattern, response.text)
        unique_versions = sorted(list(set(found_versions)))

        # Fetch metadata for each version
        for version_acc in unique_versions:
            metadata_cache_path = get_cache_path("metadata", version_acc)
            cached_metadata = load_from_cache(metadata_cache_path, max_age_days=30)

            if cached_metadata and 'metadata' in cached_metadata:
                versions.append(cached_metadata['metadata'])
                continue

            # Fetch from NCBI datasets
            try:
                # Validate accession format to prevent command injection
                # Pattern: GC[AF]_9digits.version (e.g., GCA_000001405.39)
                version_pattern_strict = r"^GC[AF]_\d{9}\.\d+$"
                if not re.match(version_pattern_strict, version_acc):
                    print(f"    Skipping unexpected accession format: {version_acc}")
                    continue

                cmd = ["datasets", "summary", "genome", "accession", version_acc, "--as-json-lines"]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore',  # Handle Unicode gracefully
                    timeout=60
                )

                if result.returncode == 0 and result.stdout and result.stdout.strip():
                    version_data = json.loads(result.stdout.strip())
                    versions.append(version_data)
                    save_to_cache(metadata_cache_path, {'metadata': version_data, 'cached_at': time.time()})
                else:
                    print(f"    Warning: No metadata for {version_acc}")

            except Exception as e:
                print(f"    Warning: Error fetching {version_acc}: {e}")
                continue

        # Cache the complete version discovery
        cache_data = {
            'versions': versions,
            'base_accession': base,
            'discovered_at': time.time(),
            'ftp_url': ftp_url
        }
        save_to_cache(version_cache_path, cache_data)

        return versions

    except Exception as e:
        print(f"  Error discovering versions for {base}: {e}")
        return []


# =============================================================================
# Parsing with Rich's Existing Functions
# =============================================================================

def parse_historical_version(
    version_data: Dict,
    config: utils.Config,
    base_accession: str,
    version_num: int,
    current_accession: str
) -> Dict:
    """
    Parse historical version using Rich's EXACT parser logic.

    This ensures consistency with current assemblies by:
    - Using process_assembly_report() with version_status="superseded"
    - Fetching sequence reports via fetch_and_parse_sequence_report()
    - Computing all metrics identically to current parser

    Args:
        version_data: Raw NCBI metadata from datasets command
        config: Config object from YAML
        base_accession: Base accession (e.g., GCA_000002035)
        version_num: Version number (1, 2, 3, etc.)
        current_accession: Latest version that superseded this one

    Returns:
        Parsed assembly dict ready for TSV output
    """
    from flows.parsers.parse_ncbi_assemblies import (
        fetch_and_parse_sequence_report,
        process_assembly_report
    )

    # Convert keys to camelCase (Rich's standard)
    version_data = utils.convert_keys_to_camel_case(version_data)

    # Process with Rich's parser (version_status="superseded")
    processed_report = process_assembly_report(
        report=version_data,
        previous_report=None,
        config=config,
        parsed={},
        version_status="superseded"
    )

    # Fetch sequence reports (chromosomes, organelles, etc.) - Rich's critical step
    fetch_and_parse_sequence_report(processed_report)

    # Set assemblyID in standard format (e.g., GCA_000222935_1)
    # The versionStatus field already indicates this is "superseded"
    processed_report["processedAssemblyInfo"]["assemblyID"] = f"{base_accession}_{version_num}"

    # Parse into TSV row format using Rich's parse functions
    row = gh_utils.parse_report_values(config.parse_fns, processed_report)

    return row


def parse_version(accession: str) -> int:
    """Extract version number from accession."""
    parts = accession.split('.')
    return int(parts[1]) if len(parts) > 1 else 1


def parse_accession(accession: str) -> Tuple[str, int]:
    """Parse accession into base and version number."""
    parts = accession.split('.')
    base = parts[0]
    version = int(parts[1]) if len(parts) > 1 else 1
    return base, version


# =============================================================================
# Checkpoint Management
# =============================================================================

def load_checkpoint(checkpoint_file: str) -> Dict:
    """Load checkpoint data if exists."""
    if Path(checkpoint_file).exists():
        with open(checkpoint_file) as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoint_file: str, processed_count: int):
    """Save checkpoint data."""
    Path(checkpoint_file).parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, 'w') as f:
        json.dump({
            'processed_count': processed_count,
            'timestamp': datetime.now().isoformat()
        }, f, indent=2)


# =============================================================================
# Main Backfill Logic
# =============================================================================

def identify_assemblies_needing_backfill(input_jsonl: str) -> List[Dict]:
    """
    Identify assemblies with version > 1 that need historical backfill.

    Args:
        input_jsonl: Path to assembly_data_report.jsonl

    Returns:
        List of assembly info dicts needing backfill
    """
    assemblies_needing_backfill = []

    with open(input_jsonl) as f:
        for line in f:
            assembly = json.loads(line)
            accession = assembly['accession']
            base_acc, version = parse_accession(accession)

            if version > 1:
                assemblies_needing_backfill.append({
                    'base_accession': base_acc,
                    'current_version': version,
                    'current_accession': accession,
                    'historical_versions_needed': list(range(1, version))
                })

    return assemblies_needing_backfill


@task(log_prints=True)
def backfill_historical_versions(
    input_jsonl: str,
    config_yaml: str,
    checkpoint_file: str = 'tmp/backfill_checkpoint.json'
):
    """
    One-time backfill of all historical assembly versions.

    Process:
    1. Identify assemblies with version > 1
    2. Discover all versions via FTP (cached)
    3. Fetch metadata via datasets (cached)
    4. Parse with Rich's parser
    5. Write to assembly_historical.tsv

    Args:
        input_jsonl: Path to assembly_data_report.jsonl
        config_yaml: Path to assembly_historical.yaml
        checkpoint_file: Path for checkpoint data
    """
    # Setup
    setup_cache_directories()
    config = utils.load_config(config_file=config_yaml)

    # Identify assemblies needing backfill
    print("Scanning for assemblies needing historical backfill...")
    assemblies_needing_backfill = identify_assemblies_needing_backfill(input_jsonl)

    if not assemblies_needing_backfill:
        print("No assemblies with version > 1 found. Nothing to backfill.")
        return

    # Load checkpoint
    checkpoint = load_checkpoint(checkpoint_file)
    start_index = checkpoint.get('processed_count', 0)

    total_assemblies = len(assemblies_needing_backfill)
    total_versions = sum(len(a['historical_versions_needed']) for a in assemblies_needing_backfill)

    print(f"\n{'='*80}")
    print("ONE-TIME HISTORICAL BACKFILL")
    print(f"{'='*80}")
    print(f"  Assemblies to process: {total_assemblies}")
    print(f"  Total historical versions: {total_versions}")

    if start_index > 0:
        print(f"  Resuming from checkpoint: {start_index}/{total_assemblies}")

    print(f"{'='*80}\n")

    parsed = {}
    processed = start_index

    for assembly_info in assemblies_needing_backfill[start_index:]:
        base_acc = assembly_info['base_accession']
        current_version = assembly_info['current_version']
        current_accession = assembly_info['current_accession']

        print(f"[{processed+1}/{total_assemblies}] {base_acc} (current: v{current_version})")

        # Discover all versions via FTP (uses cache)
        all_versions = find_all_assembly_versions(current_accession)

        if not all_versions:
            print(f"  Warning: No versions found via FTP")
            processed += 1
            continue

        # Parse each historical version (skip current)
        for version_data in all_versions:
            version_acc = version_data.get('accession', '')
            version_num = parse_version(version_acc)

            # Only process historical versions
            if version_num >= current_version:
                continue

            try:
                print(f"  Parsing v{version_num}...", end=' ', flush=True)

                # Parse using Rich's parser
                row = parse_historical_version(
                    version_data=version_data,
                    config=config,
                    base_accession=base_acc,
                    version_num=version_num,
                    current_accession=current_accession
                )

                # Add to parsed dict (keyed by genbank accession)
                genbank_acc = row.get('genbankAccession', version_acc)
                parsed[genbank_acc] = row

                print("✓")

            except Exception as e:
                print(f"✗ ({e})")
                continue

        processed += 1

        # Checkpoint every 100 assemblies
        if processed % 100 == 0:
            print(f"\n→ Checkpoint: Writing batch to disk...")
            gh_utils.write_tsv(parsed, config.headers, config.meta)
            save_checkpoint(checkpoint_file, processed)
            parsed = {}
            print(f"→ Progress: {processed}/{total_assemblies} ({processed/total_assemblies*100:.1f}%)\n")

    # Final write
    if parsed:
        print(f"\n→ Writing final batch...")
        gh_utils.write_tsv(parsed, config.headers, config.meta)

    # Final report
    print(f"\n{'='*80}")
    print("BACKFILL COMPLETE")
    print(f"{'='*80}")
    print(f"  Processed: {processed}/{total_assemblies} assemblies")
    print(f"  Output: {config.meta['file_name']}")
    print(f"\n  Next step: Run daily incremental pipeline")
    print(f"{'='*80}\n")


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='One-time historical backfill for assembly versions'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Input JSONL file (assembly_data_report.jsonl)'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Config YAML file (assembly_historical.yaml)'
    )
    parser.add_argument(
        '--checkpoint',
        default='tmp/backfill_checkpoint.json',
        help='Checkpoint file for resuming'
    )

    args = parser.parse_args()

    backfill_historical_versions(
        input_jsonl=args.input,
        config_yaml=args.config,
        checkpoint_file=args.checkpoint
    )
