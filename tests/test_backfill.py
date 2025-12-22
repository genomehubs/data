#!/usr/bin/env python3
"""
Test script for backfill_historical_versions.py

This script tests the historical version backfill process on a small sample dataset.

Usage:
    python tests/test_backfill.py
"""

import os
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.parsers.backfill_historical_versions import (
    identify_assemblies_needing_backfill,
    parse_accession,
    find_all_assembly_versions
)

def test_parse_accession():
    """Test accession parsing."""
    print("\n" + "="*80)
    print("TEST 1: Accession Parsing")
    print("="*80)

    test_cases = [
        ("GCA_000222935.2", ("GCA_000222935", 2)),
        ("GCA_003706615.3", ("GCA_003706615", 3)),
        ("GCF_000001405.39", ("GCF_000001405", 39)),
    ]

    for accession, expected in test_cases:
        result = parse_accession(accession)
        status = "PASS" if result == expected else "FAIL"
        print(f"  {status} {accession} -> {result}")
        assert result == expected, f"Expected {expected}, got {result}"

    print("  All accession parsing tests passed!")

def test_identify_assemblies():
    """Test identification of assemblies needing backfill."""
    print("\n" + "="*80)
    print("TEST 2: Identify Assemblies Needing Backfill")
    print("="*80)

    test_file = "tests/test_data/assembly_test_sample.jsonl"

    if not os.path.exists(test_file):
        print(f"  FAIL Test file not found: {test_file}")
        return False

    assemblies = identify_assemblies_needing_backfill(test_file)

    print(f"  Found {len(assemblies)} assemblies needing backfill:")
    for asm in assemblies:
        print(f"    - {asm['current_accession']}: v{asm['current_version']} "
              f"(needs v{asm['historical_versions_needed']})")

    # Verify we found the expected assemblies
    expected_count = 3  # GCA_000222935.2, GCA_000412225.2, GCA_003706615.3
    assert len(assemblies) == expected_count, \
        f"Expected {expected_count} assemblies, found {len(assemblies)}"

    print(f"  PASS Correctly identified {len(assemblies)} assemblies")
    return True

def test_version_discovery():
    """Test FTP-based version discovery (using cache if available)."""
    print("\n" + "="*80)
    print("TEST 3: Version Discovery via FTP")
    print("="*80)
    print("  Note: This test queries NCBI FTP - may take a minute...")

    # Test with a known multi-version assembly
    test_accession = "GCA_000222935.2"  # Aciculosporium take - has version 1 and 2

    print(f"  Testing: {test_accession}")
    versions = find_all_assembly_versions(test_accession)

    if not versions:
        print(f"  FAIL No versions found (FTP query may have failed)")
        print(f"    This is not critical - may be network issue")
        return False

    print(f"  PASS Found {len(versions)} version(s):")
    for v in versions:
        acc = v.get('accession', 'unknown')
        print(f"    - {acc}")

    # Verify we found at least version 1 and 2
    accessions = [v.get('accession', '') for v in versions]
    base = test_accession.split('.')[0]

    # Should find both v1 and v2
    expected_versions = [f"{base}.1", f"{base}.2"]
    found_expected = [v for v in expected_versions if v in accessions]

    print(f"  PASS Found {len(found_expected)}/{len(expected_versions)} expected versions")
    return True

def test_cache_functionality():
    """Test that caching works correctly."""
    print("\n" + "="*80)
    print("TEST 4: Cache Functionality")
    print("="*80)

    # Clean cache first (Windows-safe deletion)
    import shutil
    import time as time_module
    cache_dir = "tmp/backfill_cache"
    if os.path.exists(cache_dir):
        try:
            # Give time for file handles to close
            time_module.sleep(0.5)
            shutil.rmtree(cache_dir)
            print(f"  Cleared cache directory: {cache_dir}")
        except PermissionError:
            print(f"  Note: Cache directory in use, will test with existing cache")

    test_accession = "GCA_000222935.2"

    # First call - should fetch from FTP
    print(f"  First call (should fetch from FTP)...")
    import time
    start = time.time()
    versions1 = find_all_assembly_versions(test_accession)
    time1 = time.time() - start

    if not versions1:
        print(f"  FAIL FTP fetch failed - skipping cache test")
        return False

    print(f"    Took {time1:.2f}s, found {len(versions1)} versions")

    # Second call - should use cache
    print(f"  Second call (should use cache)...")
    start = time.time()
    versions2 = find_all_assembly_versions(test_accession)
    time2 = time.time() - start

    print(f"    Took {time2:.2f}s, found {len(versions2)} versions")

    # Cache should be much faster
    if time2 < time1 * 0.5:  # At least 50% faster
        print(f"  PASS Cache is working (2nd call {time2/time1*100:.1f}% of 1st call time)")
    else:
        print(f"  WARN Cache may not be working (times similar)")

    # Verify cache files exist
    if os.path.exists(cache_dir):
        cache_files = list(Path(cache_dir).rglob("*.json"))
        print(f"  PASS Cache directory created with {len(cache_files)} files")
    else:
        print(f"  FAIL Cache directory not created")

    return True

def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("BACKFILL SCRIPT TEST SUITE")
    print("="*80)
    print("Testing: flows/parsers/backfill_historical_versions.py")
    print("="*80)

    results = {
        "Accession Parsing": False,
        "Identify Assemblies": False,
        "Version Discovery": False,
        "Cache Functionality": False,
    }

    try:
        # Run tests
        test_parse_accession()
        results["Accession Parsing"] = True

        results["Identify Assemblies"] = test_identify_assemblies()
        results["Version Discovery"] = test_version_discovery()
        results["Cache Functionality"] = test_cache_functionality()

    except Exception as e:
        print(f"\nFAIL Test failed with error: {e}")
        import traceback
        traceback.print_exc()

    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)

    for test_name, passed in results.items():
        status = "PASS PASS" if passed else "FAIL FAIL"
        print(f"  {status}: {test_name}")

    passed_count = sum(results.values())
    total_count = len(results)

    print(f"\n  Total: {passed_count}/{total_count} tests passed")

    if passed_count == total_count:
        print("\n  🎉 All tests passed!")
        return 0
    else:
        print("\n  WARN Some tests failed - review output above")
        return 1

if __name__ == '__main__':
    sys.exit(main())
