#!/usr/bin/env python3
"""Test wrapper: run parsers on example TSVs and compare against S3 sources.

Usage:
    python scripts/run_parse_validate_tests.py [--parser PARSER] [--verbose]
    python scripts/run_parse_validate_tests.py --all

This script:
1. Discovers parsers and example TSV inputs
2. Maps examples to YAML configs (from goat-data/sources)
3. Runs each parser and validates output
4. Compares output columns & line counts vs S3 source versions
5. Generates a test report

Example files should be organized as:
  - tsv_examples/*.tsv or *.tsv.gz
  - tsv_examples/<name>/*.tsv or *.tsv.gz

S3 source files are mirrored in:
  - goat-data_s3_sources/{assembly-data,status-lists,sra,btk,conservation,genomesize-karyotype,uk-legislation}/imported/*.tsv
"""

import argparse
import gzip
import json
import os
import subprocess
import sys
import tempfile
from csv import DictReader
from pathlib import Path
from typing import Optional, Tuple, List, Dict

# Set SKIP_PREFECT before any imports from flows
os.environ["SKIP_PREFECT"] = "true"

# Use absolute import paths
sys.path.insert(0, str(Path(__file__).parent.parent))


PARSER_INPUT_MAPPING = {
    # parser_name -> (example_input, yaml_config, comparison_file)
    "SEQUENCING_STATUS": [
        ("tsv_examples/ATLASEA_expanded.tsv", "../goat-data/sources/assembly-data/FILE_ATLASEA.types.yaml", "ATLASEA_expanded.tsv"),
        ("tsv_examples/AEGIS_expanded.tsv", "../goat-data/sources/status-lists/FILE_AEGIS.types.yaml", None),  # no S3 source
    ],
    "REFSEQ_ORGANELLES": [
        ("tsv_examples/organelle_test.tsv", "../goat-data/sources/assembly-data/refseq_organelles.types.yaml", "refseq_organelles.tsv"),
    ],
    "BLOBTOOLKIT": [
        ("tsv_examples/blobtoolkit_test_results.tsv/btk.tsv.gz", "../goat-data/sources/btk/btk.types.yaml", "btk.tsv"),
    ],
    "SRA_DATA": [
        ("tsv_examples/sra.tsv", "../goat-data/sources/sra/sra.types.yaml", "sra.tsv"),
    ],
    "GENOMESIZE_KARYOTYPE": [
        ("tsv_examples/gsheets_test/DTOL_Plant_Genome_Size_Estimates.tsv", "../goat-data/sources/genomesize-karyotype/FILE_DTOL_Plant_Genome_Size_Estimates.types.yaml", None),
    ],
}

S3_SOURCES_ROOT = Path(__file__).parent.parent.parent / "goat-data_s3_sources"


def load_tsv_headers_and_count(path: str) -> Tuple[List[str], int]:
    """Load TSV headers and line count (excluding header)."""
    if path.endswith(".gz"):
        fh = gzip.open(path, "rt", encoding="utf-8", newline="")
    else:
        fh = open(path, "rt", encoding="utf-8", newline="")

    try:
        reader = DictReader(fh, delimiter="\t")
        headers = reader.fieldnames or []
        count = sum(1 for _ in reader)
        return list(headers), count
    finally:
        fh.close()


def find_s3_source(expected_name: str) -> Optional[str]:
    """Locate the S3 source file for a parser output."""
    # Search in all subdirectories
    for root, dirs, files in os.walk(S3_SOURCES_ROOT):
        for f in files:
            if f == expected_name or f == f"{expected_name}.gz":
                return os.path.join(root, f)
    return None


def run_parser(
    parser_name: str,
    input_path: str,
    yaml_path: str,
    work_dir: str,
) -> Tuple[bool, str, Optional[str]]:
    """Run a parser and return (success, output_file, error_msg)."""
    try:
        import shutil

        # Copy input to work_dir with a renamed prefix to avoid conflicting with output names
        input_abs = Path(input_path).resolve()
        work_path = Path(work_dir)
        # Rename to avoid output file conflicts (e.g., input btk.tsv.gz vs output btk.tsv.gz)
        work_input = work_path / f"_input_{input_abs.name}"

        if not work_input.exists():
            shutil.copy2(str(input_abs), str(work_input))

        # Construct the Python module path from parser name
        module_name = "flows.parsers." + "parse_" + parser_name.lower().replace("_", "_")

        cmd = [
            sys.executable,
            "-m",
            module_name,
            "-i",
            str(work_input),
            "-y",
            yaml_path,
        ]

        env = os.environ.copy()
        env["SKIP_PREFECT"] = "true"

        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        if result.returncode != 0:
            return False, None, f"Parser failed: {result.stderr}"

        # Find the output file in work_dir (excluding the input file and any _input_* files)
        output_files = list(work_path.glob("*.tsv")) + list(work_path.glob("*.tsv.gz"))
        output_files = [f for f in output_files if not f.name.startswith("_input_")]

        if not output_files:
            return False, None, "No output file generated"

        output_file = str(output_files[0])
        return True, output_file, None

    except subprocess.TimeoutExpired:
        return False, None, "Parser timeout"
    except Exception as e:
        return False, None, str(e)


def compare_outputs(
    parsed_output: str,
    s3_source: str,
) -> Dict[str, any]:
    """Compare parsed output against S3 source."""
    try:
        parsed_headers, parsed_count = load_tsv_headers_and_count(parsed_output)
        s3_headers, s3_count = load_tsv_headers_and_count(s3_source)

        headers_match = set(parsed_headers) == set(s3_headers)
        headers_extra = set(parsed_headers) - set(s3_headers)
        headers_missing = set(s3_headers) - set(parsed_headers)

        count_diff = abs(parsed_count - s3_count)
        count_pct_diff = 100.0 * count_diff / max(s3_count, 1)

        return {
            "headers_match": headers_match,
            "headers_extra": list(headers_extra),
            "headers_missing": list(headers_missing),
            "parsed_count": parsed_count,
            "s3_count": s3_count,
            "count_diff": count_diff,
            "count_pct_diff": count_pct_diff,
            "line_counts_similar": count_pct_diff < 10,  # Allow 10% variance
        }
    except Exception as e:
        return {"error": str(e)}


def run_tests(parser_name: Optional[str] = None, verbose: bool = False) -> int:
    """Run tests for specified parser(s) and compare outputs."""
    # Determine which parsers to test
    if parser_name:
        parsers_to_test = [parser_name.upper()]
        if parser_name.upper() not in PARSER_INPUT_MAPPING:
            print(f"Error: Parser {parser_name} not configured in PARSER_INPUT_MAPPING")
            return 1
    else:
        parsers_to_test = list(PARSER_INPUT_MAPPING.keys())

    results = {}

    for pname in parsers_to_test:
        print(f"\n{'='*70}")
        print(f"Testing parser: {pname}")
        print(f"{'='*70}")

        if pname not in PARSER_INPUT_MAPPING:
            print(f"  ⚠️  No test configuration found")
            continue

        test_configs = PARSER_INPUT_MAPPING[pname]
        parser_results = []

        for input_path, yaml_path, comparison_file in test_configs:
            input_abs = Path(__file__).parent.parent / input_path

            if not input_abs.exists():
                print(f"  ⚠️  Input not found: {input_path}")
                parser_results.append({"status": "skipped", "reason": "input_not_found"})
                continue

            print(f"\n  Input: {input_path}")
            print(f"  YAML:  {yaml_path}")

            # Run parser in temp directory
            with tempfile.TemporaryDirectory() as tmpdir:
                yaml_abs = Path(__file__).parent.parent / yaml_path

                success, output_file, error = run_parser(
                    pname,
                    str(input_abs),
                    str(yaml_abs),
                    tmpdir,
                )

                if not success:
                    print(f"  ❌ Parser failed: {error}")
                    parser_results.append({"status": "failed", "error": error})
                    continue

                print(f"  ✓ Parser succeeded")
                output_headers, output_count = load_tsv_headers_and_count(output_file)
                print(f"    Output: {os.path.basename(output_file)} ({output_count} rows, {len(output_headers)} cols)")

                # Compare against S3 source if available
                if comparison_file:
                    s3_source = find_s3_source(comparison_file)

                    if s3_source:
                        print(f"  Comparing against S3 source: {comparison_file}")
                        comparison = compare_outputs(output_file, s3_source)

                        if "error" in comparison:
                            print(f"    ⚠️  Comparison failed: {comparison['error']}")
                        else:
                            s3_headers, s3_count = load_tsv_headers_and_count(s3_source)
                            print(f"    S3 source: {s3_count} rows, {len(s3_headers)} cols")

                            if comparison["headers_match"]:
                                print(f"    ✓ Headers match")
                            else:
                                print(f"    ❌ Headers mismatch:")
                                if comparison["headers_extra"]:
                                    print(f"       Extra: {comparison['headers_extra']}")
                                if comparison["headers_missing"]:
                                    print(f"       Missing: {comparison['headers_missing']}")

                            if comparison["line_counts_similar"]:
                                print(f"    ✓ Line counts similar (~{comparison['count_pct_diff']:.1f}% diff)")
                            else:
                                print(f"    ⚠️  Line counts differ substantially:")
                                print(f"       Parsed: {comparison['parsed_count']}, S3: {comparison['s3_count']} ({comparison['count_pct_diff']:.1f}% diff)")

                        parser_results.append({
                            "status": "success",
                            "comparison": comparison,
                        })
                    else:
                        print(f"    ℹ️  No S3 source found for {comparison_file}")
                        parser_results.append({
                            "status": "success",
                            "comparison": None,
                        })
                else:
                    parser_results.append({
                        "status": "success",
                        "comparison": None,
                    })

        results[pname] = parser_results

    # Summary
    print(f"\n{'='*70}")
    print("Summary")
    print(f"{'='*70}")

    all_passed = True
    for pname, presults in results.items():
        passed = sum(1 for r in presults if r.get("status") == "success" and (r.get("comparison") is None or r["comparison"].get("headers_match") and r["comparison"].get("line_counts_similar")))
        total = len(presults)
        status = "✓" if passed == total else "❌"
        print(f"{status} {pname}: {passed}/{total} passed")
        if passed < total:
            all_passed = False

    return 0 if all_passed else 1


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--parser",
        help="Test a specific parser by name (e.g., SEQUENCING_STATUS)",
        default=None,
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all configured tests",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose output",
    )

    args = parser.parse_args()

    sys.exit(run_tests(parser_name=args.parser, verbose=args.verbose))


if __name__ == "__main__":
    main()
