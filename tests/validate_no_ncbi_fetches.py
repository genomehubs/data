"""Validate that the daily assembly-version parse makes no NCBI fetches.

Phase 4 step 2. Scope is parse_assembly_versions only, not the daily pipeline
as a whole: update_ncbi_datasets pulls the bulk JSONL on every run by design.
The premise of parse_assembly_versions is that superseded rows are copied from
the previous parse and gaps are checked against assembly_historical.tsv, so
zero network calls is the correct assertion for any input, not only unchanged
ones.

Two checks:

    no-network        a run over a fixture with real supersessions and a real
                      gap completes with sockets and subprocesses blocked
    unchanged-input   a run whose JSONL matches the previous parse reports
                      zero superseded and zero missing versions, and leaves
                      assembly_historical.tsv byte-identical

The second is what "minimise fetches" actually means day to day: an unchanged
input hands update_assembly_versions nothing to fetch. Before the PR-B diff
fix it reported every unchanged multi-version assembly as missing its
predecessor, so this check would have failed on roughly 3,694 entries.

Usage:
    python -m tests.validate_no_ncbi_fetches
    python -m tests.validate_no_ncbi_fetches --work_dir tmp
"""

import csv
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from glob import glob
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("SKIP_PREFECT", "true")

from flows.lib.assembly_lineage import rank_column  # noqa: E402
from flows.lib.assembly_versions_utils import (  # noqa: E402
    CURRENT_TSV_DEFAULT,
    HISTORICAL_TSV_NAME,
)
from flows.lib.shared_args import WORK_DIR  # noqa: E402
from flows.lib.shared_args import parse_args as _parse_args  # noqa: E402
from flows.parsers.parse_assembly_versions import (  # noqa: E402
    parse_assembly_versions,
)

JSONL_NAME = "assembly_data_report.jsonl"
PREVIOUS_TSV_NAME = f"{CURRENT_TSV_DEFAULT}.previous"

# Columns a real current TSV carries, including the upstream lineage columns.
FIXTURE_COLUMNS = [
    "genbankAccession",
    "assemblyID",
    "assemblyName",
    "versionStatus",
    "taxId",
    "organismName",
    "releaseDate",
    "bioProjectAccession",
    "ebpStandardDate",
] + [rank_column(rank) for rank in ("genus", "family", "order")]

# base accession -> (version current at the last run, version current today).
# Covers the four diff paths: unchanged single-version, unchanged
# multi-version, a +1 supersession, and a supersession skipping two versions.
FIXTURE_BASES = {
    "GCA_000000001": (1, 1),
    "GCA_000000002": (3, 3),
    "GCA_000000003": (1, 2),
    "GCA_000000004": (2, 5),
}

# Versions already backfilled into assembly_historical.tsv, so the gap check
# has something local to reconcile against.
FIXTURE_HISTORICAL = {
    "GCA_000000002": [1, 2],
    "GCA_000000004": [1, 3],
}


class NetworkBlocked(RuntimeError):
    """Raised when code under validation attempts to reach the network."""


def _blocked(*args, **kwargs):
    """Stand in for any call that would open a connection."""
    raise NetworkBlocked(
        "parse_assembly_versions attempted a network call; it must run "
        "entirely off the previous parse and the historical TSV"
    )


def fixture_row(base: str, version: int, status: str = "current") -> dict:
    """Build one realistic assembly row.

    Args:
        base (str): Base accession without a version suffix.
        version (int): Version number.
        status (str): versionStatus value.

    Returns:
        dict: A row carrying every column in FIXTURE_COLUMNS.
    """
    index = int(base[-1])
    row = {
        "genbankAccession": f"{base}.{version}",
        "assemblyID": f"{base}_{version}",
        "assemblyName": f"asm{index}v{version}",
        "versionStatus": status,
        "taxId": str(9000 + index),
        "organismName": f"Fixture organism {index}",
        "releaseDate": f"20{10 + version:02d}-01-01",
        "bioProjectAccession": "PRJNA533106",
        "ebpStandardDate": "",
        rank_column("genus"): str(8000 + index),
        rank_column("family"): "7000",
        rank_column("order"): "6000",
    }
    return row


def write_fixture(work_dir: str, changed: bool = True) -> tuple[str, str, str]:
    """Write a previous TSV, a historical TSV and today's JSONL.

    Args:
        work_dir (str): Directory to write the fixture into.
        changed (bool): When False, today's JSONL holds exactly the versions
            current at the last run, which is the unchanged-input case.

    Returns:
        tuple: (jsonl, previous_tsv, historical_tsv) paths.
    """
    previous_tsv = os.path.join(work_dir, PREVIOUS_TSV_NAME)
    historical_tsv = os.path.join(work_dir, HISTORICAL_TSV_NAME)
    jsonl = os.path.join(work_dir, JSONL_NAME)

    with open(previous_tsv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIXTURE_COLUMNS, delimiter="\t")
        writer.writeheader()
        for base, (previous_version, _) in FIXTURE_BASES.items():
            writer.writerow(fixture_row(base, previous_version))

    with open(historical_tsv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIXTURE_COLUMNS, delimiter="\t")
        writer.writeheader()
        for base, versions in FIXTURE_HISTORICAL.items():
            for version in versions:
                writer.writerow(fixture_row(base, version, status="superseded"))

    with open(jsonl, "w", encoding="utf-8") as f:
        for base, (previous_version, new_version) in FIXTURE_BASES.items():
            version = new_version if changed else previous_version
            f.write(
                json.dumps(
                    {
                        "accession": f"{base}.{version}",
                        "releaseDate": f"20{10 + version:02d}-01-01",
                    }
                )
                + "\n"
            )

    return jsonl, previous_tsv, historical_tsv


def copy_real_inputs(source_dir: str, work_dir: str) -> tuple[str, str, str]:
    """Copy a real working directory into a scratch directory.

    parse_assembly_versions rewrites the historical TSV, so a run against a
    real work_dir happens on a copy rather than in place.

    Args:
        source_dir (str): Directory holding the real JSONL, previous TSV and
            historical TSV.
        work_dir (str): Scratch directory to copy them into.

    Returns:
        tuple: (jsonl, previous_tsv, historical_tsv) paths in work_dir.

    Raises:
        FileNotFoundError: If no JSONL or no previous TSV is present.
    """
    jsonl_paths = sorted(glob(os.path.join(source_dir, "*.jsonl")))
    previous_paths = sorted(glob(os.path.join(source_dir, "*.previous")))
    if not jsonl_paths:
        raise FileNotFoundError(f"No jsonl file found in {source_dir}")
    if not previous_paths:
        raise FileNotFoundError(f"No previous parse (*.previous) in {source_dir}")

    jsonl = shutil.copy(jsonl_paths[0], work_dir)
    previous_tsv = shutil.copy(previous_paths[0], work_dir)

    historical_source = os.path.join(source_dir, HISTORICAL_TSV_NAME)
    historical_tsv = os.path.join(work_dir, HISTORICAL_TSV_NAME)
    if os.path.exists(historical_source):
        shutil.copy(historical_source, historical_tsv)

    return jsonl, previous_tsv, historical_tsv


def run_offline(jsonl: str, previous_tsv: str, historical_tsv: str) -> dict:
    """Run parse_assembly_versions with the network blocked.

    socket.socket covers every HTTP client the flow could reach for, and
    subprocess covers the datasets CLI, which is the other way a fetch could
    happen. Both raise NetworkBlocked rather than failing quietly.

    Args:
        jsonl (str): Path to today's assembly_data_report.jsonl.
        previous_tsv (str): Path to the previous parse.
        historical_tsv (str): Path to assembly_historical.tsv.

    Returns:
        dict: The parse_assembly_versions summary.
    """
    with mock.patch.object(socket, "socket", _blocked), mock.patch.object(
        socket, "create_connection", _blocked
    ), mock.patch.object(socket, "getaddrinfo", _blocked), mock.patch.object(
        subprocess, "Popen", _blocked
    ):
        return parse_assembly_versions(
            new_jsonl=jsonl,
            previous_tsv=previous_tsv,
            historical_tsv=historical_tsv,
        )


def check_no_network(source_dir: str = None) -> list[str]:
    """Run a parse that has real work to do, with the network blocked.

    Args:
        source_dir (str, optional): Real working directory to copy inputs
            from; the built-in fixture is used when omitted.

    Returns:
        list: Problems, empty when the run completed offline.
    """
    problems = []
    with tempfile.TemporaryDirectory() as work_dir:
        if source_dir:
            paths = copy_real_inputs(source_dir, work_dir)
        else:
            paths = write_fixture(work_dir, changed=True)

        try:
            results = run_offline(*paths)
        except NetworkBlocked as e:
            return [str(e)]

        if not source_dir and results["newly_superseded_count"] == 0:
            problems.append(
                "the fixture supersedes two assemblies but the parse found "
                "none, so the offline run proved nothing"
            )
        if not source_dir and results["missing_versions_count"] == 0:
            problems.append(
                "the fixture leaves one version genuinely missing but the "
                "parse reported none, so the gap path was never exercised"
            )

    return problems


def check_unchanged_input() -> list[str]:
    """Run a parse whose input is unchanged since the last run.

    Returns:
        list: Problems, empty when the run supersedes nothing, reports no
            missing versions and leaves the historical TSV untouched.
    """
    problems = []
    with tempfile.TemporaryDirectory() as work_dir:
        jsonl, previous_tsv, historical_tsv = write_fixture(work_dir, changed=False)
        with open(historical_tsv, "rb") as f:
            before = f.read()

        try:
            results = run_offline(jsonl, previous_tsv, historical_tsv)
        except NetworkBlocked as e:
            return [str(e)]

        if results["newly_superseded_count"]:
            problems.append(
                f"{results['newly_superseded_count']} assemblies reported as "
                "newly superseded by an unchanged input"
            )
        if results["missing_versions_count"]:
            problems.append(
                f"{results['missing_versions_count']} versions reported "
                "missing by an unchanged input, each one a fetch that is not "
                "needed"
            )

        with open(historical_tsv, "rb") as f:
            if f.read() != before:
                problems.append(
                    "assembly_historical.tsv was rewritten by a run that had "
                    "nothing to add"
                )

    return problems


def validate_no_ncbi_fetches(work_dir: str = None, max_examples: int = 5) -> int:
    """Run both checks and print a report.

    Args:
        work_dir (str, optional): Real working directory to validate against;
            the built-in fixture is used when omitted.
        max_examples (int): Problems to print per failing check.

    Returns:
        int: Number of failing checks; 0 when the parse stays offline.
    """
    separator = "=" * 80
    print(f"\n{separator}")
    print("NCBI FETCH VALIDATION")
    print(f"{separator}\n")
    print(f"  Inputs: {work_dir or 'built-in fixture'}\n")

    results = [
        {"name": "no-network", "problems": check_no_network(work_dir)},
        {"name": "unchanged-input", "problems": check_unchanged_input()},
    ]

    failures = 0
    for result in results:
        problems = result["problems"]
        if not problems:
            print(f"  PASS  {result['name']}")
            continue
        failures += 1
        plural = "" if len(problems) == 1 else "s"
        print(f"  FAIL  {result['name']} ({len(problems)} problem{plural})")
        for problem in problems[:max_examples]:
            print(f"          {problem}")

    print(f"\n{separator}")
    if failures:
        print(f"NCBI FETCH VALIDATION FAILED: {failures} checks")
    else:
        print("NCBI FETCH VALIDATION PASSED")
    print(f"{separator}\n")

    return failures


if __name__ == "__main__":
    args = _parse_args(
        [WORK_DIR],
        description="Validate that the daily version parse makes no NCBI fetches",
    )
    # WORK_DIR defaults to ".", which here means "use the built-in fixture".
    source_dir = args.work_dir if args.work_dir not in (None, ".") else None
    raise SystemExit(1 if validate_no_ncbi_fetches(work_dir=source_dir) else 0)
