"""Tests for generate_assembly_summary.py (Phase 2.2).

Covers the per-base-accession aggregation: version ordering and gaps, the
superseded count, the first row to meet the EBP metric, and the treatment of
the literal string "None" that upstream writes for an absent value.

The last of those is a cross-phase invariant rather than a local detail:
Phase 2 and Phase 3 both decide which rows carry an EBP metric, and a row the
summary counts but the milestones do not would leave the two outputs
disagreeing about the same assembly.
"""

import csv
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.lib.compute_taxon_milestones import _has_metric  # noqa: E402
from flows.lib.generate_assembly_summary import (  # noqa: E402
    OUTPUT_TSV,
    SUMMARY_FIELDNAMES,
    _has_ebp_metric,
    find_version_gaps,
    generate_assembly_summary,
    generate_summary_for_base,
    load_assemblies,
)

TAXID = 9001


def row(accession, version_status="current", ebp="", release=None, taxid=TAXID):
    """Build a minimal assembly row as the summary reads it."""
    version = int(accession.split(".")[1])
    return {
        "genbankAccession": accession,
        "accession": accession,
        "versionStatus": version_status,
        "taxId": str(taxid),
        "releaseDate": f"20{10 + version:02d}-01-01" if release is None else release,
        "ebpStandardDate": ebp,
    }


def write_tsv(path, rows):
    """Write rows to a tab-separated file."""
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path):
    """Read a tab-separated file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


class TestVersionGaps:
    def test_no_gaps_in_a_complete_run(self):
        assert find_version_gaps([1, 2, 3]) == ""

    def test_reports_every_missing_version(self):
        assert find_version_gaps([1, 4]) == "2,3"

    def test_a_series_starting_above_one_is_all_gap(self):
        assert find_version_gaps([3]) == "1,2"

    def test_no_versions_is_not_a_gap(self):
        assert find_version_gaps([]) == ""


class TestSummaryForBase:
    def test_orders_by_version_not_by_row_order(self):
        summary = generate_summary_for_base(
            "GCA_1",
            [row("GCA_1.3"), row("GCA_1.1", "superseded"), row("GCA_1.2", "superseded")],
        )
        assert summary["first_version_accession"] == "GCA_1.1"
        assert summary["current_version_accession"] == "GCA_1.3"
        assert summary["total_versions"] == 3
        assert summary["superseded_versions"] == 2

    def test_first_ebp_metric_is_the_earliest_qualifying_version(self):
        summary = generate_summary_for_base(
            "GCA_1",
            [
                row("GCA_1.1", "superseded"),
                row("GCA_1.2", "superseded", ebp="2016-01-01"),
                row("GCA_1.3", ebp="2018-01-01"),
            ],
        )
        assert summary["first_ebp_metric_accession"] == "GCA_1.2"
        assert summary["first_ebp_metric_version"] == 2

    def test_no_ebp_metric_leaves_the_columns_empty(self):
        summary = generate_summary_for_base("GCA_1", [row("GCA_1.1")])
        assert summary["first_ebp_metric_accession"] == ""
        assert summary["first_ebp_metric_version"] == ""

    def test_a_row_with_no_version_status_counts_as_current(self):
        summary = generate_summary_for_base(
            "GCA_1", [{"accession": "GCA_1.1", "genbankAccession": "GCA_1.1"}]
        )
        assert summary["superseded_versions"] == 0


class TestNoneSentinel:
    """Upstream writes the string "None" wherever a value was absent."""

    def test_ebp_metric_of_none_is_not_a_metric(self):
        assert _has_ebp_metric(row("GCA_1.1", ebp="None")) is False

    def test_phase_2_and_phase_3_agree_on_the_metric(self):
        for value in ("", "None", "2016-01-01"):
            assembly = row("GCA_1.1", ebp=value)
            assert _has_ebp_metric(assembly) == _has_metric(assembly), value

    def test_a_none_release_date_is_summarised_as_empty(self):
        summary = generate_summary_for_base(
            "GCA_1", [row("GCA_1.1", release="None")]
        )
        assert summary["first_version_date"] == ""
        assert summary["current_version_date"] == ""

    def test_a_none_taxid_is_summarised_as_empty(self):
        rows = [row("GCA_1.1")]
        rows[0]["taxId"] = "None"
        assert generate_summary_for_base("GCA_1", rows)["taxId"] == ""


class TestLoadAssemblies:
    def test_reads_both_files_and_normalises_the_accession(self, tmp_path):
        write_tsv(tmp_path / "assembly_current.tsv", [row("GCA_1.2")])
        write_tsv(
            tmp_path / "assembly_historical.tsv", [row("GCA_1.1", "superseded")]
        )
        rows = load_assemblies(
            str(tmp_path / "assembly_current.tsv"),
            str(tmp_path / "assembly_historical.tsv"),
        )
        assert [r["accession"] for r in rows] == ["GCA_1.2", "GCA_1.1"]

    def test_a_missing_file_is_a_warning_not_a_failure(self, tmp_path):
        write_tsv(tmp_path / "assembly_current.tsv", [row("GCA_1.1")])
        rows = load_assemblies(
            str(tmp_path / "assembly_current.tsv"),
            str(tmp_path / "assembly_historical.tsv"),
        )
        assert len(rows) == 1


class TestFlow:
    @pytest.fixture
    def work_dir(self, tmp_path):
        write_tsv(
            tmp_path / "assembly_current.tsv",
            [row("GCA_1.3", ebp="2018-01-01"), row("GCA_2.1")],
        )
        write_tsv(
            tmp_path / "assembly_historical.tsv",
            [row("GCA_1.1", "superseded")],
        )
        generate_assembly_summary(work_dir=str(tmp_path))
        return tmp_path

    def test_writes_one_row_per_base_accession(self, work_dir):
        rows = read_tsv(work_dir / OUTPUT_TSV)
        assert {r["base_accession"] for r in rows} == {"GCA_1", "GCA_2"}

    def test_header_matches_the_schema(self, work_dir):
        with open(work_dir / OUTPUT_TSV, encoding="utf-8") as f:
            header = f.readline().rstrip("\n").split("\t")
        assert header == SUMMARY_FIELDNAMES

    def test_the_missing_middle_version_is_reported_as_a_gap(self, work_dir):
        rows = {r["base_accession"]: r for r in read_tsv(work_dir / OUTPUT_TSV)}
        assert rows["GCA_1"]["version_gaps"] == "2"
        assert rows["GCA_2"]["version_gaps"] == ""

    def test_no_data_writes_no_output(self, tmp_path):
        generate_assembly_summary(work_dir=str(tmp_path))
        assert not (tmp_path / OUTPUT_TSV).exists()
