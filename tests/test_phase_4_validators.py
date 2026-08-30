"""Tests for the Phase 4 validators.

validate_pipeline is exercised against a work_dir built by running the real
Phase 2 and Phase 3 flows over fixture assembly TSVs, so the happy path is a
genuine end-to-end check rather than four hand-written files that agree by
construction. Each failure case then perturbs exactly one of them.

validate_no_ncbi_fetches is exercised on its own fixture, including a
deliberate network call to prove the block is real rather than incidental.
"""

import csv
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.lib.assembly_lineage import lineage_columns, rank_column  # noqa: E402
from flows.lib.compute_taxon_milestones import (  # noqa: E402
    compute_taxon_milestones,
)
from flows.lib.generate_assembly_summary import (  # noqa: E402
    generate_assembly_summary,
)
from tests import validate_no_ncbi_fetches as no_fetches  # noqa: E402
from tests import validate_pipeline as validator  # noqa: E402

GENUS_TAXID = 8001
FAMILY_TAXID = 7001
ORDER_TAXID = 6001

ASSEMBLY_COLUMNS = [
    "genbankAccession",
    "assemblyID",
    "versionStatus",
    "taxId",
    "releaseDate",
    "bioProjectAccession",
    "ebpStandardDate",
    "superseded_by",
    "superseded_by_version",
    "superseded_date",
] + lineage_columns()

# base accession -> (taxid, [versions], current version)
FIXTURE = {
    "GCA_000000001": (9001, [1, 2]),
    "GCA_000000002": (9002, [1, 2, 3]),
    "GCA_000000003": (9003, [1]),
}


def assembly_row(base, version, taxid, current, ebp=""):
    """Build one assembly row, superseded unless it is the current version."""
    row = {name: "" for name in ASSEMBLY_COLUMNS}
    row.update(
        {
            "genbankAccession": f"{base}.{version}",
            "assemblyID": f"{base}_{version}",
            "versionStatus": "current" if version == current else "superseded",
            "taxId": str(taxid),
            "releaseDate": f"20{10 + version:02d}-01-01",
            "bioProjectAccession": "PRJNA533106",
            "ebpStandardDate": ebp,
            rank_column("genus"): str(GENUS_TAXID),
            rank_column("family"): str(FAMILY_TAXID),
            rank_column("order"): str(ORDER_TAXID),
        }
    )
    if version != current:
        row["superseded_by"] = f"{base}.{version + 1}"
        row["superseded_by_version"] = str(version + 1)
        row["superseded_date"] = f"20{11 + version:02d}-01-01"
    return row


def write_tsv(path, rows, fieldnames=None):
    """Write a list of dicts to a tab-separated file."""
    fieldnames = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path):
    """Read a tab-separated file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.fixture
def work_dir(tmp_path):
    """A working directory holding all four pipeline outputs."""
    current, historical = [], []
    for base, (taxid, versions) in FIXTURE.items():
        latest = max(versions)
        for version in versions:
            ebp = "2015-01-01" if base == "GCA_000000002" and version == 3 else ""
            row = assembly_row(base, version, taxid, latest, ebp=ebp)
            (current if version == latest else historical).append(row)

    write_tsv(tmp_path / "assembly_current.tsv", current, ASSEMBLY_COLUMNS)
    write_tsv(tmp_path / "assembly_historical.tsv", historical, ASSEMBLY_COLUMNS)

    generate_assembly_summary(work_dir=str(tmp_path))
    compute_taxon_milestones(work_dir=str(tmp_path))
    return tmp_path


def failing(results):
    """Return the names of checks that reported problems."""
    return {r["name"] for r in results if r["problems"]}


def run(work_dir):
    """Load and check a working directory, returning the check results."""
    return validator.run_checks(validator.load_pipeline_outputs(str(work_dir)))


# ---------------------------------------------------------------------------
# validate_pipeline
# ---------------------------------------------------------------------------

class TestValidPipeline:
    def test_a_clean_run_passes_every_check(self, work_dir):
        assert failing(run(work_dir)) == set()
        assert validator.validate_pipeline(work_dir=str(work_dir)) == 0

    def test_strict_also_passes_when_lineage_columns_are_present(self, work_dir):
        assert validator.validate_pipeline(work_dir=str(work_dir), strict=True) == 0

    def test_all_four_outputs_are_found(self, work_dir):
        outputs = validator.load_pipeline_outputs(str(work_dir))
        assert all(output["found"] for output in outputs.values())
        assert len(outputs["current"]["rows"]) == 3
        assert len(outputs["historical"]["rows"]) == 3


class TestOutputsPresent:
    def test_missing_output_is_reported(self, work_dir):
        (work_dir / "taxon_milestone_summary.tsv").unlink()
        assert "outputs-present" in failing(run(work_dir))

    def test_empty_work_dir_fails(self, tmp_path):
        assert validator.validate_pipeline(work_dir=str(tmp_path)) > 0


class TestAssemblyIdUniqueness:
    def test_duplicate_across_current_and_historical(self, work_dir):
        rows = read_tsv(work_dir / "assembly_historical.tsv")
        rows[0]["assemblyID"] = "GCA_000000001_2"  # already current
        write_tsv(work_dir / "assembly_historical.tsv", rows, ASSEMBLY_COLUMNS)
        assert "assembly-id-uniqueness" in failing(run(work_dir))

    def test_rows_without_an_assembly_id_are_reported(self, work_dir):
        rows = read_tsv(work_dir / "assembly_current.tsv")
        rows[0]["assemblyID"] = ""
        write_tsv(work_dir / "assembly_current.tsv", rows, ASSEMBLY_COLUMNS)
        problems = validator.check_assembly_ids_unique(rows, [])
        assert problems == ["1 rows carry no assembly ID"]

    def test_lowercase_spelling_still_collides(self):
        # get_assembly_id reads assemblyID and assemblyId alike, so the same
        # assembly written both ways must not read as two distinct rows.
        current = [{"genbankAccession": "GCA_1.1", "assemblyId": "GCA_1_1"}]
        historical = [{"genbankAccession": "GCA_1.1", "assemblyID": "GCA_1_1"}]
        assert validator.check_assembly_ids_unique(current, historical)


class TestVersionGaps:
    def test_summary_gap_field_must_match_the_versions_present(self, work_dir):
        rows = read_tsv(work_dir / "assembly_version_summary.tsv")
        rows[0]["version_gaps"] = "2"
        write_tsv(work_dir / "assembly_version_summary.tsv", rows)
        assert "version-gaps" in failing(run(work_dir))

    def test_a_real_gap_must_be_reported(self, work_dir):
        # Drop v2 of the three-version base: the summary still says no gaps.
        rows = [
            row
            for row in read_tsv(work_dir / "assembly_historical.tsv")
            if row["genbankAccession"] != "GCA_000000002.2"
        ]
        write_tsv(work_dir / "assembly_historical.tsv", rows, ASSEMBLY_COLUMNS)
        assert "version-gaps" in failing(run(work_dir))

    def test_gaps_reconcile_when_the_summary_agrees(self):
        rows = [
            {"genbankAccession": "GCA_1.1"},
            {"genbankAccession": "GCA_1.3"},
        ]
        summary = [{"base_accession": "GCA_1", "version_gaps": "2"}]
        assert validator.check_version_gaps(rows, summary) == []


class TestReferentialIntegrity:
    def test_dangling_superseded_by(self, work_dir):
        rows = read_tsv(work_dir / "assembly_historical.tsv")
        rows[0]["superseded_by"] = "GCA_999999999.9"
        write_tsv(work_dir / "assembly_historical.tsv", rows, ASSEMBLY_COLUMNS)
        assert "referential-integrity" in failing(run(work_dir))

    def test_cross_base_superseded_by(self):
        rows = [
            {"genbankAccession": "GCA_1.1", "superseded_by": "GCA_2.1"},
            {"genbankAccession": "GCA_2.1"},
        ]
        problems = validator.check_superseded_by_resolves(rows)
        assert problems and "another base accession" in problems[0]

    def test_chained_supersession_resolves(self):
        # v1 superseded by v2, which is itself superseded by the current v3.
        rows = [
            {"genbankAccession": "GCA_1.1", "superseded_by": "GCA_1.2"},
            {"genbankAccession": "GCA_1.2", "superseded_by": "GCA_1.3"},
            {"genbankAccession": "GCA_1.3"},
        ]
        assert validator.check_superseded_by_resolves(rows) == []


class TestSummaryCompleteness:
    def test_unsummarised_base_is_reported(self, work_dir):
        rows = [
            row
            for row in read_tsv(work_dir / "assembly_version_summary.tsv")
            if row["base_accession"] != "GCA_000000003"
        ]
        write_tsv(work_dir / "assembly_version_summary.tsv", rows)
        assert "summary-completeness" in failing(run(work_dir))

    def test_stale_summary_row_is_reported(self):
        rows = [{"genbankAccession": "GCA_1.1"}]
        summary = [{"base_accession": "GCA_1"}, {"base_accession": "GCA_2"}]
        problems = validator.check_summary_completeness(rows, summary)
        assert problems == ["GCA_2: summarised but absent from the TSVs"]


class TestMilestoneInvariants:
    def test_inverted_dates_are_reported(self, work_dir):
        rows = read_tsv(work_dir / "taxon_milestone_summary.tsv")
        rows[0]["first_assembly_date"] = "2099-01-01"
        write_tsv(work_dir / "taxon_milestone_summary.tsv", rows)
        assert "milestone-date-ordering" in failing(run(work_dir))

    def test_each_nesting_pair_is_checked(self):
        pairs = [
            ("first_assembly_date", "first_metric_date"),
            ("first_metric_date", "first_ebp_metric_date"),
            ("first_assembly_date", "first_ebp_assembly_date"),
            ("first_ebp_assembly_date", "first_ebp_metric_date"),
        ]
        for earlier, later in pairs:
            row = {"taxid": "1", earlier: "2020-01-01", later: "2010-01-01"}
            assert validator.check_milestone_date_ordering([row])

    def test_absent_dates_are_not_compared(self):
        row = {"taxid": "1", "first_assembly_date": "2020-01-01"}
        assert validator.check_milestone_date_ordering([row]) == []

    def test_non_canonical_in_ranks_is_reported(self, work_dir):
        rows = read_tsv(work_dir / "taxon_milestone_summary.tsv")
        rows[0]["first_assembly_in_ranks"] = "genus,subfamily"
        write_tsv(work_dir / "taxon_milestone_summary.tsv", rows)
        assert "in-ranks-validity" in failing(run(work_dir))

    def test_species_is_not_a_canonical_in_rank(self):
        # in_ranks records the higher ranks a species was first in, so
        # "species" appearing there means the sweep leaked its own level.
        row = {"taxid": "1", "first_metric_in_ranks": "species"}
        assert validator.check_in_ranks_valid([row])


class TestLineageColumns:
    def test_missing_columns_warn_but_do_not_fail(self, work_dir):
        keep = [c for c in ASSEMBLY_COLUMNS if c not in lineage_columns()]
        rows = [
            {column: row[column] for column in keep}
            for row in read_tsv(work_dir / "assembly_current.tsv")
        ]
        write_tsv(work_dir / "assembly_current.tsv", rows, keep)

        assert failing(run(work_dir)) == {"lineage-columns"}
        assert validator.validate_pipeline(work_dir=str(work_dir)) == 0
        assert validator.validate_pipeline(work_dir=str(work_dir), strict=True) == 1

    def test_present_but_empty_columns_are_reported(self):
        rows = [{column: "" for column in lineage_columns()}]
        problems = validator.check_lineage_columns(rows)
        assert problems == ["every lineage column is empty on every row of "
                            "the current TSV"]

    def test_partially_populated_columns_are_reported(self):
        rows = [
            {column: "" for column in lineage_columns()},
            dict({column: "" for column in lineage_columns()},
                 **{rank_column("genus"): "8001"}),
        ]
        problems = validator.check_lineage_columns(rows)
        assert problems == ["1 of 2 current rows have an empty lineage"]

    def test_none_sentinel_counts_as_empty(self):
        rows = [{column: "None" for column in lineage_columns()}]
        assert validator.check_lineage_columns(rows)


# ---------------------------------------------------------------------------
# validate_no_ncbi_fetches
# ---------------------------------------------------------------------------

class TestNoNcbiFetches:
    def test_fixture_run_makes_no_fetches(self):
        assert no_fetches.check_no_network() == []

    def test_unchanged_input_supersedes_nothing(self):
        assert no_fetches.check_unchanged_input() == []

    def test_both_checks_pass_together(self):
        assert no_fetches.validate_no_ncbi_fetches() == 0

    def test_the_block_is_real(self, monkeypatch, tmp_path):
        # Swap in a parse that opens a socket: the harness must stop it.
        import socket as socket_module

        def fetching_parse(**kwargs):
            socket_module.socket(socket_module.AF_INET, socket_module.SOCK_STREAM)
            return {"newly_superseded_count": 0, "missing_versions_count": 0}

        monkeypatch.setattr(no_fetches, "parse_assembly_versions", fetching_parse)
        paths = no_fetches.write_fixture(str(tmp_path))
        with pytest.raises(no_fetches.NetworkBlocked):
            no_fetches.run_offline(*paths)

    def test_a_fetching_parse_fails_the_check(self, monkeypatch):
        def fetching_parse(**kwargs):
            import socket as socket_module

            socket_module.create_connection(("ftp.ncbi.nlm.nih.gov", 443))

        monkeypatch.setattr(no_fetches, "parse_assembly_versions", fetching_parse)
        assert no_fetches.check_no_network() != []

    def test_fixture_exercises_the_supersession_and_gap_paths(self, tmp_path):
        jsonl, previous_tsv, historical_tsv = no_fetches.write_fixture(str(tmp_path))
        results = no_fetches.run_offline(jsonl, previous_tsv, historical_tsv)
        assert results["newly_superseded_count"] == 2
        # Only v4 of the skipping base: v3 is already in the historical TSV.
        assert results["missing_versions_count"] == 1
        assert results["missing_versions"][0]["missing_version"] == 4
