"""Tests for parse_assembly_versions.py and update_assembly_versions.py

Covers:
- Loading and indexing previous parsed TSV results
- Building superseded and missing-version records
- Core supersession detection logic (superseded, missing-with-gap, new-series, v1-skip)
- Appending to historical TSV with deduplication
- Parser orchestrator flow behaviour
- Updater flow: fetch metadata and write JSONL
"""

import csv
import gzip
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

TAB = "\t"

HISTORICAL_YAML = (
    Path(__file__).parent.parent / "configs" / "assembly_historical.types.yaml"
)

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.lib.utils import Parser, load_config  # noqa: E402
from flows.parsers import parse_assembly_versions as incremental_module  # noqa: E402
from flows.parsers import (  # noqa: E402
    parse_backfill_historical_versions as backfill_module,
)
from flows.parsers.parse_assembly_versions import (  # noqa: E402
    append_superseded_to_tsv,
    build_gap_records,
    build_missing_version_record,
    build_superseded_row,
    derive_assembly_version_paths,
    identify_newly_superseded,
    load_historical_headers,
    load_previous_parsed_by_base,
    merge_fieldnames,
    parse_assembly_versions,
)
from flows.lib.assembly_versions_utils import (  # noqa: E402
    canonicalize_columns,
    get_accession,
    get_assembly_id,
    get_version_status,
    load_versions_by_base,
    open_tsv,
    resolve_current_tsv_paths,
    resolve_historical_yaml_path,
)
from flows.parsers.parse_backfill_historical_versions import (  # noqa: E402
    read_existing_output,
    resolve_output_path,
    write_or_append_parsed,
)
from flows.parsers.parse_ncbi_assemblies import snapshot_previous_output  # noqa: E402
from flows.updaters import update_assembly_versions as updater_module  # noqa: E402
from flows.updaters.update_assembly_versions import (  # noqa: E402
    load_missing_versions,
    update_assembly_versions,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_tsv(path: Path, rows: list[dict]) -> None:
    """Write a list of dicts to a tab-separated file."""
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path: Path) -> list[dict]:
    """Read a tab-separated file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_jsonl(path: Path, records: list[dict]) -> None:
    """Write a list of dicts as newline-delimited JSON."""
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# TestLoadPreviousParsed
# ---------------------------------------------------------------------------

class TestLoadPreviousParsed:
    """load_previous_parsed_by_base indexes rows by base accession and version."""

    def test_missing_file_returns_empty(self, tmp_path):
        result = load_previous_parsed_by_base(str(tmp_path / "nope.tsv"))
        assert result == {}

    def test_single_version_indexed(self, tmp_path):
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [{"genbankAccession": "GCA_000222935.1", "taxId": "12345"}])
        result = load_previous_parsed_by_base(str(tsv))
        assert "GCA_000222935" in result
        assert 1 in result["GCA_000222935"]
        assert result["GCA_000222935"][1]["taxId"] == "12345"

    def test_multi_version_same_base(self, tmp_path):
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [
            {"genbankAccession": "GCA_000222935.1", "taxId": "1"},
            {"genbankAccession": "GCA_000222935.2", "taxId": "1"},
        ])
        result = load_previous_parsed_by_base(str(tsv))
        assert len(result["GCA_000222935"]) == 2
        assert 1 in result["GCA_000222935"]
        assert 2 in result["GCA_000222935"]

    def test_multiple_base_accessions(self, tmp_path):
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [
            {"genbankAccession": "GCA_000222935.1", "taxId": "1"},
            {"genbankAccession": "GCA_000412225.1", "taxId": "2"},
        ])
        result = load_previous_parsed_by_base(str(tsv))
        assert len(result) == 2
        assert "GCA_000222935" in result
        assert "GCA_000412225" in result

    def test_legacy_accession_column_still_read(self, tmp_path):
        """Rows written before the schema fix used a bare 'accession' column."""
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [{"accession": "GCA_000222935.1", "taxId": "1"}])
        result = load_previous_parsed_by_base(str(tsv))
        assert 1 in result["GCA_000222935"]

    def test_rows_without_accession_skipped(self, tmp_path):
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [
            {"genbankAccession": "", "taxId": "1"},
            {"genbankAccession": "GCA_000222935.1", "taxId": "2"},
        ])
        result = load_previous_parsed_by_base(str(tsv))
        assert list(result) == ["GCA_000222935"]

    def test_gzipped_previous_snapshot_read(self, tmp_path):
        """A gzipped current TSV yields a gzipped .previous snapshot."""
        tsv = tmp_path / "ncbi_datasets_eukaryota.tsv.gz.previous"
        with gzip.open(tsv, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["genbankAccession", "taxId"], delimiter=TAB
            )
            writer.writeheader()
            writer.writerow({"genbankAccession": "GCA_000222935.1", "taxId": "1"})
        result = load_previous_parsed_by_base(str(tsv))
        assert 1 in result["GCA_000222935"]


# ---------------------------------------------------------------------------
# TestBuildSupersededRow
# ---------------------------------------------------------------------------

class TestBuildSupersededRow:
    """build_superseded_row stamps the correct metadata onto a copied row."""

    def _base_row(self):
        return {
            "genbankAccession": "GCA_000222935.1",
            "taxId": "12345",
            "assemblyLevel": "Chromosome",
        }

    def test_version_status_set(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["versionStatus"] == "superseded"

    def test_assembly_id_format(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["assemblyID"] == "GCA_000222935_1"

    def test_superseded_by_fields(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["superseded_by"] == "GCA_000222935.2"
        assert row["superseded_by_version"] == 2
        assert row["superseded_date"] == "2024-01-15"

    def test_original_row_not_mutated(self):
        original = self._base_row()
        build_superseded_row(original, 1, "GCA_000222935.2", 2, "2024-01-15")
        assert "versionStatus" not in original

    def test_existing_fields_preserved(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["taxId"] == "12345"
        assert row["assemblyLevel"] == "Chromosome"


# ---------------------------------------------------------------------------
# TestBuildMissingVersionRecord
# ---------------------------------------------------------------------------

class TestBuildMissingVersionRecord:
    """build_missing_version_record captures the gap details."""

    def test_required_fields(self):
        rec = build_missing_version_record("GCA_000222935", 2, 3, "GCA_000222935.3")
        assert rec["base_accession"] == "GCA_000222935"
        assert rec["missing_version"] == 2
        assert rec["new_version"] == 3
        assert rec["new_accession"] == "GCA_000222935.3"

    def test_no_note_by_default(self):
        rec = build_missing_version_record("GCA_000222935", 1, 2, "GCA_000222935.2")
        assert "note" not in rec

    def test_note_present_for_new_series(self):
        rec = build_missing_version_record(
            "GCA_000222935", 1, 2, "GCA_000222935.2", is_new_series=True
        )
        assert "note" in rec


# ---------------------------------------------------------------------------
# TestIdentifyNewlySuperseded
# ---------------------------------------------------------------------------

class TestIdentifyNewlySuperseded:
    """identify_newly_superseded covers all branching cases."""

    def _write_jsonl(self, tmp_path, records):
        path = tmp_path / "new.jsonl"
        write_jsonl(path, records)
        return str(path)

    def test_v1_assembly_skipped(self, tmp_path):
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.1"}])
        superseded, missing = identify_newly_superseded(jsonl, {})
        assert superseded == []
        assert missing == []

    def test_superseded_found_when_previous_version_present(self, tmp_path):
        jsonl = self._write_jsonl(
            tmp_path, [{"accession": "GCA_000222935.2", "releaseDate": "2024-01-15"}]
        )
        previous = {
            "GCA_000222935": {
                1: {"genbankAccession": "GCA_000222935.1", "taxId": "1"}
            }
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert superseded[0]["superseded_by"] == "GCA_000222935.2"
        assert missing == []

    def test_missing_with_version_gap(self, tmp_path):
        """v1 current yesterday, v3 today: v1 is superseded and v2 is a gap.

        Marking v1 superseded is the F5 fix.  The previous implementation
        looked for v2, failed to find it and dropped v1 on the floor, losing
        that row when assembly_current.tsv was overwritten.
        """
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.3"}])
        previous = {
            "GCA_000222935": {
                1: {"genbankAccession": "GCA_000222935.1", "taxId": "1"}
            }
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert superseded[0]["assemblyID"] == "GCA_000222935_1"
        assert superseded[0]["superseded_by"] == "GCA_000222935.3"
        assert len(missing) == 1
        assert missing[0]["missing_version"] == 2

    def test_new_series_no_prior_base(self, tmp_path):
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_999999999.2"}])
        superseded, missing = identify_newly_superseded(jsonl, {})
        assert superseded == []
        assert len(missing) == 1
        assert missing[0]["note"]

    def test_mixed_batch(self, tmp_path):
        jsonl = self._write_jsonl(tmp_path, [
            {"accession": "GCA_000222935.2", "releaseDate": "2024-01-01"},
            {"accession": "GCA_000412225.1"},
            {"accession": "GCA_999999999.2"},
        ])
        previous = {
            "GCA_000222935": {
                1: {"genbankAccession": "GCA_000222935.1", "taxId": "1"}
            }
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert len(missing) == 1


# ---------------------------------------------------------------------------
# TestSupersessionDiff (F5)
# ---------------------------------------------------------------------------

class TestSupersessionDiff:
    """identify_newly_superseded diffs versions instead of assuming +1.

    Covers the three failure modes of the arithmetic implementation: unchanged
    assemblies reported as missing, a skipped version losing the row it
    superseded, and an already-parsed gap being re-reported forever.
    """

    def _write_jsonl(self, tmp_path, records):
        path = tmp_path / "new.jsonl"
        write_jsonl(path, records)
        return str(path)

    def _previous(self, base, version):
        return {base: {version: {
            "genbankAccession": f"{base}.{version}", "taxId": "1",
        }}}

    # -- failure mode 1: steady-state false positives ------------------------

    def test_unchanged_assembly_reports_nothing(self, tmp_path):
        """v3 yesterday, v3 today: no supersession, and above all no gap."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000002035.3"}])
        previous = self._previous("GCA_000002035", 3)
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert superseded == []
        assert missing == []

    def test_quiet_day_reports_no_missing_versions(self, tmp_path):
        """A batch of unchanged multi-version assemblies is a complete no-op.

        This is the case that previously produced one missing-version record
        per multi-version assembly in the dataset.
        """
        bases = [f"GCA_00000{i}035" for i in range(1, 6)]
        jsonl = self._write_jsonl(
            tmp_path,
            [{"accession": f"{b}.{v}"} for v, b in enumerate(bases, 2)],
        )
        previous = {}
        for version, base in enumerate(bases, 2):
            previous.update(self._previous(base, version))
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert superseded == []
        assert missing == []

    # -- failure mode 2: skipped versions lost -------------------------------

    def test_version_jump_supersedes_previous_and_reports_gap(self, tmp_path):
        """v3 yesterday, v5 today: v3 is superseded and v4 is missing."""
        jsonl = self._write_jsonl(
            tmp_path,
            [{"accession": "GCA_000222935.5", "releaseDate": "2026-01-02"}],
        )
        previous = self._previous("GCA_000222935", 3)
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert superseded[0]["assemblyID"] == "GCA_000222935_3"
        assert superseded[0]["superseded_by_version"] == 5
        assert superseded[0]["superseded_date"] == "2026-01-02"
        assert [m["missing_version"] for m in missing] == [4]

    def test_multi_version_gap_emits_one_record_per_version(self, tmp_path):
        """A gap spanning several versions produces a record for each."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.6"}])
        previous = self._previous("GCA_000222935", 2)
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert [m["missing_version"] for m in missing] == [3, 4, 5]
        assert all(m["new_version"] == 6 for m in missing)
        assert all(m["new_accession"] == "GCA_000222935.6" for m in missing)

    def test_supersedes_the_latest_previous_version(self, tmp_path):
        """With several versions indexed for a base, the newest is superseded."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.4"}])
        previous = {
            "GCA_000222935": {
                1: {"genbankAccession": "GCA_000222935.1", "taxId": "1"},
                3: {"genbankAccession": "GCA_000222935.3", "taxId": "1"},
            }
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert superseded[0]["assemblyID"] == "GCA_000222935_3"
        assert missing == []

    # -- failure mode 3: the historical TSV is never consulted ---------------

    def test_gap_already_in_historical_is_not_reported(self, tmp_path):
        """v4 sitting in assembly_historical.tsv is not a gap."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.5"}])
        previous = self._previous("GCA_000222935", 3)
        historical = {"GCA_000222935": {1, 2, 4}}
        superseded, missing = identify_newly_superseded(
            jsonl, previous, historical
        )
        assert len(superseded) == 1
        assert missing == []

    def test_partial_historical_coverage_reports_the_remainder(self, tmp_path):
        """Only the versions the backfill has not reached are reported."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.6"}])
        previous = self._previous("GCA_000222935", 2)
        historical = {"GCA_000222935": {3, 4}}
        _, missing = identify_newly_superseded(jsonl, previous, historical)
        assert [m["missing_version"] for m in missing] == [5]

    def test_historical_index_for_another_base_is_ignored(self, tmp_path):
        """Versions known for a different base do not mask a real gap."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.5"}])
        previous = self._previous("GCA_000222935", 3)
        historical = {"GCA_000412225": {4}}
        _, missing = identify_newly_superseded(jsonl, previous, historical)
        assert [m["missing_version"] for m in missing] == [4]

    # -- bases new to the pipeline -------------------------------------------

    def test_new_base_at_high_version_reports_every_earlier_version(
        self, tmp_path
    ):
        """A base entering the dataset at v3 needs v1 and v2 backfilled."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_999999999.3"}])
        previous = self._previous("GCA_000222935", 2)
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert superseded == []
        assert [m["missing_version"] for m in missing] == [1, 2]
        assert all(m["note"] for m in missing)

    def test_new_base_gap_suppressed_by_historical(self, tmp_path):
        """A new base whose earlier versions are already parsed is quiet."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_999999999.3"}])
        previous = self._previous("GCA_000222935", 2)
        historical = {"GCA_999999999": {1, 2}}
        superseded, missing = identify_newly_superseded(
            jsonl, previous, historical
        )
        assert superseded == []
        assert missing == []

    def test_new_base_at_v1_is_skipped(self, tmp_path):
        """Nothing precedes v1, so a brand-new v1 base is not a gap."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_999999999.1"}])
        previous = self._previous("GCA_000222935", 2)
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert superseded == []
        assert missing == []

    # -- version regressions --------------------------------------------------

    def test_version_regression_supersedes_nothing(self, tmp_path, capsys):
        """v5 yesterday, v3 today: NCBI withdrew the newer assembly."""
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.3"}])
        previous = self._previous("GCA_000222935", 5)
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert superseded == []
        assert missing == []
        assert "regressed" in capsys.readouterr().out

    def test_no_regression_warning_on_a_clean_run(self, tmp_path, capsys):
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.3"}])
        previous = self._previous("GCA_000222935", 3)
        identify_newly_superseded(jsonl, previous)
        assert "regressed" not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# TestBuildGapRecords
# ---------------------------------------------------------------------------

class TestBuildGapRecords:
    """build_gap_records expands a version range minus what is already known."""

    def test_one_record_per_version(self):
        records = build_gap_records(
            "GCA_000222935", range(3, 6), set(), 6, "GCA_000222935.6"
        )
        assert [r["missing_version"] for r in records] == [3, 4, 5]

    def test_known_versions_filtered_out(self):
        records = build_gap_records(
            "GCA_000222935", range(3, 6), {3, 5}, 6, "GCA_000222935.6"
        )
        assert [r["missing_version"] for r in records] == [4]

    def test_empty_range_yields_nothing(self):
        assert build_gap_records(
            "GCA_000222935", range(4, 4), set(), 4, "GCA_000222935.4"
        ) == []

    def test_new_series_flag_propagates(self):
        records = build_gap_records(
            "GCA_999999999", range(1, 3), set(), 3, "GCA_999999999.3",
            is_new_series=True,
        )
        assert all("note" in r for r in records)


# ---------------------------------------------------------------------------
# TestLoadVersionsByBase
# ---------------------------------------------------------------------------

class TestLoadVersionsByBase:
    """load_versions_by_base indexes a TSV's versions without fetching."""

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_versions_by_base(str(tmp_path / "nope.tsv")) == {}

    def test_empty_file_returns_empty(self, tmp_path):
        path = tmp_path / "historical.tsv"
        path.write_text("", encoding="utf-8")
        assert load_versions_by_base(str(path)) == {}

    def test_versions_grouped_by_base(self, tmp_path):
        path = tmp_path / "historical.tsv"
        write_tsv(path, [
            {"genbankAccession": "GCA_000222935.1", "versionStatus": "superseded"},
            {"genbankAccession": "GCA_000222935.2", "versionStatus": "superseded"},
            {"genbankAccession": "GCA_000412225.1", "versionStatus": "superseded"},
        ])
        assert load_versions_by_base(str(path)) == {
            "GCA_000222935": {1, 2},
            "GCA_000412225": {1},
        }

    def test_accession_alias_accepted(self, tmp_path):
        path = tmp_path / "historical.tsv"
        write_tsv(path, [{"accession": "GCA_000222935.2", "taxId": "1"}])
        assert load_versions_by_base(str(path)) == {"GCA_000222935": {2}}

    def test_gzipped_file_read(self, tmp_path):
        path = tmp_path / "historical.tsv.gz"
        with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["genbankAccession"], delimiter=TAB
            )
            writer.writeheader()
            writer.writerow({"genbankAccession": "GCA_000222935.2"})
        assert load_versions_by_base(str(path)) == {"GCA_000222935": {2}}


# ---------------------------------------------------------------------------
# TestAppendSupersededToTsv
# ---------------------------------------------------------------------------

class TestAppendSupersededToTsv:
    """append_superseded_to_tsv correctly creates, appends, and deduplicates."""

    def _make_row(self, acc, assembly_id, status="superseded"):
        return {
            "genbankAccession": acc,
            "assemblyID": assembly_id,
            "versionStatus": status,
        }

    def test_creates_new_file(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        rows = [self._make_row("GCA_000222935.1", "GCA_000222935_1")]
        append_superseded_to_tsv(rows, str(tsv))
        assert tsv.exists()
        result = read_tsv(tsv)
        assert len(result) == 1
        assert result[0]["genbankAccession"] == "GCA_000222935.1"

    def test_appends_to_existing(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [self._make_row("GCA_000412225.1", "GCA_000412225_1")])
        append_superseded_to_tsv(
            [self._make_row("GCA_000222935.1", "GCA_000222935_1")], str(tsv)
        )
        result = read_tsv(tsv)
        assert len(result) == 2

    def test_dedup_on_assembly_id_keeps_new(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        old_row = {
            "genbankAccession": "GCA_000222935.1",
            "assemblyID": "GCA_000222935_1",
            "versionStatus": "superseded",
            "superseded_by": "GCA_000222935.2",
        }
        write_tsv(tsv, [old_row])
        new_row = dict(old_row)
        new_row["superseded_by"] = "GCA_000222935.3"
        append_superseded_to_tsv([new_row], str(tsv))
        result = read_tsv(tsv)
        assert len(result) == 1
        assert result[0]["superseded_by"] == "GCA_000222935.3"

    def test_no_op_when_empty_list(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        append_superseded_to_tsv([], str(tsv))
        assert not tsv.exists()

    def test_rows_without_an_assembly_id_are_not_collapsed(self, tmp_path):
        # Keying several ID-less rows on the same empty string would keep one
        # and drop the rest on the rewrite.
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [
            self._make_row("GCA_000412225.1", ""),
            self._make_row("GCA_000412225.2", ""),
        ])
        append_superseded_to_tsv(
            [self._make_row("GCA_000222935.1", "GCA_000222935_1")], str(tsv)
        )
        result = read_tsv(tsv)
        assert len(result) == 3
        assert {row["genbankAccession"] for row in result} == {
            "GCA_000412225.1", "GCA_000412225.2", "GCA_000222935.1",
        }

    def test_rows_with_neither_id_nor_accession_survive(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [
            {"genbankAccession": "", "assemblyID": "", "versionStatus": "superseded"},
            {"genbankAccession": "", "assemblyID": "", "versionStatus": "current"},
        ])
        append_superseded_to_tsv(
            [self._make_row("GCA_000222935.1", "GCA_000222935_1")], str(tsv)
        )
        assert len(read_tsv(tsv)) == 3


# ---------------------------------------------------------------------------
# TestIncrementalOrchestrator
# ---------------------------------------------------------------------------

class TestIncrementalOrchestrator:
    """parse_assembly_versions orchestrator behaviour."""

    def test_no_previous_tsv_returns_empty_result(self, tmp_path):
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [{"accession": "GCA_000222935.2"}])
        result = parse_assembly_versions(
            new_jsonl=str(jsonl),
            previous_tsv=str(tmp_path / "nope.tsv"),
            historical_tsv=str(tmp_path / "historical.tsv"),
        )
        assert result["newly_superseded_count"] == 0
        assert result["missing_versions_count"] == 0
        assert result["missing_versions"] == []

    def test_one_superseded_produces_correct_counts(self, tmp_path):
        previous_tsv = tmp_path / "previous.tsv"
        write_tsv(previous_tsv, [
            {"genbankAccession": "GCA_000222935.1", "taxId": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.2", "releaseDate": "2024-01-15"}
        ])
        result = parse_assembly_versions(
            new_jsonl=str(jsonl),
            previous_tsv=str(previous_tsv),
            historical_tsv=str(tmp_path / "historical.tsv"),
        )
        assert result["newly_superseded_count"] == 1
        assert result["missing_versions_count"] == 0

    def test_missing_version_detected_in_orchestrator_result(self, tmp_path):
        """v3 present, v2 missing → missing_versions_count == 1."""
        previous_tsv = tmp_path / "previous.tsv"
        write_tsv(previous_tsv, [
            {"genbankAccession": "GCA_000222935.1", "taxId": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.3", "releaseDate": "2024-06-01"}
        ])
        result = parse_assembly_versions(
            new_jsonl=str(jsonl),
            previous_tsv=str(previous_tsv),
            historical_tsv=str(tmp_path / "historical.tsv"),
        )
        assert result["missing_versions_count"] == 1
        assert result["missing_versions"][0]["base_accession"] == "GCA_000222935"
        assert result["missing_versions"][0]["missing_version"] == 2

    def test_historical_tsv_written(self, tmp_path):
        previous_tsv = tmp_path / "previous.tsv"
        write_tsv(previous_tsv, [
            {"genbankAccession": "GCA_000222935.1", "taxId": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.2", "releaseDate": "2024-01-15"}
        ])
        historical_tsv = tmp_path / "historical.tsv"
        parse_assembly_versions(
            new_jsonl=str(jsonl),
            previous_tsv=str(previous_tsv),
            historical_tsv=str(historical_tsv),
        )
        assert historical_tsv.exists()
        rows = read_tsv(historical_tsv)
        assert len(rows) == 1
        assert rows[0]["versionStatus"] == "superseded"


# ---------------------------------------------------------------------------
# TestOrchestratorConsultsHistorical
# ---------------------------------------------------------------------------

class TestOrchestratorConsultsHistorical:
    """The flow reads assembly_historical.tsv before deciding what is missing."""

    def _setup(self, tmp_path, historical_rows):
        previous_tsv = tmp_path / "previous.tsv"
        write_tsv(previous_tsv, [
            {"genbankAccession": "GCA_000222935.3", "taxId": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.5", "releaseDate": "2026-01-02"}
        ])
        historical_tsv = tmp_path / "historical.tsv"
        if historical_rows:
            write_tsv(historical_tsv, historical_rows)
        return str(jsonl), str(previous_tsv), str(historical_tsv)

    def test_gap_reported_when_historical_lacks_it(self, tmp_path):
        jsonl, previous_tsv, historical_tsv = self._setup(tmp_path, [])
        result = parse_assembly_versions(
            new_jsonl=jsonl,
            previous_tsv=previous_tsv,
            historical_tsv=historical_tsv,
        )
        assert result["newly_superseded_count"] == 1
        assert result["missing_versions_count"] == 1
        assert result["missing_versions"][0]["missing_version"] == 4

    def test_gap_suppressed_when_historical_holds_it(self, tmp_path):
        """v4 already backfilled: v3 still superseded, nothing reported missing."""
        jsonl, previous_tsv, historical_tsv = self._setup(tmp_path, [
            {
                "genbankAccession": "GCA_000222935.4",
                "assemblyID": "GCA_000222935_4",
                "versionStatus": "superseded",
            }
        ])
        result = parse_assembly_versions(
            new_jsonl=jsonl,
            previous_tsv=previous_tsv,
            historical_tsv=historical_tsv,
        )
        assert result["newly_superseded_count"] == 1
        assert result["missing_versions_count"] == 0

    def test_unchanged_assembly_leaves_historical_untouched(self, tmp_path):
        """A quiet day writes nothing and reports nothing."""
        previous_tsv = tmp_path / "previous.tsv"
        write_tsv(previous_tsv, [
            {"genbankAccession": "GCA_000222935.3", "taxId": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [{"accession": "GCA_000222935.3"}])
        historical_tsv = tmp_path / "historical.tsv"
        write_tsv(historical_tsv, [
            {
                "genbankAccession": "GCA_000222935.1",
                "assemblyID": "GCA_000222935_1",
                "versionStatus": "superseded",
            },
            {
                "genbankAccession": "GCA_000222935.2",
                "assemblyID": "GCA_000222935_2",
                "versionStatus": "superseded",
            },
        ])
        before = historical_tsv.read_text(encoding="utf-8")
        result = parse_assembly_versions(
            new_jsonl=str(jsonl),
            previous_tsv=str(previous_tsv),
            historical_tsv=str(historical_tsv),
        )
        assert result["newly_superseded_count"] == 0
        assert result["missing_versions_count"] == 0
        assert historical_tsv.read_text(encoding="utf-8") == before


# ---------------------------------------------------------------------------
# TestDeriveAssemblyVersionPaths
# ---------------------------------------------------------------------------

class TestDeriveAssemblyVersionPaths:
    """derive_assembly_version_paths produces correct sibling file paths."""

    def test_previous_tsv_in_same_directory(self, tmp_path):
        jsonl = tmp_path / "assembly_data_report.jsonl"
        jsonl.touch()
        previous_tsv, _ = derive_assembly_version_paths(str(jsonl))
        assert os.path.dirname(previous_tsv) == str(tmp_path)
        assert previous_tsv.endswith("assembly_current.tsv.previous")

    def test_historical_tsv_in_same_directory(self, tmp_path):
        jsonl = tmp_path / "assembly_data_report.jsonl"
        jsonl.touch()
        _, historical_tsv = derive_assembly_version_paths(str(jsonl))
        assert os.path.dirname(historical_tsv) == str(tmp_path)
        assert historical_tsv.endswith("assembly_historical.tsv")


# ---------------------------------------------------------------------------
# TestUpdateAssemblyVersionsFlow
# ---------------------------------------------------------------------------

class TestUpdateAssemblyVersionsFlow:
    """update_assembly_versions fetches metadata and writes JSONL."""

    def _write_missing_json(self, tmp_path, entries):
        path = tmp_path / "missing.json"
        with open(path, "w") as f:
            json.dump(entries, f)
        return str(path)

    @patch.object(updater_module, "setup_cache_directories")
    @patch.object(updater_module, "fetch_version_metadata")
    def test_correct_accession_fetched(self, mock_fetch, mock_setup, tmp_path):
        """new_accession from missing_json should be passed to fetch_version_metadata."""
        mock_fetch.return_value = {"accession": "GCA_000222935.2"}
        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_version": 2,
                "new_accession": "GCA_000222935.2",
            }
        ])
        update_assembly_versions(missing_json=missing_json, work_dir=str(tmp_path))
        mock_fetch.assert_called_once_with("GCA_000222935.2", str(tmp_path))

    @patch.object(updater_module, "setup_cache_directories")
    @patch.object(updater_module, "fetch_version_metadata")
    def test_jsonl_written_with_fetched_records(self, mock_fetch, mock_setup, tmp_path):
        """Fetched metadata should be written as JSONL lines."""
        mock_fetch.return_value = {"accession": "GCA_000222935.2", "someField": "value"}
        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_version": 2,
                "new_accession": "GCA_000222935.2",
            }
        ])
        update_assembly_versions(missing_json=missing_json, work_dir=str(tmp_path))
        jsonl_path = tmp_path / "missing_assembly_versions.jsonl"
        assert jsonl_path.exists()
        records = [json.loads(line) for line in jsonl_path.read_text().strip().splitlines()]
        assert len(records) == 1
        assert records[0]["accession"] == "GCA_000222935.2"

    @patch.object(updater_module, "setup_cache_directories")
    @patch.object(updater_module, "fetch_version_metadata")
    def test_fetch_failure_skipped(self, mock_fetch, mock_setup, tmp_path):
        """If fetch_version_metadata returns empty dict, entry is omitted from JSONL."""
        mock_fetch.return_value = {}
        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_version": 2,
                "new_accession": "GCA_000222935.2",
            }
        ])
        update_assembly_versions(missing_json=missing_json, work_dir=str(tmp_path))
        jsonl_path = tmp_path / "missing_assembly_versions.jsonl"
        assert jsonl_path.read_text().strip() == ""

    @patch.object(updater_module, "setup_cache_directories")
    @patch.object(updater_module, "fetch_version_metadata")
    def test_partial_fetch_failures_writes_successful(self, mock_fetch, mock_setup, tmp_path):
        """Successful fetches are written even when some entries return no metadata."""
        mock_fetch.side_effect = [
            {"accession": "GCA_000222935.2"},
            {},
        ]
        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_version": 2,
                "new_accession": "GCA_000222935.2",
            },
            {
                "base_accession": "GCA_000412225",
                "missing_version": 1,
                "new_version": 2,
                "new_accession": "GCA_000412225.2",
            },
        ])
        update_assembly_versions(missing_json=missing_json, work_dir=str(tmp_path))
        jsonl_path = tmp_path / "missing_assembly_versions.jsonl"
        records = [json.loads(line) for line in jsonl_path.read_text().strip().splitlines()]
        assert len(records) == 1
        assert records[0]["accession"] == "GCA_000222935.2"

    @patch.object(updater_module, "setup_cache_directories")
    @patch.object(updater_module, "fetch_version_metadata")
    def test_empty_missing_json_no_op(self, mock_fetch, mock_setup, tmp_path):
        """An empty missing_versions.json should not call fetch_version_metadata."""
        missing_json = self._write_missing_json(tmp_path, [])
        update_assembly_versions(missing_json=missing_json, work_dir=str(tmp_path))
        mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# TestParseAssemblyVersionsPlugin
# ---------------------------------------------------------------------------

class TestParseAssemblyVersionsPlugin:
    """plugin() returns a correctly configured Parser."""

    def test_plugin_returns_parser(self):
        result = incremental_module.plugin()
        assert isinstance(result, Parser)
        assert result.name == "PARSE_ASSEMBLY_VERSIONS"
        assert result.func is incremental_module.parse_assembly_versions_wrapper

    def test_load_missing_versions(self, tmp_path):
        """load_missing_versions reads a JSON file into a list of dicts."""
        path = tmp_path / "missing.json"
        entries = [{"base_accession": "GCA_000222935", "missing_version": 1}]
        path.write_text(json.dumps(entries))
        result = load_missing_versions(str(path))
        assert result == entries


# ---------------------------------------------------------------------------
# TestSnapshotPreviousOutput
# ---------------------------------------------------------------------------

class TestSnapshotPreviousOutput:
    """snapshot_previous_output preserves yesterday's output before it is overwritten.

    parse_assembly_versions reads assembly_current.tsv.previous to find each newly
    superseded predecessor; the snapshot must be taken before parse_ncbi_assemblies
    overwrites assembly_current.tsv in place.
    """

    @staticmethod
    def _config(file_name) -> MagicMock:
        """Build a stand-in Config exposing only meta['file_name']."""
        config = MagicMock()
        config.meta = {"file_name": str(file_name)}
        return config

    def test_creates_previous_with_identical_content(self, tmp_path):
        output = tmp_path / "assembly_current.tsv"
        write_tsv(output, [{"accession": "GCA_000002035.3", "releaseDate": "2020-01-01"}])

        snapshot_previous_output(self._config(output))

        previous = tmp_path / "assembly_current.tsv.previous"
        assert previous.exists()
        assert previous.read_bytes() == output.read_bytes()

    def test_no_op_when_no_prior_output(self, tmp_path):
        output = tmp_path / "assembly_current.tsv"  # never created

        snapshot_previous_output(self._config(output))

        assert not (tmp_path / "assembly_current.tsv.previous").exists()

    def test_no_op_when_file_name_missing(self):
        config = MagicMock()
        config.meta = {}  # no file_name key
        # Should not raise.
        snapshot_previous_output(config)

    def test_overwrites_stale_previous_snapshot(self, tmp_path):
        output = tmp_path / "assembly_current.tsv"
        previous = tmp_path / "assembly_current.tsv.previous"
        previous.write_text("stale-from-an-earlier-run\n", encoding="utf-8")
        write_tsv(output, [{"accession": "GCA_000002035.4", "releaseDate": "2021-06-01"}])

        snapshot_previous_output(self._config(output))

        assert previous.read_bytes() == output.read_bytes()


# ---------------------------------------------------------------------------
# TestNormalisingAccessors
# ---------------------------------------------------------------------------

class TestNormalisingAccessors:
    """get_* accessors read either naming convention (PR-A item 1)."""

    def test_accession_prefers_canonical_column(self):
        row = {"genbankAccession": "GCA_000222935.1", "accession": "GCA_000412225.1"}
        assert get_accession(row) == "GCA_000222935.1"

    def test_accession_falls_back_to_legacy_column(self):
        assert get_accession({"accession": "GCA_000412225.1"}) == "GCA_000412225.1"

    def test_accession_absent_returns_empty(self):
        assert get_accession({"taxId": "1"}) == ""

    def test_assembly_id_all_three_spellings(self):
        assert get_assembly_id({"assemblyID": "GCA_1_1"}) == "GCA_1_1"
        assert get_assembly_id({"assemblyId": "GCA_1_1"}) == "GCA_1_1"
        assert get_assembly_id({"assembly_id": "GCA_1_1"}) == "GCA_1_1"

    def test_assembly_id_prefers_the_yaml_spelling(self):
        """assemblyID is what the historical YAML declares, so it wins."""
        row = {"assemblyID": "canonical", "assemblyId": "upstream"}
        assert get_assembly_id(row) == "canonical"

    def test_version_status_both_conventions(self):
        assert get_version_status({"versionStatus": "superseded"}) == "superseded"
        assert get_version_status({"version_status": "superseded"}) == "superseded"

    def test_empty_value_falls_through_to_the_next_alias(self):
        row = {"genbankAccession": "", "accession": "GCA_000412225.1"}
        assert get_accession(row) == "GCA_000412225.1"


# ---------------------------------------------------------------------------
# TestResolveCurrentTsvPaths
# ---------------------------------------------------------------------------

class TestResolveCurrentTsvPaths:
    """The current-TSV name comes from the config, not a hardcoded constant (F4)."""

    def test_name_and_snapshot_from_config(self, tmp_path):
        config = MagicMock()
        config.meta = {"file_name": "configs/ncbi_datasets_eukaryota.tsv.gz"}
        current, previous, open_fn = resolve_current_tsv_paths(
            str(tmp_path), config=config
        )
        assert current.endswith("ncbi_datasets_eukaryota.tsv.gz")
        assert previous == f"{current}.previous"
        assert open_fn is open_tsv

    def test_discovery_ignores_derived_outputs(self, tmp_path):
        (tmp_path / "assembly_historical.tsv").touch()
        (tmp_path / "assembly_version_summary.tsv").touch()
        (tmp_path / "ncbi_datasets_eukaryota.tsv.gz").touch()
        current, _, _ = resolve_current_tsv_paths(str(tmp_path))
        assert current.endswith("ncbi_datasets_eukaryota.tsv.gz")

    def test_fallback_name_when_nothing_on_disk(self, tmp_path):
        current, previous, _ = resolve_current_tsv_paths(str(tmp_path))
        assert current.endswith("assembly_current.tsv")
        assert previous.endswith("assembly_current.tsv.previous")

    def test_ambiguous_directory_raises(self, tmp_path):
        (tmp_path / "one.tsv").touch()
        (tmp_path / "two.tsv").touch()
        with pytest.raises(ValueError):
            resolve_current_tsv_paths(str(tmp_path))

    def test_derived_paths_use_the_config_name(self, tmp_path):
        config = MagicMock()
        config.meta = {"file_name": "configs/ncbi_datasets_eukaryota.tsv.gz"}
        jsonl = tmp_path / "assembly_data_report.jsonl"
        jsonl.touch()
        previous_tsv, historical_tsv = derive_assembly_version_paths(
            str(jsonl), config=config
        )
        assert previous_tsv.endswith("ncbi_datasets_eukaryota.tsv.gz.previous")
        assert historical_tsv.endswith("assembly_historical.tsv")


# ---------------------------------------------------------------------------
# TestMergeFieldnames
# ---------------------------------------------------------------------------

class TestMergeFieldnames:
    """merge_fieldnames covers every column present in any row (F3)."""

    def test_union_across_rows(self):
        assert set(merge_fieldnames([{"a": 1}, {"b": 2}])) == {"a", "b"}

    def test_preferred_order_honoured(self):
        rows = [{"b": 1, "a": 2, "z": 3}]
        assert merge_fieldnames(rows, ["a", "b"]) == ["a", "b", "z"]

    def test_preferred_columns_absent_from_rows_are_dropped(self):
        assert merge_fieldnames([{"a": 1}], ["a", "b"]) == ["a"]

    def test_no_rows_gives_no_columns(self):
        assert merge_fieldnames([], ["a"]) == []


# ---------------------------------------------------------------------------
# TestAppendPreservesColumns
# ---------------------------------------------------------------------------

class TestAppendPreservesColumns:
    """Merging Phase 0 and Phase 1 row sets must not drop columns (F3)."""

    def test_no_column_lost_when_merging_row_sets(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [{
            "genbankAccession": "GCA_000412225.1",
            "assemblyID": "GCA_000412225_1",
            "versionStatus": "superseded",
        }])
        new_row = {
            "genbankAccession": "GCA_000222935.1",
            "assemblyID": "GCA_000222935_1",
            "versionStatus": "superseded",
            "superseded_by": "GCA_000222935.2",
            "superseded_by_version": 2,
            "superseded_date": "2024-01-15",
        }
        append_superseded_to_tsv([new_row], str(tsv))
        result = read_tsv(tsv)
        assert len(result) == 2
        by_accession = {r["genbankAccession"]: r for r in result}
        assert by_accession["GCA_000222935.1"]["superseded_by"] == "GCA_000222935.2"
        assert "superseded_date" in by_accession["GCA_000412225.1"]

    def test_explicit_headers_set_the_column_order(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        row = {
            "versionStatus": "superseded",
            "genbankAccession": "GCA_000222935.1",
            "assemblyID": "GCA_000222935_1",
        }
        headers = ["genbankAccession", "assemblyID", "versionStatus"]
        append_superseded_to_tsv([row], str(tsv), headers=headers)
        with open(tsv, encoding="utf-8") as f:
            written = next(csv.reader(f, delimiter=TAB))
        assert written == headers


# ---------------------------------------------------------------------------
# TestSchemaConformance
# ---------------------------------------------------------------------------

class TestSchemaConformance:
    """Phase 1 output columns must be declared in assembly_historical.types.yaml.

    This is the check that would have caught F1-F3: Phase 1 was written and
    tested against a mock schema that production never produces.
    """

    @staticmethod
    def _yaml_headers():
        return load_config(config_file=str(HISTORICAL_YAML)).headers

    def test_canonical_columns_declared(self):
        headers = self._yaml_headers()
        assert "genbankAccession" in headers
        assert "assemblyID" in headers
        assert "versionStatus" in headers

    def test_supersession_columns_declared(self):
        headers = self._yaml_headers()
        for column in ("superseded_by", "superseded_by_version", "superseded_date"):
            assert column in headers

    def test_superseded_row_columns_are_all_declared(self):
        headers = set(self._yaml_headers())
        previous_row = dict.fromkeys(headers, "")
        previous_row["genbankAccession"] = "GCA_000222935.1"
        row = build_superseded_row(previous_row, 1, "GCA_000222935.2", 2, "2024-01-15")
        assert set(row) <= headers

    def test_snake_case_columns_not_emitted(self):
        row = build_superseded_row(
            {"genbankAccession": "GCA_000222935.1"},
            1, "GCA_000222935.2", 2, "2024-01-15",
        )
        assert "version_status" not in row
        assert "assembly_id" not in row

    def test_write_side_uses_the_yaml_spelling_of_assembly_id(self):
        """Reads tolerate assemblyId; writes must stay on the declared name.

        parse_ncbi_assemblies writes `assemblyId` upstream, but the historical
        YAML declares `assemblyID`, so a row emitted with the lowercase form
        would be dropped by the GenomeHubs write path.
        """
        row = build_superseded_row(
            {"genbankAccession": "GCA_000222935.1", "assemblyId": "stale"},
            1, "GCA_000222935.2", 2, "2024-01-15",
        )
        assert row["assemblyID"] == "GCA_000222935_1"
        assert "assemblyID" in self._yaml_headers()
        assert "assemblyId" not in self._yaml_headers()

    def test_written_header_is_a_subset_of_the_yaml_schema(self, tmp_path):
        headers = self._yaml_headers()
        previous_row = dict.fromkeys(headers, "")
        previous_row["genbankAccession"] = "GCA_000222935.1"
        row = build_superseded_row(previous_row, 1, "GCA_000222935.2", 2, "2024-01-15")
        tsv = tmp_path / "historical.tsv"
        append_superseded_to_tsv([row], str(tsv), headers=headers)
        with open(tsv, encoding="utf-8") as f:
            written = next(csv.reader(f, delimiter=TAB))
        assert set(written) <= set(headers)
        assert written == list(headers)


# ---------------------------------------------------------------------------
# TestCurrentSchemaDoesNotLeakIntoHistorical
# ---------------------------------------------------------------------------

class TestCurrentSchemaDoesNotLeakIntoHistorical:
    """A Phase 1 row is copied from the current TSV, whose schema is not ours.

    Left unbounded, every current-only column — upstream's `assemblyId` spelling
    among them — lands in assembly_historical.tsv despite the YAML never
    declaring it.
    """

    @staticmethod
    def _yaml_headers():
        return list(load_config(config_file=str(HISTORICAL_YAML)).headers)

    @staticmethod
    def _current_row():
        """A row as the current TSV spells it, extra columns and all."""
        return {
            "genbankAccession": "GCA_000222935.1",
            "assemblyId": "GCA_000222935_1",
            "versionStatus": "current",
            "releaseDate": "2011-06-01",
            "sourceDatabase": "SOURCE_DATABASE_GENBANK",
        }

    def test_canonicalize_columns_drops_the_alias_spelling(self):
        row = canonicalize_columns(
            {"assemblyId": "GCA_000222935_1", "version_status": "current"}
        )
        assert row == {
            "assemblyID": "GCA_000222935_1", "versionStatus": "current",
        }

    def test_canonicalize_columns_prefers_the_canonical_value(self):
        row = canonicalize_columns(
            {"assemblyID": "canonical", "assemblyId": "stale"}
        )
        assert row["assemblyID"] == "canonical"
        assert "assemblyId" not in row

    def test_canonicalize_columns_invents_nothing(self):
        assert canonicalize_columns({"releaseDate": "2011-06-01"}) == {
            "releaseDate": "2011-06-01"
        }

    def test_superseded_row_carries_one_assembly_id_column(self):
        row = build_superseded_row(
            self._current_row(), 1, "GCA_000222935.2", 2, "2024-01-15"
        )
        assert row["assemblyID"] == "GCA_000222935_1"
        assert "assemblyId" not in row

    def test_undeclared_columns_are_not_written(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        row = build_superseded_row(
            self._current_row(), 1, "GCA_000222935.2", 2, "2024-01-15"
        )
        append_superseded_to_tsv([row], str(tsv), headers=self._yaml_headers())
        with open(tsv, encoding="utf-8") as f:
            written = next(csv.reader(f, delimiter=TAB))
        assert set(written) <= set(self._yaml_headers())
        assert "sourceDatabase" not in written
        assert read_tsv(tsv)[0]["assemblyID"] == "GCA_000222935_1"

    def test_columns_already_on_disk_are_kept(self, tmp_path):
        # The schema bounds what a new row may add; it must not evict a column
        # an earlier run already put in the file (F3).
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [{
            "genbankAccession": "GCA_000412225.1",
            "assemblyID": "GCA_000412225_1",
            "legacyColumn": "keep me",
        }])
        row = build_superseded_row(
            self._current_row(), 1, "GCA_000222935.2", 2, "2024-01-15"
        )
        append_superseded_to_tsv([row], str(tsv), headers=self._yaml_headers())
        result = read_tsv(tsv)
        by_accession = {r["genbankAccession"]: r for r in result}
        assert by_accession["GCA_000412225.1"]["legacyColumn"] == "keep me"
        assert "sourceDatabase" not in by_accession["GCA_000222935.1"]

    def test_without_headers_the_union_is_still_taken(self, tmp_path):
        # No schema resolved means no bound — the pre-existing behaviour.
        tsv = tmp_path / "historical.tsv"
        row = build_superseded_row(
            self._current_row(), 1, "GCA_000222935.2", 2, "2024-01-15"
        )
        append_superseded_to_tsv([row], str(tsv))
        with open(tsv, encoding="utf-8") as f:
            written = next(csv.reader(f, delimiter=TAB))
        assert "sourceDatabase" in written

    def test_merge_fieldnames_honours_allowed(self):
        assert merge_fieldnames(
            [{"a": 1, "b": 2}], ["a", "b"], allowed={"a"}
        ) == ["a"]


# ---------------------------------------------------------------------------
# TestHistoricalHeadersAreResolved
# ---------------------------------------------------------------------------

class TestHistoricalHeadersAreResolved:
    """The entry points must actually supply the schema, or it bounds nothing."""

    def test_repo_config_is_found_from_an_unrelated_work_dir(self, tmp_path):
        assert resolve_historical_yaml_path(str(tmp_path)) == str(HISTORICAL_YAML)

    def test_a_copy_in_work_dir_wins(self, tmp_path):
        local = tmp_path / "assembly_historical.types.yaml"
        local.write_text(HISTORICAL_YAML.read_text(encoding="utf-8"), encoding="utf-8")
        assert resolve_historical_yaml_path(str(tmp_path)) == str(local)

    def test_headers_load_from_the_repo_config(self, tmp_path):
        headers = load_historical_headers(str(tmp_path))
        assert headers == list(load_config(config_file=str(HISTORICAL_YAML)).headers)

    def test_an_unloadable_yaml_degrades_to_none(self, tmp_path):
        broken = tmp_path / "assembly_historical.types.yaml"
        broken.write_text("this: [is not: valid", encoding="utf-8")
        assert load_historical_headers(str(tmp_path)) is None

    def test_a_missing_config_degrades_to_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            incremental_module, "resolve_historical_yaml_path", lambda _: None
        )
        assert load_historical_headers(str(tmp_path)) is None

    def test_the_wrapper_passes_the_headers_through(self, tmp_path, monkeypatch):
        (tmp_path / "report.jsonl").write_text("", encoding="utf-8")
        captured = {}

        def fake_parse(**kwargs):
            captured.update(kwargs)
            return {"missing_versions_count": 0, "missing_versions": []}

        monkeypatch.setattr(
            incremental_module, "parse_assembly_versions", fake_parse
        )
        incremental_module.parse_assembly_versions_wrapper(
            working_yaml="", work_dir=str(tmp_path), append=False
        )
        assert captured["historical_headers"] == list(
            load_config(config_file=str(HISTORICAL_YAML)).headers
        )


# ---------------------------------------------------------------------------
# TestResolveOutputPath
# ---------------------------------------------------------------------------

class TestResolveOutputPath:
    """Phase 0 writes into work_dir, not the YAML file directory (F6)."""

    def test_output_lands_in_work_dir(self, tmp_path):
        config = load_config(config_file=str(HISTORICAL_YAML))
        path = resolve_output_path(config, str(tmp_path))
        assert os.path.dirname(path) == str(tmp_path)
        assert os.path.basename(path) == "assembly_historical.tsv"

    def test_config_meta_updated_in_place(self, tmp_path):
        config = load_config(config_file=str(HISTORICAL_YAML))
        path = resolve_output_path(config, str(tmp_path))
        assert config.meta["file_name"] == path


# ---------------------------------------------------------------------------
# TestNonDestructiveHistoricalWrite
# ---------------------------------------------------------------------------

class TestNonDestructiveHistoricalWrite:
    """Phase 0 must not truncate an existing historical TSV (F9).

    The F5 decision makes the gap-fill path re-invoke this parser daily, so a
    truncating write would wipe the backfill it is meant to extend.
    """

    @staticmethod
    def _config(tmp_path):
        config = load_config(config_file=str(HISTORICAL_YAML))
        resolve_output_path(config, str(tmp_path))
        return config

    @staticmethod
    def _row(accession):
        base, version = accession.split(".")
        return {
            "genbankAccession": accession,
            "assemblyID": f"{base}_{version}",
            "versionStatus": "superseded",
        }

    def test_first_write_creates_the_file_with_a_header(self, tmp_path):
        config = self._config(tmp_path)
        written = write_or_append_parsed(
            {"GCA_000412225.1": self._row("GCA_000412225.1")}, config
        )
        output = tmp_path / "assembly_historical.tsv"
        assert written == 1
        with open(output, encoding="utf-8") as f:
            header = next(csv.reader(f, delimiter=TAB))
        assert header == list(config.headers)

    def test_existing_rows_survive_a_gap_fill(self, tmp_path):
        config = self._config(tmp_path)
        output = tmp_path / "assembly_historical.tsv"
        write_or_append_parsed({
            "GCA_000412225.1": self._row("GCA_000412225.1"),
            "GCA_000412225.2": self._row("GCA_000412225.2"),
        }, config)
        assert len(read_tsv(output)) == 2

        written = write_or_append_parsed(
            {"GCA_000222935.1": self._row("GCA_000222935.1")}, config
        )
        rows = read_tsv(output)
        assert written == 1
        assert len(rows) == 3
        assert {r["genbankAccession"] for r in rows} == {
            "GCA_000412225.1", "GCA_000412225.2", "GCA_000222935.1",
        }

    def test_reparsed_versions_are_not_duplicated(self, tmp_path):
        config = self._config(tmp_path)
        parsed = {"GCA_000412225.1": self._row("GCA_000412225.1")}
        write_or_append_parsed(parsed, config)
        written = write_or_append_parsed(parsed, config)
        assert written == 0
        assert len(read_tsv(tmp_path / "assembly_historical.tsv")) == 1

    def test_read_existing_output_reads_the_header_and_accessions(self, tmp_path):
        output = tmp_path / "assembly_historical.tsv"
        write_tsv(output, [self._row("GCA_000412225.1"), self._row("GCA_000412225.2")])
        header, accessions = read_existing_output(str(output))
        assert accessions == {"GCA_000412225.1", "GCA_000412225.2"}
        assert header[0] == "genbankAccession"

    def test_read_existing_output_on_an_absent_file(self, tmp_path):
        assert read_existing_output(str(tmp_path / "nope.tsv")) == ([], set())

    def test_rows_without_an_accession_are_not_overwritten(self, tmp_path):
        # read_existing_output only collects non-empty accessions, so a file
        # whose rows all lack one yields an empty accession set.  The rewrite
        # branch must key off the header instead, or those rows are lost.
        config = self._config(tmp_path)
        output = tmp_path / "assembly_historical.tsv"
        write_tsv(output, [{
            "genbankAccession": "",
            "assemblyID": "GCA_000412225_1",
            "versionStatus": "superseded",
        }])

        written = write_or_append_parsed(
            {"GCA_000222935.1": self._row("GCA_000222935.1")}, config
        )
        rows = read_tsv(output)
        assert written == 1
        assert len(rows) == 2
        assert rows[0]["assemblyID"] == "GCA_000412225_1"
        assert rows[1]["genbankAccession"] == "GCA_000222935.1"

    def test_appends_under_a_header_phase_1_has_widened(self, tmp_path):
        # Phase 1 rewrites this file with the union of every row column, so a
        # gap fill must append under what is on disk, not the config header.
        config = self._config(tmp_path)
        output = tmp_path / "assembly_historical.tsv"
        write_or_append_parsed({"GCA_000412225.1": self._row("GCA_000412225.1")}, config)

        rows = read_tsv(output)
        widened = list(rows[0].keys()) + ["genusTaxId"]
        rows[0]["genusTaxId"] = "8001"
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=widened, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

        write_or_append_parsed({"GCA_000222935.1": self._row("GCA_000222935.1")}, config)

        with open(output, encoding="utf-8") as f:
            lines = [line.rstrip("\n").split("\t")
                     for line in f if line.strip()]
        # Every row spans the full widened header, so no column can slide.
        assert {len(line) for line in lines} == {len(widened)}
        appended = read_tsv(output)[-1]
        assert appended["genbankAccession"] == "GCA_000222935.1"
        assert appended["genusTaxId"] == ""

    def test_flow_routes_its_write_through_the_helper(self):
        source = Path(backfill_module.__file__).read_text(encoding="utf-8")
        assert "written = write_or_append_parsed(parsed, config)" in source
