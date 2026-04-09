"""Tests for update_historical_incremental.py and backfill_missing_versions.py

Covers:
- Loading and indexing previous parsed TSV results
- Building superseded and missing-version records
- Core supersession detection logic (superseded, missing-with-gap, new-series, v1-skip)
- Appending to historical TSV with deduplication
- Incremental orchestrator flow behaviour
- Loading existing historical TSV (backfill helper)
- Backfill flow: version selection and TSV merge
"""

import csv
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.parsers import (  # noqa: E402
    backfill_missing_versions as backfill_module,
    update_historical_incremental as incremental_module,
)
from flows.lib.utils import Parser  # noqa: E402
from flows.parsers.backfill_missing_versions import (  # noqa: E402
    backfill_missing_versions,
    backfill_missing_versions_wrapper,
    load_existing_historical,
)
from flows.parsers.update_historical_incremental import (  # noqa: E402
    append_superseded_to_tsv,
    build_missing_version_record,
    build_superseded_row,
    identify_newly_superseded,
    load_previous_parsed_by_base,
    run_incremental_historical_update,
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
        write_tsv(tsv, [{"accession": "GCA_000222935.1", "taxon_id": "12345"}])
        result = load_previous_parsed_by_base(str(tsv))
        assert "GCA_000222935" in result
        assert 1 in result["GCA_000222935"]
        assert result["GCA_000222935"][1]["taxon_id"] == "12345"

    def test_multi_version_same_base(self, tmp_path):
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [
            {"accession": "GCA_000222935.1", "taxon_id": "1"},
            {"accession": "GCA_000222935.2", "taxon_id": "1"},
        ])
        result = load_previous_parsed_by_base(str(tsv))
        assert len(result["GCA_000222935"]) == 2
        assert 1 in result["GCA_000222935"]
        assert 2 in result["GCA_000222935"]

    def test_multiple_base_accessions(self, tmp_path):
        tsv = tmp_path / "current.tsv"
        write_tsv(tsv, [
            {"accession": "GCA_000222935.1", "taxon_id": "1"},
            {"accession": "GCA_000412225.1", "taxon_id": "2"},
        ])
        result = load_previous_parsed_by_base(str(tsv))
        assert len(result) == 2
        assert "GCA_000222935" in result
        assert "GCA_000412225" in result


# ---------------------------------------------------------------------------
# TestBuildSupersededRow
# ---------------------------------------------------------------------------

class TestBuildSupersededRow:
    """build_superseded_row stamps the correct metadata onto a copied row."""

    def _base_row(self):
        return {
            "accession": "GCA_000222935.1",
            "taxon_id": "12345",
            "assembly_level": "Chromosome",
        }

    def test_version_status_set(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["version_status"] == "superseded"

    def test_assembly_id_format(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["assembly_id"] == "GCA_000222935_1"

    def test_superseded_by_fields(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["superseded_by"] == "GCA_000222935.2"
        assert row["superseded_by_version"] == 2
        assert row["superseded_date"] == "2024-01-15"

    def test_original_row_not_mutated(self):
        original = self._base_row()
        build_superseded_row(original, 1, "GCA_000222935.2", 2, "2024-01-15")
        assert "version_status" not in original

    def test_existing_fields_preserved(self):
        row = build_superseded_row(self._base_row(), 1, "GCA_000222935.2", 2, "2024-01-15")
        assert row["taxon_id"] == "12345"
        assert row["assembly_level"] == "Chromosome"


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
            "GCA_000222935": {1: {"accession": "GCA_000222935.1", "taxon_id": "1"}}
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert superseded[0]["superseded_by"] == "GCA_000222935.2"
        assert missing == []

    def test_missing_with_version_gap(self, tmp_path):
        jsonl = self._write_jsonl(tmp_path, [{"accession": "GCA_000222935.3"}])
        previous = {
            "GCA_000222935": {1: {"accession": "GCA_000222935.1", "taxon_id": "1"}}
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert superseded == []
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
            "GCA_000222935": {1: {"accession": "GCA_000222935.1", "taxon_id": "1"}}
        }
        superseded, missing = identify_newly_superseded(jsonl, previous)
        assert len(superseded) == 1
        assert len(missing) == 1


# ---------------------------------------------------------------------------
# TestAppendSupersededToTsv
# ---------------------------------------------------------------------------

class TestAppendSupersededToTsv:
    """append_superseded_to_tsv correctly creates, appends, and deduplicates."""

    def _make_row(self, acc, assembly_id, status="superseded"):
        return {
            "accession": acc,
            "assembly_id": assembly_id,
            "version_status": status,
        }

    def test_creates_new_file(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        rows = [self._make_row("GCA_000222935.1", "GCA_000222935_1")]
        append_superseded_to_tsv(rows, str(tsv))
        assert tsv.exists()
        result = read_tsv(tsv)
        assert len(result) == 1
        assert result[0]["accession"] == "GCA_000222935.1"

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
            "accession": "GCA_000222935.1",
            "assembly_id": "GCA_000222935_1",
            "version_status": "superseded",
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


# ---------------------------------------------------------------------------
# TestIncrementalOrchestrator
# ---------------------------------------------------------------------------

class TestIncrementalOrchestrator:
    """run_incremental_historical_update orchestrator behaviour."""

    def test_no_previous_tsv_returns_empty_result(self, tmp_path):
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [{"accession": "GCA_000222935.2"}])
        result = run_incremental_historical_update(
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
            {"accession": "GCA_000222935.1", "taxon_id": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.2", "releaseDate": "2024-01-15"}
        ])
        result = run_incremental_historical_update(
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
            {"accession": "GCA_000222935.1", "taxon_id": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.3", "releaseDate": "2024-06-01"}
        ])
        result = run_incremental_historical_update(
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
            {"accession": "GCA_000222935.1", "taxon_id": "1"}
        ])
        jsonl = tmp_path / "new.jsonl"
        write_jsonl(jsonl, [
            {"accession": "GCA_000222935.2", "releaseDate": "2024-01-15"}
        ])
        historical_tsv = tmp_path / "historical.tsv"
        run_incremental_historical_update(
            new_jsonl=str(jsonl),
            previous_tsv=str(previous_tsv),
            historical_tsv=str(historical_tsv),
        )
        assert historical_tsv.exists()
        rows = read_tsv(historical_tsv)
        assert len(rows) == 1
        assert rows[0]["version_status"] == "superseded"


# ---------------------------------------------------------------------------
# TestLoadExistingHistorical
# ---------------------------------------------------------------------------

class TestLoadExistingHistorical:
    """load_existing_historical indexes rows by genbankAccession."""

    def test_missing_file_returns_empty(self, tmp_path):
        result = load_existing_historical(str(tmp_path / "nope.tsv"))
        assert result == {}

    def test_rows_keyed_by_genbank_accession(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [
            {"genbankAccession": "GCA_000222935.1", "version_status": "superseded"},
            {"genbankAccession": "GCA_000412225.1", "version_status": "superseded"},
        ])
        result = load_existing_historical(str(tsv))
        assert "GCA_000222935.1" in result
        assert "GCA_000412225.1" in result
        assert len(result) == 2

    def test_row_data_preserved(self, tmp_path):
        tsv = tmp_path / "historical.tsv"
        write_tsv(tsv, [
            {"genbankAccession": "GCA_000222935.1", "version_status": "superseded"}
        ])
        result = load_existing_historical(str(tsv))
        assert result["GCA_000222935.1"]["version_status"] == "superseded"


# ---------------------------------------------------------------------------
# TestBackfillMissingVersionsFlow
# ---------------------------------------------------------------------------

class TestBackfillMissingVersionsFlow:
    """backfill_missing_versions selects the right version and merges into TSV."""

    def _write_missing_json(self, tmp_path, entries):
        path = tmp_path / "missing.json"
        with open(path, "w") as f:
            json.dump(entries, f)
        return str(path)

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_correct_version_selected(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """Only the requested missing version should be parsed, not all versions."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": str(tmp_path / "historical.tsv")}
        )
        mock_find.return_value = [
            {"accession": "GCA_000222935.1"},
            {"accession": "GCA_000222935.2"},
        ]
        mock_parse.return_value = {"genbankAccession": "GCA_000222935.1"}

        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_accession": "GCA_000222935.2",
            }
        ])
        backfill_missing_versions(
            missing_json=missing_json,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )
        mock_parse.assert_called_once()
        parsed_version_data = mock_parse.call_args[1]["version_data"]
        assert parsed_version_data["accession"] == "GCA_000222935.1"

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_merges_with_existing_historical(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """New rows must be merged with existing historical rows before writing."""
        historical_tsv = tmp_path / "historical.tsv"
        write_tsv(historical_tsv, [
            {"genbankAccession": "GCA_000412225.1", "version_status": "superseded"}
        ])
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": str(historical_tsv)}
        )
        mock_find.return_value = [{"accession": "GCA_000222935.1"}]
        mock_parse.return_value = {"genbankAccession": "GCA_000222935.1"}

        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_accession": "GCA_000222935.2",
            }
        ])
        backfill_missing_versions(
            missing_json=missing_json,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )
        mock_write.assert_called_once()
        written_parsed = mock_write.call_args[0][0]
        assert "GCA_000412225.1" in written_parsed
        assert "GCA_000222935.1" in written_parsed

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "utils")
    def test_no_write_when_version_not_found_in_ftp(
        self, mock_utils, mock_find, mock_write, tmp_path
    ):
        """If the FTP listing does not include the missing version, skip silently."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": str(tmp_path / "historical.tsv")}
        )
        mock_find.return_value = [{"accession": "GCA_000222935.2"}]

        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_accession": "GCA_000222935.2",
            }
        ])
        backfill_missing_versions(
            missing_json=missing_json,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )
        mock_write.assert_not_called()

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "utils")
    def test_no_versions_returned_from_ftp(
        self, mock_utils, mock_find, mock_parse, mock_write, tmp_path
    ):
        """If FTP returns an empty list, skip gracefully without parsing or writing."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": str(tmp_path / "historical.tsv")}
        )
        mock_find.return_value = []

        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_accession": "GCA_000222935.2",
            }
        ])
        backfill_missing_versions(
            missing_json=missing_json,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )
        mock_parse.assert_not_called()
        mock_write.assert_not_called()

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_partial_parse_failure_writes_successful_rows(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """If one entry fails to parse, the successfully parsed ones are still written."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": str(tmp_path / "historical.tsv")}
        )
        mock_find.side_effect = [
            [{"accession": "GCA_000222935.1"}, {"accession": "GCA_000222935.2"}],
            [{"accession": "GCA_000412225.1"}, {"accession": "GCA_000412225.2"}],
        ]
        mock_parse.side_effect = [
            {"genbankAccession": "GCA_000222935.1"},
            ValueError("simulated parse failure"),
        ]

        missing_json = self._write_missing_json(tmp_path, [
            {
                "base_accession": "GCA_000222935",
                "missing_version": 1,
                "new_accession": "GCA_000222935.2",
            },
            {
                "base_accession": "GCA_000412225",
                "missing_version": 1,
                "new_accession": "GCA_000412225.2",
            },
        ])
        backfill_missing_versions(
            missing_json=missing_json,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )
        mock_write.assert_called_once()
        written = mock_write.call_args[0][0]
        assert "GCA_000222935.1" in written
        assert "GCA_000412225.1" not in written

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "utils")
    def test_empty_missing_json_no_op(
        self, mock_utils, mock_find, mock_write, tmp_path
    ):
        """An empty missing_versions.json should not call write_to_tsv."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": str(tmp_path / "historical.tsv")}
        )
        missing_json = self._write_missing_json(tmp_path, [])
        backfill_missing_versions(
            missing_json=missing_json,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )
        mock_write.assert_not_called()
        mock_find.assert_not_called()


# ---------------------------------------------------------------------------
# TestBackfillMissingVersionsWrapper
# ---------------------------------------------------------------------------

class TestBackfillMissingVersionsWrapper:
    """backfill_missing_versions_wrapper delegates to the flow correctly."""

    @patch.object(backfill_module, "backfill_missing_versions")
    def test_delegates_to_flow(self, mock_flow, tmp_path):
        """Wrapper locates missing_versions.json and passes it to the flow."""
        missing_json = tmp_path / "missing_versions.json"
        missing_json.write_text("[]", encoding="utf-8")

        backfill_missing_versions_wrapper(
            working_yaml=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
            append=False,
        )

        mock_flow.assert_called_once_with(
            missing_json=str(missing_json),
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )

    def test_raises_when_missing_json_absent(self, tmp_path):
        """Wrapper raises FileNotFoundError if missing_versions.json does not exist."""
        with pytest.raises(FileNotFoundError):
            backfill_missing_versions_wrapper(
                working_yaml=str(tmp_path / "config.yaml"),
                work_dir=str(tmp_path),
                append=False,
            )


# ---------------------------------------------------------------------------
# TestBackfillMissingVersionsPlugin
# ---------------------------------------------------------------------------

class TestBackfillMissingVersionsPlugin:
    """plugin() returns a correctly configured Parser."""

    def test_plugin_returns_parser(self):
        result = backfill_module.plugin()
        assert isinstance(result, Parser)
        assert result.name == "BACKFILL_MISSING_VERSIONS"
        assert result.func is backfill_missing_versions_wrapper
