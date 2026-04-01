"""Tests for parse_backfill_historical_versions.py

Covers:
- Accession parsing helpers
- Assembly identification from JSONL fixture
- Cache round-trip (save/load with expiry)
- Checkpoint save/load/derive
- Accession format validation
- parse_historical_version calls correct functions with correct args
- backfill_historical_versions orchestrator: accumulates all rows and writes
  TSV exactly once (regression test for the batch-overwrite data-loss bug)
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.parsers import (  # noqa: E402
    parse_backfill_historical_versions as backfill_module,
)
from flows.parsers.parse_backfill_historical_versions import (  # noqa: E402
    ACCESSION_PATTERN,
    backfill_historical_versions,
    derive_checkpoint_path,
    get_cache_path,
    identify_assemblies_needing_backfill,
    load_checkpoint,
    load_from_cache,
    parse_accession,
    parse_version,
    save_checkpoint,
    save_to_cache,
)

FIXTURE_JSONL = "tests/test_data/assembly_test_sample.jsonl"


class TestParseAccession:
    """Unit tests for parse_accession and parse_version."""

    @pytest.mark.parametrize(
        "accession, expected",
        [
            ("GCA_000222935.2", ("GCA_000222935", 2)),
            ("GCA_003706615.3", ("GCA_003706615", 3)),
            ("GCF_000001405.39", ("GCF_000001405", 39)),
        ],
    )
    def test_parse_accession(self, accession, expected):
        assert parse_accession(accession) == expected

    def test_parse_accession_no_version(self):
        assert parse_accession("GCA_000222935") == ("GCA_000222935", 1)

    @pytest.mark.parametrize(
        "accession, expected",
        [
            ("GCA_000222935.2", 2),
            ("GCA_000222935.10", 10),
            ("GCA_000222935", 1),
        ],
    )
    def test_parse_version(self, accession, expected):
        assert parse_version(accession) == expected


class TestAccessionPattern:
    """Ensure the compiled ACCESSION_PATTERN validates correctly."""

    @pytest.mark.parametrize(
        "accession",
        ["GCA_000222935.2", "GCF_000001405.39", "GCA_123456789.1"],
    )
    def test_valid(self, accession):
        assert ACCESSION_PATTERN.match(accession)

    @pytest.mark.parametrize(
        "accession",
        ["GCA_00022293.2", "GCA_0002229350.2", "XYZ_000222935.2", "hello"],
    )
    def test_invalid(self, accession):
        assert not ACCESSION_PATTERN.match(accession)


class TestIdentifyAssemblies:
    """Identify which assemblies need backfill from the JSONL fixture."""

    @pytest.fixture()
    def fixture_path(self):
        if not os.path.exists(FIXTURE_JSONL):
            pytest.skip(f"Fixture not found: {FIXTURE_JSONL}")
        return FIXTURE_JSONL

    def test_finds_multi_version_assemblies(self, fixture_path):
        assemblies = identify_assemblies_needing_backfill(fixture_path)
        assert len(assemblies) == 3

    def test_fields_present(self, fixture_path):
        assemblies = identify_assemblies_needing_backfill(fixture_path)
        for asm in assemblies:
            assert "base_accession" in asm
            assert "current_version" in asm
            assert "current_accession" in asm
            assert "historical_versions_needed" in asm

    def test_version_ranges(self, fixture_path):
        assemblies = identify_assemblies_needing_backfill(fixture_path)
        by_base = {a["base_accession"]: a for a in assemblies}
        assert by_base["GCA_000222935"]["historical_versions_needed"] == [1]
        assert by_base["GCA_003706615"]["historical_versions_needed"] == [1, 2]

    def test_ignores_version_one(self, tmp_path):
        """An assembly at version 1 should not appear in the results."""
        jsonl = tmp_path / "v1.jsonl"
        jsonl.write_text(json.dumps({"accession": "GCA_999999999.1"}) + "\n")
        assert identify_assemblies_needing_backfill(str(jsonl)) == []


class TestCache:
    """Round-trip and expiry tests for the JSON cache layer."""

    def test_save_and_load(self, tmp_path):
        path = str(tmp_path / "test.json")
        save_to_cache(path, {"key": "value"})
        assert load_from_cache(path) == {"key": "value"}

    def test_expired_cache_returns_empty(self, tmp_path):
        path = str(tmp_path / "old.json")
        save_to_cache(path, {"key": "value"})
        os.utime(path, (0, 0))
        assert load_from_cache(path, max_age_days=1) == {}

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_from_cache(str(tmp_path / "nope.json")) == {}

    def test_get_cache_path_sanitises(self, tmp_path):
        path = get_cache_path(str(tmp_path), "metadata", "GCA_000222935.2")
        assert "GCA_000222935.2" in path
        assert path.endswith(".json")


class TestCheckpoint:
    """Checkpoint save/load/derive tests."""

    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "cp.json")
        save_checkpoint(path, 42)
        cp = load_checkpoint(path)
        assert cp["processed_count"] == 42
        assert "timestamp" in cp

    def test_missing_checkpoint_returns_empty(self, tmp_path):
        assert load_checkpoint(str(tmp_path / "nope.json")) == {}

    def test_derive_is_deterministic(self, tmp_path):
        path_a = derive_checkpoint_path("a.jsonl", "b.yaml", str(tmp_path))
        path_b = derive_checkpoint_path("a.jsonl", "b.yaml", str(tmp_path))
        assert path_a == path_b

    def test_derive_different_inputs_differ(self, tmp_path):
        path_a = derive_checkpoint_path("a.jsonl", "b.yaml", str(tmp_path))
        path_c = derive_checkpoint_path("c.jsonl", "b.yaml", str(tmp_path))
        assert path_a != path_c

    def test_completed_flag_round_trip(self, tmp_path):
        """A checkpoint saved with completed=True must expose that flag."""
        path = str(tmp_path / "cp.json")
        save_checkpoint(path, 100, completed=True)
        cp = load_checkpoint(path)
        assert cp["completed"] is True
        assert cp["processed_count"] == 100

    def test_incomplete_checkpoint_has_completed_false(self, tmp_path):
        """Periodic mid-run saves must not set the completed flag."""
        path = str(tmp_path / "cp.json")
        save_checkpoint(path, 50)
        cp = load_checkpoint(path)
        assert cp.get("completed", False) is False


class TestParseHistoricalVersion:
    """Verify parse_historical_version calls the right functions."""

    @patch.object(backfill_module, "gh_utils")
    @patch.object(backfill_module, "fetch_and_parse_sequence_report")
    @patch.object(backfill_module, "process_assembly_report")
    @patch.object(backfill_module, "utils")
    def test_calls_process_with_superseded(
        self, mock_utils, mock_process, mock_seq, mock_gh
    ):
        mock_utils.convert_keys_to_camel_case.return_value = {
            "accession": "GCA_000222935.1"
        }
        mock_process.return_value = {
            "processedAssemblyInfo": {
                "genbankAccession": "GCA_000222935.1",
                "versionStatus": "superseded",
            }
        }
        mock_gh.parse_report_values.return_value = {
            "genbankAccession": "GCA_000222935.1",
            "assemblyID": "GCA_000222935_1",
        }

        config = MagicMock()
        row = backfill_module.parse_historical_version(
            version_data={"accession": "GCA_000222935.1"},
            config=config,
            base_accession="GCA_000222935",
            version_num=1,
            current_accession="GCA_000222935.2",
        )

        mock_process.assert_called_once()
        _, kwargs = mock_process.call_args
        assert kwargs["version_status"] == "superseded"

        mock_seq.assert_called_once()

        assert row["genbankAccession"] == "GCA_000222935.1"

    @patch.object(backfill_module, "gh_utils")
    @patch.object(backfill_module, "fetch_and_parse_sequence_report")
    @patch.object(backfill_module, "process_assembly_report")
    @patch.object(backfill_module, "utils")
    def test_sets_assembly_id(
        self, mock_utils, mock_process, mock_seq, mock_gh
    ):
        mock_utils.convert_keys_to_camel_case.return_value = {
            "accession": "GCA_000222935.1"
        }
        mock_process.return_value = {
            "processedAssemblyInfo": {
                "genbankAccession": "GCA_000222935.1",
            }
        }
        mock_gh.parse_report_values.return_value = {}

        config = MagicMock()
        backfill_module.parse_historical_version(
            version_data={},
            config=config,
            base_accession="GCA_000222935",
            version_num=1,
            current_accession="GCA_000222935.2",
        )

        report = mock_process.return_value
        assert report["processedAssemblyInfo"]["assemblyID"] == "GCA_000222935_1"


class TestBackfillOrchestrator:
    """Integration tests for the backfill_historical_versions flow.

    Mocks external dependencies (FTP, datasets CLI, parser functions, TSV
    writer) so the test is fast and offline.  Focuses on verifying:
    - All discovered versions are requested and parsed
    - write_to_tsv is called exactly once with all accumulated rows
    - Checkpoint does not clear the parsed dict (regression for data-loss bug)
    """

    def _make_jsonl(self, tmp_path, records):
        """Write records to a JSONL fixture file."""
        path = tmp_path / "input.jsonl"
        lines = [json.dumps(r) for r in records]
        path.write_text("\n".join(lines) + "\n")
        return str(path)

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_writes_tsv_once_with_all_rows(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """Regression: old code wrote per-batch and cleared parsed dict."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": "assembly_historical.tsv"}
        )
        mock_find.return_value = [
            {"accession": "GCA_000222935.1"},
        ]
        mock_parse.return_value = {
            "genbankAccession": "GCA_000222935.1",
            "assemblyID": "GCA_000222935_1",
        }

        input_path = self._make_jsonl(tmp_path, [
            {"accession": "GCA_000222935.2"},
        ])
        yaml_path = str(tmp_path / "config.yaml")

        backfill_historical_versions(
            input_path=input_path,
            yaml_path=yaml_path,
            work_dir=str(tmp_path),
        )

        mock_write.assert_called_once()
        written_parsed = mock_write.call_args[0][0]
        assert "GCA_000222935.1" in written_parsed

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_multiple_assemblies_all_in_one_write(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """All rows from multiple assemblies must appear in a single write."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": "assembly_historical.tsv"}
        )

        def fake_find(accession, work_dir):
            base = accession.split(".")[0]
            return [{"accession": f"{base}.1"}]

        mock_find.side_effect = fake_find

        call_count = {"n": 0}

        def fake_parse(version_data, **kwargs):
            call_count["n"] += 1
            acc = version_data["accession"]
            return {"genbankAccession": acc, "assemblyID": acc.replace(".", "_")}

        mock_parse.side_effect = fake_parse

        input_path = self._make_jsonl(tmp_path, [
            {"accession": "GCA_000222935.2"},
            {"accession": "GCA_000412225.2"},
            {"accession": "GCA_003706615.3"},
        ])

        backfill_historical_versions(
            input_path=input_path,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )

        mock_write.assert_called_once()
        written_parsed = mock_write.call_args[0][0]
        assert len(written_parsed) == 3
        assert "GCA_000222935.1" in written_parsed
        assert "GCA_000412225.1" in written_parsed
        assert "GCA_003706615.1" in written_parsed

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_skips_current_version(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """Versions >= current should not be parsed."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": "out.tsv"}
        )
        mock_find.return_value = [
            {"accession": "GCA_000222935.1"},
            {"accession": "GCA_000222935.2"},
        ]
        mock_parse.return_value = {
            "genbankAccession": "GCA_000222935.1",
        }

        input_path = self._make_jsonl(tmp_path, [
            {"accession": "GCA_000222935.2"},
        ])

        backfill_historical_versions(
            input_path=input_path,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )

        mock_parse.assert_called_once()
        parsed_acc = mock_parse.call_args[1]["version_data"]["accession"]
        assert parsed_acc == "GCA_000222935.1"

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "utils")
    def test_no_write_when_nothing_to_backfill(
        self, mock_utils, mock_find, mock_write, tmp_path
    ):
        """If all assemblies are v1, write_to_tsv should not be called."""
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": "out.tsv"}
        )
        input_path = self._make_jsonl(tmp_path, [
            {"accession": "GCA_000222935.1"},
        ])

        backfill_historical_versions(
            input_path=input_path,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )

        mock_write.assert_not_called()

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_checkpoint_does_not_clear_parsed(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """Rows parsed before a checkpoint must still be in the final write.

        This is the core regression test for Rich's bug report: 4006
        assemblies processed, only 6 in the output file.
        """
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": "out.tsv"}
        )

        records = [
            {"accession": f"GCA_{str(i).zfill(9)}.2"} for i in range(150)
        ]
        input_path = self._make_jsonl(tmp_path, records)

        def fake_find(accession, work_dir):
            base = accession.split(".")[0]
            return [{"accession": f"{base}.1"}]

        mock_find.side_effect = fake_find

        def fake_parse(version_data, **kwargs):
            acc = version_data["accession"]
            return {"genbankAccession": acc}

        mock_parse.side_effect = fake_parse

        backfill_historical_versions(
            input_path=input_path,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )

        mock_write.assert_called_once()
        written_parsed = mock_write.call_args[0][0]
        assert len(written_parsed) == 150

    @patch.object(backfill_module, "write_to_tsv")
    @patch.object(backfill_module, "find_all_assembly_versions")
    @patch.object(backfill_module, "parse_historical_version")
    @patch.object(backfill_module, "utils")
    def test_rerun_after_completed_checkpoint_writes_all_rows(
        self, mock_utils, mock_parse, mock_find, mock_write, tmp_path
    ):
        """Re-running after a completed run must still write all rows.

        Regression test for Rich's report: second run skipped first ~4200
        entries and only wrote the remaining ~70 because the checkpoint
        held processed_count == total at the end of the first run.
        """
        mock_utils.load_config.return_value = MagicMock(
            meta={"file_name": "out.tsv"}
        )

        records = [
            {"accession": f"GCA_{str(i).zfill(9)}.2"} for i in range(20)
        ]
        input_path = self._make_jsonl(tmp_path, records)

        def fake_find(accession, work_dir):
            base = accession.split(".")[0]
            return [{"accession": f"{base}.1"}]

        mock_find.side_effect = fake_find

        def fake_parse(version_data, **kwargs):
            return {"genbankAccession": version_data["accession"]}

        mock_parse.side_effect = fake_parse

        common_args = dict(
            input_path=input_path,
            yaml_path=str(tmp_path / "config.yaml"),
            work_dir=str(tmp_path),
        )

        # First run — completes and saves a completed checkpoint.
        backfill_historical_versions(**common_args)
        assert mock_write.call_count == 1

        mock_write.reset_mock()
        mock_find.reset_mock()
        mock_parse.reset_mock()

        # Second run — must process all 20 entries, not just the tail.
        backfill_historical_versions(**common_args)
        assert mock_write.call_count == 1
        written_parsed = mock_write.call_args[0][0]
        assert len(written_parsed) == 20
