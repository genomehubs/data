"""Two-day end-to-end simulation of the assembly-version pipeline.

Stage 2 of the Phase 4 staged run, as a test: the parts of it that need no
real data and no network. Every other test in this suite exercises one module;
this one runs the four phases in sequence against one working directory and
checks they compose.

Day 1 leaves a backfilled assembly_historical.tsv and today's current TSV.
Day 2 snapshots the current TSV, hands the daily parser a JSONL exercising all
four diff paths — unchanged, +1, a skipped version, a new series — then writes
today's current TSV as parse_ncbi_assemblies would, aggregates it into the
summary and the milestones, and runs both validators over the result.

The whole run is offline: nothing here reaches NCBI, which is the property
validate_no_ncbi_fetches then asserts of the daily parse specifically.
"""

import csv
import json
import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.lib.assembly_lineage import lineage_columns  # noqa: E402
from flows.lib.compute_taxon_milestones import (  # noqa: E402
    OUTPUT_TSV as MILESTONE_TSV,
)
from flows.lib.compute_taxon_milestones import compute_taxon_milestones  # noqa: E402
from flows.lib.generate_assembly_summary import (  # noqa: E402
    OUTPUT_TSV as SUMMARY_TSV,
)
from flows.lib.generate_assembly_summary import (  # noqa: E402
    generate_assembly_summary,
)
from flows.parsers.parse_assembly_versions import (  # noqa: E402
    parse_assembly_versions,
)
from tests import validate_no_ncbi_fetches as no_fetches  # noqa: E402
from tests import validate_pipeline as validator  # noqa: E402

CURRENT_TSV = "assembly_current.tsv"
HISTORICAL_TSV = "assembly_historical.tsv"
JSONL = "assembly_data_report.jsonl"

COLUMNS = [
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

# base accession -> (taxid, version current on day 1, version current on day 2).
# Day 1 version 0 means the base is new to the pipeline on day 2.
BASES = {
    "GCA_000000001": (9001, 1, 1),  # unchanged, single version
    "GCA_000000002": (9002, 3, 3),  # unchanged, multi-version
    "GCA_000000003": (9003, 1, 2),  # superseded by the next version
    "GCA_000000004": (9004, 2, 5),  # superseded, skipping v3 and v4
    "GCA_000000005": (9005, 0, 2),  # new series, arriving at v2
}

# Versions the Phase 0 backfill had already written on day 1.
BACKFILLED = {"GCA_000000002": [1, 2], "GCA_000000004": [1]}

EBP_TAXID = 9002  # the one lineage that reaches the EBP metric


def assembly_row(base, version, taxid, status="current"):
    """Build one assembly row carrying the upstream lineage columns."""
    row = {name: "" for name in COLUMNS}
    row.update(
        {
            "genbankAccession": f"{base}.{version}",
            "assemblyID": f"{base}_{version}",
            "versionStatus": status,
            "taxId": str(taxid),
            "releaseDate": f"20{10 + version:02d}-01-01",
            "bioProjectAccession": "PRJNA533106",
            "ebpStandardDate": "2015-01-01" if taxid == EBP_TAXID else "",
            "genusTaxId": str(8000 + taxid % 100),
            "familyTaxId": "7001",
            "orderTaxId": "6001",
            # Both absent forms upstream can write, in one fixture.
            "classTaxId": "None",
            "phylumTaxId": "",
            "kingdomTaxId": "4001",
        }
    )
    return row


def write_tsv(path, rows):
    """Write rows to a tab-separated file with the full column set."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path):
    """Read a tab-separated file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_jsonl(path, day):
    """Write the bulk JSONL as update_ncbi_datasets would have fetched it."""
    with open(path, "w", encoding="utf-8") as f:
        for base, (_, day_one, day_two) in BASES.items():
            version = day_one if day == 1 else day_two
            if not version:
                continue
            f.write(
                json.dumps(
                    {
                        "accession": f"{base}.{version}",
                        "releaseDate": f"20{10 + version:02d}-01-01",
                    }
                )
                + "\n"
            )


@pytest.fixture(scope="module")
def simulation(tmp_path_factory):
    """Run both days once and return the working directory and results."""
    work_dir = tmp_path_factory.mktemp("two_day")
    current = work_dir / CURRENT_TSV
    historical = work_dir / HISTORICAL_TSV
    jsonl = work_dir / JSONL

    # --- Day 1: the state the backfill leaves behind ---------------------
    day_one_rows, backfilled_rows = [], []
    for base, (taxid, day_one, _) in BASES.items():
        if day_one:
            day_one_rows.append(assembly_row(base, day_one, taxid))
        for version in BACKFILLED.get(base, []):
            row = assembly_row(base, version, taxid, status="superseded")
            row["superseded_by"] = f"{base}.{version + 1}"
            row["superseded_by_version"] = str(version + 1)
            backfilled_rows.append(row)
    write_tsv(current, day_one_rows)
    write_tsv(historical, backfilled_rows)

    # --- Day 2: snapshot, diff, rewrite, aggregate -----------------------
    # parse_ncbi_assemblies snapshots the current TSV before overwriting it.
    shutil.copy(current, f"{current}.previous")
    write_jsonl(jsonl, day=2)

    results = parse_assembly_versions(
        new_jsonl=str(jsonl),
        previous_tsv=f"{current}.previous",
        historical_tsv=str(historical),
    )

    # ... and then writes today's parse over the current TSV.
    write_tsv(
        current,
        [
            assembly_row(base, day_two, taxid)
            for base, (taxid, _, day_two) in BASES.items()
        ],
    )

    generate_assembly_summary(work_dir=str(work_dir))
    compute_taxon_milestones(work_dir=str(work_dir))

    return {"work_dir": work_dir, "results": results}


class TestDailyDiff:
    def test_only_genuinely_superseded_versions_are_added(self, simulation):
        # The two bases whose current version moved, and only those: an
        # unchanged multi-version base must not report its predecessor.
        assert simulation["results"]["newly_superseded_count"] == 2

    def test_superseded_rows_land_in_the_historical_tsv(self, simulation):
        rows = read_tsv(simulation["work_dir"] / HISTORICAL_TSV)
        assert {row["genbankAccession"] for row in rows} == {
            "GCA_000000002.1",  # backfilled
            "GCA_000000002.2",  # backfilled
            "GCA_000000004.1",  # backfilled
            "GCA_000000003.1",  # superseded on day 2
            "GCA_000000004.2",  # superseded on day 2
        }

    def test_gaps_exclude_versions_already_backfilled(self, simulation):
        missing = {
            (record["base_accession"], record["missing_version"])
            for record in simulation["results"]["missing_versions"]
        }
        # v1 of the skipping base is already in the historical TSV, and v2 was
        # current yesterday, so only the two versions nobody has are reported.
        assert missing == {
            ("GCA_000000004", 3),
            ("GCA_000000004", 4),
            ("GCA_000000005", 1),
        }


class TestAggregation:
    def test_every_base_is_summarised(self, simulation):
        rows = read_tsv(simulation["work_dir"] / SUMMARY_TSV)
        assert {row["base_accession"] for row in rows} == set(BASES)

    def test_summary_reports_the_real_gaps(self, simulation):
        rows = {
            row["base_accession"]: row
            for row in read_tsv(simulation["work_dir"] / SUMMARY_TSV)
        }
        assert rows["GCA_000000004"]["version_gaps"] == "3,4"
        assert rows["GCA_000000005"]["version_gaps"] == "1"
        assert rows["GCA_000000001"]["version_gaps"] == ""

    def test_milestones_come_out_of_the_lineage_columns(self, simulation):
        rows = {
            int(row["taxid"]): row
            for row in read_tsv(simulation["work_dir"] / MILESTONE_TSV)
        }
        # Higher ranks are present without a taxdump ...
        assert rows[6001]["rank"] == "order"
        assert rows[4001]["rank"] == "kingdom"
        # ... and the "None" sentinel produced no class taxon.
        assert not any(row["rank"] == "class" for row in rows.values())

    def test_the_ebp_lineage_reaches_the_metric_milestones(self, simulation):
        rows = {
            int(row["taxid"]): row
            for row in read_tsv(simulation["work_dir"] / MILESTONE_TSV)
        }
        assert rows[EBP_TAXID]["first_ebp_metric_date"]
        assert rows[EBP_TAXID]["first_metric_date"]


class TestValidators:
    def test_the_run_validates_under_strict(self, simulation):
        assert validator.validate_pipeline(
            work_dir=str(simulation["work_dir"]), strict=True
        ) == 0

    def test_the_daily_parse_made_no_fetches(self, simulation):
        assert no_fetches.check_no_network(str(simulation["work_dir"])) == []
