"""Tests for assembly_lineage.py and the Phase 3 production taxonomy path.

Covers:
- The upstream lineage-column contract: column naming, the two absent
  sentinels ("" and the literal string "None"), and taxid parsing.
- register_row_taxa: nodes added for taxa only the rows know about, taxdump
  nodes left untouched, rows without a lineage left unregistered.
- compute_taxon_milestones running off the columns alone (no taxdump), the
  columns winning over a taxdump lineage, and the failure when neither
  taxonomy source is available.
"""

import csv
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.lib.assembly_lineage import (  # noqa: E402
    ABSENT_TAXID_VALUES,
    LINEAGE_RANKS,
    get_row_taxid,
    has_lineage_columns,
    lineage_columns,
    parse_taxid,
    rank_column,
    register_row_taxa,
    row_lineage,
    rows_have_lineage_columns,
)
from flows.lib.compute_taxon_milestones import (  # noqa: E402
    compute_taxon_milestones,
)
from flows.lib.load_taxonomy import CANONICAL_RANKS  # noqa: E402

FIXTURE_DIR = str(Path(__file__).parent.parent / "test" / "taxonomy" / "ncbi")

# Isopoda fixture taxids, as in test_taxon_milestones.py.
ASELLUS_AQUATICUS = 92525
ASELLUS_GENUS = 92524
ASELLIDAE_FAMILY = 63227
ISOPODA_ORDER = 29979
MALACOSTRACA_CLASS = 6681
ARTHROPODA_PHYLUM = 6656
METAZOA_KINGDOM = 33208

CERATOTHOA_STEINDACHNERI = 2922061
CERATOTHOA_GENUS = 432123
CYMOTHOIDAE_FAMILY = 142082


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def enriched_row(
    accession,
    taxid,
    release_date,
    lineage=None,
    bioproject="PRJNA1",
    ebp_date="",
    version_status="current",
):
    """Build an assembly row carrying the upstream {rank}TaxId columns.

    Ranks absent from ``lineage`` are written as the empty string, matching
    enrich_assembly_row_with_taxonomy.
    """
    row = {
        "genbankAccession": accession,
        "taxId": str(taxid),
        "releaseDate": release_date,
        "bioProjectAccession": bioproject,
        "ebpStandardDate": ebp_date,
        "versionStatus": version_status,
    }
    lineage = lineage or {}
    for rank in LINEAGE_RANKS:
        value = lineage.get(rank, "")
        row[rank_column(rank)] = "" if value == "" else str(value)
    return row


ASELLUS_LINEAGE = {
    "genus": ASELLUS_GENUS,
    "family": ASELLIDAE_FAMILY,
    "order": ISOPODA_ORDER,
    "class": MALACOSTRACA_CLASS,
    "phylum": ARTHROPODA_PHYLUM,
    "kingdom": METAZOA_KINGDOM,
}

CERATOTHOA_LINEAGE = {
    "genus": CERATOTHOA_GENUS,
    "family": CYMOTHOIDAE_FAMILY,
    "order": ISOPODA_ORDER,
    "class": MALACOSTRACA_CLASS,
    "phylum": ARTHROPODA_PHYLUM,
    "kingdom": METAZOA_KINGDOM,
}


def write_tsv(path, rows):
    """Write a list of dicts to a tab-separated file."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path):
    """Read a tab-separated file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def summary_by_taxid(work_dir):
    """Read taxon_milestone_summary.tsv into a taxid-keyed dict."""
    rows = read_tsv(Path(work_dir) / "taxon_milestone_summary.tsv")
    return {int(row["taxid"]): row for row in rows}


# ---------------------------------------------------------------------------
# The upstream column contract
# ---------------------------------------------------------------------------

class TestColumnContract:
    def test_column_names_match_upstream(self):
        assert lineage_columns() == [
            "genusTaxId",
            "familyTaxId",
            "orderTaxId",
            "classTaxId",
            "phylumTaxId",
            "kingdomTaxId",
        ]

    def test_ranks_match_load_taxonomy(self):
        # One rank set across both taxonomy sources, or the taxdump and the
        # column lineages would disagree about which ranks exist.
        assert set(LINEAGE_RANKS) == CANONICAL_RANKS

    def test_no_species_column(self):
        # Upstream emits no speciesTaxId, which is why the species walk stays.
        assert "speciesTaxId" not in lineage_columns()


class TestParseTaxid:
    @pytest.mark.parametrize("value", ["", "None", None])
    def test_absent_sentinels(self, value):
        assert parse_taxid(value) is None

    def test_none_string_is_the_documented_sentinel(self):
        # f14ea28 writes str(None) for a rank present with a null taxid.
        assert "None" in ABSENT_TAXID_VALUES

    def test_parses_int_and_str(self):
        assert parse_taxid("92525") == 92525
        assert parse_taxid(92525) == 92525
        assert parse_taxid("  92525  ") == 92525

    @pytest.mark.parametrize("value", ["0", "-1", "abc", "92525.0"])
    def test_rejects_non_positive_and_junk(self, value):
        assert parse_taxid(value) is None


class TestRowLineage:
    def test_full_lineage(self):
        row = enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE)
        assert row_lineage(row) == ASELLUS_LINEAGE

    def test_none_string_rank_is_dropped(self):
        row = enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE)
        row["classTaxId"] = "None"
        lineage = row_lineage(row)
        assert "class" not in lineage
        assert lineage["order"] == ISOPODA_ORDER

    def test_empty_rank_is_dropped(self):
        row = enriched_row(
            "GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", {"genus": ASELLUS_GENUS}
        )
        assert row_lineage(row) == {"genus": ASELLUS_GENUS}

    def test_unenriched_row_has_no_lineage(self):
        row = {"genbankAccession": "GCA_AA.1", "taxId": str(ASELLUS_AQUATICUS)}
        assert row_lineage(row) == {}
        assert has_lineage_columns(row) is False

    def test_enriched_but_empty_row_still_counts_as_enriched(self):
        # Every rank blank still means the row went through enrichment.
        row = enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01")
        assert row_lineage(row) == {}
        assert has_lineage_columns(row) is True

    def test_rows_have_lineage_columns_is_any(self):
        plain = {"genbankAccession": "GCA_AA.1", "taxId": "1"}
        enriched = enriched_row("GCA_CS.1", CERATOTHOA_STEINDACHNERI, "2019-01-01")
        assert rows_have_lineage_columns([plain]) is False
        assert rows_have_lineage_columns([plain, enriched]) is True


class TestGetRowTaxid:
    @pytest.mark.parametrize("column", ["taxId", "taxid", "tax_id"])
    def test_reads_every_spelling(self, column):
        assert get_row_taxid({column: "92525"}) == 92525

    def test_prefers_camel_case(self):
        assert get_row_taxid({"taxId": "1", "taxid": "2"}) == 1

    def test_falls_through_empty_column(self):
        assert get_row_taxid({"taxId": "", "taxid": "2"}) == 2

    def test_missing_taxid(self):
        assert get_row_taxid({"genbankAccession": "GCA_AA.1"}) is None


# ---------------------------------------------------------------------------
# register_row_taxa
# ---------------------------------------------------------------------------

class TestRegisterRowTaxa:
    def test_registers_species_and_ancestors(self):
        taxonomy = {}
        rows = [
            enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE)
        ]
        stats = register_row_taxa(taxonomy, rows)

        assert stats["rows_with_lineage"] == 1
        assert stats["nodes_added"] == 7  # the species plus six ancestors
        assert taxonomy[ASELLUS_AQUATICUS]["rank"] == "species"
        assert taxonomy[ASELLUS_AQUATICUS]["lineage"] == ASELLUS_LINEAGE
        assert taxonomy[ISOPODA_ORDER]["rank"] == "order"
        assert taxonomy[ISOPODA_ORDER]["scientific_name"] == ""

    def test_shared_ancestors_registered_once(self):
        taxonomy = {}
        rows = [
            enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE),
            enriched_row(
                "GCA_CS.1", CERATOTHOA_STEINDACHNERI, "2019-01-01", CERATOTHOA_LINEAGE
            ),
        ]
        stats = register_row_taxa(taxonomy, rows)

        # 7 for the first row, then a species, genus and family for the second.
        assert stats["nodes_added"] == 10
        assert stats["rows_with_lineage"] == 2

    def test_taxdump_nodes_are_not_overwritten(self):
        taxonomy = {
            ISOPODA_ORDER: {
                "scientific_name": "Isopoda",
                "rank": "order",
                "parent": MALACOSTRACA_CLASS,
                "lineage": {},
            }
        }
        rows = [
            enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE)
        ]
        register_row_taxa(taxonomy, rows)
        assert taxonomy[ISOPODA_ORDER]["scientific_name"] == "Isopoda"
        assert taxonomy[ISOPODA_ORDER]["parent"] == MALACOSTRACA_CLASS

    def test_rows_without_lineage_are_not_registered(self):
        taxonomy = {}
        rows = [{"genbankAccession": "GCA_AA.1", "taxId": str(ASELLUS_AQUATICUS)}]
        stats = register_row_taxa(taxonomy, rows)
        assert stats == {"rows_with_lineage": 0, "nodes_added": 0}
        assert taxonomy == {}

    @pytest.mark.parametrize("reverse", [False, True])
    def test_a_taxid_named_as_an_ancestor_keeps_its_rank(self, reverse):
        # An assembly submitted at genus level carries a taxid that another
        # row names as its genus. Which row is seen first must not decide
        # whether that taxon is a genus or a species.
        rows = [
            enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE),
            enriched_row(
                "GCA_SP.1",
                ASELLUS_GENUS,
                "2021-01-01",
                {rank: taxid for rank, taxid in ASELLUS_LINEAGE.items()
                 if rank != "genus"},
            ),
        ]
        taxonomy = {}
        register_row_taxa(taxonomy, list(reversed(rows)) if reverse else rows)
        assert taxonomy[ASELLUS_GENUS]["rank"] == "genus"

    def test_row_without_a_taxid_still_registers_its_ancestors(self):
        taxonomy = {}
        row = enriched_row("GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE)
        row["taxId"] = ""
        register_row_taxa(taxonomy, [row])
        assert ASELLUS_AQUATICUS not in taxonomy
        assert taxonomy[ASELLUS_GENUS]["rank"] == "genus"


# ---------------------------------------------------------------------------
# The production path end to end
# ---------------------------------------------------------------------------

class TestProductionPath:
    def test_columns_alone_compute_milestones(self, tmp_path):
        write_tsv(
            tmp_path / "assembly_current.tsv",
            [
                enriched_row(
                    "GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01", ASELLUS_LINEAGE
                ),
                enriched_row(
                    "GCA_CS.1",
                    CERATOTHOA_STEINDACHNERI,
                    "2017-01-01",
                    CERATOTHOA_LINEAGE,
                ),
            ],
        )

        compute_taxon_milestones(work_dir=str(tmp_path))

        by_taxid = summary_by_taxid(tmp_path)
        # Order Isopoda takes the clade-earliest assembly, exactly as it does
        # off the taxdump.
        assert by_taxid[ISOPODA_ORDER]["first_assembly_date"] == "2017-01-01"
        assert by_taxid[ISOPODA_ORDER]["first_assembly_accession"] == "GCA_CS.1"
        assert by_taxid[ISOPODA_ORDER]["rank"] == "order"
        # Names need the taxdump, which this path does not have.
        assert by_taxid[ISOPODA_ORDER]["scientific_name"] == ""
        # Species rows still carry their in-ranks and counts.
        assert by_taxid[ASELLUS_AQUATICUS]["first_assembly_in_ranks"] == "genus,family"
        assert by_taxid[ASELLUS_AQUATICUS]["total_assemblies"] == "1"

    def test_none_sentinel_does_not_collapse_lineages(self, tmp_path):
        # Two species whose class is written as the literal "None". Reading
        # that as a taxid would put both under one bogus class taxon.
        rows = []
        for accession, taxid, lineage in [
            ("GCA_AA.1", ASELLUS_AQUATICUS, ASELLUS_LINEAGE),
            ("GCA_CS.1", CERATOTHOA_STEINDACHNERI, CERATOTHOA_LINEAGE),
        ]:
            row = enriched_row(accession, taxid, "2020-01-01", lineage)
            row["classTaxId"] = "None"
            rows.append(row)
        write_tsv(tmp_path / "assembly_current.tsv", rows)

        compute_taxon_milestones(work_dir=str(tmp_path))

        out = read_tsv(tmp_path / "taxon_milestone_summary.tsv")
        assert not any(row["taxid"] == "None" for row in out)
        assert not any(row["rank"] == "class" for row in out)
        # The ranks either side of the hole are unaffected.
        by_taxid = summary_by_taxid(tmp_path)
        assert by_taxid[ISOPODA_ORDER]["rank"] == "order"
        assert by_taxid[ARTHROPODA_PHYLUM]["rank"] == "phylum"

    def test_columns_win_over_the_taxdump(self, tmp_path):
        # Same species, but upstream places it in the Ceratothoa genus and
        # family. The lineage on the row is what the sweep must use.
        row = enriched_row(
            "GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01", CERATOTHOA_LINEAGE
        )
        write_tsv(tmp_path / "assembly_current.tsv", [row])

        compute_taxon_milestones(work_dir=str(tmp_path), taxdump_path=FIXTURE_DIR)

        by_taxid = summary_by_taxid(tmp_path)
        assert CERATOTHOA_GENUS in by_taxid
        assert ASELLUS_GENUS not in by_taxid
        # The taxdump still supplies the names the columns cannot.
        assert by_taxid[CERATOTHOA_GENUS]["scientific_name"] == "Ceratothoa"
        assert by_taxid[ASELLUS_AQUATICUS]["scientific_name"] == "Asellus aquaticus"

    def test_unenriched_rows_without_a_taxdump_fail_loudly(self, tmp_path):
        write_tsv(
            tmp_path / "assembly_current.tsv",
            [
                {
                    "genbankAccession": "GCA_AA.1",
                    "taxId": str(ASELLUS_AQUATICUS),
                    "releaseDate": "2019-01-01",
                }
            ],
        )
        with pytest.raises(SystemExit, match="No taxonomy source"):
            compute_taxon_milestones(work_dir=str(tmp_path))

    def test_a_genus_level_assembly_is_skipped_not_relabelled(self, tmp_path):
        # The taxdump path already skips an assembly with no species ancestor.
        # The column path must not smuggle one in as a pseudo-species.
        write_tsv(
            tmp_path / "assembly_current.tsv",
            [
                enriched_row(
                    "GCA_AA.1", ASELLUS_AQUATICUS, "2020-01-01", ASELLUS_LINEAGE
                ),
                enriched_row(
                    "GCA_SP.1",
                    ASELLUS_GENUS,
                    "2019-01-01",
                    {rank: taxid for rank, taxid in ASELLUS_LINEAGE.items()
                     if rank != "genus"},
                ),
            ],
        )

        compute_taxon_milestones(work_dir=str(tmp_path))

        by_taxid = summary_by_taxid(tmp_path)
        assert by_taxid[ASELLUS_GENUS]["rank"] == "genus"
        # Its own date never lands anywhere: the genus keeps the date of the
        # species assembly that resolved, not the earlier unresolvable one.
        assert by_taxid[ASELLUS_GENUS]["first_assembly_date"] == "2020-01-01"
        assert by_taxid[ASELLUS_GENUS]["total_assemblies"] == ""

    def test_partial_lineage_attributes_only_the_ranks_present(self, tmp_path):
        write_tsv(
            tmp_path / "assembly_current.tsv",
            [
                enriched_row(
                    "GCA_AA.1",
                    ASELLUS_AQUATICUS,
                    "2019-01-01",
                    {"genus": ASELLUS_GENUS, "family": ASELLIDAE_FAMILY},
                )
            ],
        )

        compute_taxon_milestones(work_dir=str(tmp_path))

        out = read_tsv(tmp_path / "taxon_milestone_summary.tsv")
        taxids = {int(row["taxid"]) for row in out}
        assert taxids == {ASELLUS_AQUATICUS, ASELLUS_GENUS, ASELLIDAE_FAMILY}
        by_taxid = summary_by_taxid(tmp_path)
        assert by_taxid[ASELLUS_AQUATICUS]["first_assembly_in_ranks"] == "genus,family"
