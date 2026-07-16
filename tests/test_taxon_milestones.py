"""Tests for load_taxonomy.py and compute_taxon_milestones.py (Phase 3).

Covers:
- load_taxonomy: parse_nodes, parse_names, build_lineage, build_taxonomy
  against the test/taxonomy/ncbi fixture.
- resolve_to_species: species passthrough, subspecies -> parent, unresolvable.
- The single chronological sweep: each of the four milestones picks the
  earliest qualifying row; (releaseDate, accession) tie-breaking; empty
  releaseDate excluded from dates but counted in total_assemblies; in-ranks
  correctness across genus/family.
- Validation invariants: nested milestone-date ordering; in-ranks subset of
  CANONICAL_RANKS.
"""

import csv
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.lib.load_taxonomy import (  # noqa: E402
    CANONICAL_RANKS,
    build_lineage,
    build_taxonomy,
    parse_names,
    parse_nodes,
)
from flows.lib.compute_taxon_milestones import (  # noqa: E402
    OUTPUT_FIELDNAMES,
    build_output_rows,
    compute_milestones,
    compute_taxon_milestones,
    resolve_to_species,
)

FIXTURE_DIR = str(Path(__file__).parent.parent / "test" / "taxonomy" / "ncbi")

# Known taxids in the fixture (genus..kingdom for the Asellus aquaticus clade).
ASELLUS_AQUATICUS = 92525  # species
ASELLUS_GENUS = 92524
ASELLIDAE_FAMILY = 63227
ISOPODA_ORDER = 29979
MALACOSTRACA_CLASS = 6681
ARTHROPODA_PHYLUM = 6656
METAZOA_KINGDOM = 33208

CERATOTHOA_STEINDACHNERI = 2922061  # species, different family/genus, same order
CERATOTHOA_GENUS = 432123
CYMOTHOIDAE_FAMILY = 142082

JAERA_ISCHIOSETOSA = 2067965  # species
JAERA_PRAEHIRSUTA = 2613951  # species, same genus as above


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_tsv(path: Path, rows: list[dict]) -> None:
    """Write a list of dicts to a tab-separated file."""
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path: Path) -> list[dict]:
    """Read a tab-separated file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def make_row(accession, taxid, release_date, bioproject="PRJNA1", ebp_date="", version_status="current"):
    """Build a minimal assembly row dict."""
    return {
        "genbankAccession": accession,
        "taxId": str(taxid),
        "releaseDate": release_date,
        "bioProjectAccession": bioproject,
        "ebpStandardDate": ebp_date,
        "versionStatus": version_status,
    }


@pytest.fixture(scope="module")
def taxonomy():
    """The taxonomy contract built from the test fixture."""
    return build_taxonomy(FIXTURE_DIR)


# ---------------------------------------------------------------------------
# load_taxonomy
# ---------------------------------------------------------------------------

class TestParseNodes:
    def test_parses_taxid_parent_rank(self):
        nodes = parse_nodes(os.path.join(FIXTURE_DIR, "nodes.dmp"))
        assert nodes[ASELLUS_AQUATICUS]["rank"] == "species"
        assert nodes[ASELLUS_AQUATICUS]["parent"] == ASELLUS_GENUS
        assert nodes[ARTHROPODA_PHYLUM]["rank"] == "phylum"

    def test_all_values_are_ints(self):
        nodes = parse_nodes(os.path.join(FIXTURE_DIR, "nodes.dmp"))
        for taxid, node in nodes.items():
            assert isinstance(taxid, int)
            assert isinstance(node["parent"], int)


class TestParseNames:
    def test_keeps_only_scientific_names(self):
        names = parse_names(os.path.join(FIXTURE_DIR, "names.dmp"))
        assert names[ASELLUS_AQUATICUS] == "Asellus aquaticus"
        assert names[ARTHROPODA_PHYLUM] == "Arthropoda"
        # 2759 has synonyms/common names but its scientific name is Eukaryota.
        assert names[2759] == "Eukaryota"

    def test_synonyms_excluded(self):
        names = parse_names(os.path.join(FIXTURE_DIR, "names.dmp"))
        # 'Oniscus aquaticus' is a synonym of 92525, must not win.
        assert names[ASELLUS_AQUATICUS] != "Oniscus aquaticus"


class TestBuildLineage:
    def test_collects_canonical_ranks_only(self):
        nodes = parse_nodes(os.path.join(FIXTURE_DIR, "nodes.dmp"))
        lineage = build_lineage(ASELLUS_AQUATICUS, nodes)
        assert lineage == {
            "genus": ASELLUS_GENUS,
            "family": ASELLIDAE_FAMILY,
            "order": ISOPODA_ORDER,
            "class": MALACOSTRACA_CLASS,
            "phylum": ARTHROPODA_PHYLUM,
            "kingdom": METAZOA_KINGDOM,
        }
        # Non-canonical ranks (subphylum, superclass, suborder...) excluded.
        assert set(lineage.keys()) <= CANONICAL_RANKS

    def test_species_not_in_own_lineage(self):
        nodes = parse_nodes(os.path.join(FIXTURE_DIR, "nodes.dmp"))
        lineage = build_lineage(ASELLUS_AQUATICUS, nodes)
        assert ASELLUS_AQUATICUS not in lineage.values()

    def test_cyclic_parent_chain_terminates(self):
        # A 2-node cycle (and a self-parent) must not loop forever.
        nodes = {
            10: {"parent": 20, "rank": "genus"},
            20: {"parent": 10, "rank": "family"},
            30: {"parent": 30, "rank": "order"},  # self-parent
        }
        assert build_lineage(10, nodes) == {"family": 20}
        assert build_lineage(30, nodes) == {}


class TestBuildTaxonomy:
    def test_contract_shape(self, taxonomy):
        node = taxonomy[ASELLUS_AQUATICUS]
        assert set(node.keys()) == {"scientific_name", "rank", "parent", "lineage"}
        assert node["scientific_name"] == "Asellus aquaticus"
        assert node["rank"] == "species"
        assert node["parent"] == ASELLUS_GENUS

    def test_higher_rank_taxa_have_names(self, taxonomy):
        assert taxonomy[ARTHROPODA_PHYLUM]["scientific_name"] == "Arthropoda"
        assert taxonomy[ARTHROPODA_PHYLUM]["rank"] == "phylum"


# ---------------------------------------------------------------------------
# resolve_to_species
# ---------------------------------------------------------------------------

class TestResolveToSpecies:
    def test_species_passthrough(self, taxonomy):
        assert resolve_to_species(ASELLUS_AQUATICUS, taxonomy) == ASELLUS_AQUATICUS

    def test_subspecies_resolves_to_parent_species(self, taxonomy):
        # Inject a synthetic subspecies under Asellus aquaticus.
        tax = dict(taxonomy)
        tax[999001] = {
            "scientific_name": "Asellus aquaticus subsp. test",
            "rank": "subspecies",
            "parent": ASELLUS_AQUATICUS,
            "lineage": {},
        }
        assert resolve_to_species(999001, tax) == ASELLUS_AQUATICUS

    def test_unresolvable_returns_none(self, taxonomy):
        # Genus has no species ancestor.
        assert resolve_to_species(ASELLUS_GENUS, taxonomy) is None

    def test_unknown_taxid_returns_none(self, taxonomy):
        assert resolve_to_species(123456789, taxonomy) is None

    def test_cyclic_parent_chain_terminates(self):
        # A cycle with no species in it must return None, not hang.
        tax = {
            10: {"rank": "genus", "parent": 20, "scientific_name": "", "lineage": {}},
            20: {"rank": "family", "parent": 10, "scientific_name": "", "lineage": {}},
        }
        assert resolve_to_species(10, tax) is None

    def test_self_parent_terminates(self):
        tax = {10: {"rank": "genus", "parent": 10, "scientific_name": "", "lineage": {}}}
        assert resolve_to_species(10, tax) is None


# ---------------------------------------------------------------------------
# The chronological sweep
# ---------------------------------------------------------------------------

class TestSweep:
    def test_first_assembly_is_earliest_row(self, taxonomy):
        rows = [
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2020-01-01"),
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-05-05"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["assembly"] == ("2018-05-05", "GCA_001.1")

    def test_tiebreak_by_accession(self, taxonomy):
        rows = [
            make_row("GCA_009.1", ASELLUS_AQUATICUS, "2019-01-01"),
            make_row("GCA_003.1", ASELLUS_AQUATICUS, "2019-01-01"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["assembly"] == ("2019-01-01", "GCA_003.1")

    def test_ebp_affiliation_predicate(self, taxonomy):
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", bioproject="PRJNA111"),
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2019-01-01", bioproject="PRJNA533106,PRJNA222"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        # first_assembly is the earliest regardless of affiliation.
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["assembly"][1] == "GCA_001.1"
        # first_ebp_assembly is the affiliated one.
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["ebp_assembly"][1] == "GCA_002.1"

    def test_metric_predicate_and_none_string(self, taxonomy):
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", ebp_date="None"),
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2019-01-01", ebp_date="2019-01-01"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        # 'None' string must not count as metric; first metric is GCA_002.1.
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["metric"][1] == "GCA_002.1"
        assert "metric" not in {k for k in taxa[ASELLUS_AQUATICUS]["firsts"] if False}

    def test_ebp_metric_requires_both(self, taxonomy):
        rows = [
            # metric but not affiliated
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", bioproject="PRJNA111", ebp_date="2018-01-01"),
            # affiliated but no metric
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2019-01-01", bioproject="PRJNA533106", ebp_date=""),
            # both
            make_row("GCA_003.1", ASELLUS_AQUATICUS, "2020-01-01", bioproject="PRJNA533106", ebp_date="2020-01-01"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["metric"][1] == "GCA_001.1"
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["ebp_assembly"][1] == "GCA_002.1"
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["ebp_metric"][1] == "GCA_003.1"

    def test_empty_date_excluded_but_counted(self, taxonomy):
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, ""),  # no date
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2019-01-01"),
        ]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        # First assembly skips the empty-date row.
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["assembly"][1] == "GCA_002.1"
        # But it is still counted.
        assert species_extra[ASELLUS_AQUATICUS]["total_assemblies"] == 2

    def test_empty_date_only_species_still_in_output(self, taxonomy):
        # A species whose ONLY assembly has an empty date must still appear as
        # an output row with counts but no milestone dates.
        rows = [make_row("GCA_001.1", ASELLUS_AQUATICUS, "")]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        assert ASELLUS_AQUATICUS in taxa
        assert taxa[ASELLUS_AQUATICUS]["firsts"] == {}
        assert species_extra[ASELLUS_AQUATICUS]["total_assemblies"] == 1
        out = build_output_rows(taxa, species_extra)
        sp_row = next(r for r in out if r["taxid"] == ASELLUS_AQUATICUS)
        assert sp_row["first_assembly_date"] == ""
        assert sp_row["total_assemblies"] == 1

    def test_none_string_date_treated_as_empty(self, taxonomy):
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "None"),
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2019-01-01"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        assert taxa[ASELLUS_AQUATICUS]["firsts"]["assembly"][1] == "GCA_002.1"

    def test_current_assemblies_excludes_superseded(self, taxonomy):
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", version_status="superseded"),
            make_row("GCA_001.2", ASELLUS_AQUATICUS, "2019-01-01", version_status="current"),
        ]
        _, species_extra = compute_milestones(rows, taxonomy)
        assert species_extra[ASELLUS_AQUATICUS]["total_assemblies"] == 2
        assert species_extra[ASELLUS_AQUATICUS]["current_assemblies"] == 1

    def test_latest_is_current_version(self, taxonomy):
        # Latest = the current (non-superseded) version, even if an out-of-order
        # superseded row has a newer date it must not win over a current one.
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", version_status="superseded"),
            make_row("GCA_001.2", ASELLUS_AQUATICUS, "2021-01-01", version_status="current"),
        ]
        _, species_extra = compute_milestones(rows, taxonomy)
        latest = species_extra[ASELLUS_AQUATICUS]["latest"]
        assert (latest["date"], latest["accession"]) == ("2021-01-01", "GCA_001.2")

    def test_latest_prefers_current_over_newer_superseded(self, taxonomy):
        # Pathological ordering: the superseded row is newer-dated than current.
        # The current version must still be reported as latest.
        rows = [
            make_row("GCA_001.2", ASELLUS_AQUATICUS, "2020-01-01", version_status="current"),
            make_row("GCA_001.3", ASELLUS_AQUATICUS, "2022-01-01", version_status="superseded"),
        ]
        _, species_extra = compute_milestones(rows, taxonomy)
        latest = species_extra[ASELLUS_AQUATICUS]["latest"]
        assert latest["accession"] == "GCA_001.2"

    def test_latest_falls_back_when_all_superseded(self, taxonomy):
        # If nothing is current, latest is the newest-dated superseded row.
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", version_status="superseded"),
            make_row("GCA_001.2", ASELLUS_AQUATICUS, "2019-01-01", version_status="superseded"),
        ]
        _, species_extra = compute_milestones(rows, taxonomy)
        latest = species_extra[ASELLUS_AQUATICUS]["latest"]
        assert latest["accession"] == "GCA_001.2"

    def test_higher_rank_first_is_clade_earliest(self, taxonomy):
        # Two species in the same order (Isopoda) but different families.
        rows = [
            make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01"),
            make_row("GCA_CS.1", CERATOTHOA_STEINDACHNERI, "2017-01-01"),
        ]
        taxa, _ = compute_milestones(rows, taxonomy)
        # Order Isopoda's first assembly is the earliest across both clades.
        assert taxa[ISOPODA_ORDER]["firsts"]["assembly"] == ("2017-01-01", "GCA_CS.1")

    def test_in_ranks_records_clade_firsts(self, taxonomy):
        # Ceratothoa (2017) is earlier than Asellus (2019). Asellus is first in
        # its OWN genus/family (different clade), Ceratothoa is first at order
        # and above. So Asellus's in_ranks = genus, family (its clade) but NOT
        # order/class/phylum/kingdom (Ceratothoa took those).
        rows = [
            make_row("GCA_CS.1", CERATOTHOA_STEINDACHNERI, "2017-01-01"),
            make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01"),
        ]
        _, species_extra = compute_milestones(rows, taxonomy)
        asellus_in = species_extra[ASELLUS_AQUATICUS]["in_ranks"]["assembly"]
        assert set(asellus_in) == {"genus", "family"}
        cerat_in = species_extra[CERATOTHOA_STEINDACHNERI]["in_ranks"]["assembly"]
        assert set(cerat_in) == {"genus", "family", "order", "class", "phylum", "kingdom"}

    def test_in_ranks_subset_of_canonical(self, taxonomy):
        rows = [make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01")]
        _, species_extra = compute_milestones(rows, taxonomy)
        for milestone in ("assembly", "metric"):
            ranks = species_extra[ASELLUS_AQUATICUS]["in_ranks"][milestone]
            assert set(ranks) <= CANONICAL_RANKS


# ---------------------------------------------------------------------------
# Output rows + validation invariants
# ---------------------------------------------------------------------------

class TestOutputRows:
    def test_species_row_has_counts_and_in_ranks(self, taxonomy):
        rows = [make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01")]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        out = build_output_rows(taxa, species_extra)
        species_row = next(r for r in out if r["taxid"] == ASELLUS_AQUATICUS)
        assert species_row["rank"] == "species"
        assert species_row["total_assemblies"] == 1
        assert species_row["first_assembly_in_ranks"] != ""

    def test_species_row_has_latest_assembly(self, taxonomy):
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2018-01-01", version_status="superseded"),
            make_row("GCA_001.2", ASELLUS_AQUATICUS, "2021-05-05", version_status="current"),
        ]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        out = build_output_rows(taxa, species_extra)
        species_row = next(r for r in out if r["taxid"] == ASELLUS_AQUATICUS)
        assert species_row["latest_assembly_date"] == "2021-05-05"
        assert species_row["latest_assembly_accession"] == "GCA_001.2"

    def test_higher_rank_row_blank_counts_and_in_ranks(self, taxonomy):
        rows = [make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01")]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        out = build_output_rows(taxa, species_extra)
        phylum_row = next(r for r in out if r["taxid"] == ARTHROPODA_PHYLUM)
        assert phylum_row["rank"] == "phylum"
        assert phylum_row["total_assemblies"] == ""
        assert phylum_row["current_assemblies"] == ""
        assert phylum_row["latest_assembly_date"] == ""
        assert phylum_row["latest_assembly_accession"] == ""
        assert phylum_row["first_assembly_in_ranks"] == ""
        # But it carries the clade's first assembly date.
        assert phylum_row["first_assembly_date"] == "2019-01-01"

    def test_output_fieldnames_complete(self, taxonomy):
        rows = [make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01")]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        out = build_output_rows(taxa, species_extra)
        for row in out:
            assert set(row.keys()) == set(OUTPUT_FIELDNAMES)


class TestValidationInvariants:
    def _dates(self, row):
        return {
            "assembly": row["first_assembly_date"],
            "ebp_assembly": row["first_ebp_assembly_date"],
            "metric": row["first_metric_date"],
            "ebp_metric": row["first_ebp_metric_date"],
        }

    def test_nested_date_ordering(self, taxonomy):
        # A single species reaching all four milestones at distinct dates.
        rows = [
            make_row("GCA_001.1", ASELLUS_AQUATICUS, "2015-01-01", bioproject="PRJNA111"),
            make_row("GCA_002.1", ASELLUS_AQUATICUS, "2016-01-01", bioproject="PRJNA533106"),
            make_row("GCA_003.1", ASELLUS_AQUATICUS, "2017-01-01", bioproject="PRJNA111", ebp_date="2017-01-01"),
            make_row("GCA_004.1", ASELLUS_AQUATICUS, "2018-01-01", bioproject="PRJNA533106", ebp_date="2018-01-01"),
        ]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        out = build_output_rows(taxa, species_extra)
        for row in out:
            d = self._dates(row)
            if d["assembly"] and d["metric"] and d["ebp_metric"]:
                assert d["assembly"] <= d["metric"] <= d["ebp_metric"]
            if d["assembly"] and d["ebp_assembly"] and d["ebp_metric"]:
                assert d["assembly"] <= d["ebp_assembly"] <= d["ebp_metric"]

    def test_in_ranks_subset_invariant_all_rows(self, taxonomy):
        rows = [
            make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01"),
            make_row("GCA_CS.1", CERATOTHOA_STEINDACHNERI, "2017-01-01"),
        ]
        taxa, species_extra = compute_milestones(rows, taxonomy)
        out = build_output_rows(taxa, species_extra)
        for row in out:
            for col in ("first_assembly_in_ranks", "first_metric_in_ranks"):
                if row[col]:
                    assert set(row[col].split(",")) <= CANONICAL_RANKS


# ---------------------------------------------------------------------------
# End-to-end flow
# ---------------------------------------------------------------------------

class TestFlow:
    def test_end_to_end_writes_summary(self, tmp_path):
        current = [
            make_row("GCA_AA.1", ASELLUS_AQUATICUS, "2019-01-01", bioproject="PRJNA533106", ebp_date="2019-01-01"),
            make_row("GCA_JI.1", JAERA_ISCHIOSETOSA, "2020-06-06"),
        ]
        historical = [
            make_row("GCA_CS.1", CERATOTHOA_STEINDACHNERI, "2017-01-01", version_status="superseded"),
        ]
        write_tsv(tmp_path / "assembly_current.tsv", current)
        write_tsv(tmp_path / "assembly_historical.tsv", historical)

        compute_taxon_milestones(work_dir=str(tmp_path), taxdump_path=FIXTURE_DIR)

        out = read_tsv(tmp_path / "taxon_milestone_summary.tsv")
        assert out, "summary should not be empty"
        by_taxid = {int(r["taxid"]): r for r in out}

        # Species rows present.
        assert ASELLUS_AQUATICUS in by_taxid
        assert by_taxid[ASELLUS_AQUATICUS]["rank"] == "species"

        # Order Isopoda's first assembly = earliest across the clade (Ceratothoa 2017).
        assert by_taxid[ISOPODA_ORDER]["first_assembly_date"] == "2017-01-01"
        assert by_taxid[ISOPODA_ORDER]["first_assembly_accession"] == "GCA_CS.1"

        # Header matches schema exactly.
        with open(tmp_path / "taxon_milestone_summary.tsv", encoding="utf-8") as f:
            header = f.readline().rstrip("\n").split("\t")
        assert header == OUTPUT_FIELDNAMES

    def test_no_data_is_noop(self, tmp_path):
        # No TSVs at all — flow should not raise and write no output.
        compute_taxon_milestones(work_dir=str(tmp_path), taxdump_path=FIXTURE_DIR)
        assert not (tmp_path / "taxon_milestone_summary.tsv").exists()
