"""Read the canonical-rank lineage that upstream attaches to assembly rows.

``parse_ncbi_assemblies`` enriches every parsed assembly with the taxid of its
ancestor at each canonical rank, writing one column per rank -- ``genusTaxId``
through ``kingdomTaxId`` (upstream 13269d0, refined in f14ea28).  Phase 3
consumes those columns as its production taxonomy source and falls back to a
local NCBI taxdump in dev and test.

Two properties of that upstream contract are handled here rather than at each
call site:

* the column name is derived from a single template, so a rename upstream is a
  one-line change in this module;
* a rank the lineage does not cover is written either as an empty string or as
  the literal four-character string ``"None"``.  Both mean absent -- a literal
  ``"None"`` read as a taxid would collapse unrelated lineages into one bogus
  taxon.

The columns carry taxids only: no rank names, and no species-rank taxid.  So
the lineage they provide is complete for genus..kingdom attribution, while
scientific names and subspecies-to-species resolution still come from a
taxdump when one is supplied.
"""

from typing import Optional

# Canonical ranks, finest first.  Matches load_taxonomy.CANONICAL_RANKS; the
# order matters here because it is the order lineages are reported in.
LINEAGE_RANKS = ("genus", "family", "order", "class", "phylum", "kingdom")

# The single point of repoint if upstream renames the lineage columns.
RANK_COLUMN_TEMPLATE = "{rank}TaxId"

# Column values that mean "this rank is not in the lineage".  "" comes from
# enrich_assembly_row_with_taxonomy for a rank missing from the lookup;
# "None" from load_taxonomy_lookup for a rank present with a null taxid.
ABSENT_TAXID_VALUES = frozenset({"", "None"})

# Row columns holding the assembly's own taxid, in precedence order.
TAXID_ALIASES = ("taxId", "taxid", "tax_id")


def rank_column(rank: str) -> str:
    """Return the row column holding the ancestor taxid at ``rank``.

    Args:
        rank (str): A canonical rank name, e.g. "genus".

    Returns:
        str: The column name, e.g. "genusTaxId".
    """
    return RANK_COLUMN_TEMPLATE.format(rank=rank)


def lineage_columns() -> list[str]:
    """Return every lineage column name, finest rank first.

    Returns:
        list: Column names, e.g. ["genusTaxId", ..., "kingdomTaxId"].
    """
    return [rank_column(rank) for rank in LINEAGE_RANKS]


def parse_taxid(value) -> Optional[int]:
    """Parse a taxid cell, treating the absent sentinels as no value.

    Args:
        value: Raw cell value from a TSV row.

    Returns:
        int or None: The taxid, or None when the cell is absent, is one of
            the ABSENT_TAXID_VALUES sentinels, or does not hold a positive
            integer.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text in ABSENT_TAXID_VALUES:
        return None
    try:
        taxid = int(text)
    except ValueError:
        return None
    return taxid if taxid > 0 else None


def get_row_taxid(row: dict) -> Optional[int]:
    """Return the assembly's own taxid, whichever column holds it.

    Args:
        row (dict): An assembly TSV row.

    Returns:
        int or None: The taxid, or None when no column holds a usable one.
    """
    for key in TAXID_ALIASES:
        if key in row:
            taxid = parse_taxid(row[key])
            if taxid is not None:
                return taxid
    return None


def has_lineage_columns(row: dict) -> bool:
    """Report whether a row carries the lineage columns at all.

    Presence is about the columns existing, not about them being populated:
    a row with every rank empty still went through enrichment, whereas a row
    without the columns never did.

    Args:
        row (dict): An assembly TSV row.

    Returns:
        bool: True when at least one lineage column is present on the row.
    """
    return any(column in row for column in lineage_columns())


def rows_have_lineage_columns(rows: list[dict]) -> bool:
    """Report whether any row in ``rows`` carries the lineage columns.

    Args:
        rows (list): Assembly TSV rows.

    Returns:
        bool: True when at least one row went through upstream enrichment.
    """
    return any(has_lineage_columns(row) for row in rows)


def row_lineage(row: dict) -> dict[str, int]:
    """Extract the canonical-rank lineage carried on an assembly row.

    Args:
        row (dict): An assembly TSV row.

    Returns:
        dict: Mapping of rank name to ancestor taxid, holding only the ranks
            the row actually populates.  Empty when the row was never
            enriched, or when every rank is one of the absent sentinels.
    """
    lineage = {}
    for rank in LINEAGE_RANKS:
        taxid = parse_taxid(row.get(rank_column(rank)))
        if taxid is not None:
            lineage[rank] = taxid
    return lineage


def register_row_taxa(taxonomy: dict[int, dict], rows: list[dict]) -> dict[str, int]:
    """Add taxonomy nodes for taxa that only the assembly rows know about.

    Nodes already in ``taxonomy`` -- from a taxdump -- are left untouched, so
    a dev/test run keeps the ranks and scientific names the taxdump supplies.
    Only rows carrying a lineage are registered: a row with an unresolvable
    taxid and no lineage columns stays unresolvable, exactly as before.

    Ancestors are registered first, at the rank whose column named them, and
    every remaining row taxid is then registered at rank "species" -- the
    finest level anything knows about it, since upstream emits no
    ``speciesTaxId`` and only a taxdump can collapse a subspecies onto its
    species.  Taking the ranks in that order matters: an assembly submitted at
    genus level carries a taxid that another row names as its genus, and
    registering row taxids first would label that genus a species, or not,
    depending on which row happened to come first.  Registered as a genus it
    has no species ancestor, so the sweep skips it and says so -- the same
    thing the taxdump path does with a genus-level assembly today.

    Args:
        taxonomy (dict): The taxonomy contract, mutated in place.
        rows (list): Combined current + historical assembly rows.

    Returns:
        dict: Counts with keys ``rows_with_lineage`` and ``nodes_added``.
    """
    stats = {"rows_with_lineage": 0, "nodes_added": 0}
    lineages = []

    for row in rows:
        lineage = row_lineage(row)
        if not lineage:
            continue
        stats["rows_with_lineage"] += 1
        lineages.append((get_row_taxid(row), lineage))

    for _, lineage in lineages:
        for rank, ancestor in lineage.items():
            if ancestor not in taxonomy:
                taxonomy[ancestor] = {
                    "scientific_name": "",
                    "rank": rank,
                    "parent": None,
                    "lineage": {},
                }
                stats["nodes_added"] += 1

    for taxid, lineage in lineages:
        if taxid is not None and taxid not in taxonomy:
            taxonomy[taxid] = {
                "scientific_name": "",
                "rank": "species",
                "parent": None,
                "lineage": lineage,
            }
            stats["nodes_added"] += 1

    return stats
