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

The columns carry taxids only, no rank names, so scientific names still come
from a taxdump when one is supplied.  Species is covered: 630d327 (2026-09-07)
added it to the parser's canonical ranks, so a row names the species it
belongs to and a subspecies-level assembly no longer needs a parent chain
walked to attribute it.
"""

from typing import Optional

# Canonical ancestor ranks, finest first.  Matches load_taxonomy.
# CANONICAL_RANKS; the order matters here because it is the order lineages are
# reported in.  Species is deliberately not one of them: it is the level rows
# are grouped *at*, not an ancestor the sweep walks up to.
LINEAGE_RANKS = ("genus", "family", "order", "class", "phylum", "kingdom")

SPECIES_RANK = "species"

# Every rank upstream enriches a row with, in its own order.  Rich added
# species to the parser's CANONICAL_RANKS in 630d327 (2026-09-07), so a
# current row now names its species directly instead of leaving Phase 3 to
# walk a taxdump for it.
ENRICHED_RANKS = (SPECIES_RANK, *LINEAGE_RANKS)

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
    """Return every column upstream enriches a row with, finest rank first.

    Returns:
        list: Column names, ["speciesTaxId", "genusTaxId", ...,
            "kingdomTaxId"].
    """
    return [rank_column(rank) for rank in ENRICHED_RANKS]


def species_column() -> str:
    """Return the column holding the row's species taxid.

    Returns:
        str: "speciesTaxId".
    """
    return rank_column(SPECIES_RANK)


def row_species_taxid(row: dict) -> Optional[int]:
    """Return the species taxid upstream attached to a row.

    This is what makes the taxdump optional in production: an assembly
    submitted below species level names its species here, so it can be
    attributed to that species without a parent chain to walk.

    Args:
        row (dict): An assembly TSV row.

    Returns:
        int or None: The species taxid, or None when the column is absent or
            holds one of the absent sentinels -- which is what an assembly
            submitted at genus level or above looks like.
    """
    return parse_taxid(row.get(species_column()))


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

    Ancestors are registered first, at the rank whose column named them, then
    the species each row belongs to.  Taking them in that order matters: an
    assembly submitted at genus level carries a taxid that another row names
    as its genus, and registering row taxids first would label that genus a
    species, or not, depending on which row happened to come first.

    Which taxid is the species depends on what upstream supplied.  A row
    carrying a populated ``speciesTaxId`` names it outright, so a
    subspecies-level assembly is registered against its species rather than
    against itself.  Anything else -- an older TSV without the column, or the
    column present but empty -- falls back to the row's own taxid, the finest
    level anything then knows about it.

    The fallback deliberately covers the empty-column case rather than
    treating it as "no species".  Upstream fills the column from an entry in
    ``rec["lineage"]`` whose rank is ``species``, and blobtk puts each taxon
    into its own lineage array -- ``Node::to_json`` in ``parse/nodes.rs``
    emits the node itself at ``node_depth: 0`` ahead of its ancestors -- so a
    species-level assembly names itself and the column is populated.  What is
    left empty is an older TSV without the column, or a row above species
    rank, whose lineage genuinely has no species in it; both are attributed at
    the row's own taxid, the finest level anything then knows about it.  For a
    species-level row its own taxid *is* the species, so the fallback never
    does worse than the behaviour before the column existed.

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
        # The species column when it holds one, the row's own taxid
        # otherwise.  The fallback applies even when the column is present
        # but empty -- see the note in the docstring.
        species_taxid = row_species_taxid(row) or get_row_taxid(row)
        lineages.append((species_taxid, lineage))

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

    for species_taxid, lineage in lineages:
        if species_taxid is not None and species_taxid not in taxonomy:
            taxonomy[species_taxid] = {
                "scientific_name": "",
                "rank": SPECIES_RANK,
                "parent": None,
                "lineage": lineage,
            }
            stats["nodes_added"] += 1

    return stats
