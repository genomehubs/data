"""Parse an NCBI taxdump into the Phase 3 taxonomy contract (dev/test only).

This is a pure parser/builder. It performs no download and no caching —
fetching the taxdump is owned by update_ncbi_taxonomy.py. In production the
lineage is attached to assembly rows upstream (NCBI-dataset integration), so
this module is used only to run the pipeline end-to-end in development and in
tests.

It reads the standard NCBI dump files:

    nodes.dmp:  taxid | parent_taxid | rank | ...
    names.dmp:  taxid | name | unique_name | name_class |

and builds the in-memory contract consumed by compute_taxon_milestones:

    {
      taxid: {
        'scientific_name': str,
        'rank': str,                    # 'species', 'genus', 'subspecies', ...
        'parent': int,                  # one level up
        'lineage': {rank: taxid, ...}   # canonical ranks only, genus..kingdom
      },
      ...
    }

Usage:
    python -m flows.lib.load_taxonomy --taxdump_path test/taxonomy/ncbi
"""

import os

from flows.lib.shared_args import TAXDUMP_PATH
from flows.lib.shared_args import parse_args as _parse_args

# Canonical ranks captured in each node's lineage (genus up to kingdom).
CANONICAL_RANKS = {"genus", "family", "order", "class", "phylum", "kingdom"}

NODES_FILE = "nodes.dmp"
NAMES_FILE = "names.dmp"

# NCBI .dmp files separate fields with "\t|\t" and terminate rows with "\t|".
_FIELD_SEP = "\t|\t"
_ROW_TERMINATOR = "\t|"


def _split_dmp_line(line: str) -> list[str]:
    """Split a single .dmp line into its fields.

    Strips the trailing row terminator ("\\t|") and the line break, then splits
    on the field separator. Whitespace around each field is stripped.

    Args:
        line: A raw line from a .dmp file.

    Returns:
        List of field values (whitespace-trimmed).
    """
    line = line.rstrip("\n").rstrip("\r")
    if line.endswith(_ROW_TERMINATOR):
        line = line[: -len(_ROW_TERMINATOR)]
    return [field.strip() for field in line.split(_FIELD_SEP)]


def parse_nodes(nodes_dmp: str) -> dict[int, dict]:
    """Parse nodes.dmp into a taxid -> {parent, rank} map.

    Args:
        nodes_dmp: Path to the nodes.dmp file.

    Returns:
        Dict mapping taxid (int) to {'parent': int, 'rank': str}.
    """
    nodes = {}
    with open(nodes_dmp, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            fields = _split_dmp_line(line)
            if len(fields) < 3:
                continue
            taxid = int(fields[0])
            parent = int(fields[1])
            rank = fields[2]
            nodes[taxid] = {"parent": parent, "rank": rank}
    return nodes


def parse_names(names_dmp: str) -> dict[int, str]:
    """Parse names.dmp into a taxid -> scientific_name map.

    Only rows with name_class == 'scientific name' are retained.

    Args:
        names_dmp: Path to the names.dmp file.

    Returns:
        Dict mapping taxid (int) to its scientific name (str).
    """
    names = {}
    with open(names_dmp, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            fields = _split_dmp_line(line)
            if len(fields) < 4:
                continue
            if fields[3] == "scientific name":
                names[int(fields[0])] = fields[1]
    return names


def build_lineage(taxid: int, nodes: dict[int, dict]) -> dict[str, int]:
    """Walk the parent chain and collect the taxid at each canonical rank.

    Starts from the node's parent and walks up to (but not including) the root
    (taxid 1), recording the taxid wherever the node's rank is canonical.

    Args:
        taxid: The taxid whose lineage to build.
        nodes: The taxid -> {parent, rank} map from parse_nodes.

    Returns:
        Dict mapping canonical rank name to the ancestor taxid at that rank.
    """
    lineage = {}
    node = nodes.get(taxid)
    if node is None:
        return lineage
    # A visited set guards against cycles in the parent chain (corruption or
    # merged/deleted-taxid artifacts), which would otherwise loop forever since
    # the walk only stops at the root (taxid 1).
    visited = {taxid}
    current = node["parent"]
    while current and current != 1 and current not in visited:
        visited.add(current)
        ancestor = nodes.get(current)
        if ancestor is None:
            break
        if ancestor["rank"] in CANONICAL_RANKS:
            lineage[ancestor["rank"]] = current
        current = ancestor["parent"]
    return lineage


def build_taxonomy(taxdump_dir: str) -> dict[int, dict]:
    """Build the taxonomy contract from an NCBI taxdump directory.

    Combines nodes.dmp and names.dmp and computes a canonical-rank lineage for
    every node.

    Args:
        taxdump_dir: Directory containing nodes.dmp and names.dmp.

    Returns:
        The taxonomy contract: taxid -> {scientific_name, rank, parent, lineage}.
    """
    nodes_path = os.path.join(taxdump_dir, NODES_FILE)
    names_path = os.path.join(taxdump_dir, NAMES_FILE)

    nodes = parse_nodes(nodes_path)
    names = parse_names(names_path)

    taxonomy = {}
    for taxid, node in nodes.items():
        taxonomy[taxid] = {
            "scientific_name": names.get(taxid, ""),
            "rank": node["rank"],
            "parent": node["parent"],
            "lineage": build_lineage(taxid, nodes),
        }

    print(f"  Loaded {len(taxonomy)} taxonomic nodes from {taxdump_dir}")
    return taxonomy


if __name__ == "__main__":
    args = _parse_args([TAXDUMP_PATH], description=__doc__)
    if not args.taxdump_path:
        raise SystemExit("--taxdump_path is required")
    build_taxonomy(args.taxdump_path)
