"""Compute per-taxon assembly milestones via a single chronological sweep.

Reads assembly_current.tsv and assembly_historical.tsv (row-level version
detail) from the working directory, resolves each assembly's taxId to its
species, attaches the canonical-rank lineage, and walks all rows in
(releaseDate, accession) order to record, for every taxon at every rank, the
first assembly that reached each of four milestones:

    first_assembly       — any assembly
    first_ebp_assembly   — EBP-affiliated submitter (PRJNA533106)
    first_metric         — meets the EBP quality standard (any submitter)
    first_ebp_metric     — meets the metric AND EBP-affiliated

The any-submitter milestones (assembly, metric) additionally record, per
species, the canonical ranks at which that species was the first in its clade
to reach the milestone (first_assembly_in_ranks / first_metric_in_ranks).

Writes taxon_milestone_summary.tsv (one row per taxon at any rank) and emits a
completion event so the rest of the pipeline can react.

Taxonomy source: in production the lineage is attached to assembly rows
upstream; in dev/test this flow calls load_taxonomy.build_taxonomy on a local
NCBI taxdump (--taxdump_path).

Usage:
    SKIP_PREFECT=true python -m flows.lib.compute_taxon_milestones \\
        --work_dir tmp --taxdump_path test/taxonomy/ncbi
"""

import csv
import os

from flows.lib.conditional_import import emit_event, flow
from flows.lib.load_taxonomy import CANONICAL_RANKS, build_taxonomy
from flows.lib.shared_args import TAXDUMP_PATH, WORK_DIR
from flows.lib.shared_args import parse_args as _parse_args

CURRENT_TSV = "assembly_current.tsv"
HISTORICAL_TSV = "assembly_historical.tsv"
OUTPUT_TSV = "taxon_milestone_summary.tsv"

# EBP umbrella BioProject — affiliation is membership in this project.
EBP_BIOPROJECT = "PRJNA533106"

# Levels walked per row, species first then up the canonical lineage.
LEVELS = ["species"] + ["genus", "family", "order", "class", "phylum", "kingdom"]

# The four milestones. Each maps to its output column prefix; in_ranks is True
# for the any-submitter milestones that get a first_*_in_ranks column.
MILESTONES = [
    {"key": "assembly", "prefix": "first_assembly", "in_ranks": True},
    {"key": "ebp_assembly", "prefix": "first_ebp_assembly", "in_ranks": False},
    {"key": "metric", "prefix": "first_metric", "in_ranks": True},
    {"key": "ebp_metric", "prefix": "first_ebp_metric", "in_ranks": False},
]

OUTPUT_FIELDNAMES = [
    "taxid",
    "rank",
    "scientific_name",
    "first_assembly_date",
    "first_assembly_accession",
    "first_ebp_assembly_date",
    "first_ebp_assembly_accession",
    "first_metric_date",
    "first_metric_accession",
    "first_ebp_metric_date",
    "first_ebp_metric_accession",
    "first_assembly_in_ranks",
    "first_metric_in_ranks",
    "latest_assembly_date",
    "latest_assembly_accession",
    "total_assemblies",
    "current_assemblies",
]


def _accession(row: dict) -> str:
    """Return the assembly accession, normalised across phases."""
    return row.get("accession") or row.get("genbankAccession") or ""


def _release_date(row: dict) -> str:
    """Return the releaseDate, treating the literal 'None' as empty."""
    value = row.get("releaseDate") or ""
    return "" if value == "None" else value


def _version_status(row: dict) -> str:
    """Return versionStatus across camelCase / snake_case, default 'current'."""
    return row.get("versionStatus") or row.get("version_status") or "current"


def _is_affiliated(row: dict) -> bool:
    """Return True if the row is under the EBP umbrella BioProject."""
    projects = (row.get("bioProjectAccession") or "").split(",")
    return EBP_BIOPROJECT in (p.strip() for p in projects)


def _has_metric(row: dict) -> bool:
    """Return True if the row meets the EBP quality standard."""
    value = row.get("ebpStandardDate") or ""
    return value != "" and value != "None"


def milestone_predicate(row: dict, key: str) -> bool:
    """Evaluate a milestone predicate against a row."""
    if key == "assembly":
        return True
    if key == "ebp_assembly":
        return _is_affiliated(row)
    if key == "metric":
        return _has_metric(row)
    if key == "ebp_metric":
        return _has_metric(row) and _is_affiliated(row)
    raise ValueError(f"unknown milestone key: {key}")


def load_assemblies(current_tsv: str, historical_tsv: str) -> list[dict]:
    """Load and combine rows from the current and historical TSVs.

    Args:
        current_tsv: Path to assembly_current.tsv.
        historical_tsv: Path to assembly_historical.tsv.

    Returns:
        Combined list of row dicts.
    """
    rows = []
    for path, label in [(current_tsv, "current"), (historical_tsv, "historical")]:
        if not os.path.exists(path):
            print(f"  Warning: {label} TSV not found: {path}")
            continue
        count = 0
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                rows.append(row)
                count += 1
        print(f"  Loaded {count} {label} rows")
    return rows


def resolve_to_species(taxid: int, taxonomy: dict) -> int | None:
    """Resolve a taxid to its species ancestor.

    Returns the taxid unchanged if it is species-rank, otherwise walks up the
    parent chain to the first species ancestor.

    Args:
        taxid: The taxid to resolve.
        taxonomy: The taxonomy contract from build_taxonomy.

    Returns:
        The species taxid, or None if no species ancestor exists.
    """
    # A visited set guards against cycles in the parent chain (corruption or
    # merged/deleted-taxid artifacts), which would otherwise loop forever.
    visited = set()
    node = taxonomy.get(taxid)
    while node is not None and taxid not in visited:
        if node["rank"] == "species":
            return taxid
        visited.add(taxid)
        taxid = node.get("parent")
        node = taxonomy.get(taxid)
    return None


def compute_milestones(rows: list[dict], taxonomy: dict) -> tuple[dict, dict]:
    """Run the single chronological sweep over all assembly rows.

    Args:
        rows: Combined current + historical assembly rows.
        taxonomy: The taxonomy contract.

    Returns:
        A (taxa, species_extra) tuple:
          taxa: {taxid: {rank, scientific_name, firsts: {milestone_key:
                 (date, accession)}}} for every taxon touched, at any rank.
          species_extra: {species_taxid: {in_ranks: {milestone_key: [rank,...]},
                 total_assemblies: int, current_assemblies: int,
                 latest: {date, accession} of the current/latest version}}.
    """
    # Attach the resolved species taxid + lineage to each row; drop unresolvable.
    enriched = []
    skipped = 0
    empty_date = 0
    for row in rows:
        raw_taxid = row.get("taxId") or row.get("taxid") or ""
        try:
            taxid = int(raw_taxid)
        except (ValueError, TypeError):
            skipped += 1
            continue
        species_taxid = resolve_to_species(taxid, taxonomy)
        if species_taxid is None:
            print(f"  Skipping taxId {raw_taxid} ({_accession(row)}): no species ancestor")
            skipped += 1
            continue
        node = taxonomy[species_taxid]
        enriched.append(
            {
                "row": row,
                "species_taxid": species_taxid,
                "lineage": node["lineage"],
                "date": _release_date(row),
                "accession": _accession(row),
            }
        )

    # Per-species counts (include empty-date rows in totals) and the latest
    # (current) assembly per species.
    species_extra: dict[int, dict] = {}
    for item in enriched:
        sp = item["species_taxid"]
        extra = species_extra.setdefault(
            sp,
            {
                "in_ranks": {m["key"]: [] for m in MILESTONES if m["in_ranks"]},
                "total_assemblies": 0,
                "current_assemblies": 0,
                "latest": None,  # (date, accession) of the current/latest version
            },
        )
        extra["total_assemblies"] += 1
        is_current = _version_status(item["row"]) != "superseded"
        if is_current:
            extra["current_assemblies"] += 1
        # Latest = newest-dated current version; prefer current rows over
        # superseded, then break ties by (date, accession). An empty date sorts
        # first, so a dated current version always wins over an undated one.
        candidate = (is_current, item["date"], item["accession"])
        if extra["latest"] is None or candidate > extra["latest"]["key"]:
            extra["latest"] = {
                "key": candidate,
                "date": item["date"],
                "accession": item["accession"],
            }

    # Global chronological sort. Empty-date rows are excluded from the sweep
    # (they cannot be a "first") but were already counted above.
    dated = [item for item in enriched if item["date"]]
    empty_date = len(enriched) - len(dated)
    dated.sort(key=lambda item: (item["date"], item["accession"]))

    taxa: dict[int, dict] = {}
    seen = {m["key"]: set() for m in MILESTONES}

    def ensure_taxon(taxid: int):
        if taxid not in taxa:
            node = taxonomy.get(taxid, {})
            taxa[taxid] = {
                "rank": node.get("rank", ""),
                "scientific_name": node.get("scientific_name", ""),
                "firsts": {},
            }

    for item in dated:
        sp = item["species_taxid"]
        lineage = item["lineage"]
        for milestone in MILESTONES:
            key = milestone["key"]
            if not milestone_predicate(item["row"], key):
                continue
            for level in LEVELS:
                taxid = sp if level == "species" else lineage.get(level)
                if not taxid or taxid in seen[key]:
                    continue
                seen[key].add(taxid)
                ensure_taxon(taxid)
                taxa[taxid]["firsts"][key] = (item["date"], item["accession"])
                if milestone["in_ranks"] and level != "species":
                    species_extra[sp]["in_ranks"][key].append(level)

    # Every resolved species gets an output row even if all its rows had empty
    # releaseDate (no milestone date, but its total/current counts still apply).
    for sp in species_extra:
        ensure_taxon(sp)

    print(f"  Resolved {len(enriched)} rows ({skipped} skipped); {empty_date} with empty releaseDate excluded from dates")
    print(f"  Touched {len(taxa)} taxa across all ranks")
    return taxa, species_extra


def build_output_rows(taxa: dict, species_extra: dict) -> list[dict]:
    """Assemble the output rows from the sweep results.

    Args:
        taxa: Per-taxon firsts from compute_milestones.
        species_extra: Per-species in-ranks and counts.

    Returns:
        List of output-row dicts keyed by OUTPUT_FIELDNAMES.
    """
    out = []
    for taxid, info in taxa.items():
        is_species = info["rank"] == "species"
        row = {name: "" for name in OUTPUT_FIELDNAMES}
        row["taxid"] = taxid
        row["rank"] = info["rank"]
        row["scientific_name"] = info["scientific_name"]

        for milestone in MILESTONES:
            first = info["firsts"].get(milestone["key"])
            date, accession = first if first else ("", "")
            row[f"{milestone['prefix']}_date"] = date
            row[f"{milestone['prefix']}_accession"] = accession

        if is_species:
            extra = species_extra.get(taxid, {})
            in_ranks = extra.get("in_ranks", {})
            row["first_assembly_in_ranks"] = ",".join(in_ranks.get("assembly", []))
            row["first_metric_in_ranks"] = ",".join(in_ranks.get("metric", []))
            latest = extra.get("latest")
            row["latest_assembly_date"] = latest["date"] if latest else ""
            row["latest_assembly_accession"] = latest["accession"] if latest else ""
            row["total_assemblies"] = extra.get("total_assemblies", 0)
            row["current_assemblies"] = extra.get("current_assemblies", 0)

        out.append(row)

    # Stable, deterministic ordering: rank then taxid.
    out.sort(key=lambda r: (str(r["rank"]), int(r["taxid"])))
    return out


@flow(log_prints=True)
def compute_taxon_milestones(work_dir: str = ".", taxdump_path: str | None = None) -> None:
    """Compute per-taxon assembly milestones and write the summary TSV.

    Args:
        work_dir: Directory containing assembly_current.tsv and
            assembly_historical.tsv; output is written there too.
        taxdump_path: Dev/test NCBI taxdump directory. In production the
            lineage arrives via the upstream contract and this is omitted.
    """
    current_tsv = os.path.join(work_dir, CURRENT_TSV)
    historical_tsv = os.path.join(work_dir, HISTORICAL_TSV)
    output_tsv = os.path.join(work_dir, OUTPUT_TSV)

    separator = "=" * 80
    print(f"\n{separator}")
    print("TAXON MILESTONE COMPUTATION")
    print(f"{separator}\n")

    print("[1/4] Loading taxonomy...")
    if not taxdump_path:
        # Production path would attach lineage upstream; without it there is
        # nothing to resolve against, so fail loudly rather than silently.
        raise SystemExit(
            "No taxonomy source: pass --taxdump_path for the dev/test path, "
            "or supply upstream lineage in production."
        )
    taxonomy = build_taxonomy(taxdump_path)

    print("\n[2/4] Loading assemblies...")
    rows = load_assemblies(current_tsv, historical_tsv)

    if not rows:
        print("  No assembly data found — nothing to compute.")
        emit_event(
            event="compute.taxon_milestones.completed",
            resource={
                "prefect.resource.id": f"compute.taxon_milestones.{work_dir}",
                "prefect.resource.type": "taxon.milestones",
            },
            payload={"taxa": 0, "status": "no_op"},
        )
        return

    print("\n[3/4] Computing milestones (single chronological sweep)...")
    taxa, species_extra = compute_milestones(rows, taxonomy)
    output_rows = build_output_rows(taxa, species_extra)

    print("\n[4/4] Writing output...")
    with open(output_tsv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES, delimiter="\t")
        writer.writeheader()
        writer.writerows(output_rows)

    species_count = sum(1 for r in output_rows if r["rank"] == "species")
    higher_count = len(output_rows) - species_count

    print(f"\n{separator}")
    print("TAXON MILESTONE COMPUTATION COMPLETE")
    print(f"{separator}")
    print(f"  Species rows:     {species_count}")
    print(f"  Higher-rank rows: {higher_count}")
    print(f"  Output:           {output_tsv}")
    print(f"{separator}\n")

    emit_event(
        event="compute.taxon_milestones.completed",
        resource={
            "prefect.resource.id": f"compute.taxon_milestones.{work_dir}",
            "prefect.resource.type": "taxon.milestones",
        },
        payload={
            "taxa": len(output_rows),
            "species": species_count,
            "higher_rank": higher_count,
            "status": "success",
        },
    )


if __name__ == "__main__":
    args = _parse_args(
        [WORK_DIR, TAXDUMP_PATH],
        description="Compute per-taxon assembly milestones from version-tracked TSVs",
    )
    compute_taxon_milestones(work_dir=args.work_dir, taxdump_path=args.taxdump_path)
