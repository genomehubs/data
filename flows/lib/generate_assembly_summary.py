"""Combine current and historical assembly TSVs into a per-base-accession summary.

Reads assembly_current.tsv and assembly_historical.tsv from the working
directory, computes version history for each base accession (first version
date, current version date, EBP metric tracking, version gaps), and writes
assembly_version_summary.tsv.  Emits a completion event so downstream
Phase 3 taxon milestone flows can be triggered.

Run after parse_assembly_versions completes.

Usage:
    python -m flows.lib.generate_assembly_summary --work_dir tmp
"""

import csv
import os
from collections import defaultdict

from flows.lib.assembly_versions_utils import parse_accession
from flows.lib.conditional_import import emit_event, flow
from flows.lib.shared_args import WORK_DIR
from flows.lib.shared_args import parse_args as _parse_args

CURRENT_TSV = "assembly_current.tsv"
HISTORICAL_TSV = "assembly_historical.tsv"
OUTPUT_TSV = "assembly_version_summary.tsv"

# EBP metric columns to check (either naming convention may appear)
EBP_METRIC_COLUMNS = {"ebpStandardDate", "ebp_standard_date", "ebpMetricDate", "ebp_metric_date"}


def _has_ebp_metric(row: dict) -> bool:
    """Return True if any EBP metric date column is populated."""
    return any(row.get(col) for col in EBP_METRIC_COLUMNS)


def load_assemblies(current_tsv: str, historical_tsv: str) -> list[dict]:
    """Load and combine rows from current and historical TSVs.

    Normalises the accession column so downstream code can always use
    'accession', regardless of which phase wrote the row.

    Args:
        current_tsv: Path to assembly_current.tsv.
        historical_tsv: Path to assembly_historical.tsv.

    Returns:
        List of row dicts with a guaranteed 'accession' key.
    """
    rows = []

    for path, label in [(current_tsv, "current"), (historical_tsv, "historical")]:
        if not os.path.exists(path):
            print(f"  Warning: {label} TSV not found: {path}")
            continue
        count = 0
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                # Normalise to 'accession' so the rest of the code is uniform
                if "accession" not in row or not row["accession"]:
                    row["accession"] = row.get("genbankAccession", "")
                rows.append(row)
                count += 1
        print(f"  Loaded {count} {label} rows")

    return rows


def find_version_gaps(versions: list[int]) -> str:
    """Return a comma-separated string of missing version numbers, or ''."""
    if not versions:
        return ""
    expected = set(range(1, max(versions) + 1))
    missing = sorted(expected - set(versions))
    return ",".join(str(v) for v in missing)


def generate_summary_for_base(base_accession: str, rows: list[dict]) -> dict:
    """Compute version summary for a single base accession.

    Args:
        base_accession: Base accession without version suffix (e.g. GCA_000002035).
        rows: All rows (current + historical) for this base accession,
            sorted by version number ascending.

    Returns:
        Summary dict with one entry per output column.
    """
    # Sort by version number
    rows = sorted(rows, key=lambda r: parse_accession(r["accession"])[1])

    versions = [parse_accession(r["accession"])[1] for r in rows]
    version_gaps = find_version_gaps(versions)

    first_row = rows[0]
    current_row = rows[-1]

    # versionStatus column may be camelCase (Phase 0 YAML) or snake_case (Phase 1 copy)
    def version_status(row):
        return row.get("versionStatus") or row.get("version_status") or "current"

    superseded_count = sum(1 for r in rows if version_status(r) == "superseded")

    # First version that met EBP metric criteria
    first_ebp_row = next((r for r in rows if _has_ebp_metric(r)), None)

    return {
        "base_accession": base_accession,
        "taxId": first_row.get("taxId", ""),
        "first_version_accession": first_row["accession"],
        "first_version_number": versions[0],
        "first_version_date": first_row.get("releaseDate", ""),
        "current_version_accession": current_row["accession"],
        "current_version_number": versions[-1],
        "current_version_date": current_row.get("releaseDate", ""),
        "total_versions": len(rows),
        "superseded_versions": superseded_count,
        "first_ebp_metric_accession": first_ebp_row["accession"] if first_ebp_row else "",
        "first_ebp_metric_version": parse_accession(first_ebp_row["accession"])[1] if first_ebp_row else "",
        "first_ebp_metric_date": first_ebp_row.get("releaseDate", "") if first_ebp_row else "",
        "version_gaps": version_gaps,
    }


SUMMARY_FIELDNAMES = [
    "base_accession",
    "taxId",
    "first_version_accession",
    "first_version_number",
    "first_version_date",
    "current_version_accession",
    "current_version_number",
    "current_version_date",
    "total_versions",
    "superseded_versions",
    "first_ebp_metric_accession",
    "first_ebp_metric_version",
    "first_ebp_metric_date",
    "version_gaps",
]


@flow(log_prints=True)
def generate_assembly_summary(work_dir: str = ".") -> None:
    """Combine current and historical TSVs into a per-base-accession summary.

    Args:
        work_dir: Directory containing assembly_current.tsv and
            assembly_historical.tsv; output is written there too.
    """
    current_tsv = os.path.join(work_dir, CURRENT_TSV)
    historical_tsv = os.path.join(work_dir, HISTORICAL_TSV)
    output_tsv = os.path.join(work_dir, OUTPUT_TSV)

    separator = "=" * 80
    print(f"\n{separator}")
    print("ASSEMBLY VERSION SUMMARY")
    print(f"{separator}\n")

    print("[1/3] Loading assemblies...")
    rows = load_assemblies(current_tsv, historical_tsv)

    if not rows:
        print("  No assembly data found — nothing to summarise.")
        emit_event(
            event="generate.assembly_summary.completed",
            resource={
                "prefect.resource.id": f"generate.assembly_summary.{work_dir}",
                "prefect.resource.type": "assembly.summary",
            },
            payload={"base_accessions": 0, "status": "no_op"},
        )
        return

    print(f"\n[2/3] Generating summaries...")
    by_base: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        acc = row.get("accession", "")
        if not acc:
            continue
        base_acc, _ = parse_accession(acc)
        by_base[base_acc].append(row)

    summaries = []
    for base_acc in sorted(by_base):
        summaries.append(generate_summary_for_base(base_acc, by_base[base_acc]))

    print(f"  Processed {len(summaries)} unique base accessions")

    print(f"\n[3/3] Writing output...")
    with open(output_tsv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDNAMES, delimiter="\t")
        writer.writeheader()
        writer.writerows(summaries)

    multi_version = sum(1 for s in summaries if s["total_versions"] > 1)
    with_gaps = sum(1 for s in summaries if s["version_gaps"])
    with_ebp = sum(1 for s in summaries if s["first_ebp_metric_accession"])

    print(f"\n{separator}")
    print("ASSEMBLY VERSION SUMMARY COMPLETE")
    print(f"{separator}")
    print(f"  Base accessions:          {len(summaries)}")
    print(f"  Multi-version assemblies: {multi_version}")
    print(f"  Assemblies with gaps:     {with_gaps}")
    print(f"  Assemblies with EBP:      {with_ebp}")
    print(f"  Output:                   {output_tsv}")
    print(f"{separator}\n")

    emit_event(
        event="generate.assembly_summary.completed",
        resource={
            "prefect.resource.id": f"generate.assembly_summary.{work_dir}",
            "prefect.resource.type": "assembly.summary",
        },
        payload={
            "base_accessions": len(summaries),
            "multi_version": multi_version,
            "with_gaps": with_gaps,
            "with_ebp": with_ebp,
            "status": "success",
        },
    )


if __name__ == "__main__":
    args = _parse_args(
        [WORK_DIR],
        description="Aggregate current and historical assembly TSVs into version summary",
    )
    generate_assembly_summary(work_dir=args.work_dir)
