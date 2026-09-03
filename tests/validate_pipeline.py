"""Validate the assembly-version pipeline outputs in a working directory.

Phase 4 step 1. Run after a full chain (Phase 0 backfill, Phase 1 daily diff,
Phase 2 summary, Phase 3 milestones) to check the four TSVs against each
other rather than against a mock:

    <current>.tsv                  written by parse_ncbi_assemblies
    assembly_historical.tsv        written by Phase 0 and appended by Phase 1
    assembly_version_summary.tsv   written by generate_assembly_summary
    taxon_milestone_summary.tsv    written by compute_taxon_milestones

Checks, one per success metric in the Phase 4 plan:

    assembly-id-uniqueness   assemblyID unique across current + historical
    version-gaps             summary version_gaps matches the versions present
    referential-integrity    every superseded_by resolves to a known accession
    summary-completeness     every base accession is summarised, and no more
    milestone-date-ordering  the nested milestone dates never invert
    in-ranks-validity        every first_*_in_ranks value is a canonical rank
    lineage-columns          the upstream {rank}TaxId columns arrived populated
    lineage-coverage         how many current rows carry no lineage at all

lineage-columns is a warning by default: a dev run off a local taxdump has no
lineage columns and is still valid. Pass --strict to make warnings fail, which
is what a production run should use, since without those columns Phase 3
silently falls back to whatever taxonomy source it can find. lineage-coverage
is a note that never fails: some assemblies will always sit on a taxid the
upstream lookup does not cover, and that is a number to watch, not a defect.

Usage:
    python -m tests.validate_pipeline --work_dir tmp
    python -m tests.validate_pipeline --work_dir tmp --yaml_path <config> --strict
"""

import csv
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("SKIP_PREFECT", "true")

from flows.lib.assembly_lineage import (  # noqa: E402
    lineage_columns,
    row_lineage,
    rows_have_lineage_columns,
)
from flows.lib.assembly_versions_utils import (  # noqa: E402
    COL_SUPERSEDED_BY,
    get_accession,
    get_assembly_id,
    open_tsv,
    parse_accession,
    resolve_current_tsv_paths,
    resolve_historical_tsv_path,
)
from flows.lib.compute_taxon_milestones import OUTPUT_TSV as MILESTONE_TSV  # noqa: E402
from flows.lib.generate_assembly_summary import OUTPUT_TSV as SUMMARY_TSV  # noqa: E402
from flows.lib.load_taxonomy import CANONICAL_RANKS  # noqa: E402
from flows.lib.shared_args import WORK_DIR, YAML_PATH  # noqa: E402
from flows.lib.shared_args import parse_args as _parse_args  # noqa: E402
from flows.lib.utils import load_config  # noqa: E402

# Check severities: an error always fails the run, a warning fails it only
# under --strict, and a note never fails it.
ERROR = "error"
WARNING = "warning"
NOTE = "note"

STRICT = {
    "flags": ["--strict"],
    "keys": {
        "help": "Treat warnings as failures (use for production runs).",
        "action": "store_true",
    },
}

MAX_EXAMPLES = {
    "flags": ["--max_examples"],
    "keys": {
        "help": "Problems to print per failing check (default: 5).",
        "default": 5,
        "type": int,
    },
}

# Milestone date columns that must never invert, as adjacent pairs of the two
# nesting chains documented in README_phase_3_taxon_milestones.md.
MILESTONE_DATE_PAIRS = [
    ("first_assembly_date", "first_metric_date"),
    ("first_metric_date", "first_ebp_metric_date"),
    ("first_assembly_date", "first_ebp_assembly_date"),
    ("first_ebp_assembly_date", "first_ebp_metric_date"),
]

IN_RANKS_COLUMNS = ["first_assembly_in_ranks", "first_metric_in_ranks"]


def read_rows(path: str) -> list[dict]:
    """Read a TSV, gzipped or not, into a list of row dicts.

    Args:
        path (str): Path to the TSV.

    Returns:
        list: Row dicts, empty when the file holds only a header.
    """
    with open_tsv(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_pipeline_outputs(work_dir: str, yaml_path: str = None) -> dict:
    """Load the four pipeline TSVs from a working directory.

    The current-TSV filename comes from the config when one is given, and is
    otherwise discovered in work_dir, matching how the flows resolve it.

    Args:
        work_dir (str): Directory holding the pipeline outputs.
        yaml_path (str, optional): Config naming the current TSV.

    Returns:
        dict: Keyed by output name, each value a dict with ``path``,
            ``found`` and ``rows``.
    """
    config = load_config(config_file=yaml_path) if yaml_path else None
    current_tsv, _, _ = resolve_current_tsv_paths(work_dir, config=config)

    paths = {
        "current": current_tsv,
        "historical": resolve_historical_tsv_path(work_dir),
        "summary": os.path.join(work_dir, SUMMARY_TSV),
        "milestones": os.path.join(work_dir, MILESTONE_TSV),
    }

    outputs = {}
    for name, path in paths.items():
        found = os.path.exists(path)
        outputs[name] = {
            "path": path,
            "found": found,
            "rows": read_rows(path) if found else [],
        }
    return outputs


def versions_by_base(rows: list[dict]) -> dict[str, set[int]]:
    """Index the versions present in ``rows`` by base accession.

    Args:
        rows (list): Assembly rows from the current and historical TSVs.

    Returns:
        dict: base_accession -> set of version numbers.
    """
    index: dict[str, set[int]] = defaultdict(set)
    for row in rows:
        accession = get_accession(row)
        if not accession:
            continue
        base, version = parse_accession(accession)
        index[base].add(version)
    return dict(index)


def expected_gaps(versions: set[int]) -> set[int]:
    """Return the versions missing below the highest version present.

    Args:
        versions (set): Version numbers present for one base accession.

    Returns:
        set: Missing version numbers, empty when the run is complete.
    """
    if not versions:
        return set()
    return set(range(1, max(versions) + 1)) - versions


def parse_gap_field(value: str) -> set[int]:
    """Parse a summary version_gaps cell into a set of version numbers.

    Args:
        value (str): Comma-separated version numbers, possibly empty.

    Returns:
        set: Version numbers, ignoring anything unparseable (which the
            reconciliation check then reports as a mismatch).
    """
    gaps = set()
    for part in (value or "").split(","):
        part = part.strip()
        if part.isdigit():
            gaps.add(int(part))
    return gaps


# ---------------------------------------------------------------------------
# Checks. Each returns a list of human-readable problems, empty when it passes.
# ---------------------------------------------------------------------------

def check_assembly_ids_unique(
    current_rows: list[dict], historical_rows: list[dict]
) -> list[str]:
    """Check that no assembly ID appears twice across current and historical.

    A duplicate means the same assembly is tracked as both current and
    superseded, or that two phases minted the same ID for different versions.

    Args:
        current_rows (list): Rows from the current TSV.
        historical_rows (list): Rows from the historical TSV.

    Returns:
        list: One problem per duplicated ID, plus a count of ID-less rows.
    """
    problems = []
    seen: dict[str, str] = {}
    duplicates: dict[str, list[str]] = defaultdict(list)
    missing = 0

    for label, rows in [("current", current_rows), ("historical", historical_rows)]:
        for row in rows:
            assembly_id = get_assembly_id(row)
            if not assembly_id:
                missing += 1
                continue
            where = f"{label}:{get_accession(row) or 'unknown accession'}"
            if assembly_id in seen:
                duplicates[assembly_id].append(where)
            else:
                seen[assembly_id] = where

    for assembly_id, places in sorted(duplicates.items()):
        first = seen[assembly_id]
        problems.append(
            f"assembly ID {assembly_id} appears more than once: "
            f"{first}, {', '.join(places)}"
        )

    if missing:
        problems.append(f"{missing} rows carry no assembly ID")

    return problems


def check_version_gaps(
    assembly_rows: list[dict], summary_rows: list[dict]
) -> list[str]:
    """Check that summary version_gaps matches the versions actually present.

    Args:
        assembly_rows (list): Current + historical rows.
        summary_rows (list): Rows from assembly_version_summary.tsv.

    Returns:
        list: One problem per base accession whose gaps do not reconcile.
    """
    problems = []
    present = versions_by_base(assembly_rows)

    for row in summary_rows:
        base = row.get("base_accession", "")
        if not base:
            problems.append("summary row with no base_accession")
            continue
        reported = parse_gap_field(row.get("version_gaps", ""))
        actual = expected_gaps(present.get(base, set()))
        if reported != actual:
            problems.append(
                f"{base}: summary reports gaps {sorted(reported) or 'none'}, "
                f"versions present imply {sorted(actual) or 'none'}"
            )

    return problems


def check_superseded_by_resolves(assembly_rows: list[dict]) -> list[str]:
    """Check that every superseded_by points at a real accession of that base.

    A superseded assembly may be superseded by a version that has itself since
    been superseded, so the referent is looked up across current and
    historical rows together rather than in the current TSV alone.

    Args:
        assembly_rows (list): Current + historical rows.

    Returns:
        list: One problem per unresolvable or cross-base reference.
    """
    problems = []
    known = {get_accession(row) for row in assembly_rows if get_accession(row)}

    for row in assembly_rows:
        referent = (row.get(COL_SUPERSEDED_BY) or "").strip()
        if not referent:
            continue
        accession = get_accession(row) or "unknown accession"
        if referent not in known:
            problems.append(
                f"{accession}: superseded_by {referent} is not a known accession"
            )
            continue
        if parse_accession(referent)[0] != parse_accession(accession)[0]:
            problems.append(
                f"{accession}: superseded_by {referent} belongs to another "
                "base accession"
            )

    return problems


def check_summary_completeness(
    assembly_rows: list[dict], summary_rows: list[dict]
) -> list[str]:
    """Check that the summary covers every base accession, and only those.

    Args:
        assembly_rows (list): Current + historical rows.
        summary_rows (list): Rows from assembly_version_summary.tsv.

    Returns:
        list: One problem per unsummarised base, plus one per stale summary
            row with no assembly rows behind it.
    """
    problems = []
    present = set(versions_by_base(assembly_rows))
    summarised = {
        row.get("base_accession", "") for row in summary_rows
    } - {""}

    for base in sorted(present - summarised):
        problems.append(f"{base}: present in the TSVs but missing from the summary")
    for base in sorted(summarised - present):
        problems.append(f"{base}: summarised but absent from the TSVs")

    return problems


def check_milestone_date_ordering(milestone_rows: list[dict]) -> list[str]:
    """Check the nested milestone dates never invert.

    The four milestone predicates nest, so a taxon cannot reach a narrower
    milestone before a broader one.

    Args:
        milestone_rows (list): Rows from taxon_milestone_summary.tsv.

    Returns:
        list: One problem per inverted pair.
    """
    problems = []

    for row in milestone_rows:
        for earlier, later in MILESTONE_DATE_PAIRS:
            first = (row.get(earlier) or "").strip()
            second = (row.get(later) or "").strip()
            if first and second and first > second:
                problems.append(
                    f"taxid {row.get('taxid', '?')}: {earlier} {first} is after "
                    f"{later} {second}"
                )

    return problems


def check_in_ranks_valid(milestone_rows: list[dict]) -> list[str]:
    """Check every first_*_in_ranks value is a canonical rank name.

    Args:
        milestone_rows (list): Rows from taxon_milestone_summary.tsv.

    Returns:
        list: One problem per row holding a non-canonical rank.
    """
    problems = []

    for row in milestone_rows:
        for column in IN_RANKS_COLUMNS:
            value = (row.get(column) or "").strip()
            if not value:
                continue
            ranks = {part.strip() for part in value.split(",") if part.strip()}
            unexpected = ranks - CANONICAL_RANKS
            if unexpected:
                problems.append(
                    f"taxid {row.get('taxid', '?')}: {column} holds "
                    f"{sorted(unexpected)}, which are not canonical ranks"
                )

    return problems


def check_lineage_columns(current_rows: list[dict]) -> list[str]:
    """Check the upstream lineage columns arrived, and arrived populated.

    The write path (parse_ncbi_assemblies.write_to_tsv -> gh_utils.write_tsv ->
    print_to_tsv) emits only the columns declared in the types YAML, so
    enrichment can be a silent no-op upstream. Without these columns Phase 3
    has no production taxonomy source and would quietly fall back.

    Args:
        current_rows (list): Rows from the current TSV.

    Returns:
        list: Problems describing missing or wholly empty lineage columns.
    """
    if not current_rows:
        return []

    if not rows_have_lineage_columns(current_rows):
        return [
            "the current TSV carries none of the upstream lineage columns "
            f"({', '.join(lineage_columns())})"
        ]

    if not any(row_lineage(row) for row in current_rows):
        return ["every lineage column is empty on every row of the current TSV"]

    return []


def check_lineage_coverage(current_rows: list[dict]) -> list[str]:
    """Report how many current rows carry no lineage at all.

    Informational rather than a defect: the upstream lookup will not cover
    every taxid, and those rows still compute milestones off the taxdump when
    one is supplied. Worth watching as a number, since a jump means the
    lookup went stale.

    Args:
        current_rows (list): Rows from the current TSV.

    Returns:
        list: A single line when some rows have no lineage.
    """
    if not current_rows or not rows_have_lineage_columns(current_rows):
        return []

    populated = sum(1 for row in current_rows if row_lineage(row))
    if populated == len(current_rows):
        return []

    return [
        f"{len(current_rows) - populated} of {len(current_rows)} current rows "
        "have an empty lineage"
    ]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_checks(outputs: dict) -> list[dict]:
    """Run every check against the loaded pipeline outputs.

    Args:
        outputs (dict): The result of load_pipeline_outputs.

    Returns:
        list: One result dict per check, with ``name``, ``severity`` and
            ``problems`` keys.
    """
    missing = [
        f"{name} ({output['path']})"
        for name, output in outputs.items()
        if not output["found"]
    ]
    results = [
        {
            "name": "outputs-present",
            "severity": ERROR,
            "problems": [f"not found: {item}" for item in missing],
        }
    ]

    current_rows = outputs["current"]["rows"]
    historical_rows = outputs["historical"]["rows"]
    summary_rows = outputs["summary"]["rows"]
    milestone_rows = outputs["milestones"]["rows"]
    assembly_rows = current_rows + historical_rows

    results.extend(
        [
            {
                "name": "assembly-id-uniqueness",
                "severity": ERROR,
                "problems": check_assembly_ids_unique(current_rows, historical_rows),
            },
            {
                "name": "version-gaps",
                "severity": ERROR,
                "problems": check_version_gaps(assembly_rows, summary_rows),
            },
            {
                "name": "referential-integrity",
                "severity": ERROR,
                "problems": check_superseded_by_resolves(assembly_rows),
            },
            {
                "name": "summary-completeness",
                "severity": ERROR,
                "problems": check_summary_completeness(assembly_rows, summary_rows),
            },
            {
                "name": "milestone-date-ordering",
                "severity": ERROR,
                "problems": check_milestone_date_ordering(milestone_rows),
            },
            {
                "name": "in-ranks-validity",
                "severity": ERROR,
                "problems": check_in_ranks_valid(milestone_rows),
            },
            {
                "name": "lineage-columns",
                "severity": WARNING,
                "problems": check_lineage_columns(current_rows),
            },
            {
                "name": "lineage-coverage",
                "severity": NOTE,
                "problems": check_lineage_coverage(current_rows),
            },
        ]
    )
    return results


def report(results: list[dict], strict: bool = False, max_examples: int = 5) -> int:
    """Print the check results and return the number of failures.

    Args:
        results (list): Check results from run_checks.
        strict (bool): Count warnings as failures.
        max_examples (int): Problems to print per failing check.

    Returns:
        int: Number of failing checks.
    """
    failures = 0

    for result in results:
        problems = result["problems"]
        if not problems:
            print(f"  PASS  {result['name']}")
            continue

        severity = result["severity"]
        fails = severity == ERROR or (severity == WARNING and strict)
        failures += 1 if fails else 0
        label = "FAIL" if fails else ("WARN" if severity == WARNING else "NOTE")
        plural = "" if len(problems) == 1 else "s"
        print(f"  {label}  {result['name']} ({len(problems)} problem{plural})")
        for problem in problems[:max_examples]:
            print(f"          {problem}")
        if len(problems) > max_examples:
            print(f"          ... and {len(problems) - max_examples} more")

    return failures


def validate_pipeline(
    work_dir: str = ".",
    yaml_path: str = None,
    strict: bool = False,
    max_examples: int = 5,
) -> int:
    """Validate the pipeline outputs in ``work_dir``.

    Args:
        work_dir (str): Directory holding the pipeline outputs.
        yaml_path (str, optional): Config naming the current TSV.
        strict (bool): Treat warnings as failures.
        max_examples (int): Problems to print per failing check.

    Returns:
        int: Number of failing checks; 0 when the outputs validate.
    """
    separator = "=" * 80
    print(f"\n{separator}")
    print("PIPELINE VALIDATION")
    print(f"{separator}\n")

    outputs = load_pipeline_outputs(work_dir, yaml_path=yaml_path)
    for name, output in outputs.items():
        state = f"{len(output['rows'])} rows" if output["found"] else "NOT FOUND"
        print(f"  {name:12} {output['path']} ({state})")
    print()

    failures = report(run_checks(outputs), strict=strict, max_examples=max_examples)

    print(f"\n{separator}")
    if failures:
        print(f"PIPELINE VALIDATION FAILED: {failures} checks")
    else:
        print("PIPELINE VALIDATION PASSED")
    print(f"{separator}\n")

    return failures


if __name__ == "__main__":
    args = _parse_args(
        [WORK_DIR, YAML_PATH, STRICT, MAX_EXAMPLES],
        description="Validate the assembly-version pipeline outputs",
    )
    raise SystemExit(
        1
        if validate_pipeline(
            work_dir=args.work_dir,
            yaml_path=args.yaml_path,
            strict=args.strict,
            max_examples=args.max_examples,
        )
        else 0
    )
