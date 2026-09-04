"""Drive the staged run of the assembly-version pipeline against real data.

Phase 4 step 3, stages 1 and 2. Everything happens inside --work_dir; nothing
is written anywhere else, and no upstream or S3 path is touched.

    stage 1 (slice)    fetch a clade from NCBI, parse it, backfill its
                       historical versions, then aggregate and validate
    stage 2 (two-day)  run the daily path twice over that clade so the diff,
                       gap and supersession branches all fire

Stage 2 needs a "yesterday" that differs from today, and NCBI will not oblige
within the hour. Rather than fabricate tomorrow -- which would leave the
gap-fill fetching accessions that do not exist -- it fabricates *yesterday*:
the multi-version assemblies in the slice are rewound to an earlier version to
make day 1, and the real summary becomes day 2. Every version the pipeline
then goes looking for is a version NCBI actually has.

A slice wants multi-version assemblies or the backfill has nothing to do.
Malacostraca (6681) is a good default: 207 assemblies, 18 of them with a
version above 1, one of them at v4.

The taxonomy for Phase 3 comes from whichever source is available, in the
order compute_taxon_milestones itself prefers: the {rank}TaxId columns when
the upstream enrichment reached the TSV, --taxdump_path otherwise. Without
either, this script stops before Phase 3 rather than writing empty milestones.

Usage:
    python -m tests.staged_run --work_dir tmp/staged --root_taxid 6681
    python -m tests.staged_run --work_dir tmp/staged --stage 2 \\
        --taxdump_path <taxdump>
"""

import json
import os
import shutil
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("SKIP_PREFECT", "true")

from flows.lib.assembly_lineage import rows_have_lineage_columns  # noqa: E402
from flows.lib.assembly_versions_utils import (  # noqa: E402
    parse_accession,
    resolve_current_tsv_paths,
)
from flows.lib.compute_taxon_milestones import compute_taxon_milestones  # noqa: E402
from flows.lib.generate_assembly_summary import (  # noqa: E402
    generate_assembly_summary,
)
from flows.lib.shared_args import (  # noqa: E402
    ROOT_TAXID,
    TAXDUMP_PATH,
    WORK_DIR,
    YAML_PATH,
)
from flows.lib.shared_args import default  # noqa: E402
from flows.lib.shared_args import parse_args as _parse_args  # noqa: E402
from flows.parsers.parse_assembly_versions import (  # noqa: E402
    parse_assembly_versions,
)
from flows.parsers.parse_backfill_historical_versions import (  # noqa: E402
    backfill_historical_versions,
)
from flows.lib.utils import load_config  # noqa: E402
from flows.parsers.parse_ncbi_assemblies import parse_ncbi_assemblies  # noqa: E402
from flows.updaters.update_ncbi_datasets import update_ncbi_datasets  # noqa: E402
from tests import validate_no_ncbi_fetches as no_fetches  # noqa: E402
from tests import validate_pipeline as validator  # noqa: E402

JSONL_NAME = "assembly_data_report.jsonl"
HISTORICAL_YAML = os.path.join("configs", "assembly_historical.types.yaml")
DEFAULT_CURRENT_YAML = os.path.join("test", "ncbi_datasets.types.yaml")
DEFAULT_ROOT_TAXID = "6681"  # Malacostraca: 207 assemblies, 18 multi-version

STAGE = {
    "flags": ["--stage"],
    "keys": {
        "help": "Which stage to run: 1 (slice), 2 (two-day) or all.",
        "default": "all",
        "choices": ["1", "2", "all"],
        "type": str,
    },
}

REUSE_JSONL = {
    "flags": ["--reuse_jsonl"],
    "keys": {
        "help": "Skip the NCBI fetch and use the JSONL already in work_dir.",
        "action": "store_true",
    },
}


def banner(text: str) -> None:
    """Print a step heading."""
    print(f"\n{'=' * 80}\n{text}\n{'=' * 80}")


def stage_dir(work_dir: str, name: str) -> str:
    """Create and return a per-stage directory under work_dir.

    Each stage gets its own directory so stage 2 rewriting the current TSV
    cannot disturb what stage 1 produced.

    Args:
        work_dir (str): The run directory given on the command line.
        name (str): Stage directory name.

    Returns:
        str: Path to the stage directory.
    """
    path = os.path.join(work_dir, name)
    os.makedirs(path, exist_ok=True)
    return path


def stage_current_yaml(yaml_path: str, target_dir: str) -> str:
    """Copy the current-assembly config into the stage directory.

    ``config.meta["file_name"]`` resolves against the directory holding the
    YAML, so a config read from ``test/`` writes its TSV into ``test/`` and
    would overwrite the fixture there. Copying it first keeps every output
    inside the stage directory.

    Args:
        yaml_path (str): Path to the current-assembly types YAML.
        target_dir (str): Stage directory to copy it into.

    Returns:
        str: Path to the copied config.
    """
    return shutil.copy(yaml_path, target_dir)


def fetch_slice(root_taxid: str, work_dir: str) -> str:
    """Fetch one clade from NCBI datasets into work_dir.

    Args:
        root_taxid (str): Root taxid of the slice.
        work_dir (str): Stage directory.

    Returns:
        str: Path to the written JSONL.
    """
    jsonl = os.path.join(work_dir, JSONL_NAME)
    update_ncbi_datasets(root_taxid=root_taxid, output_path=jsonl, s3_path="")
    with open(jsonl, encoding="utf-8") as f:
        count = sum(1 for _ in f)
    print(f"  Fetched {count} assemblies for taxon {root_taxid}")
    return jsonl


def read_jsonl(path: str) -> list[dict]:
    """Read a JSONL file into a list of records."""
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(path: str, records: list[dict]) -> None:
    """Write records to a JSONL file."""
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def rewind_multi_version_assemblies(records: list[dict]) -> tuple[list[dict], dict]:
    """Build a plausible "yesterday" from today's summary.

    Every base accession whose current version is above 1 is rewound to its
    predecessor, so the day-2 diff sees a real supersession and any gap it
    reports is a version NCBI can actually be asked for.

    Args:
        records (list): Today's JSONL records.

    Returns:
        tuple: (rewound records, {base_accession: (from, to)}) describing what
            was rewound.
    """
    by_base = defaultdict(list)
    for record in records:
        base, version = parse_accession(record["accession"])
        by_base[base].append((version, record))

    rewound, changes = [], {}
    for base, versions in by_base.items():
        current_version, record = max(versions, key=lambda item: item[0])
        if current_version > 1:
            older = dict(record)
            older["accession"] = f"{base}.{current_version - 1}"
            rewound.append(older)
            changes[base] = (current_version - 1, current_version)
        else:
            rewound.append(record)

    return rewound, changes


def parse_current(jsonl: str, yaml_path: str) -> None:
    """Run the current-assembly parser, as the daily pipeline does.

    Also takes the ``.previous`` snapshot of yesterday's output, which is what
    the daily version parse diffs against.

    Args:
        jsonl (str): Path to the JSONL to parse.
        yaml_path (str): Path to the staged current-assembly config.
    """
    parse_ncbi_assemblies(input_path=jsonl, yaml_path=yaml_path, append=False)


def run_backfill(jsonl: str, work_dir: str, repo_root: str) -> None:
    """Run the Phase 0 historical backfill over the slice.

    Args:
        jsonl (str): Path to the JSONL naming the assemblies to backfill.
        work_dir (str): Stage directory; the output lands here.
        repo_root (str): Repository root, for the historical config.
    """
    backfill_historical_versions(
        input_path=jsonl,
        yaml_path=os.path.join(repo_root, HISTORICAL_YAML),
        work_dir=work_dir,
    )


def aggregate_and_validate(
    work_dir: str, yaml_path: str, taxdump_path: str = None
) -> int:
    """Run Phase 2, Phase 3 and both validators over a stage directory.

    Args:
        work_dir (str): Stage directory holding the parsed TSVs.
        yaml_path (str): Path to the staged current-assembly config.
        taxdump_path (str, optional): Dev/test taxdump for Phase 3.

    Returns:
        int: Number of validator failures.
    """
    banner("PHASE 2 — assembly version summary")
    generate_assembly_summary(work_dir=work_dir, yaml_path=yaml_path)

    banner("PHASE 3 — taxon milestones")
    outputs = validator.load_pipeline_outputs(work_dir, yaml_path=yaml_path)
    has_columns = rows_have_lineage_columns(outputs["current"]["rows"])
    if not has_columns and not taxdump_path:
        print("  No taxonomy source: the current TSV carries no lineage")
        print("  columns and no --taxdump_path was given.")
        print("  Stopping before Phase 3 rather than writing empty milestones.")
        print("  Stage the blobtk nodes.jsonl, or pass --taxdump_path.")
        return 1
    print(f"  Lineage columns present: {has_columns}")
    compute_taxon_milestones(
        work_dir=work_dir, taxdump_path=taxdump_path, yaml_path=yaml_path
    )

    banner("VALIDATE")
    failures = validator.validate_pipeline(work_dir=work_dir, yaml_path=yaml_path)
    failures += no_fetches.validate_no_ncbi_fetches(work_dir=work_dir)
    return failures


def run_stage_one(
    work_dir: str,
    root_taxid: str,
    yaml_path: str,
    repo_root: str,
    taxdump_path: str = None,
    reuse_jsonl: bool = False,
) -> int:
    """Fetch a slice, parse it, backfill it, then aggregate and validate.

    Args:
        work_dir (str): Run directory; the stage writes into work_dir/slice.
        root_taxid (str): Root taxid of the slice.
        yaml_path (str): Path to the current-assembly types YAML.
        repo_root (str): Repository root.
        taxdump_path (str, optional): Dev/test taxdump for Phase 3.
        reuse_jsonl (bool): Use the JSONL already in the stage directory.

    Returns:
        int: Number of validator failures.
    """
    slice_dir = stage_dir(work_dir, "slice")
    staged_yaml = stage_current_yaml(yaml_path, slice_dir)

    banner(f"STAGE 1 — slice run in {slice_dir}")
    jsonl = os.path.join(slice_dir, JSONL_NAME)
    if reuse_jsonl and os.path.exists(jsonl):
        print(f"  Reusing {jsonl}")
    else:
        jsonl = fetch_slice(root_taxid, slice_dir)

    banner("PHASE 1 — parse the current assemblies")
    parse_current(jsonl, staged_yaml)

    banner("PHASE 0 — backfill the historical versions")
    run_backfill(jsonl, slice_dir, repo_root)

    banner("PHASE 1 daily — first run after the backfill")
    _, previous_tsv, _ = resolve_current_tsv_paths(
        slice_dir, config=load_config(config_file=staged_yaml)
    )
    parse_assembly_versions(
        new_jsonl=jsonl,
        previous_tsv=previous_tsv,
        historical_tsv=os.path.join(slice_dir, "assembly_historical.tsv"),
    )

    return aggregate_and_validate(slice_dir, staged_yaml, taxdump_path)


def run_stage_two(
    work_dir: str,
    root_taxid: str,
    yaml_path: str,
    repo_root: str,
    taxdump_path: str = None,
    reuse_jsonl: bool = False,
) -> int:
    """Run the daily path over two days of the same slice.

    Args:
        work_dir (str): Run directory; the stage writes into work_dir/two_day.
        root_taxid (str): Root taxid of the slice.
        yaml_path (str): Path to the current-assembly types YAML.
        repo_root (str): Repository root.
        taxdump_path (str, optional): Dev/test taxdump for Phase 3.
        reuse_jsonl (bool): Use the JSONL already in the stage directory.

    Returns:
        int: Number of validator failures.
    """
    two_day_dir = stage_dir(work_dir, "two_day")
    staged_yaml = stage_current_yaml(yaml_path, two_day_dir)

    banner(f"STAGE 2 — two-day simulation in {two_day_dir}")
    today_jsonl = os.path.join(two_day_dir, JSONL_NAME)
    slice_jsonl = os.path.join(work_dir, "slice", JSONL_NAME)
    if reuse_jsonl and os.path.exists(today_jsonl):
        print(f"  Reusing {today_jsonl}")
    elif os.path.exists(slice_jsonl):
        print(f"  Reusing the stage 1 fetch: {slice_jsonl}")
        shutil.copy(slice_jsonl, today_jsonl)
    else:
        fetch_slice(root_taxid, two_day_dir)

    today = read_jsonl(today_jsonl)
    yesterday, changes = rewind_multi_version_assemblies(today)
    print(f"\n  Rewound {len(changes)} multi-version assemblies to make day 1:")
    for base, (was, now) in list(changes.items())[:5]:
        print(f"    {base}: v{was} yesterday, v{now} today")
    if len(changes) > 5:
        print(f"    ... and {len(changes) - 5} more")
    if not changes:
        print("  Nothing in this slice has a version above 1, so the diff")
        print("  paths cannot fire. Pick a clade with multi-version assemblies.")
        return 1

    banner("DAY 1 — parse yesterday, then backfill")
    write_jsonl(today_jsonl, yesterday)
    parse_current(today_jsonl, staged_yaml)
    run_backfill(today_jsonl, two_day_dir, repo_root)

    banner("DAY 2 — parse today, snapshotting yesterday")
    write_jsonl(today_jsonl, today)
    parse_current(today_jsonl, staged_yaml)

    banner("DAY 2 — diff the versions")
    _, previous_tsv, _ = resolve_current_tsv_paths(
        two_day_dir, config=load_config(config_file=staged_yaml)
    )
    results = parse_assembly_versions(
        new_jsonl=today_jsonl,
        previous_tsv=previous_tsv,
        historical_tsv=os.path.join(two_day_dir, "assembly_historical.tsv"),
    )
    print(f"\n  Rewound bases:  {len(changes)}")
    print(f"  Superseded:     {results['newly_superseded_count']}")
    print(f"  Missing:        {results['missing_versions_count']}")

    # The two counts are not expected to match. The rewind is applied to the
    # JSONL, which holds one record per assembly, while the current TSV is
    # keyed on genbankAccession -- so a RefSeq base folds into its GenBank row
    # and never appears as a base of its own. A rewind can also leave the
    # parser holding two versions of one base, and the diff is right to call
    # that base unchanged: the pipeline already knew the newer version. What
    # this stage has to show is that the path fired at all.
    failures = aggregate_and_validate(two_day_dir, staged_yaml, taxdump_path)
    if not results["newly_superseded_count"]:
        print("\n  The rewind produced no supersessions, so the diff path was")
        print("  never exercised and this stage proved nothing.")
        failures += 1
    return failures


def staged_run(
    work_dir: str,
    root_taxid: str = DEFAULT_ROOT_TAXID,
    yaml_path: str = None,
    taxdump_path: str = None,
    stage: str = "all",
    reuse_jsonl: bool = False,
) -> int:
    """Run the requested stages and report the total failures.

    Args:
        work_dir (str): Run directory; each stage writes into a subdirectory.
        root_taxid (str): Root taxid of the slice.
        yaml_path (str, optional): Current-assembly types YAML; defaults to
            the one in test/.
        taxdump_path (str, optional): Dev/test taxdump for Phase 3.
        stage (str): "1", "2" or "all".
        reuse_jsonl (bool): Reuse a JSONL already fetched.

    Returns:
        int: Total validator failures across the stages run.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    yaml_path = yaml_path or os.path.join(repo_root, DEFAULT_CURRENT_YAML)
    os.makedirs(work_dir, exist_ok=True)

    failures = 0
    if stage in ("1", "all"):
        failures += run_stage_one(
            work_dir, root_taxid, yaml_path, repo_root, taxdump_path, reuse_jsonl
        )
    if stage in ("2", "all"):
        failures += run_stage_two(
            work_dir, root_taxid, yaml_path, repo_root, taxdump_path, reuse_jsonl
        )

    banner(
        "STAGED RUN COMPLETE" if not failures
        else f"STAGED RUN FAILED: {failures} failures"
    )
    return failures


if __name__ == "__main__":
    args = _parse_args(
        [
            WORK_DIR,
            default(ROOT_TAXID, DEFAULT_ROOT_TAXID),
            YAML_PATH,
            TAXDUMP_PATH,
            STAGE,
            REUSE_JSONL,
        ],
        description="Run stages 1 and 2 of the Phase 4 staged run",
    )
    raise SystemExit(
        1
        if staged_run(
            work_dir=args.work_dir,
            root_taxid=args.root_taxid,
            yaml_path=args.yaml_path,
            taxdump_path=args.taxdump_path,
            stage=args.stage,
            reuse_jsonl=args.reuse_jsonl,
        )
        else 0
    )
