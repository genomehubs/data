# Phase 4: Validation, CI and the Full Run

This guide covers the Phase 4 implementation, which has four parts:

- **Validators** (`tests/validate_pipeline.py`, `tests/validate_no_ncbi_fetches.py`):
  run after a pipeline run to check the four output TSVs against each other, and to
  prove the daily version parse never reaches the network.
- **Production taxonomy** (`flows/lib/assembly_lineage.py`): Phase 3 now reads the
  `{rank}TaxId` columns that `parse_ncbi_assemblies` attaches to every assembly row,
  so milestones can be computed without a local NCBI taxdump.
- **CI** (`.github/workflows/pytest.yml`): the suite runs on every pull request.
- **The staged full run**: an operational runbook, not code — see below.

Phases 0–3 must have run at least once before the validators have anything to check.

## Files added/changed in Phase 4

| File | Purpose |
|---|---|
| `flows/lib/assembly_lineage.py` | Reads the upstream `{rank}TaxId` columns: one rank→column mapping, the two absent sentinels, and `register_row_taxa` to turn row lineages into taxonomy nodes |
| `flows/lib/compute_taxon_milestones.py` | Uses the row lineage where present and the taxdump otherwise; no longer requires `--taxdump_path` |
| `tests/validate_pipeline.py` | Seven cross-file checks over the four output TSVs |
| `tests/validate_no_ncbi_fetches.py` | Runs the daily version parse with sockets and subprocesses blocked |
| `tests/test_assembly_lineage.py` | Unit tests for the column contract and the production path |
| `tests/test_phase_4_validators.py` | Unit tests for both validators, including each failure mode |
| `.github/workflows/pytest.yml` | Runs `pytest tests/` on every pull request |

## Step 1: `validate_pipeline.py`

```bash
python -m tests.validate_pipeline --work_dir tmp
python -m tests.validate_pipeline --work_dir tmp --yaml_path <config> --strict
```

It resolves the current-TSV filename the same way the flows do — from
`config.meta["file_name"]` when `--yaml_path` is given, by discovery in `work_dir`
otherwise — then loads all four outputs and runs seven checks:

| Check | Assertion |
|---|---|
| `outputs-present` | all four TSVs exist |
| `assembly-id-uniqueness` | no assembly ID appears twice across current + historical, and no row lacks one |
| `version-gaps` | each summary `version_gaps` matches the versions actually present for that base |
| `referential-integrity` | every `superseded_by` resolves to a known accession of the same base |
| `summary-completeness` | every base accession is summarised, and no summary row is stale |
| `milestone-date-ordering` | the nested milestone dates never invert |
| `in-ranks-validity` | every `first_*_in_ranks` value is a canonical rank |
| `lineage-columns` | the upstream `{rank}TaxId` columns arrived, populated |

Exit status is 0 when every check passes, 1 otherwise. `lineage-columns` is a
**warning** by default, because a dev run off a local taxdump legitimately has no
lineage columns; `--strict` promotes warnings to failures, which is what a
production run should use. Without those columns Phase 3 has no production
taxonomy source, and `print_to_tsv` only writes columns declared in the types
YAML — so the enrichment can be a silent no-op upstream and this is the check
that catches it.

Two deliberate looser readings of the Phase 4 plan:

- **Referential integrity** looks the referent up across current *and* historical
  rows, not the current TSV alone. A v1 superseded by v2 is correct even after v2
  has itself been superseded by v3, at which point v2 lives in the historical TSV.
- **Milestone ordering** checks each adjacent pair of the two nesting chains
  independently, rather than only whole triples, so an inversion is caught even
  when the third date is absent.

## Step 2: `validate_no_ncbi_fetches.py`

```bash
python -m tests.validate_no_ncbi_fetches            # built-in fixture
python -m tests.validate_no_ncbi_fetches --work_dir tmp   # real inputs, on a copy
```

Scope is `parse_assembly_versions` only, **not** the daily pipeline: it fetches the
bulk JSONL on every run by design, via `update_ncbi_datasets`. Two checks:

- **`no-network`** — a run over inputs with real supersessions and a real version
  gap completes with `socket.socket`, `socket.create_connection`,
  `socket.getaddrinfo` and `subprocess.Popen` all raising. Sockets cover any HTTP
  client; `Popen` covers the `datasets` CLI, the other route a fetch could take.
  The fixture is checked for actually having superseded something, so a run that
  proves nothing cannot pass silently.
- **`unchanged-input`** — a run whose JSONL matches the previous parse reports zero
  superseded, zero missing, and leaves `assembly_historical.tsv` byte-identical.
  This is what minimising fetches means day to day: nothing is handed to
  `update_assembly_versions` to fetch. Before the PR-B diff fix this check would
  have failed on roughly 3,694 spurious entries.

With `--work_dir`, the JSONL, `.previous` snapshot and historical TSV are copied
into a scratch directory first, since the parse rewrites the historical TSV.

## Step 4: the production taxonomy path

`parse_ncbi_assemblies` attaches the canonical-rank lineage to every assembly row
as `genusTaxId`, `familyTaxId`, `orderTaxId`, `classTaxId`, `phylumTaxId` and
`kingdomTaxId` (upstream `13269d0`, refined in `f14ea28`). `flows/lib/assembly_lineage.py`
is the single place that knows those names — `RANK_COLUMN_TEMPLATE` is a one-line
repoint if they change.

`compute_taxon_milestones` now has three ways to run:

| Inputs | Behaviour |
|---|---|
| Rows with lineage columns, no `--taxdump_path` | Production. Lineage from the columns; scientific names stay empty; each row is attributed at its own taxid |
| `--taxdump_path`, rows without lineage columns | Dev/test, unchanged from Phase 3 |
| Both | Columns win for lineage; the taxdump supplies names and the subspecies walk |
| Neither | `SystemExit` naming the columns it looked for |

Two traps the module handles:

- **`"None"` is not absent-looking.** `f14ea28` writes the four-character string
  `"None"` into a taxid column for a rank present in the lineage with a null taxid;
  a rank absent from the lineage yields `""`. Both are treated as absent. Read as a
  taxid, `"None"` would collapse every affected lineage into one bogus taxon —
  pinned by `test_none_sentinel_does_not_collapse_lineages`.
- **Enrichment is best-effort upstream.** `enrich_parsed_assemblies` catches a
  missing `nodes.jsonl`, warns, and returns rows without the columns. That is what
  the `lineage-columns` check in `validate_pipeline` exists to catch.

**Still open with Rich** (neither blocks a run):

1. Are rank **names** coming alongside the taxids? Until they do, higher-rank rows
   in `taxon_milestone_summary.tsv` carry an empty `scientific_name` unless a
   taxdump is supplied, so production runs should keep passing `--taxdump_path`
   until this is settled.
2. Is the literal `"None"` deliberate? Handled either way.
3. Are the `*TaxId` columns declared in the types YAML in `goat-data-main`? If not,
   the enrichment is a silent no-op and `--strict` validation will say so.

There is still no `speciesTaxId` upstream, so `resolve_to_species` stays: without a
taxdump, a subspecies-level assembly is attributed at its own taxid rather than
being collapsed onto its species. The flow prints that caveat when it runs without
a taxdump.

## Step 3: the staged full run

Operational, not code. Do not go straight to 57,236 assemblies, and **do not run
against real data until PR-A is merged** — before the non-destructive write fix,
any run exercising the gap-fill path discards the backfill it just built.

1. **Slice run** — the full chain on a ~200-assembly eukaryote slice:

   ```bash
   python -m flows.parsers.parse_backfill_historical_versions --work_dir tmp ...
   python -m flows.parsers.parse_assembly_versions --input_path tmp/<jsonl> --yaml_path <config>
   python -m flows.lib.generate_assembly_summary --work_dir tmp --yaml_path <config>
   python -m flows.lib.compute_taxon_milestones --work_dir tmp --taxdump_path <taxdump>
   python -m tests.validate_pipeline --work_dir tmp --yaml_path <config> --strict
   python -m tests.validate_no_ncbi_fetches --work_dir tmp
   ```

2. **Two-day simulation** — snapshot the current TSV to `.previous`, bump a version
   in the JSONL, and run the daily step again, covering the unchanged, +1, skipped
   and new-series diff paths end to end. Re-run both validators.
3. **Full backfill** — overnight, checkpointed; Phase 0 resumes from
   `tmp/checkpoints/` if interrupted.
4. **Full daily + summary + milestones**, then both validators with `--strict`.

## Running the test suite

```bash
SKIP_PREFECT=true python3 -m pytest tests/ -q
```

CI runs exactly this on every pull request (`.github/workflows/pytest.yml`), on
Python 3.12 with `pip install genomehubs pytest` — the suite needs no network and
no `datasets` CLI. `flake8.yml` continues to lint `flows/` only; `tests/` is not
linted.

Note that `tests/test_data` is excluded by `.git/info/exclude`, so **any new fixture
placed there will not be committed**. Every Phase 4 fixture is therefore built
inline in `tmp_path`, or by `write_fixture` in `validate_no_ncbi_fetches.py`.
