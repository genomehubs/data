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

Phase 4 shipped as three stacked PRs — #13 (schema and path contracts), #14
(version diffing) and #15 (this one). Two rows below landed with #13 and are
already on `main`; they are listed because the guide describes them, not
because they are in this diff.

| File | Purpose |
|---|---|
| `flows/lib/assembly_lineage.py` | Reads the upstream `{rank}TaxId` columns: one rank→column mapping, the two absent sentinels, and `register_row_taxa` to turn row lineages into taxonomy nodes |
| `flows/lib/compute_taxon_milestones.py` | Uses the row lineage where present and the taxdump otherwise; no longer requires `--taxdump_path` |
| `tests/validate_pipeline.py` | Nine cross-file checks over the four output TSVs |
| `tests/validate_no_ncbi_fetches.py` | Runs the daily version parse with sockets and subprocesses blocked |
| `tests/test_assembly_lineage.py` | Unit tests for the column contract and the production path |
| `tests/test_phase_4_validators.py` | Unit tests for both validators, including each failure mode |
| `tests/test_two_day_simulation.py` | Runs all four phases in sequence over one working directory |
| `tests/test_assembly_summary.py` | Unit tests for the Phase 2.2 summary generator, which shipped without any — *merged in #13* |
| `tests/staged_run.py` | Drives stages 1 and 2 of the staged run against real NCBI data |
| `.github/workflows/pytest.yml` | Runs `pytest tests/` on every pull request — *merged in #13* |

## Step 1: `validate_pipeline.py`

```bash
python -m tests.validate_pipeline --work_dir tmp
python -m tests.validate_pipeline --work_dir tmp --yaml_path <config> --strict
```

It resolves the current-TSV filename the same way the flows do — from
`config.meta["file_name"]` when `--yaml_path` is given, by discovery in `work_dir`
otherwise — then loads all four outputs and runs nine checks:

| Check | Assertion |
|---|---|
| `outputs-present` | all four TSVs exist |
| `assembly-id-uniqueness` | no assembly ID appears twice across current + historical, and no row lacks one |
| `version-gaps` | each summary `version_gaps` matches the versions actually present for that base |
| `referential-integrity` | every `superseded_by` resolves to a known accession of the same base |
| `summary-completeness` | every base accession is summarised, and no summary row is stale |
| `milestone-date-ordering` | the nested milestone dates never invert |
| `in-ranks-validity` | every `first_*_in_ranks` value is a canonical rank |
| `assembly-id-coverage` | whether a source carries an assembly-ID column at all |
| `lineage-columns` | the upstream `{rank}TaxId` columns arrived, populated |
| `lineage-coverage` | how many current rows carry no lineage at all |

Exit status is 0 when every check passes, 1 otherwise. Checks carry one of
three severities: an **error** always fails the run, a **warning** fails it only
under `--strict`, and a **note** never fails it.

`lineage-columns` is a warning, because a dev run off a local taxdump
legitimately has no lineage columns, while a production run should treat their
absence as fatal — the write path (`write_to_tsv` -> `gh_utils.write_tsv` ->
`print_to_tsv`) emits only the columns the types YAML declares, so the
enrichment can be a silent no-op upstream and this is the check that catches
it. `lineage-coverage` is a note: some assemblies will always sit
on a taxid the upstream lookup does not cover, so it is a number to watch rather
than a defect, and a single uncovered row should not fail a 57,000-row run.

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
  `update_assembly_versions` to fetch. Before the diff fix in #14 this check
  would have failed on roughly 3,694 spurious entries.

With `--work_dir`, the JSONL, `.previous` snapshot and historical TSV are copied
into a scratch directory first, since the parse rewrites the historical TSV.

## Step 3: the production taxonomy path

`parse_ncbi_assemblies` attaches the canonical-rank lineage to every assembly row
as `genusTaxId`, `familyTaxId`, `orderTaxId`, `classTaxId`, `phylumTaxId` and
`kingdomTaxId` (upstream `13269d0`, refined in `f14ea28`). `flows/lib/assembly_lineage.py`
is the single place that knows those names — `RANK_COLUMN_TEMPLATE` is a one-line
repoint if they change.

`compute_taxon_milestones` now has four ways to run:

| Inputs | Behaviour |
|---|---|
| Rows with lineage columns, no `--taxdump_path` | Production. Lineage from the columns; scientific names stay empty; a row is attributed at its own taxid unless another row names that taxid as an ancestor |
| `--taxdump_path`, rows without lineage columns | Dev/test, unchanged from Phase 3 |
| Both | Columns win for lineage and species; the taxdump supplies the names they cannot |
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

**Rank order when registering taxa.** `register_row_taxa` registers every
ancestor first and only then the row taxids, at rank `species`. The order
matters for an assembly submitted above species level — "Genus sp." — whose own
taxid another row names as its genus: registering row taxids first would label
that taxon a species or a genus depending on which row came first in the file.
Registered as a genus it has no species ancestor, so the sweep skips it and
says so — the same thing the taxdump path does with such an assembly today.
`compute_milestones` collects those skips and prints five with a count, rather
than one line per row on a 57,000-row input.

**One sentinel, one place.** `assembly_versions_utils.cell` is what every phase
reads a cell through, so `""` and the literal `"None"` mean the same thing to
all of them. Phase 3's `_has_metric` already read `ebpStandardDate` through
`cell`, so a literal `"None"` counted as absent; Phase 2's `_has_ebp_metric`
read the raw column and counted it as an EBP metric, which would have left the
summary and the milestones disagreeing about the same assembly on real data.
Resolved in #13 by routing Phase 2 through the same helper rather than by
special-casing the string in either place —
`test_phase_2_and_phase_3_agree_on_the_metric` pins the two predicates
together over `""`, `"None"` and a real date.

**Why the taxdump is no longer needed in production.** `speciesTaxId` landed
in `630d327` (2026-09-07), so Phase 3 reads the species a row belongs to
instead of walking to it, and a subspecies-level assembly is collapsed onto
its species with no taxdump present. Species-level rows carry the column too —
blobtk includes a taxon in its own `lineage` array, emitting it at
`node_depth: 0` ahead of its ancestors — so the column is populated for the
bulk of the dataset, not just for the rows submitted below species level.

Scientific names do not change that. The milestone output declares a
`scientific_name` column, but nothing in this codebase reads the file back —
it is a GoaT import and the import resolves taxids — so the column stays empty
in production. A taxdump is a dev and test convenience, not a production
input.

`resolve_to_species` stays as the fallback, and it still matters: an empty
`speciesTaxId` means an older TSV without the column, or a row above species
rank whose lineage genuinely has no species in it. Both are attributed at the
row's own taxid rather than dropped — for a species-level row its own taxid
*is* the species, so this never does worse than the behaviour before the
column existed. The flow prints that caveat when it runs without a taxdump.

**The lineage columns reach the TSV.** They are declared in `goat-data`
`sources/assembly-data/ncbi_datasets_eukaryota.types.yaml` on `origin/main`,
first present in release `2026.08.27` — `genus_taxon_id: {header: genusTaxId}`
and the same for family through kingdom — so `lineage-columns` should pass on
a production run. It stays a warning rather than an error because the dev case,
a run off a local taxdump with no lineage columns, is still legitimate.

## Step 4: the staged full run

Operational, not code. Do not go straight to 57,236 assemblies. The
non-destructive write fix this depends on — without it, any run exercising the
gap-fill path discards the backfill it just built — merged with #13, so a real
run is no longer blocked on it.

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

   The synthetic half of this stage is now a test — `test_two_day_simulation.py`
   runs all four phases over one working directory on every CI run, so what the
   staged run adds here is real data, not new coverage of the composition.

Stages 1 and 2 are scripted in `tests/staged_run.py`:

```bash
export PATH="<the env holding the datasets CLI>/bin:$PATH"
export PYTHONUTF8=1
python -m tests.staged_run --work_dir tmp/staged --taxdump_path <taxdump>
```

Everything lands under `--work_dir`, one directory per stage. The default slice
is Malacostraca (6681): 207 assemblies, 18 of them above version 1, one at v4 —
small enough to run in minutes and rich enough that the backfill has real work.
A clade whose assemblies are all at v1 exercises nothing.

Stage 2 needs a yesterday that differs from today. Rather than fabricate
tomorrow — which would leave the gap-fill asking NCBI for accessions that do
not exist — it fabricates *yesterday*, rewinding the multi-version assemblies
by one version. Every version the pipeline then goes looking for is one NCBI
actually has.

The rewound count and the supersession count are not expected to match. The
rewind applies to the JSONL, one record per assembly, while the current TSV is
keyed on `genbankAccession`, so a RefSeq base folds into its GenBank row and
never appears as a base of its own. A rewind can also leave the parser holding
two versions of one base, and the diff is then right to call that base
unchanged — the pipeline already knew the newer version. The stage fails only
if *nothing* was superseded, which would mean the diff path never fired.

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
