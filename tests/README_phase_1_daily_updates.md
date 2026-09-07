# Phase 1: Daily Incremental Assembly Version Tracking

This guide explains how to run and test the Phase 1 implementation: the daily
incremental pipeline that detects assembly versions newly superseded since the
last run and records them in `assembly_historical.tsv`.

Phase 0 (one-time backfill via `parse_backfill_historical_versions.py`) must
have run at least once before Phase 1's daily flow can detect superseded
versions — Phase 1 reads yesterday's `assembly_current.tsv` to look up the
previous state and compares it against today's NCBI JSONL.

Phase 1 also reads `assembly_historical.tsv` as an index of versions already
parsed, so a gap Phase 0 has filled is not re-reported on the next run. Both
reads are local; the step makes no network calls.

## Files added in Phase 1

| File | Purpose |
|---|---|
| `flows/parsers/parse_assembly_versions.py` | Daily incremental parser — no NCBI fetches |
| `flows/updaters/update_assembly_versions.py` | Fetches metadata for any version missing from the previous parse |
| `flows/lib/assembly_versions_utils.py` | Shared helpers (FTP discovery, cache, accession parsing) used by both the parser and the backfill parser |
| `configs/assembly_historical.types.yaml` | Schema/config for `assembly_historical.tsv` (added in Phase 0, referenced here) |

## Prerequisites

### Dependencies

```bash
conda activate genomehubs_data  # or: conda env create -f env.yaml
```

### NCBI datasets CLI

Required only by the updater and the Phase 0 backfill; `parse_assembly_versions.py`
itself makes no network calls.

```bash
# https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/
datasets --version
```

## How the daily incremental pipeline works

```
[yesterday]  assembly_current.tsv  ──────────────────────────────────┐
[today]      assembly_data_report.jsonl  ──► parse_assembly_versions ─┤
                                                                       ├─► assembly_historical.tsv (updated)
                                                                       └─► missing_versions.json (if any)

missing_versions.json  ──► update_assembly_versions  ──► missing_assembly_versions.jsonl
                                                               │
                                                               └──► parse_backfill_historical_versions  ──► assembly_historical.tsv
```

**`parse_assembly_versions.py`** (daily, no network):
- Reads the previous current TSV (snapshotted to `.previous` before each run);
  its filename and compression come from `config.meta["file_name"]`
- Indexes the versions already in `assembly_historical.tsv`
- Reads today's JSONL and, for each base accession, compares the version
  current now with the version that was current at the last run:

  | Case | Action |
  |---|---|
  | same version | nothing — the assembly did not change |
  | higher version | the previous version is marked superseded; any versions between the two are gap candidates |
  | lower version | logged as a regression (suppressed or rolled back at NCBI); nothing is written |
  | base not seen before | every version below the current one is a gap candidate |

- A superseded row copies the previous row and stamps `versionStatus`,
  `assemblyID`, `superseded_by`, `superseded_by_version` and `superseded_date`
  — the column names declared in `assembly_historical.types.yaml` — then merges
  into `assembly_historical.tsv`
- Gap candidates already present in `assembly_historical.tsv` are dropped; the
  rest go to `missing_versions.json`, one record per missing version

Because unchanged assemblies are skipped and filled gaps are filtered out, a
normal day after a complete backfill reports close to zero missing versions.
The two cases that legitimately remain are a base entering the dataset above
version 1, and a multi-version jump between runs.

**`update_assembly_versions.py`** (runs when `missing_versions.json` exists):
- Fetches the current accession's raw NCBI metadata for each missing entry
- Writes `missing_assembly_versions.jsonl` — feed back into
  `parse_backfill_historical_versions.py` (Phase 0) to complete the backfill

## Minimal end-to-end walkthrough

Uses `tests/test_data/assembly_test_sample.jsonl` — three real assemblies already
in the repo (`GCA_000222935.2`, `GCA_000412225.2`, `GCA_003706615.3`), covering
both the "predecessor found" and "predecessor missing" code paths.

```bash
mkdir -p /tmp/assembly-versions
cp tests/test_data/assembly_test_sample.jsonl /tmp/assembly-versions/assembly_data_report.jsonl
```

> **Note on output path**: `parse_backfill_historical_versions.py` resolves
> `assembly_historical.tsv` against `--work_dir`, so the YAML can stay where it
> is in the repo. (Before PR-A the output landed next to the YAML instead, and
> this walkthrough copied the config into `--work_dir` to work around it.)

### Step 1 — one-time Phase 0 backfill (if not already done)

```bash
SKIP_PREFECT=true python3 -m flows.parsers.parse_backfill_historical_versions \
  --input_path /tmp/assembly-versions/assembly_data_report.jsonl \
  --yaml_path configs/assembly_historical.types.yaml \
  --work_dir /tmp/assembly-versions
# Writes: /tmp/assembly-versions/assembly_historical.tsv
```

Requires `datasets` CLI and network (FTP version discovery + `datasets summary`).
Without them, each assembly reports a clean per-accession fetch failure and the
run completes with 0 records written.

### Step 2 — simulate "yesterday's" parse

```bash
# Minimal previous TSV: only GCA_000222935.1 was known yesterday.
# Column names must match what the assembly parsers actually emit.
printf "genbankAccession\tassemblyID\nGCA_000222935.1\tGCA_000222935_1\n" \
  > /tmp/assembly-versions/assembly_current.tsv.previous
```

In production this file is a verbatim snapshot of the previous run's current
TSV, taken by `snapshot_previous_output` before the new run overwrites it. Its
name — and whether it is gzipped — follow `config.meta["file_name"]`, so with
the production NCBI config it is `ncbi_datasets_eukaryota.tsv.gz.previous`.
`parse_assembly_versions` resolves both from the config it is given and opens a
compressed snapshot transparently; the plain `assembly_current.tsv.previous`
above is what a config emitting `assembly_current.tsv` produces.

### Step 3 — run the daily incremental parser

```bash
SKIP_PREFECT=true python3 -m flows.parsers.parse_assembly_versions \
  --input_path /tmp/assembly-versions/assembly_data_report.jsonl
```

Expected output:

```
[1/4] Loading previous parsed results...
Loaded 1 assemblies from previous parsed results.
  Unique base accessions: 1

[2/4] Indexing versions already in the historical TSV...
  Versions already parsed: 0
  Across base accessions: 0

[3/4] Identifying newly superseded versions...
  Found: 1 newly superseded versions.
  Examples:
    GCA_000222935.1 -> superseded by v2

  Warning: 3 assemblies have missing previous versions.
  These may need manual backfill:
    GCA_000412225: need v1, have v2
    GCA_003706615: need v1, have v3
    GCA_003706615: need v2, have v3

  To backfill missing versions, run:
    python -m flows.updaters.update_assembly_versions

[4/4] Updating historical TSV...
  Added 1 newly superseded versions.

ASSEMBLY VERSION PARSE COMPLETE  Superseded: 1  Missing: 3
```

Writes:
- `/tmp/assembly-versions/assembly_historical.tsv` — one row appended for `GCA_000222935.1`
- `/tmp/assembly-versions/missing_versions.json` — three entries: `GCA_000412225`
  v1, and `GCA_003706615` v1 and v2

Both new bases are reported at every version below their current one, because
the historical TSV is empty on this first run. Re-running after step 5 has
backfilled them reports 0 missing — that filtering is what keeps the daily run
quiet once the backfill is complete.

No network calls are made in this step.

### Step 4 — fetch metadata for missing versions (requires datasets CLI)

```bash
SKIP_PREFECT=true python3 -m flows.updaters.update_assembly_versions \
  --missing_json /tmp/assembly-versions/missing_versions.json \
  --work_dir /tmp/assembly-versions
# Writes: /tmp/assembly-versions/missing_assembly_versions.jsonl
```

### Step 5 — backfill the missing versions

```bash
SKIP_PREFECT=true python3 -m flows.parsers.parse_backfill_historical_versions \
  --input_path /tmp/assembly-versions/missing_assembly_versions.jsonl \
  --yaml_path configs/assembly_historical.types.yaml \
  --work_dir /tmp/assembly-versions
# Appends the missing predecessors to assembly_historical.tsv
```

The append is genuine: when `assembly_historical.tsv` already exists the parser
adds only the versions it does not already hold, leaving every earlier row in
place. (Before PR-A this step rewrote the file wholesale, so a single gap-fill
would have discarded the whole backfill.)

## Running the test suite

```bash
cd "$(git rev-parse --show-toplevel)"
SKIP_PREFECT=true python3 -m pytest tests/test_assembly_versions.py -v
```

The tests cover: accession parsing, config-driven path resolution,
superseded-row building, missing-version detection, TSV merge/deduplication,
schema conformance against `assembly_historical.types.yaml`, the
non-destructive Phase 0 write, updater JSONL writing, and the `plugin()`
registration hook.
