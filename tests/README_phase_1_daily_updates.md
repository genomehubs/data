# Phase 1: Daily Incremental Assembly Version Tracking

This guide explains how to run and test the Phase 1 implementation: the daily
incremental pipeline that detects assembly versions newly superseded since the
last run and records them in `assembly_historical.tsv`.

Phase 0 (one-time backfill via `parse_backfill_historical_versions.py`) must
have run at least once before Phase 1's daily flow can detect superseded
versions — Phase 1 reads yesterday's `assembly_current.tsv` to look up the
previous state and compares it against today's NCBI JSONL.

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
- Reads the previous `assembly_current.tsv` (renamed `.previous` before each run)
- Reads today's JSONL; for every assembly at version > 1 checks whether its
  predecessor exists in the previous TSV
- If found: copies the row and marks it `superseded`, appends to `assembly_historical.tsv`
- If missing: writes a record to `missing_versions.json` for the updater to handle

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
cp configs/assembly_historical.types.yaml /tmp/assembly-versions/
cp tests/test_data/assembly_test_sample.jsonl /tmp/assembly-versions/assembly_data_report.jsonl
```

> **Note on output path**: `parse_backfill_historical_versions.py` writes
> `assembly_historical.tsv` relative to the *YAML file's directory*, not
> `--work_dir`. Copying the YAML into `--work_dir` (as above) puts the output
> there.

### Step 1 — one-time Phase 0 backfill (if not already done)

```bash
SKIP_PREFECT=true python3 -m flows.parsers.parse_backfill_historical_versions \
  --input_path /tmp/assembly-versions/assembly_data_report.jsonl \
  --yaml_path /tmp/assembly-versions/assembly_historical.types.yaml \
  --work_dir /tmp/assembly-versions
# Writes: /tmp/assembly-versions/assembly_historical.tsv
```

Requires `datasets` CLI and network (FTP version discovery + `datasets summary`).
Without them, each assembly reports a clean per-accession fetch failure and the
run completes with 0 records written.

### Step 2 — simulate "yesterday's" parse

```bash
# Minimal previous TSV: only GCA_000222935.1 was known yesterday
printf "accession\tassembly_id\nGCA_000222935.1\tGCA_000222935_1\n" \
  > /tmp/assembly-versions/assembly_current.tsv.previous
```

In production this file is the `assembly_current.tsv` from the previous daily
run, renamed before the new run writes a fresh copy.

### Step 3 — run the daily incremental parser

```bash
SKIP_PREFECT=true python3 -m flows.parsers.parse_assembly_versions \
  --input_path /tmp/assembly-versions/assembly_data_report.jsonl
```

Expected output:

```
[1/3] Loading previous parsed results...
Loaded 1 assemblies from previous parsed results.

[2/3] Identifying newly superseded versions...
  Found: 1 newly superseded versions.
  Examples:
    GCA_000222935.1 -> superseded by v2

  Warning: 2 assemblies have missing previous versions.
    GCA_000412225: need v1, have v2
    GCA_003706615: need v2, have v3

[3/3] Updating historical TSV...
  Added 1 newly superseded versions.

ASSEMBLY VERSION PARSE COMPLETE  Superseded: 1  Missing: 2
```

Writes:
- `/tmp/assembly-versions/assembly_historical.tsv` — one row appended for `GCA_000222935.1`
- `/tmp/assembly-versions/missing_versions.json` — two entries (GCA_000412225, GCA_003706615)

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
  --yaml_path /tmp/assembly-versions/assembly_historical.types.yaml \
  --work_dir /tmp/assembly-versions
# Appends the missing predecessors to assembly_historical.tsv
```

## Running the test suite

```bash
cd "$(git rev-parse --show-toplevel)"
SKIP_PREFECT=true python3 -m pytest tests/test_assembly_versions.py -v
```

All 34 tests cover: accession parsing, path derivation, superseded-row
building, missing-version detection, TSV append/deduplication, updater JSONL
writing, and the `plugin()` registration hook.
