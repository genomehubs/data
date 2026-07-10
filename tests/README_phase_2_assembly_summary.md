# Phase 2: Assembly Version Summary

This guide covers the Phase 2 implementation, which has two parts:

- **Phase 2.1 — previous-current snapshot** (`parse_ncbi_assemblies.py`): preserves
  yesterday's `assembly_current.tsv` as `assembly_current.tsv.previous` before today's
  parse overwrites it, so the Phase 1 incremental can find each superseded predecessor.
- **Phase 2.2 — assembly version summary** (`generate_assembly_summary.py`): combines
  `assembly_current.tsv` and `assembly_historical.tsv` into a per-base-accession version
  summary.

Phase 0 (one-time backfill) and Phase 1 (daily incremental) must have run at
least once so that both input TSVs exist before running Phase 2.2.

## Files added/changed in Phase 2

| File | Phase | Purpose |
|---|---|---|
| `flows/parsers/parse_ncbi_assemblies.py` | 2.1 | Adds `snapshot_previous_output`, called just before `write_to_tsv`, to copy the prior `assembly_current.tsv` → `assembly_current.tsv.previous` before the overwrite |
| `flows/lib/generate_assembly_summary.py` | 2.2 | Aggregates current + historical TSVs into `assembly_version_summary.tsv`; emits `generate.assembly_summary.completed` for downstream Phase 3 flows |

## How it fits in the daily pipeline

```
update_ncbi_datasets          →  update.ncbi.datasets.finished
  → fetch_parse_validate            →  validate.file.pair.finished
      (parse_ncbi_assemblies: snapshot .previous, then write assembly_current.tsv)   [2.1]
    → parse_assembly_versions       →  update.assembly_versions.completed
      → generate_assembly_summary  →  generate.assembly_summary.completed            [2.2]
        → [Phase 3 taxon milestones]
```

`generate_assembly_summary` is triggered by `update.assembly_versions.completed`
and emits `generate.assembly_summary.completed` so Phase 3 can be wired as a
further downstream trigger.

## Phase 2.1: previous-current snapshot

`parse_assembly_versions` (Phase 1 daily incremental) needs **yesterday's** full
current set to find each newly superseded predecessor (`v_n-1`). But
`parse_ncbi_assemblies` overwrites `assembly_current.tsv` **in place** with today's
parse, so without intervention yesterday's copy is lost before the incremental runs.

`snapshot_previous_output(config)` closes that gap: immediately before `write_to_tsv`,
it copies `config.meta["file_name"]` → `<same>.previous`. At that point
`fetch_previous_file_pair` has already placed yesterday's published
`assembly_current.tsv` on disk, so the snapshot captures yesterday's copy at exactly
`<work_dir>/assembly_current.tsv.previous` — the path `parse_assembly_versions` reads.
It is a no-op on the first run, when no prior output exists.

This is a *different* mechanism from how `parse_ncbi_assemblies` reuses prior results
(`fetch_previous_file_pair` / `Config.load_previous`), which reads the prior run's TSV
under its **plain** name to copy over unchanged rows.

**Ordering constraint:** the current parse and the historical parse must run in the
**same `work_dir`**, current first, so the `.previous` snapshot is visible to the
incremental step. The existing pipeline already satisfies this.

## Phase 2.2: what it produces

`assembly_version_summary.tsv` — one row per base accession:

| Column | Description |
|---|---|
| `base_accession` | Base accession without version suffix (e.g. `GCA_000002035`) |
| `taxId` | NCBI taxonomy ID of the organism |
| `first_version_accession` | Full accession of the earliest known version |
| `first_version_number` | Version number of the earliest known version |
| `first_version_date` | Release date of the earliest known version |
| `current_version_accession` | Full accession of the latest version |
| `current_version_number` | Version number of the latest version |
| `current_version_date` | Release date of the latest version |
| `total_versions` | Count of all versions (current + historical) |
| `superseded_versions` | Count of versions with `versionStatus = superseded` |
| `first_ebp_metric_accession` | Accession of the first version meeting EBP criteria |
| `first_ebp_metric_version` | Version number of first EBP metric |
| `first_ebp_metric_date` | Release date of first EBP metric |
| `version_gaps` | Comma-separated missing version numbers, or empty |

EBP metric is detected from any of: `ebpStandardDate`, `ebp_standard_date`,
`ebpMetricDate`, `ebp_metric_date`.

## Prerequisites

```bash
conda activate genomehubs_data
```

No network access or NCBI `datasets` CLI required — all inputs are local TSVs.

## Running

```bash
SKIP_PREFECT=true python3 -m flows.lib.generate_assembly_summary \
  --work_dir /tmp/assembly-versions
```

Expects in `--work_dir`:
- `assembly_current.tsv` — written by `parse_ncbi_assemblies` (Phase 1 daily pipeline)
- `assembly_historical.tsv` — written by Phase 0 backfill and updated by Phase 1 incremental

Writes to `--work_dir`:
- `assembly_version_summary.tsv`

## Minimal end-to-end test

```bash
mkdir -p /tmp/assembly-versions

# assembly_current.tsv: three assemblies, one with an EBP metric date
cat > /tmp/assembly-versions/assembly_current.tsv << 'EOF'
accession	releaseDate	ebpStandardDate
GCA_000222935.2	2011-08-03
GCA_000412225.2	2014-06-05	2020-03-01
GCA_003706615.3	2023-07-14
EOF

# assembly_historical.tsv: one superseded version (Phase 0/1 output)
cat > /tmp/assembly-versions/assembly_historical.tsv << 'EOF'
accession	versionStatus	releaseDate	ebpStandardDate
GCA_000222935.1	superseded	2010-01-01
EOF

SKIP_PREFECT=true python3 -m flows.lib.generate_assembly_summary \
  --work_dir /tmp/assembly-versions

cat /tmp/assembly-versions/assembly_version_summary.tsv
```

Expected output:
- `GCA_000222935`: 2 versions, `superseded_versions=1`, `first_version_date=2010-01-01`
- `GCA_000412225`: 1 version, `version_gaps=1`, `first_ebp_metric_accession=GCA_000412225.2`
- `GCA_003706615`: 1 version, `version_gaps=1,2`

## Running the test suite

Phase 1 tests (which Phase 2 builds on) and the Phase 2.1 snapshot tests both live in
`tests/test_assembly_versions.py`:

```bash
SKIP_PREFECT=true python3 -m pytest tests/test_assembly_versions.py -v
```

- **Phase 2.1** — `TestSnapshotPreviousOutput` covers: `.previous` created with
  identical content, no-op when no prior output exists, no-op when `file_name` is
  missing, and overwrite of a stale `.previous`.
- **Phase 2.2** — `generate_assembly_summary` unit tests will be added in a follow-up
  once the downstream Phase 3 taxon milestone integration is defined; for now, validate
  it with the minimal end-to-end test above.
