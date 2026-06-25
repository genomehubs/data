# Phase 2: Assembly Version Summary

This guide explains how to run and test the Phase 2 implementation:
`generate_assembly_summary.py`, which combines `assembly_current.tsv` and
`assembly_historical.tsv` into a per-base-accession version summary.

Phase 0 (one-time backfill) and Phase 1 (daily incremental) must have run at
least once so that both input TSVs exist before running Phase 2.

## Files added in Phase 2

| File | Purpose |
|---|---|
| `flows/lib/generate_assembly_summary.py` | Aggregates current + historical TSVs into `assembly_version_summary.tsv`; emits `generate.assembly_summary.completed` for downstream Phase 3 flows |

## How it fits in the daily pipeline

```
update_ncbi_datasets          →  update.ncbi.datasets.finished
  → fetch_parse_validate            →  validate.file.pair.finished
    → parse_assembly_versions       →  update.assembly_versions.completed
      → generate_assembly_summary  →  generate.assembly_summary.completed
        → [Phase 3 taxon milestones]
```

`generate_assembly_summary` is triggered by `update.assembly_versions.completed`
and emits `generate.assembly_summary.completed` so Phase 3 can be wired as a
further downstream trigger.

## What it produces

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

Tests for Phase 1 (which Phase 2 builds on) are in `tests/test_assembly_versions.py`:

```bash
SKIP_PREFECT=true python3 -m pytest tests/test_assembly_versions.py -v
```

Phase 2 unit tests will be added in a follow-up once the downstream Phase 3
taxon milestone integration is defined.
