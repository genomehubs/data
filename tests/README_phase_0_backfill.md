# Phase 0: Testing Guide for Historical Assembly Backfill

This guide explains how to run and test the one-time historical backfill
implementation (`parse_backfill_historical_versions.py`).

## Prerequisites

### 1. Install Dependencies

```bash
# Recommended: use the project conda environment
conda env create -f env.yaml
conda activate genomehubs_data
```

### 2. Install NCBI datasets CLI

The backfill script requires the NCBI `datasets` command-line tool (included in
`env.yaml` via `conda-forge::ncbi-datasets-cli`; install manually if needed).

```bash
# https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/
datasets --version
```

## Test Suite

### Quick Unit Tests

Run the automated test suite with pytest:

```bash
SKIP_PREFECT=true python3 -m pytest tests/test_backfill.py -v
```

Tests cover: accession parsing, FTP version discovery, cache load/save,
checkpoint save/resume, `parse_historical_version`, and the full backfill
orchestrator flow.

### Full Backfill Run (Small Dataset)

Test the complete backfill on the 3-assembly fixture already in the repo
(`GCA_000222935.2`, `GCA_000412225.2`, `GCA_003706615.3`):

```bash
mkdir -p /tmp/assembly-versions
cp configs/assembly_historical.types.yaml /tmp/assembly-versions/

SKIP_PREFECT=true python3 -m flows.parsers.parse_backfill_historical_versions \
  --input_path tests/test_data/assembly_test_sample.jsonl \
  --yaml_path /tmp/assembly-versions/assembly_historical.types.yaml \
  --work_dir /tmp/assembly-versions
```

**PowerShell:**

```powershell
New-Item -ItemType Directory -Force -Path C:\Temp\assembly-versions
Copy-Item configs\assembly_historical.types.yaml C:\Temp\assembly-versions\

$env:SKIP_PREFECT = "true"
python -m flows.parsers.parse_backfill_historical_versions `
  --input_path tests\test_data\assembly_test_sample.jsonl `
  --yaml_path C:\Temp\assembly-versions\assembly_historical.types.yaml `
  --work_dir C:\Temp\assembly-versions
```

> **Note on output path**: the output TSV (`assembly_historical.tsv`) is written
> relative to the *YAML file's directory*, not `--work_dir`. Copying the YAML
> into `--work_dir` (as above) puts the output there.

Requires `datasets` CLI and network access (FTP version discovery +
`datasets summary` per version). Without them each assembly reports a clean
per-accession failure and the run completes with 0 records written.

## CLI reference

```
python3 -m flows.parsers.parse_backfill_historical_versions \
  --input_path <assembly_data_report.jsonl>  # required
  --yaml_path  <assembly_historical.types.yaml>  # required
  --work_dir   <path>                            # optional, default: .
```

The run is resumable: if interrupted, re-running with the same arguments picks
up from the last saved checkpoint in `<work_dir>/checkpoints/`.
