# Testing Guide for Historical Assembly Backfill

This guide explains how to test the backfill implementation before creating a PR.

## Prerequisites

### 1. Install Dependencies

```bash
# Install genomehubs and other dependencies
pip install -r requirements.txt

# Or if using conda:
conda install -c conda-forge genomehubs>=2.10.14
pip install prefect requests
```

### 2. Install NCBI datasets CLI

The backfill script requires the NCBI `datasets` command-line tool.

```bash
# Download and install from:
# https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/

# Verify installation:
datasets --version
```

## Test Suite

### Quick Unit Tests

Run the automated test suite:

```bash
cd "c:\Users\fchen13\ASU Dropbox\Fang Chen\Work Documents\EBP\genomehubs-data"
python tests/test_backfill.py
```

### Full Backfill Test (Small Dataset)

Test the complete backfill process on 3 assemblies:

```bash
export SKIP_PREFECT=true
python -m flows.parsers.backfill_historical_versions \
    --input_path tests/test_data/assembly_test_sample.jsonl \
    --yaml_path configs/assembly_historical.yaml \
    --checkpoint tests/test_data/test_checkpoint.json
```

```powershell
$env:SKIP_PREFECT="true"
python -m flows.parsers.backfill_historical_versions --input_path tests/test_data/assembly_test_sample.jsonl --yaml_path configs/assembly_historical.yaml --checkpoint tests/test_data/test_checkpoint.json
```

