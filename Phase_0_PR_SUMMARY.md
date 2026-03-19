# Pull Request: Historical Assembly Version Backfill (Phase 0)

## Summary

One-time backfill process to populate historical (superseded) assembly versions
from NCBI for all existing assemblies with version > 1.  Enables version-aware
milestone tracking by capturing previously untracked superseded versions.

## What Changed (latest revision)

### Bug fixes
- **Fixed data-loss bug**: `write_to_tsv` overwrites by default, so the
  original per-batch write + `parsed = {}` clearing lost all but the last
  batch.  All rows now accumulate in memory and `write_to_tsv` is called
  exactly once at the end.  Checkpoints only record resume progress.

### Structural changes
- **Renamed** `backfill_historical_versions.py` →
  `parse_backfill_historical_versions.py` so `register.py` discovers it as a
  plugin via the `parse_` prefix convention.
- **Changed orchestrator decorator** from `@task` to `@flow(log_prints=True)`
  to match how `parse_ncbi_assemblies` structures its flow/task hierarchy.
- **Split `find_all_assembly_versions`** into `discover_version_accessions`
  (FTP) + `fetch_version_metadata` (datasets CLI) for modularity and
  independent testability.
- **Added `backfill_historical_versions_wrapper`** matching the
  `fetch_parse_validate` parser signature so the flow integrates with the
  Prefect pipeline.

### Convention alignment
- **CLI arguments** now use `shared_args` exclusively (`INPUT_PATH`,
  `YAML_PATH`, `WORK_DIR`); removed ad-hoc `--checkpoint` argument.
  Checkpoint path is derived via `derive_checkpoint_path`.
- **Replaced `subprocess.run`** with `utils.run_quoted` for shell-safe
  argument quoting (consistent with `parse_ncbi_assemblies`).
- **Code style** aligned with GenomeHubs conventions: Google-style docstrings,
  lowercase type hints (`dict`, `list`, `tuple`), `e` for exception variables,
  removed shebang and section banners.
- **`assembly_historical.yaml`**: moved `needs` under `file:` section and
  references `ATTR_assembly.types.yaml` (matches `ncbi_datasets_eukaryota`
  convention).

### Test suite rewrite
- Rewrote `tests/test_backfill.py` using pytest (was a custom runner).
- **33 tests**, all passing, covering:
  - Accession parsing helpers
  - Assembly identification from JSONL fixture
  - Cache round-trip with expiry
  - Checkpoint save/load/derive
  - Accession format validation
  - `parse_historical_version` delegation (mocked)
  - Orchestrator: single TSV write, multi-assembly accumulation,
    current-version skipping, no-op for v1-only input
  - **Regression test for the batch-overwrite data-loss bug**

## Solution Overview

The backfill script:
1. Identifies assemblies with version > 1 from the input JSONL
2. Discovers all historical versions via NCBI FTP directory listing
3. Fetches metadata for each version using NCBI Datasets CLI
4. Parses each version through `process_assembly_report` with
   `version_status="superseded"`
5. Writes all accumulated rows to a single TSV via `write_to_tsv`

### Smart 2-Tier Caching
- **Version discovery cache** (7-day expiry): FTP directory listings
- **Metadata cache** (30-day expiry): Datasets CLI responses
- Reduces reruns from hours to minutes

### Checkpoint System
- Saves progress every 100 assemblies to `{work_dir}/checkpoints/`
- Allows resuming after interruptions
- Does **not** trigger intermediate TSV writes (avoids the overwrite bug)

## Files

### New
- `flows/parsers/parse_backfill_historical_versions.py` — Main backfill flow
- `configs/assembly_historical.yaml` — Output schema configuration
- `tests/test_backfill.py` — pytest suite (33 tests)
- `tests/test_data/assembly_test_sample.jsonl` — Test fixture (3 assemblies)

### Modified
- `flows/parsers/parse_ncbi_assemblies.py` — Added `version_status` parameter

## Usage

### As a standalone CLI
```bash
python -m flows.parsers.parse_backfill_historical_versions \
    --input_path data/assembly_data_report.jsonl \
    --yaml_path configs/assembly_historical.yaml \
    --work_dir tmp
```

### Via Prefect pipeline
Discovered automatically by `register.py` and invoked through
`fetch_parse_validate` with the standard parser signature.

### Expected performance
- **First run**: ~10–15 hours (~3,700 assemblies, ~8,500 versions)
- **Subsequent runs**: ~15–30 minutes (cache hits)

### Running tests
```bash
set SKIP_PREFECT=true
python -m pytest tests/test_backfill.py -v
```

## Next Steps (after merge)

1. Run full backfill on production dataset
2. Upload output to S3
3. Implement Phase 1: incremental daily updates for new historical versions
4. Implement Phase 2: version-aware milestone tracking
