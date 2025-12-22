# Pull Request: Historical Assembly Version Backfill (Phase 0)

## Summary

This PR implements a one-time backfill process to populate historical assembly versions for all existing assemblies in the genomehubs dataset. It enables version-aware milestone tracking by capturing superseded assembly versions that were previously not tracked.

## Solution Overview

Implements a **one-time backfill script** that:
1. Identifies assemblies with version > 1 (indicating historical versions exist)
2. Discovers all historical versions via NCBI FTP
3. Fetches metadata for each historical version using NCBI Datasets CLI
4. Outputs historical versions to a separate TSV file with `versionStatus = "superseded"`
5. Uses smart caching to avoid re-fetching data on reruns

## Key Features

### 1. FTP-Based Version Discovery
- Queries NCBI FTP to find all versions of each assembly
- More reliable than API for historical data
- Based on proven DToL prototype implementation

### 2. Reuses existing Parser
- Calls `parse_ncbi_assemblies.process_assembly_report()` with `version_status="superseded"`
- Fetches sequence reports and computes metrics identically to current assemblies
- Uses same YAML config system
- Generates identical TSV schema (plus `versionStatus` field)

### 3. Smart 2-Tier Caching
- **Version discovery cache** (7-day expiry): Stores which versions exist
- **Metadata cache** (30-day expiry): Stores assembly metadata from Datasets CLI
- Dramatically reduces runtime on reruns (from hours to minutes)

### 4. Checkpoint System
- Saves progress every 100 assemblies
- Allows resuming after interruptions
- Critical for processing ~3,694 assemblies

## Files Modified/Created

### New Files
- `flows/parsers/backfill_historical_versions.py` - Main backfill script
- `configs/assembly_historical.yaml` - Output schema configuration
- `tests/test_backfill.py` - Automated test suite
- `tests/test_data/assembly_test_sample.jsonl` - Test dataset (3 assemblies)
- `tests/README_test_backfill.md` - Testing documentation

### Modified Files
- `flows/parsers/parse_ncbi_assemblies.py` - Added `version_status` parameter
- `flows/lib/utils.py` - Added `parse_s3_file` stub function

## Schema Changes

### New Field in Output
- Same schema as current assemblies, plus `versionStatus` column
- `versionStatus` - Indicates if assembly is "current" or "superseded"

### Expected Results
- **Input**: ~3,694 assemblies with version > 1
- **Output**: ~8,500 historical version records
- **Runtime**: ~10-15 hours (first run), ~15-30 minutes (with cache)

## Testing

### Automated Tests (4/4 passing)
```bash
python tests/test_backfill.py
```

Tests verify:
1. ✅ Accession parsing
2. ✅ Assembly identification
3. ✅ FTP version discovery
4. ✅ Cache functionality

### Manual Test (3 assemblies)
```bash
python flows/parsers/backfill_historical_versions.py \
    --input tests/test_data/assembly_test_sample.jsonl \
    --config configs/assembly_historical.yaml \
    --checkpoint tests/test_data/test_checkpoint.json
```

## Usage

### One-Time Backfill
```bash
python flows/parsers/backfill_historical_versions.py \
    --input flows/parsers/eukaryota/ncbi_dataset/data/assembly_data_report.jsonl \
    --config configs/assembly_historical.yaml \
    --checkpoint backfill_checkpoint.json
```
On first run:
- Takes ~10-15 hours (fetches all historical versions)
- Creates tmp/backfill_cache/ directory
- Checkpoints every 100 assemblies
- Safe to Ctrl+C and resume

On subsequent runs (after input update):
- Takes ~15-30 minutes (uses cache for existing)
- Only fetches NEW assemblies
- Cache expires: version discovery (7 days), metadata (30 days)

Output:
- outputs/assembly_historical.tsv (all superseded versions)
- Each row has version_status="superseded"
- Includes all sequence reports and metrics

## Important Notes

### 1. Stub Function Warning
The `parse_s3_file()` function in `flows/lib/utils.py` is currently a stub:

```python
def parse_s3_file(s3_path: str) -> dict:
    return {}  # Placeholder
```

**Action needed**: If Rich has a real implementation, replace this stub. The function is imported by `parse_ncbi_assemblies.py` but not used by the backfill script.

### 2. One-Time Process
This backfill is designed to run **once** to populate historical data. Future updates should be handled by the incremental daily pipeline.

### 3. Cache Directory
The cache directory `tmp/backfill_cache/` can be safely deleted after successful completion. It contains:
- Version discovery data (FTP queries)
- Assembly metadata (Datasets CLI responses)

## Next Steps (After PR Merge)

1. **Run full backfill** on production dataset (~10-15 hours)
2. **Upload output** to appropriate S3 location
3. **Implement Phase 1**: Incremental daily updates for new historical versions
4. **Implement Phase 2**: Version-aware milestone tracking

## Questions for Reviewer

1. Is the `parse_s3_file()` stub acceptable, or do you have a real implementation to use?
2. Should historical versions go to a different S3 path than current assemblies?
3. Any preferences for checkpoint file location/naming?

