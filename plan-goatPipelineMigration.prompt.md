# GoaT Data Import Pipeline Migration Plan

## TL;DR
Migrate all data fetching from the legacy `goat-data` GitHub Actions workflow to scheduled Prefect-backed updaters in the `data` repo, then wire up parsers and validators to produce import-ready TSV/YAML pairs on S3. Five phases: fetch (Phase 1), parse+validate (Phase 2), switch S3 source (Phase 3), replace import (Phase 4), full pipeline (Phase 5).

---

## Gap Analysis: Updater Coverage

### Already Implemented (10 updaters)
| Updater | Legacy Equivalent | Schedule |
|---------|------------------|----------|
| `update_ncbi_datasets` | fetch-ncbi-datasets-zip | Daily |
| `update_ncbi_taxonomy` | taxdump download | Weekly |
| `update_ena_taxonomy_extra` | ENA taxonomy API | Weekly |
| `update_genomehubs_taxonomy` | fetch-genomehubs-taxonomy | Daily |
| `update_tolid_prefixes` | ToLID GitLab fetch | Weekly |
| `update_ott_taxonomy` | OTT download | Monthly |
| `update_tol_portal_status` | STS API (replaced) | Daily (orchestrated) |
| `update_tol_genome_notes` | (new source) | Daily (orchestrated) |
| `update_nhm_status_list` | NHM Data Portal API | Weekly |
| `update_boat_config` | GoaT API + Lustre | Daily |

### Missing — Need New Updaters (8 categories, ~11 updaters)

| # | Source | Legacy Job | Priority | Schedule | Complexity |
|---|--------|-----------|----------|----------|------------|
| 1 | **BlobToolKit** | fetch-blobtoolkit (Docker `genomehubs parse --btk`) | HIGH | Daily | Medium — API pagination + Docker |
| 2 | **RefSeq Organelles** | fetch-refseq-organelles (FTP + BioPython) | HIGH | Weekly | Medium — FTP + GenBank parsing |
| 3 | **VGP Status** | fetch-from-apis (GitHub YAML) | MEDIUM | Weekly | Low — simple HTTP + YAML parse |
| 4 | **JGI 1KFG** | fetch-from-apis (OAuth REST) | MEDIUM | Weekly | Medium — OAuth token exchange |
| 5 | **Ensembl Metadata** (×6) | fetch-assembly-links (6 JSON endpoints) | MEDIUM | Monthly | Low — HTTP + JSON→TSV, one parameterized updater |
| 6 | **UCSC Assembly Hubs** | fetch-assembly-links | LOW | Monthly | Low — HTTP + text parsing |
| 7 | **Google Sheets Status** (~20+ projects) | fetch-from-apis (R + Python) | HIGH | Weekly | High — rewrite R→Python, normalize tables |
| 8 | **SRA Data** | (parse_sra_data.py) | MEDIUM | Weekly | Medium — NCBI API + XML parsing |

### Static/Semi-Static Sources (no external fetch needed)
These exist as curated YAML/TSV pairs in `goat-data/sources/` and are uploaded directly to S3:
- **Genomesize/Karyotype** — 25 FILE_ sources (genome size databases, chromosome counts)
- **Conservation** — CITES index (periodically updated manually)
- **UK Legislation** — 9 FILE_ sources (very static)
- **Regional Lists** — 7 FILE_ sources (static geographic lists)
- **Lineages** — ODB10 lineage mappings
- **OTT IDs** — OTT taxonomy mappings
- **ToLIDs** — Tree of Life ID naming

These should be synced to S3 via a simple `sync_static_sources` utility or manually, not via updaters.

---

## Phase 1: External Data Fetching

### Goal
All external data fetching implemented as Prefect updaters with scheduled deployments, uploading raw data to S3 and emitting events for downstream parsing.

### Steps

#### Group A: API-Based Updaters (parallel development)

**Step 1: `update_vgp_status` — VGP Status List**
- Fetch GitHub YAML from `https://raw.githubusercontent.com/vgl-hub/genome-portal/master/_data/table_tracker.yml`
- Parse YAML, extract fields: common_name, family, order, scientific_name, status, taxon_id, vgp_phase
- Write TSV to `s3://goat/resources/status-lists/vgp.tsv`
- Schedule: Weekly
- Reuse: `safe_get()` from `flows/lib/utils.py`, `parse_args/shared_args` pattern
- Reference: `goat-data/scripts/api/api_config.py` VGL handlers

**Step 2: `update_jgi_status` — JGI 1KFG**
- OAuth token exchange: offline_token → access_token via `https://signon.jgi.doe.gov/signon/create`
- Paginated API: `https://gold-ws.jgi.doe.gov/projects?studyGoldId=Gs0000001`
- Write TSV to `s3://goat/resources/status-lists/jgi_1kfg.tsv`
- Schedule: Weekly
- Requires: `JGI_OFFLINE_TOKEN` secret (Prefect Secret block or env var)
- Reference: `goat-data/scripts/jgi_to_tsv.py`
- Bug risk: Legacy code has fragile OAuth flow — add proper token refresh and expiry handling

**Step 3: `update_ensembl_metadata` — Ensembl Species Metadata (6 databases)**
- Single parameterized updater deployed 6 times with different division parameters
- Divisions: Fungi, Metazoa, Plants, Protists, Vertebrates, Rapid Release
- Fetch JSON from Ensembl REST API endpoints
- Transform JSON→TSV (replace legacy `jq` one-liners with explicit Python)
- Write to `s3://goat/resources/assembly-data/species_metadata_Ensembl{Division}.tsv.gz`
- Schedule: Monthly
- Reference: `goat-data/.github/workflows/fetch-resources.yml` fetch-assembly-links job

**Step 4: `update_ucsc_assemblies` — UCSC Genome Browser**
- Fetch assembly hub list from UCSC API
- Parse to TSV
- Write to `s3://goat/resources/assembly-data/ucsc_ids.tsv`
- Schedule: Monthly
- Reuse: `safe_get()`, standard arg parsing

**Step 5: `update_sra_data` — SRA Metadata**
- Fetch from NCBI SRA API (Entrez or BigQuery)
- Parse XML/JSON responses to TSV
- Write to `s3://goat/resources/sra/sra.tsv.gz`
- Schedule: Weekly
- Reference: `goat-data/scripts/parse_sra_data.py`
- Bug risk: Legacy script has hardcoded batch sizes and silent error swallowing

#### Group B: Complex Updaters (sequential, more effort)

**Step 6: `update_blobtoolkit` — BlobToolKit Analysis Data**
- Approach A (preferred): Direct API fetch from `https://blobtoolkit.genomehubs.org/api/v1/search/Eukaryota` + per-assembly detail queries
- Approach B: Docker-isolated `genomehubs parse --btk` via orchestrator pattern (like tol_genome_notes)
- Outputs: `btk.tsv.gz` + `btk.files.yaml` to `s3://goat/resources/btk/`
- Schedule: Daily
- Reference: `goat-data/scripts/parse_blobtoolkit.py`
- Bug risk: Legacy has `print(plots)` debug line left in (line 66); pagination may miss entries

**Step 7: `update_refseq_organelles` — RefSeq Organelle Data**
- Fetch from NCBI FTP: `ftp.ncbi.nlm.nih.gov/refseq/release/`
- Parse GenBank flat files for mitochondrion/plastid sequences
- Extract: accession, taxon_id, organism, sequence_length, references
- Write to `s3://goat/resources/assembly-data/refseq_organelles.tsv.gz`
- Schedule: Weekly
- Reference: `goat-data/scripts/parse_refseq_organelles.py` (uses BioPython)
- Consideration: BioPython dependency may need Docker isolation (check pydantic conflicts)

**Step 8: `update_google_sheets_status` — Google Sheets Project Status Lists**
- Rewrite R script (`get_googlesheets.R`) entirely in Python
- Fetch TSVs from public Google Sheets URLs (no auth needed for public sheets)
- Use `import_status_lib.py` patterns for table normalization but rewrite cleanly:
  - Replace pandas one-liners with explicit column mapping
  - Handle encoding robustly (UTF-8 with fallback)
  - Normalize species names, taxon IDs
- Projects list parameterized (deploy once per project group or batch)
- Outputs: One TSV per project to `s3://goat/resources/status-lists/{project}_expanded.tsv`
- Schedule: Weekly
- Sub-steps:
  - 8a: Core fetcher function (reusable across all sheets)
  - 8b: Table normalizer (species name cleaning, status field mapping)
  - 8c: Per-project configuration (sheet URLs, field mappings, column renames)
  - 8d: Deploy as single flow with project list parameter
- Reference: `goat-data/scripts/import_status_lib.py`, `goat-data/scripts/import_status.py`
- Bug risks in legacy:
  - Code duplication (import_status_lib.py copied to ebp_import/)
  - Silent encoding failures
  - Hardcoded 24-project list
  - Pandas operations that silently drop data on merge conflicts

#### Group C: Infrastructure & Static Data

**Step 9: `sync_static_sources` — Static YAML/TSV pairs**
- Utility to upload curated YAML/TSV pairs from goat-data/sources/ to S3
- Not a scheduled updater — run manually or on goat-data repo changes
- Covers: genomesize-karyotype, conservation, uk-legislation, regional-lists, lineages
- Could be triggered by a webhook on goat-data repo pushes

**Step 10: Secrets & Configuration**
- Configure Prefect Secret blocks for: `JGI_OFFLINE_TOKEN`, Google Sheets URLs
- STS_AUTHORIZATION_KEY no longer needed (replaced by tol-sdk)
- Add deployment entries to `flows/prefect.yaml` for all new updaters

### Relevant Files (Phase 1)

**New files to create:**
- `flows/updaters/update_vgp_status.py`
- `flows/updaters/update_jgi_status.py`
- `flows/updaters/update_ensembl_metadata.py`
- `flows/updaters/update_ucsc_assemblies.py`
- `flows/updaters/update_sra_data.py`
- `flows/updaters/update_blobtoolkit.py`
- `flows/updaters/update_refseq_organelles.py`
- `flows/updaters/update_google_sheets_status.py`
- `flows/lib/google_sheets.py` (shared Google Sheets fetching utilities)
- `flows/lib/api_helpers.py` (shared API helpers: OAuth, pagination, JSON→TSV)

**Existing files to modify:**
- `flows/prefect.yaml` — add deployments for all new updaters
- `flows/lib/utils.py` — add any missing shared utilities
- `flows/lib/shared_args.py` — add new argument definitions if needed
- `requirements.txt` — add BioPython if needed for RefSeq parsing

**Reference files (goat-data, read-only):**
- `goat-data/scripts/api/api_config.py` — API endpoint definitions
- `goat-data/scripts/api/api_tools.py` — retry/pagination patterns
- `goat-data/scripts/jgi_to_tsv.py` — JGI OAuth flow
- `goat-data/scripts/parse_blobtoolkit.py` — BTK API parsing
- `goat-data/scripts/parse_refseq_organelles.py` — GenBank parsing
- `goat-data/scripts/parse_sra_data.py` — SRA parsing
- `goat-data/scripts/import_status_lib.py` — table normalization
- `goat-data/scripts/get_googlesheets.R` — Google Sheets URLs
- `goat-data/.github/workflows/fetch-resources.yml` — complete fetch workflow

### Verification (Phase 1)
1. Each updater runs locally with `SKIP_PREFECT=true` and produces valid output TSV
2. Output TSV format matches goat-data legacy output (diff comparison where possible)
3. S3 upload succeeds to `s3://goat/resources/` paths
4. Events emitted with correct resource types for downstream triggering
5. All tests pass: `python -m pytest tests/`
6. No secret values hardcoded; all auth via env vars or Prefect Secret blocks
7. `prefect deploy --prefect-file flows/prefect.yaml --all` succeeds

### Decisions (Phase 1)
- **Google Sheets**: Rewrite in Python (not R) for consistency with the rest of the codebase
- **BlobToolKit**: Prefer direct API approach over Docker genomehubs parse (simpler, avoids Docker-in-Docker); fall back to orchestrator pattern if API is insufficient
- **RefSeq Organelles**: Use BioPython in Docker container if pydantic conflicts arise
- **Static sources**: Not updaters — sync utility or manual upload
- **STS replaced by ToL Portal**: No migration needed (already done via `update_tol_portal_status`)

---

## Phase 2: YAML-Backed Parsers & Validation

### Goal
All data sources processed by fetch-parse-validate pipeline. Parsing triggered by update events. Validated TSV/YAML pairs uploaded to new S3 directories (`s3://goat/validated/`).

### Steps

**Step 1: Implement `parse_sequencing_status` parser**
- Handle all status list TSV formats (VGP, JGI, Google Sheets projects, NHM, ToL Portal)
- Config-driven: read YAML to determine column mappings
- Reuse `Config` class from `flows/lib/utils.py`
- One parser handles all ~65 status list YAML configs

**Step 2: Implement `parse_refseq_organelles` parser**
- Replace stub with working implementation
- Read YAML config, apply field mappings from `refseq_organelles.types.yaml`
- Validate organelle accessions, taxonomy

**Step 3: Implement `parse_blobtoolkit` parser**
- Parse BTK TSV using YAML config from `btk.types.yaml`
- Handle BUSCO stats, base composition, read mapping fields

**Step 4: Implement `parse_ensembl_metadata` parser**
- Handle all 6 Ensembl division TSVs
- Single generic parser, config-driven via YAML

**Step 5: Implement `parse_sra_data` parser**
- Parse SRA TSV with YAML config from `sra.types.yaml`

**Step 6: Implement `parse_genomesize_karyotype` parser**
- Handle the 25+ genomesize/karyotype FILE_ sources
- Generic parser for simple TSV→validated TSV transformation

**Step 7: Implement `parse_conservation` and `parse_legislation` parsers**
- Static data validation parsers
- Check CITES categories, legislation references against YAML constraints

**Step 8: Wire all fetch-parse-validate deployments**
- Add trigger entries in `prefect.yaml` for each parser
- Events from Phase 1 updaters trigger corresponding parse-validate flows
- `validate_file_pair()` runs `blobtk validate` on each output
- Gate S3 upload on validation success

**Step 9: Configure S3 output paths**
- Validated outputs go to `s3://goat/validated/{directory}/` (NOT `s3://goat/resources/` or `s3://goat/sources/`)
- Both validated TSV and validated YAML uploaded
- Validation report (JSONL) uploaded alongside for audit

### Relevant Files (Phase 2)
- `flows/parsers/parse_sequencing_status.py` — complete implementation
- `flows/parsers/parse_refseq_organelles.py` — replace stub
- `flows/parsers/parse_blobtoolkit.py` — new
- `flows/parsers/parse_ensembl_metadata.py` — new
- `flows/parsers/parse_sra_data.py` — new
- `flows/parsers/parse_genomesize_karyotype.py` — new (generic)
- `flows/parsers/parse_conservation.py` — new
- `flows/orchestration/wrapper_fetch_parse_validate.py` — existing, may need updates
- `flows/validators/validate_file_pair.py` — existing, may need S3 path updates
- `flows/prefect.yaml` — add trigger entries
- Local copies of YAML configs from `goat-data/sources/` for development

### Verification (Phase 2)
1. Each parser produces TSV matching the YAML config headers
2. `blobtk validate -g <yaml>` passes for each output with ≥95% valid rows
3. Event chain works: updater → parse → validate → S3 upload
4. Validated files appear in `s3://goat/validated/` directories
5. Row counts comparable to legacy pipeline output
6. No data loss: compare parsed row counts against raw input counts

### Decisions (Phase 2)
- **S3 validated path**: `s3://goat/validated/` (separate from `resources/` and `sources/`)
- **Parser reuse**: `parse_sequencing_status` handles ALL status list formats via YAML config
- **Parser reuse**: `parse_genomesize_karyotype` handles ALL genomesize/karyotype sources generically
- **YAML configs**: Develop with local copies, production fetches from goat-data sources/
- **Scope boundary**: Phase 2 does NOT change the legacy import at all

---

## Phase 3: Switch Legacy Import to Validated Data

### Goal
Legacy import workflow reads from `s3://goat/validated/` instead of `s3://goat/resources/` or `s3://goat/sources/`, removing all fetch steps from the import.

### Steps
1. Verify data parity: compare `s3://goat/validated/` against `s3://goat/sources/` for all directories
2. Update `goat-data/.github/workflows/genomehubs-index.yml` to read from `s3://goat/validated/`
3. Remove fetch jobs from `goat-data/.github/workflows/fetch-resources.yml` (or disable)
4. Update `goat-data/.github/workflows/s3_release.yml` to skip fetch-resources
5. Run test release with validated data; compare with latest production release
6. Staged rollout: switch one directory at a time, verify, proceed

### Verification (Phase 3)
1. Test release produces identical (or improved) Elasticsearch indices
2. API test suite passes
3. UI test suite passes
4. Row counts match or exceed previous release
5. Rollback path confirmed: can revert to `s3://goat/sources/` if issues

### Risk Mitigation
- Keep `s3://goat/sources/` and `s3://goat/resources/` intact as rollback
- Phase 3 changes only S3 paths in workflow config, easily reversible
- Switch one source directory at a time (assembly-data first, then status-lists, etc.)

---

## Phase 4: Replace Legacy Import (Future)

### Goal
Replace `genomehubs index` with updated import code that reads validated TSV/YAML pairs directly.

### Scope
- Requires new import code not yet available
- Skip validation/lookup steps (already done in Phase 2)
- Direct TSV→Elasticsearch indexing

---

## Phase 5: Full Pipeline Migration (Future)

### Goal
Remove all GitHub Actions workflow dependencies; full pipeline runs in Prefect.

### Scope
- Yet to be defined
- Includes: ES init, indexing, fill, test, release promotion
- Replaces: s3_release.yml, genomehubs-init.yml, genomehubs-index.yml, genomehubs-fill.yml, genomehubs-test.yml

---

## Conventions Reference

### YAML/TSV Pair Convention (goat-data)
- **Prefix patterns**: `ATTR_` (attribute defs), `TAXON_` (taxonomy), `FILE_` (data sources), unprefixed (primary)
- **YAML structure**: `file:` metadata, `attributes:` field mappings, `taxonomy:` taxon matching, `identifiers:` ID columns
- **`needs:`** directive: lists dependent YAML files that must be co-located
- **TSV naming**: matches `file.name` in YAML config, often `.gz` compressed

### Data Repo Code Conventions
- Absolute imports: `from flows.lib import utils`
- Google-style docstrings with type hints
- `SKIP_PREFECT=true` for local testing
- `run_quoted()` for subprocess (never `shell=True`)
- `safe_get()` for HTTP requests
- `parse_args()` with `shared_args` constants for CLI
- Tasks: focused, idempotent, with `@task(retries=N, log_prints=True)`
- Events: `emit_event()` with `prefect.resource.id/type/matches.previous`
- Black formatter, 88-char line length

### Legacy Code Bug Risks to Avoid
1. `parse_blobtoolkit.py` line 66: debug `print(plots)` left in production
2. `import_status_lib.py`: duplicated across directories, encoding silently fails
3. `fetch-or-fallback.sh`: `|| exit 0` masks real errors
4. Google Sheets: hardcoded `gid` parameters break on URL changes
5. JGI OAuth: no token refresh/expiry handling
6. NCBI API: hardcoded 30s timeouts, silent failure on rate limit
7. Pandas merge conflicts silently drop data in status list processing
