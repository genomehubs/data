# GoaT Data Import Pipeline Migration Plan — v2 (current)

> **Status as of this revision:** Phase 1 complete · Phase 2 mostly complete (cleanup tasks remaining) · Phases 3–5 not yet started.
>
> For the original framing and historical context see [v1-initial-plan.md](v1-initial-plan.md).

## TL;DR

Phase 1 (external data fetching) and the bulk of Phase 2 (YAML-backed parsers + validation) are now implemented. Every legacy fetch job from `goat-data/.github/workflows/fetch-resources.yml` has a corresponding Prefect updater, and every source directory that needs transformation has either a parser or a `SKIP_PARSING` assignment plus a YAML config. What remains in Phase 2 is targeted cleanup: confirm parser-vs-skip assignments, ensure every status-list YAML can be validated end-to-end locally, get `blobtk` on the worker PATH, and finalize a couple of YAML schemas. After that, Phase 3 cuts the legacy import over to `s3://goat/validated/`.

## Status at a glance

| Phase                                | State          | Notes                                                                               |
| ------------------------------------ | -------------- | ----------------------------------------------------------------------------------- |
| Phase 1 — External data fetching     | ✅ Complete    | 18 updaters deployed (see inventory below).                                         |
| Phase 2 — Parsers & validation       | 🔄 In progress | 11 parsers + fetch-parse-validate wrapper landed. Cleanup tasks tracked below.      |
| Phase 3 — Switch legacy import       | ⬜ Not started | Gated on Phase 2 cleanup + a parity comparison between `validated/` and `sources/`. |
| Phase 4 — Replace `genomehubs index` | ⬜ Future      | Requires new import code; out of scope for this revision.                           |
| Phase 5 — Full pipeline migration    | ⬜ Future      | Replaces remaining GitHub Actions workflows (release / init / index / fill / test). |

---

## Phase 1 — External Data Fetching (COMPLETE)

Every external fetch job from the legacy workflow now has a Prefect-backed updater that writes raw data to `s3://goat/resources/...` and emits an `update.*.finished` event.

**Updater inventory (`flows/updaters/`):**

- `update_ncbi_datasets.py`
- `update_ncbi_taxonomy.py`
- `update_ena_taxonomy_extra.py`
- `update_genomehubs_taxonomy.py`
- `update_tolid_prefixes.py`
- `update_ott_taxonomy.py`
- `update_tol_portal_status.py`
- `update_tol_genome_notes.py`
- `update_nhm_status_list.py`
- `update_boat_config.py`
- `update_vgp_status.py`
- `update_vgp_original_status.py`
- `update_jgi_status.py`
- `update_ensembl_metadata.py`
- `update_ucsc_assemblies.py`
- `update_sra_data.py`
- `update_blobtoolkit.py`
- `update_refseq_organelles.py`
- `update_google_sheets_status.py`

Shared helpers: `tol_utils.py`, `flows/updaters/api/`, `flows/lib/utils.py`, `flows/lib/shared_args.py`.

All deployments are wired in `flows/prefect.yaml`. There are no remaining Phase 1 items.

---

## Phase 2 — YAML-Backed Parsers & Validation (IN PROGRESS)

### What is in place

**Parsers (`flows/parsers/`):**

| Parser                                  | Handles                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------ |
| `parse_ncbi_assemblies.py`              | NCBI Datasets + data-freeze assembly TSVs.                               |
| `parse_refseq_organelles.py`            | NCBI RefSeq mitochondrion / plastid GenBank → pivoted TSV.               |
| `parse_sequencing_status.py`            | JGI 1KFG (and any other status list whose source format needs pivoting). |
| `parse_blobtoolkit.py`                  | BlobToolKit analysis exports (stub; see cleanup).                        |
| `parse_sra_data.py`                     | SRA metadata TSV.                                                        |
| `parse_genomesize_karyotype.py`         | Generic genomesize / karyotype `FILE_` sources.                          |
| `parse_conservation.py`                 | CITES + conservation sources.                                            |
| `parse_legislation.py`                  | UK legislation FILE\_ sources.                                           |
| `parse_skip_parsing.py`                 | Pass-through for inputs that already match their YAML schema.            |
| `parse_backfill_historical_versions.py` | Historical assembly version backfill.                                    |

Discovery is automatic via `flows/parsers/register.py` (any `parse_*.py` is picked up). `Parser` enum members serialize to lowercase (e.g. `skip_parsing`) but the `PARSERS.parsers` dict is keyed by `Parser.name` (uppercase, e.g. `SKIP_PARSING`).

**Wrappers, validators, and orchestrators:**

- `flows/lib/wrapper_fetch_parse_validate.py` — production fetch → parse → validate → S3 upload pipeline.
- `flows/lib/local_fetch_parse_validate.py` — local equivalent: copies the YAML + TSV into a work directory, runs the parser, runs `validate_file_pair` with `s3_path=None`, and gracefully skips validation if the `blobtk` binary is not on PATH. Handles plain ↔ gz conversion so the input file matches the YAML's `file.name`. Handles the lowercase-enum / uppercase-dict-key mismatch when looking up parsers.
- `flows/lib/validate_file_pair.py` — wraps the `blobtk validate` Rust binary.
- `flows/orchestrators/batch_validate_status_lists.py` — triggered by `update.google.sheets.status.finished`. Iterates every `FILE_*.types.yaml` under `goat-data/sources/status-lists/`, calls the standard `fetch_parse_validate(parser=Parser.SKIP_PARSING, …)` for each TSV present, and reports pass / fail / skip-no-tsv / skip-config-error counts. CLI flags: `--yaml-dir --work-dir --taxdump-path --s3-path --dry-run --min-valid --min-assigned`.

**Other lib modules in current use:** `conditional_import.py`, `fetch_genomehubs_target_list.py`, `fetch_previous_file_pair.py`, `for_each_record.py`, `index_assembly_features.py`, `process_features.py`, `shared_args.py`, `shared_tasks.py`, `utils.py`.

### Parser ↔ source assignment audit (current)

| Source directory                             | Deployment                            | Parser                 |
| -------------------------------------------- | ------------------------------------- | ---------------------- |
| `assembly-data/ncbi_datasets`                | `fpv-ncbi-datasets`                   | `NCBI_ASSEMBLIES`      |
| `assembly-data/data_freeze`                  | `fpv-data-freeze`                     | `NCBI_ASSEMBLIES`      |
| `assembly-data/refseq_organelles`            | `fpv-refseq-organelles`               | `REFSEQ_ORGANELLES`    |
| `assembly-data/ucsc`                         | `fpv-ucsc`                            | `SKIP_PARSING`         |
| `btk/`                                       | `fpv-blobtoolkit`                     | `SKIP_PARSING`         |
| `sra/`                                       | `fpv-sra`                             | `SKIP_PARSING`         |
| `status-lists/vgp` (FILE_VGP_Ordinal_Phase1) | `fpv-vgp`                             | `SKIP_PARSING`         |
| `status-lists/nhm`                           | `fpv-nhm`                             | `SKIP_PARSING`         |
| `status-lists/jgi_1kfg`                      | `fpv-jgi`                             | `SEQUENCING_STATUS`    |
| `status-lists/google_sheets/*`               | `batch-validate-google-sheets-status` | `SKIP_PARSING` (batch) |

Rationale for `SKIP_PARSING` on BTK and UCSC: `blobtk validate` can derive the taxonomy columns from a `taxon_id` column automatically, so no pre-parse transformation is required. The YAML schema is the source of truth.

### Phase 2 cleanup — remaining work

1. **`blobtk` on worker PATH.** Validation currently no-ops locally on the developer machine because the binary is not installed. Add it to the worker image (and document a local install option) so `local_fetch_parse_validate.py` reports real validation outcomes instead of skipping.
2. **`BLOBTOOLKIT` parser placeholder.** `parse_blobtoolkit.py` exists but is a thin pass-through. Decide whether to keep `SKIP_PARSING` permanently for `btk/` (current production setting) or graduate to a real parser once the BTK API export gains structured fields the YAML cannot describe.
3. **`GENOMESIZE_KARYOTYPE` schema confirmation.** `parse_genomesize_karyotype.py` is generic, but a handful of `FILE_` sources still need their YAMLs cross-checked against the parser's column expectations. Walk every YAML under `goat-data/sources/genomesize-karyotype/` and run `local_fetch_parse_validate.py` once per file.
4. **JGI YAML.** Confirm `sources/status-lists/jgi_1kfg/jgi_1kfg.types.yaml` matches the columns emitted by `update_jgi_status` after the OAuth pagination rewrite.
5. **End-to-end parity check.** Run the batch validator (`batch_validate_status_lists.py`) over all current `status-lists/` YAMLs locally and record the pass / fail / skip rates. Fix anything that fails before Phase 3.

### Verification (Phase 2)

1. `python -m flows.lib.local_fetch_parse_validate --yaml … --tsv …` returns exit 0 for every (parser, source) pair in the table above.
2. `python -m flows.orchestrators.batch_validate_status_lists --dry-run` lists every `FILE_*.types.yaml` under `status-lists/` with the expected parser assignment.
3. `prefect deploy --prefect-file flows/prefect.yaml --all` succeeds and the trigger for `batch-validate-google-sheets-status` shows `update.google.sheets.status.finished`.

---

## Phase 3 — Switch Legacy Import to Validated Data (NOT STARTED)

Unchanged from v1. Recap:

1. Confirm parity between `s3://goat/validated/` and `s3://goat/sources/` per directory.
2. Update `goat-data/.github/workflows/genomehubs-index.yml` to read from `validated/`.
3. Disable fetch jobs in `goat-data/.github/workflows/fetch-resources.yml` and skip them from `s3_release.yml`.
4. Test release; compare ES indices, API tests, UI tests.
5. Staged rollout: assembly-data first, then status-lists, then the rest.

Rollback path: revert the S3 path in the workflow — `sources/` and `resources/` remain intact.

## Phase 4 — Replace Legacy Import (FUTURE)

Unchanged from v1. Requires the new import code (skips re-validation/lookup, reads validated TSV/YAML pairs directly into Elasticsearch).

## Phase 5 — Full Pipeline Migration (FUTURE)

Unchanged from v1. Move ES init, indexing, fill, test, and release promotion out of GitHub Actions into Prefect.

---

## Implemented surface area (snapshot)

- **Parsers:** 11 (see Phase 2 table).
- **Updaters:** 19 (Phase 1 inventory).
- **Orchestrators:** 5 — `batch_validate_status_lists`, `tasks`, `tol_data_pipeline`, `tol_genome_notes_orchestration`, `tol_portal_status_orchestration`.
- **Lib modules:** 12 — `conditional_import`, `fetch_genomehubs_target_list`, `fetch_previous_file_pair`, `for_each_record`, `index_assembly_features`, `local_fetch_parse_validate`, `process_features`, `shared_args`, `shared_tasks`, `utils`, `validate_file_pair`, `wrapper_fetch_parse_validate`.
- **Deployments in `flows/prefect.yaml`:** 34 (including the new `batch-validate-google-sheets-status`).

## Reference material carried forward from v1

The following sections of [v1-initial-plan.md](v1-initial-plan.md) remain authoritative and have not been duplicated here:

- **Gap analysis** — historical record of which legacy jobs needed updaters. Now fully implemented.
- **Network robustness review** — `safe_get()` hardening guidance, per-source timeout table, paginated-API partial-failure handling, idempotency / freshness checks, S3 upload atomicity, connection pooling, DNS / TLS handling.
- **Logging review** — `log_progress()` helper proposal, network-call summaries, output-file summaries, event-emission logging, exception context, Docker orchestrator logging.
- **Conventions reference** — YAML/TSV pair conventions, repo coding conventions, list of legacy code bug risks to avoid.

These are general-purpose engineering guidance and apply to any future updater or parser work.

---

## Change log

**v1 → v2 (this revision):**

- Marked Phase 1 complete; replaced the "missing updaters" table with the implemented inventory.
- Marked Phase 2 mostly complete; added the parser-vs-source assignment audit table.
- Added the **Phase 2 cleanup** section enumerating the remaining items (blobtk PATH, BLOBTOOLKIT parser decision, GENOMESIZE_KARYOTYPE schema sweep, JGI YAML, end-to-end parity).
- Documented `flows/lib/local_fetch_parse_validate.py` and `flows/orchestrators/batch_validate_status_lists.py` (both new since v1).
- Recorded the BTK and UCSC `SKIP_PARSING` decision (auto-taxonomy in `blobtk validate`).
- Recorded the VGP YAML correction (`FILE_VGP_Ordinal_Phase1.types.yaml`).
- Phases 3–5 unchanged.
- Network-robustness, logging, and conventions sections kept in v1 by reference rather than duplicated.
