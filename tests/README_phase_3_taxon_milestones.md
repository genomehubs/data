# Phase 3: Taxon Milestone Computation

This guide covers the Phase 3 implementation, which computes per-taxon assembly
**milestone firsts** from the version-tracked assembly data produced by Phases
0–2 and writes `taxon_milestone_summary.tsv`.

Phase 3 is **two modules**:

- **`flows/lib/load_taxonomy.py`** (*dev/test only*) — parses a local NCBI
  taxdump (`nodes.dmp` + `names.dmp`) into the taxonomy contract. In production
  the lineage is attached to assembly rows upstream (NCBI-dataset integration),
  so this module is used only to run the pipeline end-to-end without that join.
- **`flows/lib/compute_taxon_milestones.py`** — a single chronological sweep
  that computes the two milestone deliverables — **species milestones** and
  **higher-rank milestones** — then writes `taxon_milestone_summary.tsv`. The
  **first-in-ranks** flags are not a third computation: they are a derived
  species-level annotation, emitted as a by-product of the higher-rank step
  (see below).

Phase 0 (one-time backfill) and Phase 1 (daily incremental) must have run at
least once so that `assembly_current.tsv` and `assembly_historical.tsv` exist
before running Phase 3. Phase 2 and Phase 3 are **independent** — both trigger
from `update.assembly_versions.completed` and run in parallel; neither consumes
the other's output.

## Files added in Phase 3

| File | Purpose |
|---|---|
| `flows/lib/load_taxonomy.py` | Pure NCBI-taxdump parser (no download, no cache); builds the taxonomy contract for the dev/test path |
| `flows/lib/compute_taxon_milestones.py` | Single-sweep milestone computation → `taxon_milestone_summary.tsv`; emits `compute.taxon_milestones.completed` |
| `tests/test_taxon_milestones.py` | Unit + end-to-end tests for both modules |
| `test/taxonomy/ncbi/` | Test taxdump fixture (`nodes.dmp` + `names.dmp`) for the *Isopoda* clade |

## How it fits in the daily pipeline

```
parse_assembly_versions     (writes assembly_historical.tsv; emits no event)
  → update_assembly_versions   →  update.assembly_versions.completed
       ├──► generate_assembly_summary   →  generate.assembly_summary.completed   [Phase 2]
       └──► compute_taxon_milestones    →  compute.taxon_milestones.completed     [Phase 3]
            (production: lineage attached upstream / supplied as the contract;
             dev/test:   load_taxonomy parses test/taxonomy/ncbi)
```

The trigger event `update.assembly_versions.completed` is emitted by
`update_assembly_versions.py` (the missing-version updater), **not** by
`parse_assembly_versions.py` (which writes the historical TSV but emits no
event). The updater emits the event on **every** run — including a `no_op`
emit when there are no missing versions to fetch — so Phase 2 and Phase 3 fan
out from it daily, in parallel, regardless of whether a backfill occurred.

## The taxonomy contract

Both environments feed the same in-memory structure; downstream code never
branches on the source:

```python
CANONICAL_RANKS = {'genus', 'family', 'order', 'class', 'phylum', 'kingdom'}

{
  taxid: {
    'scientific_name': str,
    'rank': str,                    # 'species', 'genus', 'subspecies', ...
    'parent': int,                  # one level up — used by resolve_to_species()
    'lineage': {rank: taxid, ...}   # canonical ranks only, genus..kingdom
  },
  ...
}
```

`scientific_name` and `parent` are present for **higher-rank** taxids too, not
just species — higher-rank output rows carry the rank taxon's own name, and
`resolve_to_species` walks `parent` to attribute subspecies to their species.

`load_taxonomy.build_lineage` walks each node's parent chain up to the root
(taxid 1), recording the taxid at each canonical rank and skipping the
non-canonical intermediate ranks NCBI inserts (subphylum, superclass, suborder,
clade, …).

## The four milestones

All four are predicates over the **same** assembly rows on the **same**
`releaseDate` timeline:

| Milestone | Date column (+`_accession`) | Predicate on a row |
|---|---|---|
| Any assembly | `first_assembly_date` | always true |
| EBP-affiliated assembly | `first_ebp_assembly_date` | `PRJNA533106` in `bioProjectAccession.split(',')` |
| Meets EBP metric (any submitter) | `first_metric_date` | `ebpStandardDate` non-empty |
| Meets metric **and** affiliated | `first_ebp_metric_date` | `ebpStandardDate` non-empty **AND** `PRJNA533106`-affiliated |

**Naming convention (locked):** the `ebp_` prefix means *EBP-affiliated
submitter* (under the `PRJNA533106` umbrella project); `metric` means *meets the
EBP quality standard*, regardless of who submitted.

> `ebpStandardDate` is set equal to a version's own `releaseDate` when EBP
> criteria pass (`check_ebp_criteria` in `flows/lib/utils.py`), so all four
> milestones share the `releaseDate` timeline. `first_ebp_metric_date` differs
> from `first_metric_date` only by the affiliation filter.
>
> Note on real data: `ebpStandardDate` and `releaseDate` may appear as the
> literal string `"None"` (not just empty) — Phase 3 treats `"None"` as empty
> for both the metric predicate and the milestone date.

## The single chronological sweep

```
1. Load current + historical rows (csv module).
2. Per row: resolve taxId → species taxid (resolve_to_species); skip+log if
   unresolvable. Attach species taxid + its canonical lineage.
3. Sort ALL dated rows by (releaseDate, accession) ascending — one global sort.
   Rows with empty releaseDate are excluded from milestone dates but still
   counted in total_assemblies; the count is logged.
4. For each milestone M, keep seen[M] = taxids already credited. Walk rows in
   date order; for each row satisfying M, for each level in
   [species, genus, family, order, class, phylum, kingdom]:
       t = species_taxid (level==species) else lineage.get(rank)
       if t and t not in seen[M]:
           record (date, accession) as the first for (t, M); seen[M].add(t)
           if M in {assembly, metric} and level != species:
               append rank to that species's in_ranks[M] list
```

First-touch-in-date-order *is* that taxon's milestone — no aggregation, no
inverted index, no name-string matching.

Note that the higher-rank milestone and the first-in-ranks flag are produced by
the **same** first-touch (step 4's inner block): the moment a higher-rank taxid
is first credited, the species that triggered it appends that rank to its
`in_ranks` list. So first-in-ranks is a re-attribution of the higher-rank
milestone back onto the species — not a separate pass. Only the two
any-submitter milestones (`assembly`, `metric`) do this re-attribution (the
`M in {assembly, metric}` guard); the two affiliated milestones still produce
higher-rank rows but no in-ranks flags. The earlier design computed first-in-ranks
in a separate third step that matched species to ranks by scientific-name string;
the single sweep removes that step entirely.

Every resolved species also gets an output row even if all its assemblies have
empty dates (so its `total_assemblies` count is never lost).

## Output: `taxon_milestone_summary.tsv`

One row per taxon at any rank. Species rows carry in-ranks flags and counts;
higher-rank rows carry the earliest milestone date/accession for the clade.

| Column | Species rows | Higher-rank rows |
|---|---|---|
| `taxid`, `rank`, `scientific_name` | ✓ | ✓ |
| `first_assembly_date` / `_accession` | ✓ | ✓ (earliest in clade) |
| `first_ebp_assembly_date` / `_accession` | ✓ | ✓ |
| `first_metric_date` / `_accession` | ✓ | ✓ |
| `first_ebp_metric_date` / `_accession` | ✓ | ✓ |
| `first_assembly_in_ranks` (e.g. `genus,family`) | ✓ | blank |
| `first_metric_in_ranks` | ✓ | blank |
| `latest_assembly_date` / `_accession` | ✓ | blank |
| `total_assemblies` | ✓ | blank |
| `current_assemblies` | ✓ | blank |

`first_*_in_ranks` list the canonical ranks at which this species was the first
in its clade to reach that milestone (a subset of `CANONICAL_RANKS`). Only the
any-submitter milestones (`assembly`, `metric`) get in-ranks columns —
"first in clade to be assembled" and "first in clade to reach EBP quality" are
the scientifically meaningful firsts.

`latest_assembly_date` / `latest_assembly_accession` report the species'
**current** (latest) version: the newest-dated non-superseded row, falling back
to the newest-dated row if none are current. `current_assemblies` counts rows
whose `versionStatus` (or `version_status`) is not `superseded`.

## Assembly columns used

| Column | Purpose |
|---|---|
| `taxId` | Taxonomy linkage (resolved to species) |
| `releaseDate` | Milestone date + global sort key |
| `bioProjectAccession` | EBP affiliation: `PRJNA533106` in `split(',')` |
| `ebpStandardDate` | Metric achieved when non-empty (and not `"None"`) |
| `genbankAccession` | Milestone accession + sort tiebreak (no `accession` column exists in the source TSVs) |
| `versionStatus` | current vs superseded (for `current_assemblies`) |

## Prerequisites

```bash
conda activate genomehubs_data
```

No network access or NCBI `datasets` CLI required — all inputs are local TSVs
plus the dev/test taxdump.

## Running

```bash
SKIP_PREFECT=true python3 -m flows.lib.compute_taxon_milestones \
  --work_dir /tmp/assembly-versions \
  --taxdump_path test/taxonomy/ncbi
```

Expects in `--work_dir`:
- `assembly_current.tsv` — written by `parse_ncbi_assemblies` (Phase 1 daily pipeline)
- `assembly_historical.tsv` — written by Phase 0 backfill, updated by Phase 1 incremental

Writes to `--work_dir`:
- `taxon_milestone_summary.tsv`

`--taxdump_path` is the dev/test taxonomy directory (containing `nodes.dmp` and
`names.dmp`). In production the lineage arrives via the upstream contract and
this argument is omitted; without either source the flow fails loudly rather
than producing an empty result.

`load_taxonomy` can also be run standalone to inspect the parsed contract:

```bash
SKIP_PREFECT=true python3 -m flows.lib.load_taxonomy \
  --taxdump_path test/taxonomy/ncbi
```

## Minimal end-to-end test

```bash
mkdir -p /tmp/assembly-versions

# Two species in different families of order Isopoda; Ceratothoa (2017) predates
# Asellus (2019), so it wins the higher-rank firsts at order and above.
cat > /tmp/assembly-versions/assembly_current.tsv << 'EOF'
genbankAccession	taxId	releaseDate	bioProjectAccession	ebpStandardDate	versionStatus
GCA_AA.1	92525	2019-01-01	PRJNA533106	2019-01-01	current
EOF

cat > /tmp/assembly-versions/assembly_historical.tsv << 'EOF'
genbankAccession	taxId	releaseDate	bioProjectAccession	ebpStandardDate	versionStatus
GCA_CS.1	2922061	2017-01-01	PRJNA1		superseded
EOF

SKIP_PREFECT=true python3 -m flows.lib.compute_taxon_milestones \
  --work_dir /tmp/assembly-versions \
  --taxdump_path test/taxonomy/ncbi

cat /tmp/assembly-versions/taxon_milestone_summary.tsv
```

Illustrative Output:
- Order *Isopoda* (29979): `first_assembly_date=2017-01-01`, `accession=GCA_CS.1`
  (earliest across the clade).
- *Asellus aquaticus* (92525): `first_assembly_in_ranks=genus,family` only — it
  is first in its own genus/family but not at order and above (Ceratothoa took
  those); `first_ebp_metric_date=2019-01-01` (affiliated + metric).
- *Ceratothoa steindachneri* (2922061): `first_assembly_in_ranks` = all six
  canonical ranks; `current_assemblies=0` (its only version is superseded).

## Validation invariants

The predicates nest, so per taxon the milestone dates satisfy:

```
first_assembly_date ≤ first_metric_date       ≤ first_ebp_metric_date
first_assembly_date ≤ first_ebp_assembly_date  ≤ first_ebp_metric_date
```

(`first_metric_date` and `first_ebp_assembly_date` are not ordered relative to
each other.) Every `first_*_in_ranks` value is a subset of `CANONICAL_RANKS`.
These are asserted in the test suite.

## Running the test suite

```bash
SKIP_PREFECT=true python3 -m pytest tests/test_taxon_milestones.py -v
```

Covers:
- **load_taxonomy** — `parse_nodes`, `parse_names`, `build_lineage` (canonical
  ranks only, species not in its own lineage), `build_taxonomy` against
  `test/taxonomy/ncbi/`.
- **resolve_to_species** — species passthrough; subspecies → parent species;
  genus / unknown taxid → `None`.
- **The sweep** — each of the four milestones picks the earliest qualifying row;
  `(releaseDate, accession)` tie-breaking; the `"None"`-string trap;
  empty-`releaseDate` excluded from dates but counted (incl. an empty-date-only
  species still appearing in the output); in-ranks correctness across
  genus/family/order; `current_assemblies` excludes superseded.
- **Output + invariants** — schema-exact rows; higher-rank rows blank their
  in-ranks/counts; the nested date-ordering and `in_ranks ⊆ CANONICAL_RANKS`
  invariants; an end-to-end flow run asserting the header and clade-earliest
  dates.
