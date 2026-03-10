# Elembio Support Plan

## Goal

Support Elembio sequencing data in the NPG analysis stack alongside the
existing Illumina path.

The working assumption is:

- Illumina keeps using the current `p4_stage1_analysis` and related flow.
- Elembio stage 1 is based on `bases2fastq` plus `samtools import`.
- Runfolder detection and run metadata for Elembio should come from the
  existing Elembio support in `npg_tracking`, not from
  `npg_tracking::illumina::runfolder`.

## Summary

This is not a small swap of one executable for another.

The current `npg_seq_pipeline` analysis path is structurally Illumina-first:

- `npg_pipeline::base` extends `npg_tracking::illumina::runfolder`.
- The central graph starts with `qc_interop` and `p4_stage1_analysis`.
- `p4_stage1_analysis` depends on Illumina InterOp data, Illumina cycle
  ranges, `bambi i2b`, and `bambi decode`.
- The analysis daemon still resolves runfolders through
  `npg_tracking::illumina::runfolder`.
- The analysis launch path still expects `id_flowcell_lims` / batch id and a
  samplesheet-backed LIMS cache.

Because of that, Elembio should be added as a manufacturer-specific path first,
with shared downstream code reused only where it is genuinely platform-neutral.

## Current State

### In `npg_seq_pipeline`

The biggest Illumina-specific areas are:

- Base class and runfolder metadata.
- Stage 1 generation and deplexing.
- InterOp-based lane QC.
- Analysis daemon handoff.
- Samplesheet / LIMS cache setup.
- Some stage 2 heuristics.
- Run-data archival and iRODS naming.

Important consequences:

1. The current stage 1 is not only "BCL/BAM conversion".
   It also performs decode metrics, lane-level outputs, PhiX handling,
   seqchksum generation, and stage-1 split semantics.

2. The current scaffolding assumes Illumina `Data/Intensities/BaseCalls`
   structure.

3. The current auto-launch path assumes a run reaches `analysis pending`
   and has a usable batch id.

### In other repos

Useful Elembio support already exists:

- `npg_tracking`
  - `Monitor::Elembio::RunParametersParser`
  - `Monitor::Elembio::RunFolder`
  - `Monitor::Elembio::Staging`
  - `ResultSet::Run->find_with_attributes(...)`

- `npg_qc`
  - `npg_qc::elembio::run_stats`
  - `npg_qc::elembio::tag_metrics_generator`
  - `bin/elembio_runstats_parser.pl`

- `npg_ml_warehouse`
  - Elembio run info loader
  - Elembio run QC/product loader

This means the missing work is mainly pipeline integration, not raw Elembio
metadata parsing.

## Recommended Architecture

### 1. Add a dedicated Elembio analysis path

Do not try to force Elembio into `npg_tracking::illumina::runfolder`.

Recommended approach:

- Add an Elembio-aware base or metadata wrapper in `npg_seq_pipeline`.
- Add an Elembio-specific central graph.
- Keep the existing Illumina graph intact.

This reduces risk and avoids destabilising the current Illumina production
path.

### 2. Treat Elembio stage 1 as a new pipeline shape

Recommended Elembio stage 1 flow:

1. Run-level `bases2fastq`
2. Product-level `samtools import`
3. Emit per-product CRAM/BAM into the existing product directory scaffold

This is preferable to trying to patch `p4_stage1_analysis` because the current
Illumina stage 1 is lane-centric and undecoded, while Elembio deplexing is
naturally run-centric and already deplexed.

### 3. Reuse stage 2 only where it is genuinely platform-neutral

`seq_alignment` should remain mostly shared, but several platform checks should
be turned into capability-style decisions rather than Illumina platform tests.

Examples:

- whether spatial filtering is relevant
- optical duplicate distance
- default markdup method
- aligner defaults
- i5 reverse-complement handling

## Proposed Work By Repo

### `src/npg_seq_pipeline`

Primary changes:

- Add Elembio-aware run metadata handling.
- Add a new Elembio central graph, for example
  `function_list_central_elembio.json`.
- Add a new Elembio stage 1 function instead of reusing
  `p4_stage1_analysis.pm`.
- Replace `qc_interop` for Elembio with a QC step driven by
  `RunStats.json` / `RunManifest.json`.
- Make the analysis daemon choose the correct runfolder resolver by
  manufacturer.
- Remove the hard dependency on samplesheet generation for Elembio, or fail
  early with a clear policy when a batchless run is unsupported.
- Review stage 2 assumptions in `seq_alignment.pm`.

Likely new Elembio-specific functions:

- `elembio_stage1_analysis`
- `elembio_qc_runstats`
- optionally separate `elembio_bases2fastq` and `elembio_import`

Areas that probably need platform branching:

- `npg_pipeline::base`
- `npg_pipeline::runfolder_scaffold`
- `npg_pipeline::daemon`
- `npg_pipeline::daemon::analysis`
- `npg_pipeline::function::autoqc`
- `npg_pipeline::function::seq_alignment`
- `npg_pipeline::function::warehouse_archiver`
- `npg_pipeline::function::run_data_to_irods_archiver`
- `npg_pipeline::product::release::irods`

### `src/npg_tracking`

Primary changes:

- Add the equivalent of Illumina move-to-analysis handoff for Elembio runs.
- Decide where Elembio runs transition from `run complete` to
  `analysis pending`.
- Expose any extra run metadata methods needed by `npg_seq_pipeline`.

Current gap:

- Elembio staging creates and updates runs, but does not currently move them
  into `analysis pending` for automatic analysis launch.

### `src/npg_qc`

Primary changes:

- Reuse existing Elembio parsing to produce lane-level `tag_metrics`.
- Decide whether any additional Elembio-native QC result types are needed.
- Define what replaces Illumina-only checks:
  - `interop`
  - `cluster_count`
  - `spatial_filter`
  - possibly `seqchksum_comparator`

Good candidate for pipeline reuse:

- `bin/elembio_runstats_parser.pl`

### `src/npg_ml_warehouse`

Likely smaller changes:

- Confirm the final Elembio analysis layout matches loader expectations.
- Confirm tag-zero handling is acceptable.
- Confirm product linking behaviour when batch id is missing.

This repo already appears to understand Elembio deplexing output and run info.

### `src/p4`

Two options:

1. Bypass `p4` for Elembio stage 1.
2. Add a new Elembio stage-1 template to `p4`.

Recommendation:

- Prefer bypassing `p4` for Elembio stage 1 unless there is a strong reason to
  keep the same template engine.

Reason:

- The current stage-1 template is built around Illumina inputs, `bambi i2b`,
  decode, stage-1 split behaviour, and Illumina-specific metrics generation.

### `src/npg_irods`

Likely required if Elembio publication must match current production archival.

Current issue:

- The publishing scripts used from `npg_seq_pipeline` are explicitly
  Illumina-named and Illumina-oriented.

Required decision:

- generalise existing scripts, or
- add Elembio-specific publication scripts

### `npg_esa`

Operational changes:

- Package and deploy `bases2fastq`.
- Verify the deployed `samtools` build and wrapper support the import workflow.
- Add any new Elembio pipeline scripts/daemons/services.
- Keep `npg_tracking`, `npg_qc`, `npg_seq_pipeline`, `npg_ml_warehouse`, and
  possibly `p4` / `npg_irods` in the deployment set.

## Functional Design For Elembio

### Runfolder and run metadata

Use Elembio-native metadata from `RunParameters.json`:

- folder name
- instrument name
- flowcell id
- lane count
- paired/indexed status
- expected cycle count

Do not infer these through Illumina XML conventions.

### Stage 1 outputs

Preferred target outcome:

- product-level imported CRAM/BAM files placed where the existing stage 2 can
  consume them
- lane-level QC outputs generated from Elembio run stats
- short-file cache populated only if downstream checks still require FASTQ input

### QC strategy

For Elembio:

- use `RunStats.json` / `RunManifest.json` for tag metrics and lane stats
- skip `qc_interop`
- probably skip `spatial_filter`
- review whether a cluster-count consistency check still makes sense

## Open Decisions

These need to be agreed before implementation starts.

### 1. Batchless Elembio runs

Question:

- Are walk-up / batchless Elembio runs in scope for automatic analysis?

If yes:

- `npg_seq_pipeline` needs a non-samplesheet path for LIMS/product setup.

If no:

- fail early with a clear policy and error message.

### 2. Stage 1 granularity

Question:

- Do we want Elembio stage 1 to emit lane-level intermediates, or go directly
  to product-level imports?

Recommendation:

- go directly to product-level imports unless a downstream requirement forces
  lane-level stage-1 files.

### 3. Tag-zero semantics

Question:

- Do we need actual tag-zero read files, or are tag-zero metrics sufficient?

This matters because Elembio run stats can synthesise tag-zero metrics even if
  the file-level semantics do not match the Illumina path.

### 4. iRODS namespace

Question:

- Keep Elembio under current `illumina/...` collection roots, or introduce
  `elembio/...` roots?

Recommendation:

- decide this early because it affects archiving, metadata, and any downstream
  consumers.

### 5. Pipeline entry point

Question:

- Use the current `npg_pipeline_central` with manufacturer branching, or add a
  parallel Elembio central runner?

Recommendation:

- one entry point is fine if the graph and function selection are
  manufacturer-aware;
- otherwise add a separate runner rather than over-complicating the current
  Illumina path.

## Delivery Order

### Phase 1: Tracking and launch

- Define Elembio transition into `analysis pending`
- Make daemon runfolder lookup manufacturer-aware
- Decide policy for missing batch id

### Phase 2: Elembio pipeline skeleton

- Add Elembio base / metadata wrapper
- Add Elembio central graph
- Add Elembio stage-1 function scaffolding

### Phase 3: Elembio stage 1

- Implement `bases2fastq`
- Implement `samtools import`
- Write outputs into existing product scaffold

### Phase 4: QC integration

- Generate Elembio tag metrics from run stats
- Replace or suppress Illumina-only stage-1 checks

### Phase 5: Shared downstream validation

- Run Elembio-imported products through `seq_alignment`
- Patch remaining Illumina assumptions in shared stage 2

### Phase 6: Archival and ops

- Elembio run-data archiving
- ML warehouse integration confirmation
- iRODS path finalisation
- `npg_esa` packaging and service rollout

## Main Risks

- Architectural mismatch between Illumina lane-centric stage 1 and Elembio
  run-centric deplexing.
- Current dependence on batch id / samplesheet-backed LIMS cache.
- Missing automatic transition to `analysis pending`.
- Hidden Illumina assumptions in shared downstream logic.
- Product archival and run-data archival still being explicitly Illumina-based.

## Recommended First Implementation Slice

The smallest useful end-to-end slice is:

1. Elembio run reaches `analysis pending`.
2. Elembio central graph runs.
3. `bases2fastq` plus `samtools import` produce product files.
4. Elembio `tag_metrics` are generated from `RunStats.json`.
5. A minimal subset of `seq_alignment` runs successfully on imported files.

That slice will prove the overall architecture before archival and all
downstream edge cases are tackled.
