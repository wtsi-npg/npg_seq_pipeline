# Elembio Support Plan

## Goal

Support Elembio sequencing data in the NPG analysis stack alongside the
existing Illumina path.

The working assumption is:

- Illumina keeps using the current `p4_stage1_analysis` and related flow.
- Elembio stage 1 is based on per-lane `bases2fastq` plus `samtools import`.
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

Alternative approach (Alt Phase 1):

- Add Elembio functions in parallel to the existing DAG.
- Use `->can_run` functionality in existing functions to determine if they are Illumina or Elembio specific.
- This integrates Elembio into the single graph, reducing duplication but requiring careful conditional logic.

This reduces risk and avoids destabilising the current Illumina production
path.

### 2. Treat Elembio stage 1 as a new pipeline shape

Recommended Elembio stage 1 flow:

1. Lane-level `elembio_bases2fastq`, one invocation per lane, each writing
   under `BAM_basecalls/fastq/lane{lane}` and running `bases2fastq` in a
   non-deplexing mode using:
   - a lane-specific read selector such as `L1R` / `L2R`
   - `--settings "I1Fastq,True"` and `--settings "I2Fastq,True"` where
     index FASTQ output is required
   - a minimal lane-level `[Samples]` CSV, for example `SampleName` /
     `WholeLane`, rather than a fully deplexing samplesheet
2. Extend `p4_stage1_analysis` options / templating so `samtools import`
   can be driven with the appropriate mode for:
   - single-read or paired-read input
   - 0, 1, or 2 index reads
   - single-read input via
     `-0 <fastq_dir>/Samples/WholeLane_L<lane>_R1.fastq`
   - paired-read input via
     `-1 <fastq_dir>/Samples/WholeLane_L<lane>_R1.fastq` and
     `-2 <fastq_dir>/Samples/WholeLane_L<lane>_R2.fastq`
   - one index read via
     `--i1 <fastq_dir>/Samples/WholeLane_L<lane>_I1.fastq`
   - two index reads via the above plus
     `--i2 <fastq_dir>/Samples/WholeLane_L<lane>_I2.fastq`

This is preferable to forcing the whole Elembio path through the existing
Illumina `p4_stage1_analysis` behaviour unchanged because the current
Illumina stage 1 is lane-centric and already performs deplexing via the
existing `bambi`-based flow, while the preferred Elembio path should stay
lane-centric for launch/layout purposes, use `bases2fastq` with a lane-specific
selector plus a minimal `[Samples]` CSV, and reuse the existing
`p4_stage1_analysis` machinery only where it is still a good fit for stage-1
import semantics.

Alternative Elembio stage 1 flow:

1. Lane-level `elembio_bases2fastq` in deplexing mode
2. Explicit per-lane&tag `elembio_import` / `samtools import`, producing the
   lane-and-tag CRAM/BAM files used by stage 2

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
- Add a new Elembio `elembio_bases2fastq` stage-1 entry function, while
  reusing / extending `p4_stage1_analysis.pm` where it remains a good fit for
  import option handling.
- Extend `p4_stage1_analysis` option generation so Elembio can select the
  appropriate `samtools import` mode from read structure
  (single/paired; 0/1/2 index reads), including the correct
  `-0` vs `-1/-2` and optional `--i1/--i2` FASTQ inputs from the per-lane
  `bases2fastq` output tree.
- Keep Elembio pipeline setup driven by instrument-runfolder metadata
  (`RunParameters.json`, and `RunManifest.json` where needed), then replace
  `qc_interop` with a post-`bases2fastq` QC step driven by `RunStats.json` /
  `RunManifest.json` from the `bases2fastq` output folder.
- Make the analysis daemon choose the correct runfolder resolver by
  manufacturer.
- Implement or reuse Elembio samplesheet generation for the NPG samplesheet cache from MLWH-backed LIMS data, keyed primarily by `id_flowcell_lims` / batch id as the main link back to LIMS, with `id_run` used for sequencing-run context and as a secondary lookup path where needed.
- Change the Elembio default for library merging so it is true only when the
  product is to be aligned, while keeping the behaviour configurable on the
  command line in the same way as Illumina today.
- Review stage 2 assumptions in `seq_alignment.pm`.

Likely new Elembio-specific functions:

- `elembio_bases2fastq`
- `elembio_qc_runstats`

Alternative / fallback Elembio-specific functions:

- `elembio_import`

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

Primary changes (critical):

- Implement or confirm the ability to pull LIMS information from MLWH for Elembio runs to generate the NPG samplesheet, keyed primarily by `id_flowcell_lims` / batch id from the Elembio flowcell tables, with `id_run` used only for run-record context or secondary lookup where required.
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

Key distinction:

- `id_flowcell_lims` is the primary key back to LIMS-linked sample and study
  information.
- `id_run` is primarily a key for the sequencing run as recorded in tracking /
  MLWH run-level tables, including instrument setup and later run outputs.
- Analysis setup should be possible from the runfolder plus the relevant
  `eseq_flowcell` / `iseq_flowcell`-style flowcell tables and their links to
  samples and studies, without depending on run-level MLWH records being the
  source of truth for LIMS linkage.

### Stage 1 outputs

Preferred target outcome:

- lane-and-tag imported CRAM/BAM files placed where the existing stage 2 can
  consume them, with later merging used only where needed to create final
  products
- lane-level QC outputs generated from `RunStats.json` in the `bases2fastq`
  output folder
- short-file cache populated only if downstream checks still require FASTQ input

### QC strategy

For Elembio:

- do not rely on `RunStats.json` for pipeline setup; it is only available after `bases2fastq` has run
- use `RunStats.json` / `RunManifest.json` from the `bases2fastq` output folder for tag metrics and lane stats
- treat LIMS / MLWH data, or the derived NPG samplesheet, as the source of
  truth for barcode-to-sample/study linking, and treat the `bases2fastq`
  output as the QC source that should reflect any such amendments or
  corrections
- skip `qc_interop`
- probably skip `spatial_filter`
- review whether a cluster-count consistency check still makes sense

## Open Decisions

These need to be agreed before implementation starts.

### 1. Batchless Elembio runs

Assumption: Walk-up / batchless Elembio runs are in scope for automatic analysis, but they should be treated as the exception path. The normal setup route should use `id_flowcell_lims` / batch id as the primary LIMS linkage, with `id_run` and runfolder metadata used for sequencing-run context.

### 2. Stage 1 granularity

Question:

- Should the main Elembio path use per-lane non-deplexing `bases2fastq`
  followed by `p4_stage1_analysis` / `samtools import` modes selected from
  read structure, or should it use a fully deplexing `bases2fastq` step plus
  explicit per-lane&tag import as a separate stage?

Recommendation:

- make the non-deplexing per-lane `bases2fastq` plus
  `p4_stage1_analysis`-driven import path the main implementation, and keep
  the fully deplexing plus explicit-import path as an alternative if the main
  route proves too awkward.

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

### Phase 1: Elembio pipeline skeleton

- Add Elembio base / metadata wrapper
- Add Elembio central graph
- Add Elembio lane-level `elembio_bases2fastq` scaffolding, including
  generation of per-lane selector / minimal `[Samples]` CSV inputs under
  `BAM_basecalls/fastq/lane{lane}` in non-deplexing mode
- Add Elembio-aware `p4_stage1_analysis` option scaffolding for
  `samtools import` mode selection across single/paired and 0/1/2-index cases,
  including the expected `Samples/WholeLane_L<lane>_{R1,R2,I1,I2}.fastq` inputs
- Integrate Elembio samplesheet / metadata-cache bootstrapping so the central
  runner can start with MLWH-backed LIMS data, keyed primarily by
  `id_flowcell_lims` / batch id

### Phase 2: Elembio stage 1

- Implement per-lane `elembio_bases2fastq`, with output rooted in
  `BAM_basecalls/fastq/lane{lane}` in non-deplexing mode, using lane-specific
  selectors plus minimal `[Samples]` CSV inputs
- Implement Elembio `p4_stage1_analysis` option sets for `samtools import`
  across single-read / paired-read and 0 / 1 / 2 index-read cases, using
  `-0` for single-read data, `-1/-2` for paired data, and optional
  `--i1/--i2` FASTQ inputs from `Samples/WholeLane_L<lane>_*.fastq`
- Keep the deplexing `elembio_import` route as a documented fallback /
  alternative implementation

### Phase 3: QC integration

- Generate Elembio tag metrics from the `bases2fastq` `RunStats.json` output
- Replace or suppress Illumina-only stage-1 checks

### Phase 4: Shared downstream validation

- Run Elembio-imported products through `seq_alignment`
- Make Elembio default library-merging conditional on alignment, with CLI
  override support
- Patch remaining Illumina assumptions in shared stage 2

### Phase 5: Archival and ops

- Elembio run-data archiving
- ML warehouse integration confirmation
- iRODS path finalisation
- `npg_esa` packaging and service rollout (sufficient for manual processing)

### Phase 6: Tracking and launch

- Define Elembio transition into `analysis pending`
- Make daemon runfolder lookup manufacturer-aware
- Wire automatic launch
- `npg_esa` packaging and service rollout for this

## Main Risks

- Architectural mismatch between the current Illumina lane-centric stage 1 and
  a different Elembio lane-centric stage 1 built around `bases2fastq`,
  lane-specific manifests, and Elembio-specific `samtools import` modes.
- Current dependence on batch id / samplesheet-backed LIMS cache.
- Missing automatic transition to `analysis pending`.
- Hidden Illumina assumptions in shared downstream logic.
- Product archival and run-data archival still being explicitly Illumina-based.

## Recommended First Implementation Slice

The smallest useful end-to-end slice is:

1. Manually launch the Elembio central graph on a runfolder.
2. The analysis starts with creation of the analysis folder hierarchy, plus caching of LIMS information in an NPG samplesheet (`metadata_cache`) keyed primarily by `id_flowcell_lims` / batch id, together with metadata from the instrument runfolder (`RunParameters.json`, and `RunManifest.json` where needed), rather than depending on `RunStats.json`.
3. `elembio_bases2fastq` runs once per lane, using a lane-specific selector
   and a minimal lane-level `[Samples]` CSV, running in non-deplexing mode,
   and producing output under `BAM_basecalls/fastq/lane{lane}`.
4. `p4_stage1_analysis` selects the appropriate `samtools import` mode from
   the run structure and produces lane-and-tag files:
   - `-0 <fastq_dir>/Samples/WholeLane_L<lane>_R1.fastq` for single-read
   - `-1/-2` with `R1` and `R2` FASTQs for paired-read
   - optional `--i1` / `--i2` with `I1` / `I2` FASTQs for indexed runs
5. Elembio `tag_metrics` are generated from `RunStats.json` in the `bases2fastq` output folder.
6. A minimal subset of `seq_alignment` runs successfully on imported files.

That slice will prove the overall architecture before archival and all
downstream edge cases are tackled.
