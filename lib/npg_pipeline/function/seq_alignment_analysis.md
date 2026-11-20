# seq_alignment.pm: Alignment Pipeline Configuration Module

## Overview

**Purpose**: Generates alignment pipeline commands for Illumina sequencing data by translating LIMS metadata and run configuration into P4 (Process and Pipe Pipeline Panacea) execution parameters.

**Role in Pipeline**: Stage 2 function that bridges LIMS data and bioinformatics tools, orchestrating reference alignment, quality control, and specialized analyses based on library types and study requirements.

---

## INPUTS

### 1. Sequencing Run Configuration

**Source**: Inherited from `npg_pipeline::base_resource` via the pipeline environment

| Input | Type | Description | Example |
|-------|------|-------------|---------|
| `run_folder` | Path | Illumina run directory | `/seq/illumina/runs/37/37541/` |
| `recalibrated_path` | Path | Base-called CRAM files from Stage 1 | `/nfs/sf19/ILorHSany_sf19/analysis/170109_K00333_0287_AH75VHBBXX/` |
| `archive_path` | Path | Output directory for aligned data | `/nfs/sf19/ILorHSany_sf19/analysis/170109_K00333_0287_AH75VHBBXX/` |
| `repository` | Path | Reference genome repository | `/lustre/scratch110/srpipe/references/` |
| `id_run` | Integer | Sequencing run identifier | `12597` |
| `platform` | String | Sequencer platform | `NovaSeq`, `NovaSeqX`, `MiSeq`, `HiSeq` |
| `is_paired_read` | Boolean | Single or paired-end sequencing | `true`/`false` |
| `read_cycle_counts` | Array | Read lengths per cycle | `[76, 76]` |
| `uses_patterned_flowcell` | Boolean | Patterned vs non-patterned flowcell | `true` (NovaSeq) / `false` (HiSeq) |

### 2. LIMS (Laboratory Information Management System) Data

**Source**: `st::api::lims` objects accessed via `$product->lims`

Each data product receives LIMS metadata that drives alignment decisions:

#### Sample-Level Attributes
```perl
$lims->is_pool                  # Is this lane pooled (multiplexed)?
$lims->is_phix_spike            # Is this a PhiX control spike-in?
$lims->library_type             # Critical: determines analysis type
```

**Library Types** (examples and their effects):
- `"RNA-seq dUTP"` / `"cDNA"` → Triggers RNA-seq analysis (STAR/TopHat2/HISAT2)
- `"Duplex-Seq"` / `"Targeted NanoSeq Pulldown *"` → Uses duplex-seq duplicate marking
- `"Haplotagging"` → Enables haplotagging barcode processing
- `"Chromium"` → 10X Genomics processing, skips target alignment
- `"Hi-C"` → Enables Hi-C specific BWA flags
- `"GbS"` / `"GnT MDA"` → Genotyping-by-Sequencing analysis
- `"BGE"` → Enables bait stats with autosome filtering

#### Reference Genome Specification
```perl
$lims->reference_genome         # Format: "Organism (assembly + transcriptome (aligner_hint))"
```

**This field is overloaded**: It specifies the reference genome AND can optionally include an aligner hint.

**Examples**:
- Basic: `"Homo_sapiens (GRCh38_15_plus_hs38d1)"`
- With transcriptome: `"Homo_sapiens (GRCh38_15_plus_hs38d1 + ensembl_94_transcriptome)"`
- With aligner hint: `"Homo_sapiens (GRCh38_15_plus_hs38d1 (bwa_mem2))"`
- Full specification: `"Mus_musculus (NCBIm37 + ensembl_67_transcriptome (star))"`

**Parsed into**:
- Organism: `Homo_sapiens`
- Strain/Assembly: `GRCh38_15_plus_hs38d1`
- Transcriptome version: `ensembl_94_transcriptome` (for RNA-seq, optional)
- Aligner hint: `bwa_mem2`, `star`, `hisat2`, `bwa_mem`, `minimap2`, etc. (optional)

#### Study-Level Configuration Flags
```perl
$lims->alignments_in_bam                    # Output aligned BAM? (vs unaligned only)
$lims->contains_nonconsented_xahuman        # Split X chromosome + autosomes?
$lims->separate_y_chromosome_data           # Separate Y chromosome?
$lims->contains_nonconsented_human          # Split human vs non-human reads?
```

#### Additional Metadata
- Bait/target regions: `npg_tracking::data::bait->bait_intervals_path()`
- GBS plex panel: `npg_tracking::data::gbs_plex->gbs_plex_path()`
- Transcriptome indices: `npg_tracking::data::transcriptome->transcriptome_index_path()`

### 3. Product Composition

**Source**: `$self->products->{data_products}` (array of `npg_pipeline::product` objects)

Each product represents an analysis unit:
```perl
$product->rpt_list              # "12597:4:3" (run:position:tag_index)
$product->composition           # Collection of components (lanes/plexes)
$product->is_tag_zero_product   # Is this tag#0 (undetermined indices)?
$product->lims                  # Associated LIMS object
```

**Example compositions**:
- Single lane: `12597:4` (one component, no tag)
- Single plex: `12597:4:3` (one component, tag_index=3)
- Multi-lane merge: `12597:1:3;12597:2:3` (two components, same tag across lanes)

---

## PROCESSING LOGIC

### Key Decision Tree

```
For each data product:
├─ Extract LIMS metadata (library_type, reference_genome, flags)
├─ Validate human split options (max one type allowed)
├─ Determine analysis type:
│  ├─ RNA-seq? → Check library_type =~ /RNA|cDNA|DAFT/
│  ├─ GBS plex? → Check library_type =~ /GbS|GnT MDA/ + gbs_plex_name
│  ├─ Haplotagging? → Check library_type =~ /Haplotagging/
│  └─ Standard DNA alignment (default)
├─ Decide alignment method:
│  ├─ Check reference_genome for aligner hint (overrides defaults)
│  ├─ RNA: STAR (NovaSeqX default) / TopHat2 / HISAT2
│  ├─ GBS: BWA aln (old version: bwa0_6)
│  ├─ DNA: BWA mem2 (NovaSeqX) / BWA mem / BWA aln (short reads <100bp)
│  └─ Special: minimap2, bwa_mem_bwakit (alt alleles)
├─ Configure P4 parameters:
│  ├─ Reference paths (fasta, dict, aligner-specific indices)
│  ├─ Markdup method (biobambam/samtools/picard/duplexseq/none)
│  ├─ Optical duplicate distance (2500px patterned / 100px non-patterned)
│  ├─ Human split configuration (if requested)
│  ├─ Bait/target regions (if specified)
│  └─ Library-specific flags (haplotag_processing, HiC flags, etc.)
├─ Prune/splice P4 template nodes:
│  ├─ Remove unnecessary processing steps (e.g., no alignment → skip fixmate)
│  ├─ Remove unused stats (e.g., no bait → skip bait stats)
│  └─ Adjust for single-end vs paired-end
└─ Generate outputs:
   ├─ P4 parameter JSON file (alignment configuration)
   ├─ Composition JSON files (product + subsets)
   └─ vtfp.pl + viv.pl + qc command line
```

### Critical Configuration Examples

**1. Standard DNA Alignment (WGS / Pulldown / Exome)**:
```
Input: library_type not matching special types (RNA/Chromium/Hi-C/GBS/etc.),
       reference="Homo_sapiens (GRCh38)", alignments_in_bam=1
       bait_name (optional - adds targeted capture QC)
↓
alignment_method = bwa_mem2 (NovaSeqX+) or bwa_mem (NovaSeq 6000/MiSeq/HiSeq)
                   or bwa_aln (MiSeq with reads <100bp)
markdup_method = samtools (NovaSeqX+ or single-end)
                 or biobambam (pre-NovaSeqX paired-end)
do_target_alignment = true
Outputs: CRAM + BAM + bam_flagstats + substitution_metrics
         + bait_stats + target_stats (if bait_name provided)
```

**2. RNA-seq Analysis**:
```
Input: library_type="RNA-seq dUTP", reference="Mus_musculus (NCBIm37 + ensembl_67_transcriptome)"
↓
alignment_method = star (NovaSeqX) or tophat2
library_type = fr-firststrand (dUTP) or fr-unstranded
quant_method = salmon
Outputs: BAM + RNA-SeQC + alignment metrics + salmon quantification
```

**3. X/Autosome Split (Human Reference)**:
```
Input: contains_nonconsented_xahuman=1, reference="Homo_sapiens (GRCh38)"
↓
Forces alignment even if alignments_in_bam=0
Aligns to target human reference (GRCh38)
Splits: X + autosomes (chr 1-22) → _xahuman file, Y → main file
Outputs: CRAM_xahuman (X+autosomes) + CRAM (Y) + flagstats for each
```

**4. Human Contamination Removal (Non-Human Target)**:
```
Input: contains_nonconsented_human=1, reference="Plasmodium_falciparum (...)"
       (or any non-human reference)
↓
Target alignment to non-human reference (or unaligned)
Additional alignment to T2T-CHM13v2.0 to identify human reads
Splits: human reads vs target reads
Outputs: CRAM_human (T2T aligned) + CRAM (target aligned/unaligned)
```

**5. 10X Chromium (No Target Alignment)**:
```
Input: library_type="Chromium single cell 3'v3", reference=*
↓
do_target_alignment = false
Skips target alignment completely
Still runs alignment filtering, PhiX split handling, and standard QC commands
Outputs: Unaligned CRAM + PhiX subset CRAM (if applicable)
         + bam_flagstats + alignment_filter_metrics
```

**6. Human Chromosome Split Without Target Alignment (Edge Case)**:
```
Input: separate_y_chromosome_data=1 (or contains_nonconsented_xahuman=1),
       reference="Homo_sapiens (GRCh38)",
       BUT target alignment is otherwise off (e.g. alignments_in_bam=0,
       or library_type="Chromium*")
↓
Override: Forces temporary alignment to human reference
Aligns solely to identify chromosome membership
Performs split: Y → _yhuman file (or X+autosomes → _xahuman file)
Undo alignment: Uses final_output_prep_chrsplit_noaln.json P4 template
Outputs: Unaligned CRAM (main) + _yhuman/_xahuman CRAM (split subset)

Note: Alignment is performed temporarily only to determine which reads belong
to Y (or X+autosomes), then reverted to unaligned for the main output.
This applies regardless of why alignment is off (Chromium, alignments_in_bam=0, etc.)
```

---

## OUTPUTS

### 1. P4 Parameter JSON File

**Location**: `{recalibrated_path}/{rpt}_p4s2_pv_in.json`

**Structure**:
```json
{
  "assign": [{
    "outdatadir": "/path/to/archive/12597_4#3/",
    "reference_genome_fasta": "/references/Homo_sapiens/GRCh38/all/fasta/hsa.fasta",
    "alignment_reference_genome": "/references/Homo_sapiens/GRCh38/all/bwa0_6/hsa.fasta",
    "alignment_method": "bwa_mem",
    "markdup_method": "biobambam",
    "markdup_optical_distance_value": "2500",
    "phix_reference_genome_fasta": "/references/PhiX/all/fasta/phix_unsnipped_short_no_N.fa",
    "spatial_filter_switch": "off",
    "haplotag_processing": "off",
    ...
  }],
  "assign_local": {
    "final_output_prep_target": {...}
  },
  "ops": {
    "prune": [
      "fop.*_bmd_multiway:calibration_pu-",
      "foptgt.*samtools_stats_F0.*_target_autosome.*-"
    ],
    "splice": [
      "alignment_filter:target_bam_out-foptgt_bmd_multiway:"
    ]
  }
}
```

**Key Parameters Configured**:
- **Reference paths**: FASTA, dict, BWA/STAR/HISAT2 indices, PhiX reference
- **Alignment method**: `bwa_mem`, `bwa_mem2`, `star`, `tophat2`, `hisat2`, `minimap2`
- **Duplicate marking**: Method (biobambam/samtools/picard/duplexseq/none) + optical distance
- **Spatial filtering**: On/off based on platform (off for NovaSeq)
- **Special processing**: Haplotagging, Hi-C flags, primer clipping
- **Human splits**: Chromosome subset flags, invert flags
- **Target regions**: Bait intervals, target intervals (for capture data)
- **RNA-seq specific**: Library type (stranded), SJDB overhang, transcriptome index
- **Template operations**: Nodes to prune (remove) or splice (bypass)

### 2. Composition JSON Files

**Purpose**: Track analysis provenance and data product lineage

**Files generated** (per product):
- Main: `12597_4#3.composition.json`
- PhiX subset: `12597_4#3_phix.composition.json` (if not spike)
- Human splits: `12597_4#3_xahuman.composition.json`, `12597_4#3_yhuman.composition.json`, `12597_4#3_human.composition.json`

**Structure**:
```json
{
  "__CLASS__": "npg_tracking::glossary::composition",
  "components": [
    {
      "__CLASS__": "npg_tracking::glossary::composition::component::illumina",
      "id_run": 12597,
      "position": 4,
      "tag_index": 3,
      "subset": null
    }
  ]
}
```

**For subset** (e.g., phix):
```json
{
  "components": [{
    "id_run": 12597,
    "position": 4,
    "tag_index": 3,
    "subset": "phix"
  }]
}
```

### 3. P4 Command Line

**Generated command** (bash script, lines 656-716):

```bash
bash -c '
  # Setup working directory
  mkdir -p /archive/tmp_abc123xyz/12597_4#3
  cd /archive/tmp_abc123xyz/12597_4#3

  log_label=$(date +%Y%m%d%H%M%S)

  # vtfp.pl: Template processing (P4 stage)
  vtfp.pl \
    -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib \
    -param_vals /recal/12597_4#3_p4s2_pv_in.json \
    -export_param_vals 12597_4#3_p4s2_pv_out_abc123xyz_${log_label}.json \
    -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ \
    -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 16` \
    -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 16 --exclude 1 --divide 2` \
    -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 16 --exclude 2 --divide 2` \
    $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json \
    > run_12597_4#3_${log_label}.json

  # viv.pl: Execute generated pipeline
  && viv.pl -s -x -v 3 -o viv_12597_4#3_${log_label}.log run_12597_4#3_${log_label}.json

  # QC checks (variable based on analysis type)
  && qc --check bam_flagstats --rpt_list "12597:4:3" --filename_root 12597_4#3 \
        --qc_in /archive/12597_4#3/ --qc_out /archive/12597_4#3/qc/ \
        --input_files /archive/12597_4#3/12597_4#3.cram

  && qc --check bam_flagstats --rpt_list "12597:4:3" --filename_root 12597_4#3_phix \
        --subset phix --qc_in /archive/12597_4#3/ --qc_out /archive/12597_4#3/qc/ \
        --input_files /archive/12597_4#3/12597_4#3_phix.cram --skip_markdups_metrics

  && qc --check alignment_filter_metrics --rpt_list "12597:4:3" --filename_root 12597_4#3 \
        --qc_in $PWD --qc_out /archive/12597_4#3/qc/ \
        --input_files 12597_4#3_bam_alignment_filter_metrics.json

  # Additional QC based on analysis type:
  # RNA-seq: && qc --check rna_seqc ...
  # GBS: && qc --check genotype_call ...
  # Aligned: && qc --check substitution_metrics ...
  # Human split: && qc --check bam_flagstats ... --subset xahuman ...
'
```

### 4. P4 Template Selection

**Templates present in p4/data/vtlib/**:
- `alignment_wtsi_stage2_template.json`
- `alignment_wtsi_stage2_humansplit_template.json`
- `alignment_wtsi_stage2_humansplit_notargetalign_template.json`
- `alignment_wtsi_stage2_humansplit_extrasplit_notargetalign_template.json`

**Templates selected by `seq_alignment.pm`**:
- Standard path: `alignment_wtsi_stage2_template.json`
- Non-consented human split (`contains_nonconsented_human`): `alignment_wtsi_stage2_humansplit_template.json`
- Chromosome-split cases (`contains_nonconsented_xahuman`, `separate_y_chromosome_data`) do **not** switch the stage-2 template; they stay on the standard template and are controlled by P4 parameters such as `final_output_prep_target_name=split_by_chromosome` and, when target alignment is otherwise off, `final_output_prep_no_y_target=final_output_prep_chrsplit_noaln.json`

**Template selection logic**:
```perl
my $nchs_template_label = $nchs? q{humansplit_}: q{};
# Result: alignment_wtsi_stage2_${nchs_template_label}template.json
```

Where:
- `$nchs` is true only for `contains_nonconsented_human`
- X/autosome and Y-chromosome split workflows are parameterized within the standard template rather than selecting a different stage-2 template

### 5. vtfp.pl Operation

**What vtfp.pl does**:
1. **Loads template**: JSON file describing pipeline graph (nodes, edges, parameters)
2. **Applies parameters**: Substitutes values from `_p4s2_pv_in.json`
3. **Performs operations**:
   - **Prune**: Remove nodes matching patterns (e.g., unused QC checks)
   - **Splice**: Bypass nodes (e.g., skip alignment if not needed)
4. **Expands subgraphs**: Includes nested templates (e.g., `final_output_prep.json`)
5. **Calculates threads**: Distributes CPU resources across tools
6. **Outputs runfile**: Executable JSON describing complete pipeline graph

**Result**: `run_12597_4#3_${timestamp}.json`

### 6. viv.pl Operation

**What viv.pl does**:
1. **Parses runfile**: Loads pipeline graph from vtfp.pl output
2. **Executes nodes**: Runs commands in dependency order
3. **Manages data flow**: Creates named pipes, temporary files
4. **Monitors execution**: Logs progress, captures errors
5. **Flags**:
   - `-s`: Strict mode (fail on any error)
   - `-x`: Execute (not dry-run)
   - `-v 3`: Verbose level 3

**P4 Node Examples from Template**:
```json
{
  "id": "bwa_mem",
  "type": "EXEC",
  "cmd": ["bwa", "mem", "-t", {"subst": "aligner_numthreads"},
          {"subst": "alignment_reference_genome"},
          {"port": "in1"}, {"port": "in2"}],
  "use_STDIN": true,
  "use_STDOUT": true
}
```

### 7. QC Outputs Generated

**Location**: `{archive_path}/{rpt}/qc/`

**Files produced** (depends on analysis type):

| QC Check | File Pattern | Conditions |
|----------|--------------|------------|
| BAM flagstats | `{rpt}.bam_flagstats.json` | Always (for main CRAM) |
| BAM flagstats (PhiX) | `{rpt}_phix.bam_flagstats.json` | If not spike tag |
| BAM flagstats (human split) | `{rpt}_{xahuman|yhuman|human}.bam_flagstats.json` | If human split |
| Alignment filter metrics | `{rpt}_bam_alignment_filter_metrics.json` | If not spike tag |
| Substitution metrics | `{rpt}.substitution_metrics.json` | If aligned + not tag#0 |
| RNA-SeQC | `{rpt}.rna_seqc.json` | If RNA-seq + not tag#0 |
| Genotype call | `{rpt}.genotype_call.json` | If GBS plex |

---

## DATA FLOW DIAGRAM

```
┌─────────────────────────────────────────────────────────────────────┐
│ INPUTS                                                              │
├─────────────────────────────────────────────────────────────────────┤
│ Sequencing Run Config          LIMS Data                            │
│ • run_folder                   • library_type                       │
│ • recalibrated_path           • reference_genome                    │
│ • archive_path                • alignments_in_bam                   │
│ • id_run, platform            • contains_nonconsented_*             │
│ • is_paired_read              • is_phix_spike, is_pool              │
│ • uses_patterned_flowcell     • bait_name, gbs_plex_name            │
│                                                                      │
│ Product Composition            Reference Repository                 │
│ • rpt_list (12597:4:3)        • /references/Homo_sapiens/GRCh38/    │
│ • components (lanes/plexes)   • /references/PhiX/                   │
│ • is_tag_zero_product         • /references/transcriptomes/         │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ PROCESSING: seq_alignment.pm::_alignment_command()                  │
├─────────────────────────────────────────────────────────────────────┤
│ Decision Logic:                                                     │
│ 1. Validate human split options                                     │
│ 2. Determine analysis type (RNA/GBS/DNA)                            │
│ 3. Select aligner (BWA/STAR/TopHat2/HISAT2)                         │
│ 4. Configure markdup method                                         │
│ 5. Set library-specific flags                                       │
│ 6. Determine human split strategy                                   │
│ 7. Configure target/bait regions                                    │
│ 8. Build P4 parameter hash                                          │
│ 9. Generate prune/splice operations                                 │
│ 10. Write parameter JSON                                            │
│ 11. Construct vtfp.pl + viv.pl command                              │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ OUTPUTS                                                             │
├─────────────────────────────────────────────────────────────────────┤
│ 1. P4 Parameter JSON                                                │
│    {recal}/{rpt}_p4s2_pv_in.json                                    │
│    • Reference paths, alignment method, markdup config              │
│    • Prune/splice operations, library flags                         │
│                                                                      │
│ 2. Composition JSONs                                                │
│    {archive}/{rpt}.composition.json                                 │
│    {archive}/{rpt}_phix.composition.json (if applicable)            │
│    {archive}/{rpt}_xahuman.composition.json (if split)              │
│    {archive}/{rpt}_yhuman.composition.json (if split)               │
│    {archive}/{rpt}_human.composition.json (if split)                │
│                                                                      │
│ 3. Shell Command                                                    │
│    bash -c 'vtfp.pl ... && viv.pl ... && qc ... && qc ...'          │
│    • vtfp.pl: Template expansion → run JSON                         │
│    • viv.pl: Pipeline execution → CRAM/BAM outputs                  │
│    • qc: Quality control checks → JSON metrics                      │
│                                                                      │
│ 4. Job Definition                                                   │
│    npg_pipeline::function::definition object                        │
│    • job_name, command, composition, resources                      │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ P4 EXECUTION (vtfp.pl + viv.pl)                                     │
├─────────────────────────────────────────────────────────────────────┤
│ vtfp.pl:                                                            │
│ • Loads: alignment_wtsi_stage2_template.json                        │
│   or alignment_wtsi_stage2_humansplit_template.json for nchs        │
│ • Applies: Parameters from _p4s2_pv_in.json                         │
│ • Prunes: Unnecessary nodes (e.g., unused QC)                       │
│ • Splices: Bypasses nodes (e.g., no alignment path)                 │
│ • Outputs: run_{rpt}_{timestamp}.json (executable pipeline)         │
│                                                                      │
│ viv.pl:                                                             │
│ • Executes: Pipeline graph from run JSON                            │
│ • Orchestrates: BWA/STAR → samtools → Picard → bambi               │
│ • Produces: CRAM files, BAM files, metrics JSONs                    │
│                                                                      │
│ Final Outputs:                                                      │
│ • {archive}/{rpt}.cram (aligned or unaligned)                       │
│ • {archive}/{rpt}_phix.cram (PhiX reads)                            │
│ • {archive}/{rpt}_xahuman.cram (human split if requested)           │
│ • {archive}/{rpt}_yhuman.cram (Y split if requested)                │
│ • {archive}/{rpt}_human.cram (nchs split if requested)              │
│ • {archive}/{rpt}/qc/*.json (QC metrics)                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## KEY CONCEPTS FOR REIMPLEMENTATION

### 1. **Product-Centric Design**
- Everything revolves around `npg_pipeline::product` objects
- Each product = one analysis output (lane, plex, or merge)
- Products carry LIMS metadata and composition structure

### 2. **Template-Based Pipeline Configuration**
- Don't hardcode pipelines—use declarative JSON templates (P4)
- Parameters drive template selection and node pruning
- Enables flexible pipeline variants without code changes

### 3. **LIMS as Source of Truth**
- All biological decisions derived from LIMS attributes
- Library type determines analysis path (RNA vs DNA vs special)
- Study flags control human splits and consent-driven workflows

### 4. **Reference Genome Parsing**
- Format: `"Organism (assembly + transcriptome (analysis))"`
- Single string encodes: species, strain, transcriptome, aligner
- Parsed to locate correct reference indices

### 5. **Subset Tracking**
- Products can have subsets: `phix`, `human`, `xahuman`, `yhuman`
- Each subset gets own composition file and CRAM output
- Enables split QC and downstream analysis

### 6. **Platform-Specific Behavior**
- NovaSeq/NovaSeqX: Different duplicate detection distance, skip spatial filter
- MiSeq/HiSeq: May use older BWA aln for short reads
- Patterned vs non-patterned: Optical duplicate detection distance (2500 vs 100 pixels)

### 7. **P4 Template Operations**
- **Prune**: Remove entire subgraph (node + downstream dependencies)
- **Splice**: Bypass node (connect input directly to output)
- Enables conditional pipeline paths without duplicating templates

### 8. **Modular QC**
- QC checks called as separate commands after pipeline execution
- Each check reads CRAM/BAM and writes JSON to qc/
- Conditional based on analysis type and split mode (main flagstats always; alignment_filter_metrics for non-spike products; RNA-SeQC for RNA only)

---

## GLOSSARY

| Term | Definition |
|------|------------|
| **rpt_list** | Run-Position-Tag identifier, e.g., "12597:4:3" (run 12597, lane 4, tag 3) |
| **Product** | Analysis unit (lane, plex, or merge) with composition and LIMS data |
| **Composition** | Collection of components (lane/plex combinations) forming a product |
| **LIMS** | Laboratory Information Management System—source of sample metadata |
| **P4 / vtfp.pl** | Process and Pipe Pipeline Panacea—template-based pipeline framework |
| **viv.pl** | P4 executor—runs pipeline graph, manages data flow |
| **Markdup** | Duplicate marking—identifies PCR/optical duplicates |
| **PhiX spike** | PhiX control library spiked into sequencing lane |
| **Tag zero** | Reads that couldn't be demultiplexed (undetermined indices) |
| **Human split** | Separating human/non-human or X/autosome/Y reads (consent reasons) |
| **Bait** | Target capture regions (for exome/targeted sequencing) |
| **GBS plex** | Genotyping-by-Sequencing panel definition |
| **Haplotagging** | Long-range phasing barcoding technology |
| **Spatial filter** | Removes spatially-adjacent duplicate reads (HiSeq/HiSeqX) |

---

## BIOINFORMATICS DECISION SUMMARY

| Scenario | LIMS Input | Decision | Output |
|----------|-----------|----------|---------|
| **Standard DNA (WGS/Exome/Pulldown)** | `library_type` not special type, `ref=Homo_sapiens`, `align=1` | BWA mem2 (NovaSeqX+) or BWA mem (NovaSeq/MiSeq), samtools markdup (NovaSeqX+) or biobambam (pre-NovaSeqX) | `*.cram` + BAM + QC |
| **+ Targeted capture QC** | Same as above + `bait_name=*` | Same alignment, adds bait/target stats | Above + bait/target metrics |
| **RNA-seq** | `library_type=RNA-seq dUTP`, `ref=*_transcriptome` | STAR alignment, salmon quant, fr-firststrand | BAM + RNA-SeQC + salmon |
| **10X Chromium** | `library_type=Chromium*` | Skip target alignment, but still run alignment filtering and QC | Unaligned `*.cram` + PhiX subset/QC if applicable |
| **Human chr split without alignment** | No target alignment (Chromium, `align=0`, etc.) + `separate_y_chromosome_data=1` or `xahuman=1` | Force temp align, split, revert to unaligned | Unaligned `*.cram` + `*_yhuman.cram` or `*_xahuman.cram` |
| **X/Autosome split** | `contains_nonconsented_xahuman=1`, `ref=Homo_sapiens` | Force align, X+autosomes→_xahuman, Y→main | `*_xahuman.cram` (X+1-22) + `*.cram` (Y) |
| **Y chromosome split** | `separate_y_chromosome_data=1`, `ref=Homo_sapiens` | Align, Y→_yhuman, X+autosomes→main | `*_yhuman.cram` (Y) + `*.cram` (X+1-22) |
| **Human contamination removal** | `contains_nonconsented_human=1`, `ref` NOT human | Align to target + T2T to remove human | `*_human.cram` (T2T) + `*.cram` |
| **PhiX spike** | `is_phix_spike=1` | Align to PhiX ref, markdup, no human split | `*.cram` (PhiX aligned) |
| **GBS** | `library_type=GbS`, `gbs_plex_name=*` | BWA aln (old), no markdup, genotype call | BAM + genotype VCF |
| **Haplotagging** | `library_type=Haplotagging*` | BWA, haplotag barcode processing | BAM with haplotag RG |
| **Hi-C** | `library_type=Hi-C*` | BWA mem with -5 -S -P flags, high mismatch penalty | BAM for Hi-C analysis |
| **Duplex-Seq** | `library_type=Duplex-Seq` | BWA, duplex-seq aware duplicate marking | CRAM with duplex tags |

---

## REFERENCES

**Related Documentation**:
- P4 (vtfp.pl/viv.pl): [src/p4](src/p4) repository
- npg_seq_pipeline: [src/npg_seq_pipeline](src/npg_seq_pipeline) repository
- LIMS interface: st::api::lims (npg_tracking package)

**Key Files**:
- Main module: [lib/npg_pipeline/function/seq_alignment.pm](src/npg_seq_pipeline/lib/npg_pipeline/function/seq_alignment.pm)
- Test suite: [t/20-function-seq_alignment.t](src/npg_seq_pipeline/t/20-function-seq_alignment.t)
- P4 templates: [data/vtlib/alignment_wtsi_stage2_*.json](src/p4/data/vtlib/)
