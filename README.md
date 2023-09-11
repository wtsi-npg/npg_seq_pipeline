# NPG Pipelines for Processing Illumina Sequencing Data

This software provides the Sanger NPG team's automation for analysing and internally archiving Illumina sequencing on behalf of DNA Pipelines for their customers.

There are two main pipelines:

- data product and QC metric creation: "central"
- internal archival of data products, metadata, QC metrics and logs: "post_qc"

and the daemons which automatically start these pipelines.

Processing is performed as a appropriate for the entire run, for each lane in the sequencing flowcell, or each tagged library (within a pool on the flowcell).

## Batch Processing and Dependency Tracking with LSF or wr

With this system, all of a pipeline's steps are submitted for execution to the
LSF, or wr, batch/job processing system as the pipeline is initialised. As such, a submitted pipeline does not have a
orchestration script of daemon running: managing the runtime dependencies of jobs within
an instance of a pipeline is delegated to the batch/job processing system.

How is this done? The job representing the start point of a graph
is submitted to LSF, or wr, in a suspended state and is resumed once all other jobs
have been submitted thus ensuring that the execution starts only if all steps
are successfully submitted to LSF, or wr. If an error occurs at any point, all submitted
jobs, apart from the start job, are killed.

## Pipeline Creation

Steps of each of the pipelines and dependencies between the steps are
defined in JSON input files located in data/config_files directory.
The files follow [JSON Graph Format](https://github.com/jsongraph/json-graph-specification)
syntax. Individual pipeline steps are defined as graph nodes, dependencies
between them as directed graph edges. If step B should be executed
after step A finishes, step B is is considered to be dependant on step A.

The graph represented by the input file should be a directed acyclic
graph (DAG). Each graph node should have an id, which should be unique,
and a label, which is the name of the pipeline step.

Parallelisation of processing may be performed at different levels within the DAG: some steps are appropriate for

- per run
- per lane
- per lane and tagged library, or per tagged library
- per tagged library

parallelisation.

### Visualizing Input Graphs

JSON Graph Format (JGF) is relatively new, with little support for visualization.
Convert JGF to GML [Graph Modeling Language](http://www.fim.uni-passau.de/fileadmin/files/lehrstuhl/brandenburg/projekte/gml/gml-technical-report.pdf)
format using a simple script supplied with this package, `scripts/jgf2gml`.
Many graph visualization tools, for example [Cytoscape](http://www.cytoscape.org/),
support the GML format.

## Per Sequencing Run Pipelines

The processing is performed per sequencing run. Many different studies and sequencing assays for different "customers" may be performed on a single run. Unlike contemporary (2020s) sharable bioinformatics  pipelines, the logic for informatics is tied closely to the business logic e.g. what aligner is required with what reference, whether human read separation is required, is determined per indexed library within a lane of sequencing and scheduled for work in parallel.

The information required for the logic is obtained from the upstream "LIMS" via a MLWH (Multi-LIMS warehouse) database and the run folder output by the sequencing instrument.

### Analysis Pipeline

Processes data coming from Illumina sequencing instruments.

The input for an instance of the pipeline is the instrument output run folder (BCL and associated files) and LIMS information which drives appropriate processing.

The key data products are aligned CRAM files and indexes, or unaligned CRAM files. However (per study) configuration allows for the creation of GATK gVCF files, or the running for external tool/pipeline e.g. ncov2012-artic-nf

### Archival Pipeline

Archives sequencing data (CRAM files) and other related artifacts e.g. index files. QC metrics.

### Pipeline Script Outputs

Log file - in the run folder (as in the current pipeline).
Example: `/nfs/sf55/IL_seq_data/outgoing/path_to_runfolder/bin_npg_pipeline_central_25438_20180321-080455-2214166102.log`

File with JSON serialization of definition objects - in the analysis directory directory.
Example: `/path_to_runfolder/bin_npg_pipeline_central_25438_20180321-080455-2214166102.log.json`

File with saved commands hashed by function name, LSF job id and array index - in the analysis directory.
Example: `/path_to_runfolder/Data/Intensities/BAM_basecalls_20180321-075511/bin_npg_pipeline_central_25438_20180321-080455-2214166102.log.commands4jobs.json`

## Dependencies

This software relies heavily on the npg_tracking software to abstract information from the MLWH and instrument runfolder, and coordination of the state of the run.

This software integrates heavily with the npg_qc system for calculating and recording for internal display QC metrics for operational teams to assess the sequencing and upstream processes.

Also, the npg_irods system is essential for the internal archival of data products.
