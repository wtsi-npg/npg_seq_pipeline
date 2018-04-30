## Pipelines for Processing Sequencing Data

### Analysis Pipeline

Processes data coming from Illumina sequencing instruments.
Input data - bcl files, output - CRAM files. In most cases CRAM files are aligned.

### Archival Pipeline

Archives sequencing data (CRAM files) and other related artefacts.

### Configuring Pipeline's Steps

Steps of each of the pipelines and dependencies between the steps are
defined in JSON input files located in data/config_files directory.
The files follow [JSON Graph Format](https://github.com/jsongraph/json-graph-specification)
systax. Individual pipeline steps are defined as graph nodes, dependencies
between them as directed graph edges. If step B should be executed
after step A finishes, step B is is considered to be dependant on step A.

The graph represented by the input file should be a directed acyclic
graph (DAG). Each graph node should have an id, which should be unique,
and a label, which is the name of the pipeline step.

### Visualizing Input Graphs

JSON Graph Format (JGF) is relatively new, with little support for visualization.
Convert JGF to GML [Graph Modeling Language](http://www.fim.uni-passau.de/fileadmin/files/lehrstuhl/brandenburg/projekte/gml/gml-technical-report.pdf)
format using a simple script supplied with this package, scripts/jgf2gml.
Many graph visualization tools, for example [Cytoscape](http://www.cytoscape.org/),
support the GML format.

### Pipeline Script Outputs

Log file - in the run folder (as in the current pipeline).
Example: /nfs/sf55/IL_seq_data/outgoing/path_to_runfolder/bin_npg_pipeline_central_25438_20180321-080455-2214166102.log

File with JSON serialization of definition objects - in the analysis directory directory.
Example: /path_to_runfolder/bin_npg_pipeline_central_25438_20180321-080455-2214166102.log.json

File with saved commands hashed by function name, LSF job id and array index - in the analysis directory.
Example: /path_to_runfolder/Data/Intensities/BAM_basecalls_20180321-075511/bin_npg_pipeline_central_25438_20180321-080455-2214166102.log.commands4jobs.json

### Batch Processing and Dependencies Tracking with LSF

In this package the pipeline steps are submitted for execution to the
LSF batch processing system. The LSF job representing the start point of a graph
is submitted to LSF in a suspended state and is resumed once all other LSF jobs
have been submitted thus ensuring that the execution starts only if all steps
are successfully submitted to LSF. If an error occurs at any point, all submitted
jobs, apart from the start job, are killed.

 
 

