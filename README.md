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
between them as directed graph edges. The graph represented by an
input file should be a directed acyclic graph (DAG). Each graph node
should have an id, which should be unique, and a label, which is the
name of the pipeline step. 

### Visualizing Input Graphs

JSON Graph Format (JGF) is relatively new, with little support for visualization.
Convert JGF to GML [Graph Modeling Language](http://www.fim.uni-passau.de/fileadmin/files/lehrstuhl/brandenburg/projekte/gml/gml-technical-report.pdf)
format using a simple script supplied with this package, scripts/jgf2gml.
Many graph visualization tools, for example [Cytoscape](http://www.cytoscape.org/),
support the GML format.

### Batch Processing and Dependencies Tracking with LSF

In this package the pipeline steps are submitted for execution to the
LSF batch processing system. Firstly, LSF jobs are defined and submitted
to the LSF job scheduler in a suspended state. Then the jobs are modified
to set the dependencies between them and resumed. The LSF job representing
the start point of a graph is resumed last thus ensuring that all LSF jobs
are properly configured before the execution starts.

 
 

