# Config files for npg_seq_pipeline

Pipeline-wide parameters can be set here

## Inventory
- function_list_*.json

  Graph definition files in JSON Graph Format. These define a network of pipeline functions to be performed in dependent order. We have used the metadata key to define a descriptive name for each node as well as any specific resources required by the function. Read more on resource specifications below.

- function_list_*.png

  Graphical renderings of the pipeline graph to show job interdependencies. Node names refer to entries in npg_pipeline::pluggable::registry

- general_values.ini

  Settings for the pipeline in general that constrain global behaviour, e.g. how many Irods errors to tolerate before stopping execution

- wr.json

  A configuration file specifically for the WR scheduler. WR is used in an Openstack environment so it needs to know what VM flavours it can use.

- lsf.ini

  A configuration file specifically for the LSF scheduler used on seqfarm clusters. This file maps generalised queue names to specific queues used by the scheduler. It also has settings for the maximum job array size to schedule as well as token pool settings used to control parallelism of demanding jobs.

## Format of the function_list_* files

    "graph": {
        "edges": [],
        "nodes": [],
        "metadata": {}
    }

All nodes must be connected via edges. We cannot handle disjoint graphs. Additionally the graph shape should not include cycles and must begin with a "pipeline_start" and end with a "pipeline_end" node.

A node represents a single class of job and the pipeline may create many jobs from a single node. The number of jobs of a given class is defined by the input data. If data precludes the need for a particular function, the node will be automatically removed from the graph at runtime so that no futile jobs are submitted to the scheduler.

    "metadata": {
        "default_resources": {
                "minimum_cpu": 1,
                "memory": 2,
                "array_cpu_limit": 64,
                "nfs_token": 1
            }
    }

The top-level *metadata* field contains pipeline-wide resource defaults that are applied to all jobs. Each node can override these values when they specify the same key in their *resources* block. We specify *array_cpu_limit* even for pipelines that will be run in WR. WR does not understand job arrays and ignores *array_cpu_limit*.

    "nodes": [
        {
            "id": "seq_alignment",
            "label": "seq_alignment",
            "metadata": {
                "resources": {
                    "default": {
                        "minimum_cpu": 12,
                        "maximum_cpu": 16,
                        "memory": 32,
                        "fs_slots_num": 4
                    },
                    "star": {
                        "memory": 38
                    }
                }
            }
        }
    ]

In this example we run sequence alignments with specific resources depending on the situation. The job inherits global resources, then the node-specific defaults, and finally the *star* resources can be applied if the job calls for it. This is an example of having dynamic resources specified for particular jobs in a class.

If resources are supplied, there *must* be something under a *default* block

## Valid resource properties

- apply_array_cpu_limit - array_cpu_limit is not used unless this flag is set
- array_cpu_limit - the maximum number of jobs of this class to run in an array. Allows for efficient job scheduling with LSF
- fs_slots_num - the number of file system tokens this job requires
- maximum_cpu - the most CPUs this job can run with. Depends on the real CPU core counts in the hardware
- memory - maximum memory required in Gibibytes (GiB).
- minimum_cpu - the fewest CPUs this job can run with
- nfs_token - the number of NFS tokens required by the job
- queue - [small, default, lowload, ...] defining which queue the scheduler should use
- reserve_irods_slots - the number of Irods tokens required by the job

Note the number of settings that are required to protect assets from being overloaded with requests. Irods and NFS in particular are vulnerable to saturation that leads to complicated pipeline failures.
