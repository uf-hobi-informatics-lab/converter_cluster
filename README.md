# OneFlorida+ Cluster

The OneFlorida+ Cluster is a virtualized cluster handler that uses custom software to dynamically instantiate clusters, submit jobs to the clusters, and clean up these clusters. This software leverages a custom Docker image, Docker compose, and PySpark to create an ultra fast parallel processing system, that is fully customizable and easily automatable through a command-line interface. This system was developed by Jason Glover. <br>

## Installing the cluster

1. Navigate to the root of your system using `cd /`.
> **Note**: This package assumes the repo will be cloned in the root. If you decide to install the package in a different spot, the package will have to be refactored accordingly.
2. In this location, clone the repo by calling 
        
        git clone https://github.com/uf-hobi-informatics-lab/converter_cluster.git
3. User will have received the custom image through OneFlorida+ directly. Copy this file to `/converter_cluser/image_files/`.
3. Navigate into the repo with `cd /converter_cluster` and run 

        sudo python3 install.py
> **Note**: This script assumes you have Docker installed. Please install Docker first if you have not already.
> **Note**: 'sudo' permissions are used to ensure that calling 'chmod' on the repo is allowed.
4. You can verify that everything has been installed correctly by using the `verify_cluster.py` script, located at the top level of the repo. From here, run

        cluster run verify_cluster.py
    The script has run correctly if in the output there is a line that reads 'Cluster is running as expected.'

## Using the Cluster

### Terminology

1. `cluster`: A collection of nodes that processes the passed in program
2. `node`: A component of the cluster. There are three types: Worker - processes the tasks assigned by the master node, Master/Executor - the scheduler which delegates tasks to the worker nodes, Submitter - sends the passed in script to the master node for processing
3. `session_id`: The unique identifier of a cluster. This will always be in the format [NAME]\_[PROCESS_ID]\_[YYYYMMDD]
4. `state`: The current status of the cluster in question. A cluster can be 'Free' - Not currently running a job, 'Running' - currently running a job, 'Booting' - The nodes and network for the cluster are being initialized, 'Shutting Down' - The hardware allocated for the nodes and network are being freed.  

### Commands

All commands to the cluster should be preceded by the keyword `cluster`.

#### 1. run

This should be the primary command send to the cluster. Calling `run` boots a cluster, submits a specified file, and then shuts down the cluster. This command requires a file for input and takes in any args that file may have. Since a file must be passed in for this command, the cluster will be named after the passed in file name.

*Accepted Flags:*

- `-d`: The data directory, where the data to be processed is stored. Default is the current working directory.
- `-w`: The work directory, where the scripts to submit to the cluster are located. Default is the current working directory.
- `-l`: Set the desired log level. Default is 'INFO'. Valid values: 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'
- `-h`: Help.

*Syntax:*

    $  cluster run [-h] [-d DATADIR] [-w WORKDIR] [-l LOG] file [args ...]

> **Note**: If the file you are passing in takes its own command line args, you need to add -- before the file name and its args. 
Ex: I want to run foo.py which takes -p as a flag.

        Wrong: cluster run -d /some/path foo.py -p

        Right: cluster run -d /some/path -- foo.py -p

#### 2. boot

Instantiate a cluster that sits in the 'FREE' state.

*Accepted Flags:*

- `-n`: Set a custom name for the cluster. Default value is 'boot'.
- `-d`: The data directory, where the data to be processed is stored. Default is the current working directory.
- `-w`: The work directory, where the scripts to submit to the cluster are located. Default is the current working directory.
- `-h`: Help.

*Syntax:*

    $ cluster boot [-h] [-n NAME] [-d DATADIR] [-w WORKDIR]

#### 3. submit

Submit a file to a free cluster.

*Accepted Flags:*

- `-s`: [REQUIRED] The session ID of the cluster you want to submit to. User may obtain this ID from the 'cluster status' command.
- `-d`: The data directory, where the data to be processed is stored. Default is the current working directory.
- `-w`: The work directory, where the scripts to submit to the cluster are located. Default is the current working directory.
- `-h`: Help.

*Syntax:*

    $ cluster submit [-h] -s session_id [-d DATADIR] [-w WORKDIR] file [args ...]

#### 4. shutdown

Shut down a specified cluster. By default, only clusters in the 'FREE' state can be shut down.

*Accepted Flags:*

- `-f`: Force a running cluster to shut down. Use this flag cautiously; it's generally not recommended unless there's a runtime issue.
- `-h`: Help.

> **Note**: When invoking `cluster shutdown -h`, users will observe a '-c' flag. This is a planned feature that has not been implemented yet. Please disregard for the time being.

*Syntax:*

    $ cluster shutdown [-h] [-f] session_id

#### 5. status

Display a list of the currently instantiated clusters with relevant details.

*Accepted Flags:*

- `-h`: Help.

*Syntax:*

    $ cluster status [-h]

*Example output of `cluster status`:*

    CLUSTER                                       STATE           LAST COMMAND    LAST RUN           
    -------------------------------------------------------------------------------------------------
    foo_1800_20230816                             Free            SUBMIT          2023-08-16 15:44:05
    var_3883_20230816                             Shutting Down   BOOT
    IAmACluster_5889_20230816                     Free            SUBMIT          2023-08-16 15:42:29
    go_gators_7764_20230816                       Free            BOOT
    ad_merge_test_18040_20230816                  Running         SUBMIT



## Developing for the Cluster

1. For any operations involving the Spark environment, the script must include a call to the `SparkSession.builder`, to create the Spark session. Here is where the master node location will be set. In order to submit the script to the cluster, you must pass `spark://master:7077` to the master parameter. Example of this can be found in `verify_cluster.py`.

2. When developing programs to run against the cluster, keep the following in mind. Since the python script being submitted to the cluster is run in a Docker container, hardcoded directories on your host machine will throw an error. The `/app` directory in the container is mounted to the repo on the host machine, this is where output or input should be written to. For example: I want to write the output `result` from the cluster. My repo is located at `/Users/jason/code/cluster` on my host machine. Thus, `/app` is mounted to `/Users/jason/code/cluster`. In my python script, I would write the output to `/app/result`, not `/Users/jason/code/cluster/result`.

## Author

Jason Glover <br>
Contact: jasonglover@ufl.edu <br>
Last Updated: 09/01/2023
