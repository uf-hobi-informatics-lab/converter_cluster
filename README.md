# OneFlorida+ Cluster

The OneFlorida+ Cluster is a virtualized cluster handler that uses custom software to dynamically instantiate clusters, submit jobs to the clusters, and clean up these clusters. This software leverages a custom Docker image, Docker compose, and PySpark to create an ultra fast parallel processing system, that is fully customizable and easily automatable through a command-line interface. This system was developed by Jason Glover. <br>

## Using the Cluster

### Terminology

1. cluster:
2. session_id: The unique identifier of a cluster. This will always be in the format [NAME]\_[PROCESS_ID]\_[YYYYMMDD]
3. state: The current status of the cluster in question. A cluster

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

### Examples of comands

Let's assume we are working with a file called `foo.py` that takes args `arg1`, `arg2`, `arg3` and our cluster has `session_id`: var_12345_20230816

## Author

Jason Glover <br>
Contact: jasonglover@ufl.edu <br>
Last Updated: 08/16/2023