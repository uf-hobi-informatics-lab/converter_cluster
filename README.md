# OneFlorida+ Cluster

## Introduction

The OneFlorida+ Cluster is a virtualized cluster handler that uses custom software to dynamically instantiate clusters, submit jobs to the clusters, and clean up these clusters. This software leverages a custom Docker image, Docker compose, and PySpark to create an ultra fast parallel processing system, that is fully customizable and easily automatable through a command-line interface. This system was developed by Jason Glover. <br>

## Using the Cluster

### Commands

All commands to the cluster should be preceded by the keyword `cluster`.

#### 1. `run`

This should be the primary command send to the cluster. Calling `run` boots a cluster, submits a specified file, and then shuts down the cluster.

**Accepted Flags:**

- `-d`: The data directory, where the data to be processed is stored. Default is the current working directory.
- `-w`: The work directory, where the scripts to submit to the cluster are located. Default is the current working directory.
- `-l`: Set the desired log level. Default is 'INFO'.

**Syntax:**
`cluster run [-h] [-d DATADIR] [-w WORKDIR] [-l LOG] file [args ...]`
<br>
#### 2. `boot`

Instantiate a cluster that sits in the 'FREE' state.

**Accepted Flags:**

- `-n`: Set a custom name for the cluster.
- `-d`: The data directory, where the data to be processed is stored. Default is the current working directory.
- `-w`: The work directory, where the scripts to submit to the cluster are located. Default is the current working directory.

**Syntax:**
cluster boot [-h] [-n NAME] [-d DATADIR] [-w WORKDIR]
<br>
#### 3. `submit`

Submit a file to a free cluster.

**Accepted Flags:**

- `-s`: The session ID of the cluster you want to submit to. Obtain this ID from the 'cluster status' command.
- `-d`: The data directory, where the data to be processed is stored. Default is the current working directory.
- `-w`: The work directory, where the scripts to submit to the cluster are located. Default is the current working directory.

**Syntax:**
cluster submit [-h] -s SESSIONID [-d DATADIR] [-w WORKDIR] file [args ...]
<br>
#### 4. `shutdown`

Shut down a specified cluster. By default, only clusters in the 'FREE' state can be shut down.

**Accepted Flags:**

- `-f`: Force a running cluster to shut down. Use this flag cautiously; it's generally not recommended unless there's a runtime issue.

**Note**: When invoking 'cluster shutdown -h', users will observe a '-c' flag. This is a planned feature that has not been implemented yet. Please disregard for the time being.

**Syntax:**
cluster shutdown [-h] [-f] session_id
<br>
#### 5. `status`

Display a list of the currently instantiated clusters with relevant details.

**Accepted Flags:**

- N/A

**Syntax:**
cluster status [-h]
<br>
## Conclusion

Make sure to check the syntax and use the appropriate flags for the desired operation. Understanding the core functionalities will ensure smooth operation with the cluster command-line interface.

## Author

Jason Glover <br>
Contact: jasonglover@ufl.edu <br>
Last Updated: 08/16/2023