#!/usr/bin/env python3

"""
    Script to handle all of the command line arguments 


----- update the read me - sudo CHMOD    not sudo chown


    To - Dos/POssible things:

    - Add better error handling for config file 
    - force user to perform first time setup of config
    - set default run mode to be config (maybe?)
    - add additional options to config
            * Set logging level
            * Set file output format - separated files? one big file?
            * 
    - format run flags to override config commands



    - FIX ARGS PASSING
"""
import argparse
import os
import logging
import time
import math
import pwd
import json
import info.cluster_status as stat
from datetime import timedelta
from datetime import datetime
from filelock import SoftFileLock


#============ MEMORY ALLOCATION PARAMETERS ============
MAX_MASTER_MEM = 8
MAX_WORKER_MEM = 16
DEFAULT_MASTER_MEM = 1
DEFAULT_WORKER_MEM = 5
#======================================================


#======== Globals (Reference variables) ==========
ascii_art='''
 ██████╗ ███╗   ██╗███████╗███████╗██╗          ██████╗██╗     ██╗   
██╗███████╗████████╗███████╗██████╗ 
██╔═══██╗████╗  ██║██╔════╝██╔════╝██║         ██╔════╝██║     ██║   
██║██╔════╝╚══██╔══╝██╔════╝██╔══██╗
██║   ██║██╔██╗ ██║█████╗  █████╗  ██║         ██║     ██║     ██║   ██║███████╗   ██║   █████╗  ██████╔╝
██║   ██║██║╚██╗██║██╔══╝  ██╔══╝  ██║         ██║     ██║     ██║   ██║╚════██║   ██║   ██╔══╝  ██╔══██╗
╚██████╔╝██║ ╚████║███████╗██║     ███████╗    ╚██████╗███████╗╚██████╔╝███████║   ██║   
███████╗██║  ██║
 ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚═╝     ╚══════╝     ╚═════╝╚══════╝ ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝╚═╝  
╚═╝
                                                                                                       
'''

absolute_cluster_path='[CHANGE ME]'

logger = logging.getLogger()

valid_log_levels = {
    'DEBUG':logging.DEBUG,  
    'INFO':logging.INFO,
    'WARN':logging.WARN,
    'ERROR':logging.ERROR,
    'CRITICAL':logging.CRITICAL
}

#========= Functions =========
def set_path(session_id):
    #set the path of the docker compose yml for the requested partner
    if not os.path.exists('{}/clusters/{}'.format(absolute_cluster_path,session_id)):
        os.mkdir('{}/clusters/{}'.format(absolute_cluster_path, session_id))
    return '{}/clusters/{}/docker-compose.yml'.format(absolute_cluster_path, session_id)


def arr_to_str(array):
    #Handles the submitted file's command line arguments
    return ' '.join(array)


def set_session_id(file):
    #Build a unique session id for the process for logging and cluster construction
    file_name, _ = os.path.splitext(file)
    return '{}_{}_{}'.format(file_name, os.getpid(), datetime.now().strftime("%Y%m%d"))


def make_cluster(session_id, cluster_path, datadir, workdir, outdir):
    #Dynamically build the docker compose file on boot with appropriate mount dirs

    #Get the desired worker memory allocation
    with SoftFileLock(f'{absolute_cluster_path}/hardware_config.lock', timeout=10):
        with open(f'{absolute_cluster_path}/hardware_config.json','r') as f:
            memory = json.load(f)
    worker_mem = memory['hardware']['memory_worker']
    master_mem = memory['hardware']['memory_master']



    #The text variable looks this way because .yml files are super picky about white space so the formatting of this string has to be precise
    text = f'''
version: "3.8"

services:
  master:
    image: 'onefl-cluster-image:latest'
    mem_limit: {master_mem}
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - {workdir}:/app
      - {datadir}:/data
      - {outdir}:/output
    networks:
      - pyspark_cluster_network_{session_id}

  worker:
    image: 'onefl-cluster-image:latest'
    mem_limit: {worker_mem}
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://master:7077
      - SPARK_WORKER_MEMORY={worker_mem}
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - {workdir}:/app
      - {datadir}:/data
      - {outdir}:/output
    depends_on:
      - master
    networks:
      - pyspark_cluster_network_{session_id}

networks:
    pyspark_cluster_network_{session_id}:
        name: {session_id}_pyNet
'''
    cf = open(cluster_path, 'w')
    num_written = cf.write(text) 
    if num_written != len(text):
        logger.error('Failed to write {}.yml at {}. Exiting'.format(session_id,cluster_path))
        quit(-1)
    else:
        logger.info('Successfully wrote {} to {}'.format(session_id,cluster_path))
    

# Build the submit statement and submit
def spark_submit(session_id, workdir, datadir, script_name, args, outdir, ali=False):

    #Get the desired worker memory allocation
    with SoftFileLock(f'{absolute_cluster_path}/hardware_config.lock', timeout=10):
        with open(f'{absolute_cluster_path}/hardware_config.json','r') as f:
            memory = json.load(f)
    worker_mem = memory['hardware']['memory_worker']
    master_mem = memory['hardware']['memory_master']

    if stat.is_valid_id(session_id)==False:
        logger.error('The requested cluster, {}, could not be found. Exiting.'.format(session_id))
        quit(-1)
    else:
        state = stat.get_state(session_id)
    if state=='Free':
        if not ali:
            submit_statement= f'''
                docker run \
                --memory=500m \
                --network={session_id}_pyNet \
                -v {workdir}:/app \
                -v {datadir}:/data \
                -v {outdir}:/output \
                --name {session_id}_submitter \
                --rm onefl-cluster-image \
                /opt/bitnami/spark/bin/spark-submit \
                --conf "spark.pyspark.python=python3" \
                --conf "spark.driver.memory={master_mem}" \
                --conf "spark.executor.memory={worker_mem}" \
                --master spark://master:7077 \
                --deploy-mode client \
                --name my_pyspark_job /app/{script_name} {args}'''
        if ali:
            submit_statement= f'''
                docker run \
                --memory=500m \
                --network={session_id}_pyNet \
                -v {workdir}:/app \
                -v {datadir}:/data \
                -v {outdir}:/output \
                --name {session_id}_submitter \
                --rm onefl-cluster-image \
                /opt/bitnami/spark/bin/spark-submit \
                --conf "spark.pyspark.python=python3" \
                --conf "spark.driver.memory={master_mem}" \
                --conf "spark.executor.memory={worker_mem}" \
                --master spark://master:7077 \
                --deploy-mode client \
                --py-files /app/common/* \
                --jars mssql-jdbc-driver.jar\
                --name my_pyspark_job /app/{script_name} {args}'''
        
        #Update the status to reflect running
        stat.update_cluster(session_id, 'Running', 'SUBMIT')

        #Submit the statement to the cluster
        os.system(submit_statement)

        #Update status to free after runninng
        # Format the time
        stat.update_cluster(session_id, 'Free', 'SUBMIT', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        if ali:
            os.system('docker run --network=\'host\' --rm -v $(pwd):/app onefl-cluster-image /opt/bitnami/spark/bin/spark-submit /app/email_app.py --docker True')

    else:
        logger.error("Cannot submit job to {}. Cluster is currently {}. Exiting".format(session_id,state.lower()))
        quit(-1)

def boot_cluster(session_id, cluster_path, username, datadir, workdir):

    #Create the session's cluster
    logger.info('Creating the cluster directory for {}'.format(session_id))
    make_cluster(session_id, cluster_path, datadir, workdir)

    #boot up the cluster
    logger.info('Booting the cluster for {}'.format(session_id))
    stat.add_cluster(session_id, 'Booting', 'BOOT', username)
    os.system('docker compose -f {} up -d --scale worker=5'.format(cluster_path))

    #Update the cluster status
    stat.update_cluster(session_id, 'Free')

def shutdown_cluster(session_id, cluster_path, override=False):
    #state = check_status(session_id)

    if stat.is_valid_id(session_id)==False:
        logger.error('The requested cluster, {}, could not be found. Exiting.'.format(session_id))
        quit(-1)
    else:
        state = stat.get_state(session_id)
    if state=='Running':
        if override==False:
            logger.error('Unable to shutdown {}. Cluster is currently running. Pass the \'-f\' flag to force shut down on this cluster. Exiting'.format(session_id))
            quit(-1)
        elif override:
            logger.info('Shutting down cluster with session id: {}'.format(session_id))
            stat.update_cluster(session_id, 'Shutting Down')
            os.system('docker compose -f {} down'.format(cluster_path))
            logger.info('Removing cluster directory for {}'.format(session_id))
            os.remove(cluster_path)
            os.rmdir('{}/clusters/{}'.format(absolute_cluster_path, session_id))

            stat.remove_cluster(session_id)
    if state=='Booting':
        logger.error('Unable to shutdown {}. Cluster is currently booting. Exiting'.format(session_id))
        quit(-1)
    if state=='Shutting Down':
        logger.error('Unable to shutdown {}. Cluster is already shutting down. Exiting'.format(session_id))
        quit(-1)
    if state =='Free':
        logger.info('Shutting down cluster with session id: {}'.format(session_id))
        stat.update_cluster(session_id, 'Shutting Down')
        os.system('docker compose -f {} down'.format(cluster_path))
        logger.info('Removing cluster directory for {}'.format(session_id))
        os.remove(cluster_path)
        os.rmdir('{}/clusters/{}'.format(absolute_cluster_path, session_id))

        stat.remove_cluster(session_id)
        

def update_hardware(node_type, value):
    #Utility function for updating the memory allocationg config of the cluster using the 'set' command
    with SoftFileLock(f'{absolute_cluster_path}/hardware_config.lock', timeout=10):
        with open(f'{absolute_cluster_path}/hardware_config.json','r') as f:
            hardware = json.load(f)
    if node_type=='master':
        hardware['hardware']['memory_master'] = '{}g'.format(str(value))
    if node_type=='worker':
        hardware['hardware']['memory_worker'] = '{}g'.format(str(value))
    with SoftFileLock(f'{absolute_cluster_path}/hardware_config.lock', timeout=10):
        with open(f'{absolute_cluster_path}/hardware_config.json', 'w') as f:
            json.dump(hardware, f)

def main():
    
    
    #Get username calling the script for status (Implementation pulled from ChatGPT)
    user_id = os.geteuid()
    username = pwd.getpwuid(user_id).pw_name

####################################################
#                                                  #
#      Declare and define parsing operations       #
#                                                  #                                                  
####################################################
    # Create the top-level parser
    parser = argparse.ArgumentParser(prog='cluster')
    subparsers = parser.add_subparsers(dest='command')


    # create second level commands
    #Config commands
    '''
    config_parse = subparsers.add_parser('config', help='config help')
    config_sub = config_parse.add_subparsers(dest='config_command')
    cat_parse = config_sub.add_parser('cat', help='cat help')
    init_parse = config_sub.add_parser('init', help='init help')
    '''

    #Run commands
    run_parse = subparsers.add_parser('run', help='Command for running the boot, submit, and shut down processes of the cluster')
    # Add flags for the 'run' command
    run_parse.add_argument(
        '-d','--datadir',
        default=os.getcwd(),
        required=False,
        help='Directory containing data to be processed. If not passed, output is put in the working directory.'
    )
    run_parse.add_argument(
        '-w','--workdir',
        default=os.getcwd(),
        required=False,
        help='Directory containing the scripts to be submitted to the cluster. By default, this is the directory the command was called from.'
    )

    run_parse.add_argument(
        '-o','--outbox',
        default=os.getcwd(),
        required=False,
        help='Directory to output data to. By default, this is the working directory.'
    )
    run_parse.add_argument(
        '-a', '--ali',
        action='store_true',
        required=False,
        help='Select separate submit statement for Ali testing'
    )
    run_parse.add_argument(
        '-l', '--log',
        default='INFO',
        help='Set the logging level (DEBUG, INFO, WARN, ERROR, CRITICAL)'
    )

    #Handle the passed in file name and arguments
    run_parse.add_argument('file', type=str, default='x', help='The file to run.')
    run_parse.add_argument('args', type=str, nargs='*', default=[], help='The arguments for the file.')
#=========== END OF RUN_PARSE STATEMENTS ============

#============= BOOT_PARSE ===============
    boot_parse = subparsers.add_parser('boot', help='Command to spin up a cluster')
    boot_parse.add_argument(
        '-n','--name',
        default='boot',
        required=False,
        help='Pass in a unique name for the cluster'
    )
    boot_parse.add_argument(
        '-d','--datadir',
        default=os.getcwd(),
        required=False,
        help='Directory containing data to be processed. If not passed, output is put in the working directory.'
    )
    boot_parse.add_argument(
        '-w','--workdir',
        default=os.getcwd(),
        required=False,
        help='Directory containing the scripts to be submitted to the cluster. By default, this is the directory the command was called from.'
    )
    boot_parse.add_argument(
        '-o','--outbox',
        default=os.getcwd(),
        required=False,
        help='Directory to output data to. By default, this is the working directory.'
    )
#========== END OF BOOT_PARSE STATEMENTS =========

#=========== SUBMIT_PARSE ============
    submit_parse = subparsers.add_parser('submit', help='Command to submit a job to an existing cluster')
    submit_parse.add_argument(
        '-s','--sessionid',
        default='',
        required=True,
        help='Pass in the unique session identifier of the cluster you want to run on. Call \'cluster status\' to see freed clusters'
    )
    submit_parse.add_argument(
        '-d','--datadir',
        default=os.getcwd(),
        required=False,
        help='Directory containing data to be processed. If not passed, output is put in the working directory.'
    )
    submit_parse.add_argument(
        '-w','--workdir',
        default=os.getcwd(),
        required=False,
        help='Directory containing the scripts to be submitted to the cluster. By default, this is the directory the command was called from.'
    )
    submit_parse.add_argument(
        '-o','--outbox',
        default=os.getcwd(),
        required=False,
        help='Directory to output data to. By default, this is the working directory.'
    )
    submit_parse.add_argument('file', type=str, default='x', help='The file to run.')
    submit_parse.add_argument('args', type=str, nargs='*', default=[], help='The arguments for the file.')
#=========== END OF SUBMIT_PARSE =========

#=========== SHUTDOWN_PARSE ===========
    #Shutsdown one or more cluster depending on additional flags
    shutdown_parse = subparsers.add_parser('shutdown', help='Command to shutdown one or more clusters')

    shutdown_parse.add_argument(
        '-c','--clean',
        action='store_true',
        required=False,
        help='Remove all free clusters'
    )
    shutdown_parse.add_argument(
        '-f','--force',
        action='store_true',
        required=False,
        help='Force a running cluster to terminate'
    )

    shutdown_parse.add_argument('session_id', type=str, default='', help='The session id to shutdown')
#=========== END OF SHUTDOWN_PARSE =========

#========== STATUS_PARSE ===============
    status_parse = subparsers.add_parser('status',help='Command to shutdown one or more clusters')
    
#========== END OF STATUS_PARSE ==========

#========== HARDWARE_CONFIG_PARSE ===========
    config_parse = subparsers.add_parser('set', help='Command for adjusting the memory allocation of the master and worker nodes')
    sub_config_parse = config_parse.add_subparsers(dest='subcommand')

    master = sub_config_parse.add_parser('master', help='Denote the master node for memory allocation')
    master.add_argument('value', type=str,default='', help='An integer value for the number of gigabytes of memory to allocate to the master node')

    worker = sub_config_parse.add_parser('worker', help='Denote the worker node for memory allocation')
    worker.add_argument('value', type=str,default='', help='An integer value for the number of gigabytes of memory to allocate to the worker nodes')

    default = sub_config_parse.add_parser('default', help='Reset the memory allocation of the cluster to default parameters')


#========== END OF HARDWARE_CONFIG_PARSE

    # Parse the arguments
    args = parser.parse_args()
####################################################


####################################################
#                                                  #
#          Set environment configuration           #
#                                                  #                                                  
####################################################
#========= VARIABLES =======
    if args.command!='status' and args.command!='set':
        #We need to filter the status command out because session_id is not created for it, plus we don't need logging for status
        #Same for the set command
        if args.command=='run':
            session_id = set_session_id(args.file)
        elif args.command=='boot':
            session_id = set_session_id(args.name)
        elif args.command=='submit':
            session_id = args.sessionid
        elif args.command=='shutdown' or args.command=='kill':
            session_id = args.session_id
        cluster_path = set_path(session_id)
#===========================


#========= LOGGING =========
    #Create logger and set the logger configuration
    if args.command!='status' and args.command!='set':
        #Same as variables
        if args.command=='run':
            log_level = args.log.upper()
        else:
            log_level = 'INFO'

        logger.setLevel(valid_log_levels[log_level])  

        #Set a logging subdirectory and check if it exists
        log_directory = 'cluster_logs'
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        # Define the log file's path within the newly ensured directory
        log_file_path = os.path.join(log_directory, '{}.log'.format(session_id))

        # Create a file handler
        file_handler = logging.FileHandler(log_file_path, 'w') 
        file_handler.setLevel(valid_log_levels[log_level]) 

        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(valid_log_levels[log_level])

        # Create a formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    #===========================


##################################################

    # Check what command was given
    if args.command == 'run':
        with open('hardware_config.json','r') as f:
            memory = json.load(f)
        worker_mem = memory['hardware']['memory_worker']
        master_mem = memory['hardware']['memory_master']
        print(ascii_art)
        
        #Write cluster settings to the log
        logger.info('Initializing cluster with the following settings:')
        logger.info("Session ID: {}".format(session_id))
        logger.info("Data Dir: {}".format(args.datadir))
        logger.info("Work Dir: {}".format(args.workdir))
        logger.info("File: {}".format(args.file))
        logger.info("Args: {}".format(args.args))
        logger.info("Master memory: {}".format(master_mem))
        logger.info("Worker memory: {}".format(worker_mem))
        

       
        #Boot Cluster
        start = time.time()
        boot_cluster(session_id, cluster_path, username, args.datadir, args.workdir)

        #Submit to cluster
        if args.ali==True:
            spark_submit(session_id, args.workdir, args.datadir, args.file, arr_to_str(args.args), True)
        else:
            spark_submit(session_id, args.workdir, args.datadir, args.file, arr_to_str(args.args))

        #Shutdown cluster
        shutdown_cluster(session_id, cluster_path)
        end = time.time()
        run_time = end - start

        #Format the time output
        hours, rem = divmod(run_time, 3600)
        minutes, seconds = divmod(rem, 60)
        formatted_time = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
        logger.info('Processed finished in {}'.format(formatted_time))
    
    if args.command=='boot':
        boot_cluster(session_id, cluster_path, username, args.datadir, args.workdir)

    if args.command=='submit':
        logger.info('Submitting job {} to cluster with session ID: {}'.format(args.file, session_id))
        logger.info('Handing off logging to the Spark environment')
        spark_submit(session_id, args.workdir, args.datadir, args.file, arr_to_str(args.args))

    if args.command=='shutdown':
        if args.force:
            os.system('docker kill {}_submitter'.format(session_id))
            shutdown_cluster(session_id,cluster_path,True)
        else:
            shutdown_cluster(session_id, cluster_path)
        
    if args.command=='status':
        stat.print_status()

    if args.command=='set':
        if args.subcommand=='master':
            amt_mem = -1
            try:
                amt_mem = int(args.value)
            except Exception as e:
                print("ERROR: Please enter an integer value.")

            if amt_mem==-1:
                quit(-1)
            elif amt_mem >= MAX_MASTER_MEM:
                print(f'Woah there! That\'s a lot of memory you\'re allocating. The master node should never be allocated more than {MAX_MASTER_MEM} of memory. Try again.')
                quit()
            else:
                update_hardware('master',amt_mem)

        elif args.subcommand=='worker':
            amt_mem = -1
            try:
                amt_mem = int(args.value)
            except Exception as e:
                print("ERROR: Please enter an integer value.")

            if amt_mem==-1:
                quit(-1)
            elif amt_mem >= MAX_WORKER_MEM:
                print(f'Woah there! That\'s a lot of memory you\'re allocating. The master node should never be allocated more than {MAX_WORKER_MEM} of memory. Remember that 5 
workers are instantiated at a time, so you\'re actually allocating {str(amt_mem*5)} gb of memory to the cluster. Try again.')
                quit()
            else:
                update_hardware('worker',amt_mem)
        elif args.subcommand=='default':
            update_hardware('master', DEFAULT_MASTER_MEM)
            update_hardware('worker', DEFAULT_WORKER_MEM)
        else:
            print('yuh wrong!')
if __name__=='__main__':
    main()

