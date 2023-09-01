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
import info.cluster_status as stat
from datetime import timedelta
from datetime import datetime


#======== Globals (Reference variables) ==========
ascii_art='''
 ██████╗ ███╗   ██╗███████╗███████╗██╗          ██████╗██╗     ██╗   ██╗███████╗████████╗███████╗██████╗ 
██╔═══██╗████╗  ██║██╔════╝██╔════╝██║         ██╔════╝██║     ██║   ██║██╔════╝╚══██╔══╝██╔════╝██╔══██╗
██║   ██║██╔██╗ ██║█████╗  █████╗  ██║         ██║     ██║     ██║   ██║███████╗   ██║   █████╗  ██████╔╝
██║   ██║██║╚██╗██║██╔══╝  ██╔══╝  ██║         ██║     ██║     ██║   ██║╚════██║   ██║   ██╔══╝  ██╔══██╗
╚██████╔╝██║ ╚████║███████╗██║     ███████╗    ╚██████╗███████╗╚██████╔╝███████║   ██║   ███████╗██║  ██║
 ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚═╝     ╚══════╝     ╚═════╝╚══════╝ ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝╚═╝  ╚═╝
                                                                                                       
'''

absolute_cluster_path='/onefl_cluster'

logger = logging.getLogger()

valid_partners = {'AVH', 
    'BND',
    'CHM',
    'CHR',
    'EMY',
    'NCH',
    'ORL',
    'TMA',
    'TMC',
    'UAB',
    'UFH',
    'UMI',
    'USF'}

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


def make_cluster(session_id, cluster_path, datadir, workdir):
    #Dynamically build the docker compose file on boot with appropriate mount dirs
    #The text variable looks this way because .yml files are super picky about white space so the formatting of this string has to be precise
    text = '''
version: "3.8"

services:
  master:
    image: 'glove-cluster:latest'
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - {}:/app
      - {}:/data
    networks:
      - pyspark_cluster_network_{}

  worker:
    image: 'glove-cluster:latest'
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://master:7077
      - SPARK_WORKER_MEMORY=8g
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - {}:/app
      - {}:/data
    depends_on:
      - master
    networks:
      - pyspark_cluster_network_{}

networks:
    pyspark_cluster_network_{}:
        name: {}_pyNet
'''.format(workdir,datadir,session_id,workdir,datadir,session_id,session_id,session_id)
    
    cf = open(cluster_path, 'w')
    num_written = cf.write(text) 
    if num_written != len(text):
        logger.error('Failed to write {}.yml at {}. Exiting'.format(session_id,cluster_path))
        quit(-1)
    else:
        logger.info('Successfully wrote {} to {}'.format(session_id,cluster_path))
    

# Build the submit statement and submit
def spark_submit(session_id, workdir, datadir, script_name, args, ali=False):
    if stat.is_valid_id(session_id)==False:
        logger.error('The requested cluster, {}, could not be found. Exiting.'.format(session_id))
        quit(-1)
    else:
        state = stat.get_state(session_id)
    if state=='Free':
        if not ali:
            submit_statement= '''
                docker run \
                --network={}_pyNet \
                -v {}:/app \
                -v {}:/data \
                --name {}_submitter \
                --rm glove-cluster \
                /opt/bitnami/spark/bin/spark-submit \
                --conf "spark.pyspark.python=python3" \
                --conf "spark.driver.memory=16g" \
                --conf "spark.executor.memory=8g" \
                --master spark://master:7077 \
                --deploy-mode client \
                --name my_pyspark_job /app/{} {}'''.format(session_id, workdir, datadir, session_id, script_name, args)
        if ali:
            submit_statement= '''
                docker run \
                --network={}_pyNet \
                -v {}:/app \
                -v {}:/data \
                --name {}_submitter \
                --rm glove-cluster \
                /opt/bitnami/spark/bin/spark-submit \
                --conf "spark.pyspark.python=python3" \
                --conf "spark.driver.memory=16g" \
                --conf "spark.executor.memory=8g" \
                --master spark://master:7077 \
                --deploy-mode client \
                --py-files /app/common/* \
                --jars mssql-jdbc-driver.jar\
                --name my_pyspark_job /app/{} {}'''.format(session_id, workdir, datadir, session_id, script_name, args)
        
        #Update the status to reflect running
        stat.update_cluster(session_id, 'Running', 'SUBMIT')

        #Submit the statement to the cluster
        os.system(submit_statement)

        #Update status to free after runninng
        # Format the time
        stat.update_cluster(session_id, 'Free', 'SUBMIT', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        logger.error("Cannot submit job to {}. Cluster is currently {}. Exiting".format(session_id,state.lower()))
        quit(-1)

def boot_cluster(session_id, cluster_path, datadir, workdir):
    #Create the session's cluster
    logger.info('Creating the cluster directory for {}'.format(session_id))
    make_cluster(session_id, cluster_path, datadir, workdir)

    #boot up the cluster
    logger.info('Booting the cluster for {}'.format(session_id))
    stat.add_cluster(session_id, 'Booting', 'BOOT')
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
        

def main():
    
    
    
    #Pull user's id for config operations
    #uid = os.getuid()


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
    '''
    run_parse.add_argument(
        '-p', '--partner',
        default='.',
        required=True,
        help='Three letter partner name'
    )
    '''
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
        '-a', '--ali',
        action='store_true',
        required=False,
        help='Select separate submit statement for Ali testing')
    #run_parse.add_argument(
    #    '-c', '--cfg', 
    #    action='store_true', 
    #    required=False,
    #    help='Run the cluster based on the user\'s predefined config file. User can set their config file using the \'cluster config --init\' command')
    run_parse.add_argument(
        '-l', '--log',
        default='INFO',
        help='Set the logging level (DEBUG, INFO, WARN, ERROR, CRITICAL)')

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

    # Parse the arguments
    args = parser.parse_args()
####################################################


####################################################
#                                                  #
#          Set environment configuration           #
#                                                  #                                                  
####################################################

#========= VARIABLES =======
    if args.command!='status':
        #We need to filter the status command out because session_id is not created for it, plus we don't need logging for status
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
    if args.command!='status':
        #Same as variables
        if args.command=='run':
            log_level = args.log.upper()
        else:
            log_level = 'INFO'

        logger.setLevel(valid_log_levels[log_level])  

        # Create a file handler
        file_handler = logging.FileHandler('{}.log'.format(session_id), 'w') 
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
        print(ascii_art)
        
        #Write cluster settings to the log
        logger.info('Initializing cluster with the following settings:')
        logger.info("Session ID: {}".format(session_id))
        logger.info("Data Dir: {}".format(args.datadir))
        logger.info("Work Dir: {}".format(args.workdir))
        logger.info("File: {}".format(args.file))
        logger.info("Args: {}".format(args.args))

       
        #Boot Cluster
        start = time.time()
        boot_cluster(session_id, cluster_path, args.datadir, args.workdir)

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
        boot_cluster(session_id, cluster_path, args.datadir, args.workdir)

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


if __name__=='__main__':
    main()
