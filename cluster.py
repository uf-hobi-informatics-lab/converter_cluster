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

"""
import argparse
import os

#======== Globals (Reference variables) ==========
valid_partners = {'avh', 
    'bnd',
    'chm',
    'chr',
    'emy',
    'nch',
    'orl',
    'tma',
    'tmc',
    'uab',
    'ufh',
    'umi',
    'usf'}

#========= Functions =========
def set_path(partner):
    #set the path of the docker compose yml for the requested partner
    return '/Users/jasonglover/Documents/Code/onefl_cluster/clusters/{}/{}.yml'.format(partner,partner)
    #return '/onefl_cluster/clusters/{}/{}.yml'.format(partner,partner)

# function to dynamically build the docker compose file on boot with appropriate mount dirs
def make_cluster(partner, partner_path, datadir, workdir):
    text = '''
    version: "3.8"

    services:
    master:
        image: 'cluster-image:latest'
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
        - pyspark_cluster_network

    worker:
        image: 'cluster-image:latest'
        environment:
        - SPARK_MODE=worker
        - SPARK_MASTER_URL=spark://master:7077
        - SPARK_WORKER_MEMORY=2g
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
    '''.format(workdir,datadir,workdir,datadir,partner,partner,partner)
    
    cf = open(partner_path, 'w')
    cf.write(text) 

# Build the submit statement 
def spark_submit(partner, partner_path, workdir, datadir, script_name, args):
    submit_statement= '''docker run \
        --network={}_pyNet \
        -v {}:/app \
        -v {}:/data \
        --rm cluster-image \
        /opt/bitnami/spark/bin/spark-submit \
        --conf "spark.pyspark.python=python3" \
        --conf "spark.driver.memory=2g" \
        --conf "spark.executor.memory=1g" \
        --master spark://master:7077 \
        --deploy-mode client \
        --name my_pyspark_job /app/{} {}'''.format(partner,workdir,datadir, script_name, args)

    os.system(submit_statement)

def main():
    #Pull user's id for config operations
    uid = os.getuid()

    # Create the top-level parser
    parser = argparse.ArgumentParser(prog='cluster')
    subparsers = parser.add_subparsers(dest='command')

    # create second level commands
    #Run commands
    run_parse = subparsers.add_parser('run', help='run help')
    run_sub = run_parse.add_subparsers(dest='run_command')

    #Config commands
    config_parse = subparsers.add_parser('config', help='config help')
    config_sub = config_parse.add_subparsers(dest='config_command')
    cat_parse = config_sub.add_parser('cat', help='cat help')
    init_parse = config_sub.add_parser('init', help='init help')


    # Add flags for the 'run' command
    run_parse.add_argument(
        '-p', '--partner',
        default='.',
        required=True,
        help='Three letter partner name'
    )
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
        '-c', '--cfg', 
        action='store_true', 
        required=False,
        help='Run the cluster based on the user\'s predefined config file. User can set their config file using the \'cluster config --init\' command')


    # Add flags for the config command
    config_parse.add_argument('--set', type=str, help='Set a config value')
    config_parse.add_argument(
        '--init',
        action='store_true',
        help ='Initialize a new config for the current user.')


    # Parse the arguments
    args = parser.parse_args()

    # Check what command was given
    if args.command == 'run':
        if args.cfg:
            #Retrieve the user's id's - use this to set a custom config for each user
            if os.path.exists('/Users/jasonglover/Documents/Code/onefl_cluster/configs/config_{}'.format(uid)):
                cfge = open('/Users/jasonglover/Documents/Code/onefl_cluster/configs/config_{}'.format(uid),'r')
            else:
                print('ERROR: No config found for this user. \nInitialize your config using the \'cluster config --init\' command. Exiting.')
                quit(-1)

        else:
            print('Setting from command line')
        partner = args.partner.lower()
        # Check that the passed in partner code is valid
        if partner not in valid_partners:
            print('FATAL ERROR: Invalid partner. Exiting')
            quit(-1)

        print("Partner: {}".format(partner))
        print("Data Dir: {}".format(args.datadir))
        print("Work Dir: {}".format(args.workdir))

        #Set the path to docker compose yaml for the requested partner and write the file out
        partner_path = set_path(partner)
        make_cluster(partner, partner_path, args.datadir, args.workdir)

        #Boot Cluster
        #os.system('docker compose -f {} up -d --scale worker=5'.format(partner_path))
        #spark_submit(partner, partner_path, workdir, datadir, script, script_args)
        #os.system('docker compose -f {} down'.format(partner_path))
        

    elif args.command == 'config':
        if args.config_command=='init':
            #Run initialization of user's config
            print('Running config setup\n')
            init_conf = open('/Users/jasonglover/Documents/Code/onefl_cluster/configs/config_{}'.format(uid),'w')
            name = input('Enter name: ')
            convert_source = input('Enter the absolute path to your converter instance: ')
            init_conf.write('Name: {}\n'.format(name))
            init_conf.write('Converter Path: {}\n'.format(convert_source))
            init_conf.close()
        elif args.config_command=='cat':
            os.system('cat /Users/jasonglover/Documents/Code/onefl_cluster/configs/config_{}'.format(uid))
        

if __name__=='__main__':
    main()