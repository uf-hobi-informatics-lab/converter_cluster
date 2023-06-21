import os
import sys
import argparse
"""
Goals:

- Take in the Partner from user - and necessary files
- parse args
- call boot_cluster with appropriate args
- confirm succesful boot
- run submit

implementations:

- determine the mount directory
- config file
- automatic and manual file submission modes
- 


to-dos:

- fix the cluster logging



parser.add_argument(
        '-p', '--partner',
        default='.',
        required=True,
        help='partner name')


"""

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

#======= Functions ==========
def set_path(partner):
    #set the path of the docker compose yml for the requested partner
    return '/Users/jasonglover/Documents/Code/onefl_cluster/clusters/{}/{}.yml'.format(partner,partner)
    #return '/onefl_cluster/clusters/{}/{}.yml'.format(partner,partner)


def make_cluster(partner, partner_path, datadir, workdir):
    #function to dynamically build the docker compose file on boot with appropriate mount dirs
    
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

def spark_submit(partner, partner_path, workdir, datadir):
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
        --name my_pyspark_job /app/{} {}'''.format(partner,workdir,datadir)

    os.system(submit_statement)


def main():
    #Read in and parse command line args
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-p', '--partner',
        default='.',
        required=True,
        help='Three letter partner name'
    )

    parser.add_argument(
        '-d','--datadir',
        default='.',
        required=False,
        help='Directory containing data to be processed. If not passed, output is put in the working directory.'
    )

    parser.add_argument(
        '-w','--workdir',
        default='.',
        required=True,
        help='Directory containing the scripts to be submitted to the cluster.'
    )

    args = parser.parse_args()
    
    #Initialize environment
    partner = args.partner
    datadir = args.datadir
    workdir = args.workdir
    
    partner = partner.lower()
    if partner not in valid_partners:
        #log invalid partner
        quit()
    
    #Set the path to docker compose yaml for the requested partner and write the file out
    partner_path = set_path(partner)
    make_cluster(partner, partner_path, datadir, workdir)

    #Boot Cluster
    os.system('docker compose -f {} up -d --scale worker=5'.format(partner_path))
    spark_submit(partner, partner_path, workdir, datadir)
    os.system('docker compose -f {} down'.format(partner_path))
    
if __name__ == '__main__':
    main()