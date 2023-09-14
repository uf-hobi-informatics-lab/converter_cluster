import os
import json

def run_command(command):
    """Execute the provided command and print success or failure message."""
    print(f"Running: {command}")
    result = os.system(command)
    if result == 0:
        print(f"Success: {command}")
    else:
        print(f"Failure: {command}")
    return result

# Changing permissions
if run_command('sudo chmod -R 777 /converter_cluster') == 0:
    print("Changed permissions for /converter_cluster successfully.")
else:
    print("Failed to change permissions for /converter_cluster.")

# Building docker image
if run_command('gzip -d /converter_cluster/image_files/glove_cluster.tar.gz') == 0:
    print('Succesfully unpacked the Docker image.')
else:
    print('Failed to unzip the Docker image.')

if run_command('docker load -i /converter_cluster/image_files/glove_cluster.tar') == 0:
    print("Built Docker image successfully.")
else:
    print("Failed to build Docker image.")

# Creating symlink
if run_command('ln -s /converter_cluster/cluster.py /usr/local/bin/cluster') == 0:
    print("Created symlink successfully.")
else:
    print("Failed to create symlink.")

# Writing JSON data to file
data = {
    "dummy_cluster": {
        "state": "inactive",
        "last_run_time": "1970-01-01 00:00:00",
        "last_command": "none"
    }
}

try:
    with open('/converter_cluster/info/clusters.json', 'w') as file:
        json.dump(data, file)
    print("Initialized the cluster status JSON at /converter_cluster/info/clusters.json successfully.")
except Exception as e:
    print(f"Failed to write to /converter_cluster/info/clusters.json. Error: {e}")



while True:
    response = input('Would you like to run a test script to verify installation? (Y/n)')

    if response.lower()=='y' or response.lower()=='yes':
        run_command('cluster run /converter_cluster/verify_cluster.py')
        quit(0)
    if response.lower()=='n' or response.lower()=='no':
        print('Goodbye!')
        quit(0)
    else:
        print('Invalid response! Please enter yes or no')
        