import os
import json

install_path = os.getcwd()

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
if run_command('sudo chmod -R 777 {}'.format(install_path)) == 0:
    print("Changed permissions for {} successfully.".format(install_path))
else:
    print("Failed to change permissions for {}.")




# Building docker image
if run_command('gzip -d {}/image_files/onefl-cluster-image.tar.gz'.format(install_path)) == 0:
    print('Succesfully unpacked the Docker image.')
else:
    print('Failed to unzip the Docker image.')

if run_command('docker load -i {}/image_files/onefl-cluster-image.tar'.format(install_path)) == 0:
    print("Built Docker image successfully.")
else:
    print("Failed to build Docker image.")

# Creating symlink
if run_command('ln -s {}/cluster.py /usr/local/bin/cluster'.format(install_path)) == 0:
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
    with open('cluster.py', 'r') as file:
        lines = file.readlines()
        
    with open('cluster.py', 'w') as file:
        for line in lines:
            if line.strip() == "absolute_cluster_path='[CHANGE ME]'":
                file.write("absolute_cluster_path='{}'\n".format(install_path))
            else:
                    file.write(line)
except FileNotFoundError:
        print("File 'cluster.py' not found!")
except Exception as e:
        print(f"An error occurred: {e}")
print("Succesfully updated the path in 'cluster.py'")

try:
    with open('{}/info/clusters.json'.format(install_path), 'w') as file:
        json.dump(data, file)
    print("Initialized the cluster status JSON at {}/info/clusters.json successfully.".format(install_path))
except Exception as e:
    print("Failed to write to {}/info/clusters.json. Error: {}".format(install_path, e))



while True:
    response = input('Would you like to run a test script to verify installation? (Y/n)')

    if response.lower()=='y' or response.lower()=='yes':
        run_command('cluster run {}/verify_cluster.py'.format(install_path))
        quit(0)
    if response.lower()=='n' or response.lower()=='no':
        print('Goodbye!')
        quit(0)
    else:
        print('Invalid response! Please enter yes or no')
        