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

#Create the clusters directory
if run_command('mkdir {}/clusters'.format(install_path)) == 0:
    print("Created the clusters directory successfully.".format(install_path))
else:
    print("Failed to create the clusters directory.")

# Changing permissions
if run_command('sudo chmod -R 777 {}'.format(install_path)) == 0:
    print("Changed permissions for {} successfully.".format(install_path))
else:
    print("Failed to change permissions for {}.".format(install_path))




# Building docker image
if run_command('docker load -i {}/image_files/onefl-cluster-image.tar'.format(install_path)) == 0:
    print("Built Docker image successfully.")
else:
    print("Failed to build Docker image.")

# Creating symlink
if run_command('ln -s {}/cluster.py /usr/local/bin/cluster'.format(install_path)) == 0:
    print("Created symlink successfully.")
else:
    print("Failed to create symlink.")



#Update the hardcoded paths
try:
    with open('{}/cluster.py'.format(install_path), 'r') as file:
        lines = file.readlines()
        
    with open('{}/cluster.py'.format(install_path), 'w') as file:
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
    with open('{}/info/cluster_status.py'.format(install_path), 'r') as file:
        lines = file.readlines()
        
    with open('{}/info/cluster_status.py'.format(install_path), 'w') as file:
        for line in lines:
            if line.strip() == "info_path = '[CHANGE ME]'":
                file.write("info_path = '{}/info/'\n".format(install_path))
            else:
                    file.write(line)
except FileNotFoundError:
        print("File 'info/cluster_status.py' not found!")
except Exception as e:
        print(f"An error occurred: {e}")
print("Succesfully updated the path in 'cluster.py'")


# Writing JSON data to file
data = {
    "dummy_cluster": {
        "state": "inactive",
        "last_run_time": "1970-01-01 00:00:00",
        "last_command": "none",
        "user":"none"
    }
}

#Create the cluster json
try:
    with open('{}/info/clusters.json'.format(install_path), 'w') as file:
        json.dump(data, file)
    print("Initialized the cluster status JSON at {}/info/clusters.json successfully.".format(install_path))
except Exception as e:
    print("Failed to write to {}/info/clusters.json. Error: {}".format(install_path, e))

data = {"hardware": {"memory_worker": "5g", "memory_master": "1g"}}

try:
    with open('hardware_config.json','w') as file:
          json.dump(file)
    print("Initialized the hardware config JSON at {}/hardware_config.json".format(install_path))
except Exception as e:
    print("Failed to write to {}/ihardware_config.json. Error: {}".format(install_path, e))