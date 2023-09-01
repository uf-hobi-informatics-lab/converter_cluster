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
if run_command('sudo chmod -R 777 /onefl_cluster') == 0:
    print("Changed permissions for /onefl_cluster successfully.")
else:
    print("Failed to change permissions for /onefl_cluster.")

# Building docker image
if run_command('docker load -i /onefl_cluster/image_files/glove_cluster.tar') == 0:
    print("Built Docker image successfully.")
else:
    print("Failed to build Docker image.")

# Creating symlink
if run_command('ln -s /onefl_cluster/cluster.py /usr/local/bin/cluster') == 0:
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
    with open('/onefl_cluster/info/clusters.json', 'w') as file:
        json.dump(data, file)
    print("Wrote JSON data to /onefl_cluster/info/clusters.json successfully.")
except Exception as e:
    print(f"Failed to write to /onefl_cluster/info/clusters.json. Error: {e}")
