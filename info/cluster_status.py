import json
from filelock import SoftFileLock
#Need file locks to prevent race conditions

info_path = '[CHANGE ME]'

json_path = info_path + 'clusters.json'
lock_path = info_path + 'clusters.lock'


def add_cluster(session_id, state, last_command, username, last_run_time=''):
    
    with SoftFileLock(lock_path, timeout=10):
        #Add a cluster to the status list
        with open(json_path, 'r') as f:
            clusters = json.load(f)

    clusters[session_id] = {
        'state': state,
        'last_command': last_command,
        'user': username,
        'last_run_time': last_run_time
    }

    with SoftFileLock(lock_path, timeout=10):    
        with open(json_path, 'w') as f:
            json.dump(clusters, f)

def update_cluster(session_id, state=None, last_command=None, username=None, last_run_time=None):
    #Update a cluster in the status list
    #Set default values to None - if user wants to update only one of these values, then it will not be None valued

    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)

    if state != None:
        clusters[session_id]['state'] = state
    if last_command != None:
        clusters[session_id]['last_command'] = last_command
    if username!=None:
        clusters[session_id]['user'] = username
    if last_run_time != None:
        clusters[session_id]['last_run_time'] = last_run_time

    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'w') as f:
            json.dump(clusters, f)

def remove_cluster(session_id):
    # Load the data from the JSON file

    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)
        
        # Remove the cluster, if it exists
    if session_id in clusters:
        del clusters[session_id]

    # Write the updated data back to the JSON file
    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'w') as f:
            json.dump(clusters, f)

def print_status():
    #Print the info held in the status list
    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)

    header = f"{'CLUSTER':<50} {'STATE':<15} {'LAST COMMAND':<15} {'CALLED BY':<20} {'LAST RUN':<19}"
    print(header)
    print('-'*len(header))  # Divider line

    # Data
    for cluster, info in clusters.items():
        if cluster != 'dummy_cluster':    
            state = info['state']
            last_command = info['last_command']
            user = info['user']
            last_run_time = info['last_run_time']
            
            row = f"{cluster:<50} {state:<15} {last_command:<15} {user:<20} {last_run_time:<19}"
            print(row)

def get_state(session_id):
    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)
    return clusters[session_id]['state']

def get_last_command(session_id):
    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)
    return clusters[session_id]['last_command']

def get_last_run_time(session_id):
    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)
    return clusters[session_id]['last_run_time']

def is_valid_id(session_id):
    with SoftFileLock(lock_path, timeout=10):
        with open(json_path, 'r') as f:
            clusters = json.load(f)
    try:
        _ = clusters[session_id]
        return True
    except KeyError:
        return False