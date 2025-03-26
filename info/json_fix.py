import json

# Writing JSON data to file
data = {
    "dummy_cluster": {
        "state": "inactive",
        "last_run_time": "1970-01-01 00:00:00",
        "last_command": "none",
        "user":"none"
    }
}

try:
    with open('clusters.json','w') as file:
        json.dump(data, file)
    print("Initialized the cluster status JSON successfully.")
except Exception as e:
    print("Failed to write to cluster.json. Error: {}".format(e))
