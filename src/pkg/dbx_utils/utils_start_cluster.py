# Check if the cluster is running
def is_cluster_running():
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
    }
    response = requests.get(api_endpoint + "/clusters/get?cluster_id=" + cluster_id, headers=headers)
    data = response.json()
    cluster_status = data["state"]
    return cluster_status == "RUNNING"

# Start the cluster if it's not running
def start_cluster():
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
    }
    response = requests.post(api_endpoint + "/clusters/start?cluster_id=" + cluster_id, headers=headers)
    if response.status_code == 200:
        print("Cluster is starting...")
    else:
        print("Failed to start the cluster.")

# Wait until the cluster is running
def wait_for_cluster():
    while not is_cluster_running():
        print("Waiting for the cluster to start...")
        time.sleep(10)

# Check cluster status and start if necessary
if not is_cluster_running():
    start_cluster()

# Wait for the cluster to start
wait_for_cluster()

# Next stages of the code
print("Cluster is running. Proceeding to the next stages...")