import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

FLOWER_URL = "http://localhost:5555/api/tasks?state=SUCCESS"
USERNAME = os.getenv("FLOWER_USER")
PASSWORD = os.getenv("FLOWER_PASS")

resp = requests.get(FLOWER_URL, auth=(USERNAME, PASSWORD))
tasks = resp.json()

for task_uuid, task in tasks.items():
    args_str = task.get("args")  # this is a string like "('{...}',)"
    
    # Clean it and parse JSON
    try:
        # Remove tuple wrapping and quotes
        cleaned = args_str.strip("(),'")
        args_json = json.loads(cleaned) # Convert to python dictionary
        task_id_from_args = args_json["ti"]["task_id"] # Get task id from the key "ti"
        logical_date_from_args = args_json["ti"]["run_id"] # Get logical date
        logical_date_from_args = logical_date_from_args.split("__")[1] # Split using __ character
        logical_date_from_args = logical_date_from_args.split("T")[0] # Split using T character
    except Exception as e:
        task_id_from_args = None
        print(f"Failed to parse args for {task_uuid}: {e}")

    runtime = task.get("runtime")
    state = task.get("state")
    name = task.get("name")

    if runtime:
        print(f"{task_uuid} | {logical_date_from_args} | {name} | {task_id_from_args} | {state} | runtime={runtime:.2f}s")
