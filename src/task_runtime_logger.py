import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime
from sqlalchemy import text

load_dotenv()

def tasks_logger(db_engine, schema_name:str):

    FLOWER_URL = "http://flower:5555/api/tasks"
    USERNAME = 'airflow'
    PASSWORD = 'airflow'

    if not USERNAME or not PASSWORD:
        raise ValueError("FLOWER_USER and FLOWER_PASSWORD must be set in environment")

    resp = requests.get(FLOWER_URL, auth=(USERNAME, PASSWORD))
    tasks = resp.json()

    if resp.status_code != 200:
        raise Exception(f"Flower API error {resp.status_code}: {resp.text}")

    try:
        tasks = resp.json()
    except json.JSONDecodeError:
        raise Exception(f"Non-JSON response from Flower: {resp.text[:500]}")

    # Prepare task data
    task_rows = []
    for task_uuid, task in tasks.items():
        # Parse args to extract task_id and possibly logical date
        args_str = task.get("args")
        try:
            cleaned = args_str.strip("(),'")
            args_json = json.loads(cleaned)
            task_id_from_args = args_json["ti"]["task_id"]
            logical_date_from_args = args_json["ti"]["run_id"]  # 
            # Extract logical date if run_id has a date
            logical_date = None
            if logical_date_from_args:
                logical_date_from_args = logical_date_from_args.split("__")[1] # Split using __ character
                logical_date_from_args = logical_date_from_args.split("T")[0] # Split using T character

                logical_date = datetime.fromisoformat(logical_date_from_args)
        except Exception:
            task_id_from_args = None
            logical_date = None

        task_rows.append({
        "task_uuid": task_uuid,
        "task_name": task.get("name"),
        "task_id": task_id_from_args,
        "state": task.get("state"),
        "runtime": task.get("runtime"),
        "logical_date": logical_date
        })
    
    # Insert tasks into database using SQLAlchemy engine
    if task_rows:
        query = text(f"""            
            INSERT INTO {schema_name}.tasks_logger (
                task_uuid,
                task_name,
                task_id,
                state,
                runtime,
                logical_date
            )
            VALUES (
                :task_uuid,
                :task_name,
                :task_id,
                :state,
                :runtime,
                :logical_date   
                )
            ON CONFLICT (task_uuid) DO NOTHING;
        """)
        with db_engine.begin() as conn:   # handles transaction & commit
            conn.execute(query, task_rows)