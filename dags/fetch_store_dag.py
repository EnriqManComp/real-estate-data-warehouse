from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine


def fetch_data_api(**context):

    logical_date = context["logical_date"]
    day = logical_date.strftime("%Y-%m-%dT00:00:00.000")

    base_url = "https://data.ct.gov/resource/5mzw-sjtu.csv"
    params = {
        'daterecorded': day,
        '$limit': 1000,
        '$offset': 0
    }

    daily_data = pd.DataFrame()

    while True:

        results = requests.get(base_url, params=params)
        # Convert to pandas DataFrame
        results_df = pd.read_csv(StringIO(results.text))

        if results_df.empty:
            break

        daily_data = pd.concat([daily_data, results_df], axis=0)

        params['$offset'] += params['$limit']

    # Push raw data into Postgres staging table
    engine = create_engine("postgresql://airflow:airflow@172.19.0.4:5432/real_estate_database")
    daily_data.to_sql("stage_table", engine, if_exists="append", index=False, schema="high_roles")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'fetch_and_store_real_estate_connecticut',
    default_args=default_args,
    description='DAG to fetch real estate data from gov api and store it in Postres db',
    schedule=timedelta(days=1),
)

# fetch data from api
fetch_stage_task = PythonOperator(
    task_id="fetch_data_api",
    python_callable=fetch_data_api,
    dag=dag,
)

# Handling null values 
null_handler_task = SQLExecuteQueryOperator(
    task_id='null_process',
    conn_id='real_estate_connection',
    sql='sql/3_null_analysis.sql',
    dag=dag,
)

fetch_stage_task >> null_handler_task


