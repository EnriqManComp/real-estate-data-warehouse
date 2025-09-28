from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine

conn_ip = "172.18.0.7"

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
    engine = create_engine(f"postgresql://airflow:airflow@{conn_ip}:5432/real_estate_database")
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

formatting_task = SQLExecuteQueryOperator(
    task_id='formatting_fields',
    conn_id='real_estate_connection',
    sql='sql/4_formatting_fields.sql',
    dag=dag,
)

duplicates_handler_task = SQLExecuteQueryOperator(
    task_id='handling_duplicates',
    conn_id='real_estate_connection',
    sql='sql/5_duplicates.sql',
    dag=dag,
)

property_dim_task = SQLExecuteQueryOperator(
    task_id='property_dim',
    conn_id='real_estate_connection',
    sql='sql/6_property_dim.sql',
    dag=dag,
)

agent_dim_task = SQLExecuteQueryOperator(
    task_id='agent_dim',
    conn_id='real_estate_connection',
    sql='sql/7_agent_dim.sql',
    dag=dag,
)

fact_table_task = SQLExecuteQueryOperator(
    task_id='fact_table',
    conn_id='real_estate_connection',
    sql='sql/8_fact_table.sql',
    dag=dag,
)

agent_property_dim_task = SQLExecuteQueryOperator(
    task_id='agent_property_dim',
    conn_id='real_estate_connection',
    sql='sql/9_agent_property_dim.sql',
    dag=dag,
)

sales_dim_task = SQLExecuteQueryOperator(
    task_id='sales_dim',
    conn_id='real_estate_connection',
    sql='sql/10_sales_dim.sql',
    dag=dag,
)

fetch_stage_task >> null_handler_task >> formatting_task >> duplicates_handler_task >> property_dim_task >> agent_dim_task >> fact_table_task >> agent_property_dim_task >> sales_dim_task


