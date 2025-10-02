from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine
from python_script.daily.update_sales_dim import update_sales_dim
from python_script.daily.insert_sales_dim import insert_sales_dim
from python_script.daily.update_same_day import update_same_day

conn_ip = "172.18.0.5"

def fetch_data_api(**context):

    logical_date = context["logical_date"]
    day = logical_date.strftime("%Y-%m-%dT00:00:00.000")

    base_url = "https://data.ct.gov/resource/5mzw-sjtu.csv"

    field_names = [
        "serial_number",
	    "list_year",
	    "date_recorded",
	    "town",
	    "address",
	    "assessed_value",
	    "sale_amount",
	    "sales_ratio",
	    "property_type",
	    "residencial_type",
	    "non_use_code",
	    "assessor_remarks",
	    "opm_remarks",
	    "location"
    ]

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
    if not daily_data.empty:
        engine = create_engine(f"postgresql://airflow:airflow@{conn_ip}:5432/real_estate_database")
        daily_data.columns=field_names
        daily_data.to_sql("stage_table", engine, if_exists='replace', index=False, schema="high_roles")
    else:
        raise AirflowSkipException("No data found, skipping downstream tasks.")

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
    catchup=False,
)

# fetch data from api
fetch_stage_task = PythonOperator(
    task_id="fetch_data_api",
    python_callable=fetch_data_api,
    dag=dag,
)

add_property_id_task = SQLExecuteQueryOperator(
    task_id='add_property_id',
    conn_id='real_estate_connection',
    sql='sql_scripts/preprocessing/2_add_property_id.sql',
    dag=dag,
)

# Handling null values 
null_handler_task = SQLExecuteQueryOperator(
    task_id='null_process',
    conn_id='real_estate_connection',
    sql='sql_scripts/preprocessing/3_null_analysis.sql',
    dag=dag,
)

formatting_task = SQLExecuteQueryOperator(
    task_id='formatting_fields',
    conn_id='real_estate_connection',
    sql='sql_scripts/preprocessing/4_formatting_fields.sql',
    dag=dag,
)

duplicates_handler_task = SQLExecuteQueryOperator(
    task_id='handling_duplicates',
    conn_id='real_estate_connection',
    sql='sql_scripts/preprocessing/5_duplicates.sql',
    dag=dag,
)

property_dim_task = SQLExecuteQueryOperator(
    task_id='property_dim',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/6_property_dim.sql',
    dag=dag,
)

agent_dim_task = SQLExecuteQueryOperator(
    task_id='agent_dim',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/7_agent_dim.sql',
    dag=dag,
)

fact_table_task = SQLExecuteQueryOperator(
    task_id='fact_table',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/8_fact_table.sql',
    dag=dag,
)

agent_property_dim_task = SQLExecuteQueryOperator(
    task_id='agent_property_dim',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/9_agent_property_dim.sql',
    dag=dag,
)

update_sales_dim_task = PythonOperator(
    task_id='update_sales_dim',
    python_callable=update_sales_dim,
    op_args=[
        create_engine(f"postgresql://airflow:airflow@{conn_ip}:5432/real_estate_database"),
        "{{ ds }}"
    ],
    dag=dag,
)

update_same_day_task = PythonOperator(
    task_id='update_same_day',
    python_callable=update_same_day,
    op_args=[
        create_engine(f"postgresql://airflow:airflow@{conn_ip}:5432/real_estate_database"),
        "{{ ds }}"
    ],
    dag=dag,
)

insert_sales_dim_task = PythonOperator(
    task_id='insert_sales_dim',
    python_callable=insert_sales_dim,
    op_args=[
        create_engine(f"postgresql://airflow:airflow@{conn_ip}:5432/real_estate_database")
    ],
    dag=dag,
)

fetch_stage_task >> add_property_id_task >> null_handler_task >> formatting_task >> duplicates_handler_task >> property_dim_task >> agent_dim_task >> fact_table_task >> agent_property_dim_task >> update_sales_dim_task >> insert_sales_dim_task >> update_same_day_task


