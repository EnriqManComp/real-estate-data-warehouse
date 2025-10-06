from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import requests
import sys
sys.path.append("/opt/airflow/src")
from io import StringIO
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine
from python_script.daily.update_sales_dim import update_sales_dim
from python_script.daily.insert_sales_dim import insert_sales_dim
from python_script.daily.update_same_day import update_same_day
from task_runtime_logger import tasks_logger
from fetch_data import fetch_data_api

# Getting real estate postgres connection info 
try: # ✅ Error Handler Airflow-database connection
    # Using BaseHook to get connection object
    conn = BaseHook.get_connection("real_estate_connection")
except Exception as e:
    print(f"Failed to get Airflow connection: {e}")
else:
    try: # ✅ Error Handler db engine
        # Creating database engine connected to real estate database from postgres
        db_engine = create_engine(f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    except Exception as e:
        print(f"Failed to create SQLAlchemy engine: {e}")

# Default args for the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

# Set up dag 
dag = DAG(
    'fetch_and_store_real_estate_connecticut', # ID
    default_args=default_args,                 # Args
    description='DAG to fetch real estate data from gov api and store it in Postres db', 
    schedule=timedelta(days=1), # Schedule interval
    catchup=False,
    template_searchpath=['/opt/airflow/src'] # Path template for the code
)

# fetch data from api
fetch_stage_task = PythonOperator(
    task_id="fetch_data_api",
    python_callable=fetch_data_api,
    op_args=[
        db_engine,
        "{{ ds }}"
    ],
    dag=dag,
)

# Get the property id
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

# Formatting the fields
formatting_task = SQLExecuteQueryOperator(
    task_id='formatting_fields',
    conn_id='real_estate_connection',
    sql='sql_scripts/preprocessing/4_formatting_fields.sql',
    dag=dag,
)

# Handling duplicates (same property, serial number, date recorded)
duplicates_handler_task = SQLExecuteQueryOperator(
    task_id='handling_duplicates',
    conn_id='real_estate_connection',
    sql='sql_scripts/preprocessing/5_duplicates.sql',
    dag=dag,
)

# Adding data to property dimension
property_dim_task = SQLExecuteQueryOperator(
    task_id='property_dim',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/6_property_dim.sql',
    dag=dag,
)

# Adding data to agent dimension
agent_dim_task = SQLExecuteQueryOperator(
    task_id='agent_dim',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/7_agent_dim.sql',
    dag=dag,
)

# Adding data to fact table
fact_table_task = SQLExecuteQueryOperator(
    task_id='fact_table',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/8_fact_table.sql',
    dag=dag,
)

# Adding data to agent-property dimension
agent_property_dim_task = SQLExecuteQueryOperator(
    task_id='agent_property_dim',
    conn_id='real_estate_connection',
    sql='sql_scripts/daily_sql/9_agent_property_dim.sql',
    dag=dag,
)

# Update sales dimension based on previous data
update_sales_dim_task = PythonOperator(
    task_id='update_sales_dim',
    python_callable=update_sales_dim,
    op_args=[
        db_engine,
        "{{ ds }}"
    ],
    dag=dag,
)

# Update sales dimension in case same logical date were multiple changes over a same property
update_same_day_task = PythonOperator(
    task_id='update_same_day',
    python_callable=update_same_day,
    op_args=[
        db_engine,
        "{{ ds }}"
    ],
    dag=dag,
)

# Adding data to sales dimension
insert_sales_dim_task = PythonOperator(
    task_id='insert_sales_dim',
    python_callable=insert_sales_dim,
    op_args=[
        db_engine
    ],
    dag=dag,
)

tasks_logger_task = PythonOperator(
    task_id='tasks_runtime_logger',
    python_callable=tasks_logger,
    op_args=[
        db_engine,
        'de_log'
    ],
    dag=dag,
)

# dag sequence
fetch_stage_task >> add_property_id_task >> null_handler_task >> formatting_task >> duplicates_handler_task >> property_dim_task >> agent_dim_task >> fact_table_task >> agent_property_dim_task >> update_sales_dim_task >> insert_sales_dim_task >> update_same_day_task >> tasks_logger_task


