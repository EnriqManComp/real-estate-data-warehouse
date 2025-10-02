from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

####    DAG for one run to create the DDL in the Postgres server
dag = DAG(
    dag_id='ddl_dag',
    description='One-time run to create the dimension and fact tables',
    start_date=datetime(2018,7,5),
    catchup=False,
    schedule=None,
    template_searchpath=['/opt/airflow/src/sql_scripts/ddls']
)

# Creates property dimension with info about the properties
create_property_dim = SQLExecuteQueryOperator(
    task_id='property_dim_ddl',
    conn_id='real_estate_connection',
    sql='6_property_dim.sql',
    dag=dag,
) 

# Creates agent dimension with info about the realtors.
create_agent_dim = SQLExecuteQueryOperator(
    task_id='agent_dim_ddl',
    conn_id='real_estate_connection',
    sql='7_agent_dim.sql',
    dag=dag,
)

# Creates fact table
create_fact_table = SQLExecuteQueryOperator(
    task_id='fact_table_ddl',
    conn_id='real_estate_connection',
    sql='8_fact_table.sql',
    dag=dag,
)

# Creates agent-property dimension with info about the pair realtor-property
create_agent_property_dim = SQLExecuteQueryOperator(
    task_id='agent_property_dim_ddl',
    conn_id='real_estate_connection',
    sql='9_agent_property_dim.sql',
    dag=dag,
)

# Creates sales dimension with type 2 SCD to track changes: realtor id, sale ratio, is not ready to sell  
create_sales_dim = SQLExecuteQueryOperator(
    task_id='sales_dim_ddl',
    conn_id='real_estate_connection',
    sql='10_sales_dim.sql',
    dag=dag,
)

# dag sequence
create_property_dim >> create_agent_dim >> create_fact_table >> create_agent_property_dim >> create_sales_dim
