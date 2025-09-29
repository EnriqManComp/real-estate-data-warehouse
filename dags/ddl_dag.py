from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


conn_ip = "172.18.0.3"

dag = DAG(
    dag_id='ddl_dag',
    description='One-time run to create the dimension and fact tables',
    start_date=datetime(2018,7,5),
    catchup=False,
    schedule=None,
)

create_property_dim = SQLExecuteQueryOperator(
    task_id='property_dim_ddl',
    conn_id='real_estate_connection',
    sql='sql_scripts/ddls/6_property_dim.sql',
    dag=dag,
) 

create_agent_dim = SQLExecuteQueryOperator(
    task_id='agent_dim_ddl',
    conn_id='real_estate_connection',
    sql='sql_scripts/ddls/7_agent_dim.sql',
    dag=dag,
)

create_fact_table = SQLExecuteQueryOperator(
    task_id='fact_table_ddl',
    conn_id='real_estate_connection',
    sql='sql_scripts/ddls/8_fact_table.sql',
    dag=dag,
)

create_agent_property_dim = SQLExecuteQueryOperator(
    task_id='agent_property_dim_ddl',
    conn_id='real_estate_connection',
    sql='sql_scripts/ddls/9_agent_property_dim.sql',
    dag=dag,
)

create_sales_dim = SQLExecuteQueryOperator(
    task_id='sales_dim_ddl',
    conn_id='real_estate_connection',
    sql='sql_scripts/ddls/10_sales_dim.sql',
    dag=dag,
)

create_property_dim >> create_agent_dim >> create_fact_table >> create_agent_property_dim >> create_sales_dim
