import logging
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

def log_success_with_duration(context):
    start = context['task_instance'].start_date
    end = context['task_instance'].end_date
    duration = end - start if end else "N/A"
    logging.info(f"Task {context['task_instance'].task_id} succeeded. Duration: {duration}")

def log_failure_with_duration(context):
    start = context['task_instance'].start_date
    end = context['task_instance'].end_date
    duration = end - start if end else "N/A"
    logging.error(f"Task {context['task_instance'].task_id} failed. Duration: {duration}")

default_args = {
    "owner": "Omar",
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="dwh_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 9, 14, 12, 0),
    schedule="0 12 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "etl", "sql data warehouse"]
) as dag:

    init_db = SQLExecuteQueryOperator(
        task_id="init_db",
        conn_id="sql_server_dwh_conn",
        sql="scripts/init_DBs.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    ddl_bronze_layer = SQLExecuteQueryOperator(
        task_id="ddl_bronze_layer",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/Bronze/ddl_bronze.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )


    load_into_bronze_layer = SQLExecuteQueryOperator(
        task_id="load_into_bronze_layer",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/Bronze/proc_load_bronze.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    
    execute_bronze_procedure = SQLExecuteQueryOperator(
        task_id="execute_bronze_procedure",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="EXEC bronze.load_bronze;",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    ddl_silver_layer = SQLExecuteQueryOperator(
        task_id="ddl_silver_layer",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/Silver/ddl_load_silver.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    load_into_silver_layer = SQLExecuteQueryOperator(
        task_id="load_into_silver_layer",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/Silver/proc_load_silver.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    execute_silver_procedure = SQLExecuteQueryOperator(
        task_id="execute_silver_procedure",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="EXEC silver.load_silver;",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )   
    silver_quality_check = SQLExecuteQueryOperator(
        task_id="silver_quality_check",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="test/quality_silver.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    create_gold_dim_customers = SQLExecuteQueryOperator(
        task_id="create_gold_dim_customers",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/gold/create_dim_customers.sql",
        split_statements=True,
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    
    create_gold_dim_products = SQLExecuteQueryOperator(
        task_id="create_gold_dim_products",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/gold/create_dim_products.sql",
        split_statements=True,
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    
    create_gold_fact_sales = SQLExecuteQueryOperator(
        task_id="create_gold_fact_sales",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="scripts/gold/create_fact_sales.sql",
        split_statements=True,
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    gold_quality_check = SQLExecuteQueryOperator(
        task_id="gold_quality_check",
        conn_id="sql_server_dwh_conn",
        database="datawarehouse",
        sql="test/quality_gold.sql",
        on_success_callback=log_success_with_duration,
        on_failure_callback=log_failure_with_duration
    )
    init_db >> ddl_bronze_layer >> load_into_bronze_layer >> execute_bronze_procedure >> ddl_silver_layer >> load_into_silver_layer >> execute_silver_procedure >> silver_quality_check >> [create_gold_dim_customers, create_gold_dim_products] >> create_gold_fact_sales >> gold_quality_check    