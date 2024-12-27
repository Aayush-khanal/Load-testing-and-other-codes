from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from pymongo import MongoClient, InsertOne
import time

# MySQL configuration
mysql_config = {
    'host': '',
    'port': ,
    'user': '',
    'password': '',
    'database': ''
}

# MongoDB configuration
mongo_config = {
    'uri': '',
    'db_name': '',
    'collection_name': ''
}

# SQL Queries
queries = {
    'Active': "SELECT * FROM plan_overview WHERE pstatus = 1",
    'Termed': "SELECT * FROM plan_overview WHERE pstatus = 2",
    'Withdrawn': "SELECT * FROM plan_overview WHERE pstatus = 3"
}

# Initialization
def initialize_connections(**kwargs):
    start_time = datetime.now()
    print(f"{start_time} - [Initialize Connections] - Starting initialization...")
    kwargs['ti'].xcom_push(key='mysql_config', value=mysql_config)
    kwargs['ti'].xcom_push(key='mongo_config', value=mongo_config)
    end_time = datetime.now()
    print(f"{end_time} - [Initialize Connections] - Initialization completed. Duration: {(end_time - start_time).total_seconds()} seconds.")

# Data transfer logic
def transfer_data(status, query, **kwargs):
    task_start_time = datetime.now()
    print(f"{task_start_time} - [Task {status}] - Starting data transfer...")

    ti = kwargs['ti']
    mysql_config = ti.xcom_pull(task_ids='initialize_connections', key='mysql_config')
    mongo_config = ti.xcom_pull(task_ids='initialize_connections', key='mongo_config')

    # Step 1: Fetch data from MySQL
    fetch_start_time = datetime.now()
    print(f"{fetch_start_time} - [Task {status}] - Connecting to MySQL and executing query...")
    mysql_conn = mysql.connector.connect(**mysql_config)
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    mysql_conn.close()
    fetch_end_time = datetime.now()
    print(f"{fetch_end_time} - [Task {status}] - Fetched {len(rows)} rows. Duration: {(fetch_end_time - fetch_start_time).total_seconds()} seconds.")

    # Step 2: Insert data into MongoDB in batches
    insert_start_time = datetime.now()
    print(f"{insert_start_time} - [Task {status}] - Connecting to MongoDB and inserting data...")
    mongo_client = MongoClient(mongo_config['uri'])
    mongo_collection = mongo_client[mongo_config['db_name']][mongo_config['collection_name']]
    if rows:
        batch_size = 1500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            bulk_operations = [InsertOne(row) for row in batch]
            mongo_collection.bulk_write(bulk_operations)
    mongo_client.close()
    insert_end_time = datetime.now()
    print(f"{insert_end_time} - [Task {status}] - Inserted {len(rows)} rows. Duration: {(insert_end_time - insert_start_time).total_seconds()} seconds.")

    # Log task completion time
    task_end_time = datetime.now()
    print(f"{task_end_time} - [Task {status}] - Task completed. Total duration: {(task_end_time - task_start_time).total_seconds()} seconds.")

# Task-specific functions
def transfer_active_data(**kwargs):
    transfer_data('Active', queries['Active'], **kwargs)

def transfer_termed_data(**kwargs):
    transfer_data('Termed', queries['Termed'], **kwargs)

def transfer_withdrawn_data(**kwargs):
    transfer_data('Withdrawn', queries['Withdrawn'], **kwargs)

# Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'weight_rule': 'absolute'  # To ensure task prioritization works uniformly
}

# DAG Definition
with DAG(
    'mysql_to_mongo_optimized_dag',
    default_args=default_args,
    description='Transfer MySQL data to MongoDB with concurrent tasks',
    schedule_interval=None,
    start_date=datetime(2023, 11, 28),
    catchup=False,
    concurrency=3,  # Allow up to 3 parallel tasks
    max_active_runs=3  # Allow up to 3 simultaneous DAG runs
) as dag:

    # Task to initialize connections
    start_task = PythonOperator(
        task_id='initialize_connections',
        python_callable=initialize_connections,
        provide_context=True
    )

    # Tasks for data transfer
    active_task = PythonOperator(
        task_id='transfer_active_data',
        python_callable=transfer_active_data,
        provide_context=True
    )

    termed_task = PythonOperator(
        task_id='transfer_termed_data',
        python_callable=transfer_termed_data,
        provide_context=True
    )

    withdrawn_task = PythonOperator(
        task_id='transfer_withdrawn_data',
        python_callable=transfer_withdrawn_data,
        provide_context=True
    )

    # Set dependencies
    start_task >> [active_task, termed_task, withdrawn_task]
