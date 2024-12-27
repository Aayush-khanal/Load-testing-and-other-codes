import mysql.connector
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# MySQL connection details
mysql_config = {
    'host': '',
    'port': ,
    'user': '',
    'password': '',
    'database': ''
}

# MongoDB connection details
mongo_uri = ""
client = MongoClient(mongo_uri)
db = client['']
collection = db['']  


# Step 1: Extract data from MySQL
def extract_data(**kwargs):
    start_time = time.time()
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM plan_overview")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    end_time = time.time()
    print(f"Time taken to connect to MySQL and retrieve data: {end_time - start_time} seconds")
    kwargs['ti'].xcom_push(key='extracted_data', value=rows)  # Push data to XCom


# Step 2: Transform data 
def transform_data(**kwargs):
    start_time = time.time()
    data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data')
    # Add transformations 
    transformed_data = data  
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)  
    end_time = time.time()
    print(f"Time taken to transform data: {end_time - start_time} seconds")


# Step 3: Load data into MongoDB
def load_data(**kwargs):
    start_time = time.time()
    data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
    if data:
        collection.insert_many(data)
    end_time = time.time()
    print(f"Time taken to connect to MongoDB and insert data: {end_time - start_time} seconds")


# Full pipeline as an Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(hours=1),
}

with DAG(
    'mysql_to_mongodb_pipeline',
    default_args=default_args,
    description='A simple MySQL to MongoDB pipeline',
    schedule_interval='*/10 * * * *',  
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Task dependencies
    task_extract >> task_transform >> task_load
