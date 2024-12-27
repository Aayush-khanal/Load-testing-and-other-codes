import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time
from io import StringIO
from pymongo import MongoClient

# MySQL connection details
mysql_config = {
    'host': '',
    'port': ,
    'user': '',
    'password': '',
    'database': ''
}

# MongoDB connection details
mongo_config = {

}

#Extract data from MySQL
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
    kwargs['ti'].xcom_push(key='extracted_data', value=rows)  

#Convert to CSV and upload to MongoDB
def upload_to_mongo(**kwargs):
    start_time = time.time()
    data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data')

    if not data:
        print("No data retrieved from MySQL.")
        return

    df = pd.DataFrame(data)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    csv_data = csv_buffer.getvalue()

    # Connect to MongoDB
    client = MongoClient(mongo_config['host'])
    db = client[mongo_config['database']]
    collection = db[mongo_config['collection']]

    # Insert data into MongoDB
    documents = [row for row in df.to_dict(orient='records')]
    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} records into MongoDB collection '{mongo_config['collection']}'.")

    end_time = time.time()
    print(f"Time taken to upload data to MongoDB: {end_time - start_time} seconds")

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
    'mysql_to_mongo_pipeline',
    default_args=default_args,
    description='Pipeline to extract MySQL data, convert to CSV, and upload to MongoDB',
    schedule_interval='@hourly',  # Adjust schedule as needed
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    task_upload_to_mongo = PythonOperator(
        task_id='upload_to_mongo',
        python_callable=upload_to_mongo,
        provide_context=True,
    )

    # Task dependencies
    task_extract >> task_upload_to_mongo
