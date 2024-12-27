# runs with python threads for parallel processing 
# starts all the data transfer at once
#Finished processing Active in 133.93 seconds.
#Finished processing Termed in 130.50 seconds.
#Finished processing Withdrawn in 246.86 seconds.
#Data extraction and save completed. Total time: 247.30 seconds.





import mysql.connector
from pymongo import MongoClient, InsertOne
from datetime import datetime, timedelta
import threading
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

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
mongo_db_name = ""
mongo_collection_name = ""

# Function to process each status
def process_status(status, query, mongo_collection):
    task_start_time = time.time()
    print(f"{datetime.now()} - [Task {status}] - Starting data transfer for {status}...")

    # Step 1: Connect to MySQL
    try:
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        print(f"{datetime.now()} - [Task {status}] - Connected to MySQL.")
    except Exception as e:
        print(f"{datetime.now()} - [Task {status}] - MySQL connection error: {e}")
        return

    # Step 2: Fetch data from MySQL
    fetch_start_time = time.time()
    try:
        mysql_cursor.execute(query)
        rows = mysql_cursor.fetchall()
        fetch_end_time = time.time()
        print(f"{datetime.now()} - [Task {status}] - Fetched {len(rows)} rows for {status}. Time taken: {fetch_end_time - fetch_start_time:.2f} seconds.")
    except Exception as e:
        print(f"{datetime.now()} - [Task {status}] - Error fetching data: {e}")
        rows = []
    finally:
        mysql_cursor.close()
        mysql_conn.close()

    # Step 3: Insert data into MongoDB in batches
    if rows:
        batch_size = 1500
        for row in rows:
            row['status'] = status  # Add status field
            row['transfer_timestamp'] = datetime.now().isoformat()  # Add timestamp

        print(f"{datetime.now()} - [Task {status}] - Inserting data into MongoDB...")
        insert_start_time = time.time()
        try:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                bulk_operations = [InsertOne(doc) for doc in batch]
                mongo_collection.bulk_write(bulk_operations)
            insert_end_time = time.time()
            print(f"{datetime.now()} - [Task {status}] - Inserted {len(rows)} rows for {status} into MongoDB. Time taken: {insert_end_time - insert_start_time:.2f} seconds.")
        except Exception as e:
            print(f"{datetime.now()} - [Task {status}] - Error inserting data into MongoDB: {e}")
    else:
        print(f"{datetime.now()} - [Task {status}] - No data found to insert.")

    # Step 4: Log completion time
    task_end_time = time.time()
    print(f"{datetime.now()} - [Task {status}] - Task completed. Total time: {task_end_time - task_start_time:.2f} seconds.")

# Main function to handle concurrent execution
def mysql_to_mongo_concurrent():
    overall_start_time = time.time()
    print(f"{datetime.now()} - Starting concurrent data transfer...")

    # Connect to MongoDB
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    # Define queries for each status
    queries = {
        'Active': "SELECT * FROM plan_overview WHERE pstatus = 1",
        'Termed': "SELECT * FROM plan_overview WHERE pstatus = 2",
        'Withdrawn': "SELECT * FROM plan_overview WHERE pstatus = 3"
    }

    # Start threads for each status
    threads = []
    for status, query in queries.items():
        thread = threading.Thread(target=process_status, args=(status, query, mongo_collection))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Close MongoDB connection
    mongo_client.close()

    # Log overall completion time
    overall_end_time = time.time()
    print(f"{datetime.now()} - All tasks completed. Total time: {overall_end_time - overall_start_time:.2f} seconds.")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'mysql_to_mongo_threads_status',
    default_args=default_args,
    description='A pipeline to transfer data from MySQL to MongoDB concurrently',
    schedule_interval=None,
    start_date=datetime(2023, 11, 28),
    catchup=False
) as dag:

    transfer_task = PythonOperator(
        task_id='mysql_to_mongo_transfer_concurrent',
        python_callable=mysql_to_mongo_concurrent
    )
