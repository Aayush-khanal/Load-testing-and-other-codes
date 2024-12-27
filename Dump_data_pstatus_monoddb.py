# dumps data step by step no multi threading involved
# fetch data and inserts data individually one at a time.
#Finished processing Active in 63.99 seconds.
#Finished processing Termed in 130.50 seconds.
#Finished processing Withdrawn in 17.51 seconds.
#Data extraction and save completed. Total time: 214.86 seconds.


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
mongo_db_name = ""
mongo_collection_name = ""

# Extract data from MySQL and insert into MongoDB
def mysql_to_mongo(**kwargs):
    overall_start_time = time.time()
    print(f"{datetime.now()} - Connecting to MySQL...")
    mysql_conn = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    print(f"{datetime.now()} - Connecting to MongoDB...")
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    # Queries for each status
    queries = {
        'Active': "SELECT * FROM plan_overview WHERE pstatus = 1",
        'Termed': "SELECT * FROM plan_overview WHERE pstatus = 2",
        'Withdrawn': "SELECT * FROM plan_overview WHERE pstatus = 3"
    }

    total_inserted = 0
    for status, query in queries.items():
        status_start_time = time.time()
        print(f"{datetime.now()} - Fetching data for {status}...")
        mysql_cursor.execute(query)
        rows = mysql_cursor.fetchall()
        fetch_time = time.time() - status_start_time
        print(f"{datetime.now()} - Fetched {len(rows)} rows for {status} in {fetch_time:.2f} seconds.")
        
        # Insert into MongoDB with timestamp
        if rows:
            for row in rows:
                row['status'] = status  
                row['transfer_timestamp'] = datetime.now().isoformat()  
            try:
                insert_start_time = time.time()
                result = mongo_collection.insert_many(rows)
                insert_time = time.time() - insert_start_time
                total_inserted += len(result.inserted_ids)
                print(f"{datetime.now()} - Inserted {len(result.inserted_ids)} rows for {status} into MongoDB in {insert_time:.2f} seconds.")
            except Exception as e:
                print(f"{datetime.now()} - Error inserting data for {status}: {e}")
        else:
            print(f"{datetime.now()} - No data found for {status}.")
        
        status_time = time.time() - status_start_time
        print(f"{datetime.now()} - Finished processing {status} in {status_time:.2f} seconds.")
    
    overall_time = time.time() - overall_start_time
    print(f"{datetime.now()} - Data extraction and save completed. Total time: {overall_time:.2f} seconds.")
    mysql_cursor.close()
    mysql_conn.close()
    mongo_client.close()

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
    'mysql_to_mongo_pstatus_individual',
    default_args=default_args,
    description='A pipeline to transfer data from MySQL to MongoDB',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 28),
    catchup=False
) as dag:

    transfer_task = PythonOperator(
        task_id='mysql_to_mongo_transfer',
        python_callable=mysql_to_mongo
    )
