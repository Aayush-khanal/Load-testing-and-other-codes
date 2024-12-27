import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time
import threading

# MySQL connection details
mysql_config = {
    'host': '',
    'port': ,
    'user': '',
    'password': '',
    'database': ''
}

# File path for output
output_file_path = r''

# Shared dictionary to store data fetched by threads
data_dict = {}
data_lock = threading.Lock()  # Lock for thread-safe operations

# Function to fetch and process data for a specific status
def fetch_data(sheet_name, query):
    sheet_start_time = time.time()
    print(f"{datetime.now()} - Fetching data for {sheet_name}...")
    
    # Establish a separate connection for each thread
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    fetch_time = time.time() - sheet_start_time
    print(f"{datetime.now()} - Fetched {len(rows)} rows for {sheet_name} in {fetch_time:.2f} seconds.")
    
    # Store the data in the shared dictionary
    with data_lock:
        data_dict[sheet_name] = rows
    
    cursor.close()
    connection.close()

# Main function to extract and save data concurrently
def extract_and_save_to_excel(**kwargs):
    start_time = time.time()
    print(f"{datetime.now()} - Starting concurrent data extraction...")
    
    # Queries for each status
    queries = {
        'Active': "SELECT * FROM plan_overview WHERE pstatus = 1",
        'Termed': "SELECT * FROM plan_overview WHERE pstatus = 2",
        'Withdrawn': "SELECT * FROM plan_overview WHERE pstatus = 3"
    }

    # Start threads for each query
    threads = []
    for sheet_name, query in queries.items():
        thread = threading.Thread(target=fetch_data, args=(sheet_name, query))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Write the collected data to an Excel file
    print(f"{datetime.now()} - Writing data to Excel file...")
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
    for sheet_name, rows in data_dict.items():
        sheet_start_time = time.time()
        if rows:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            print(f"{datetime.now()} - No data found for {sheet_name}.")  
        print(f"{datetime.now()} - Finished processing {sheet_name} in {time.time() - sheet_start_time:.2f} seconds.")

    writer.close()

    total_time = time.time() - start_time
    print(f"{datetime.now()} - Data extraction and save completed. Total time: {total_time:.2f} seconds.")

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
    'mysql_to_excel_pstatus_concurrent',
    default_args=default_args,
    description='A pipeline to generate data based on pstatus with timestamps using threads',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 28),
    catchup=False
) as dag:

    save_to_excel_task = PythonOperator(
        task_id='extract_and_save_to_excel',
        python_callable=extract_and_save_to_excel
    )

    save_to_excel_task
