import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time

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

# Extract and save data to Excel
def extract_and_save_to_excel(**kwargs):
    start_time = time.time()
    print(f"{datetime.now()} - Connecting to MySQL...")
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)

    # Queries for each status
    queries = {
        'Active': "SELECT * FROM plan_overview WHERE pstatus = 1",
        'Termed': "SELECT * FROM plan_overview WHERE pstatus = 2",
        'Withdrawn': "SELECT * FROM plan_overview WHERE pstatus = 3"
    }
    
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
    for sheet_name, query in queries.items():
        sheet_start_time = time.time()
        print(f"{datetime.now()} - Fetching data for {sheet_name}...")
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"{datetime.now()} - Fetched {len(rows)} rows for {sheet_name}.")
        
        # Convert to DataFrame and write to the corresponding sheet
        if rows:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            print(f"{datetime.now()} - No data found for {sheet_name}.")

        print(f"{datetime.now()} - Finished processing {sheet_name} in {time.time() - sheet_start_time:.2f} seconds.")

    writer.close()

    cursor.close()
    connection.close()
    print(f"{datetime.now()} - Data extraction and save completed. Total time: {time.time() - start_time:.2f} seconds.")

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
    'mysql_to_excel_pipeline_pstatus',
    default_args=default_args,
    description='A pipeline to generate data based on pstatus with timestamps',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 28),
    catchup=False
) as dag:

    save_to_excel_task = PythonOperator(
        task_id='extract_and_save_to_excel',
        python_callable=extract_and_save_to_excel
    )

    save_to_excel_task
