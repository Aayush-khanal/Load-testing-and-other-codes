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

# Tier and marital status mappings
tier_mappings = {
    'IO': 'Single',
    'IS': 'Member & Spouse',
    'IC': 'Member & Child',
    'IF': 'Family',
    'IC2': 'Member & Child'
}

marital_status_mappings = {
    '0': 'Single',
    '1': 'Married',
    '2': 'Divorced',
    '3': 'Widowed'
}

# Helper function for date parsing
def parse_date(date_string, formats):
    if not date_string:
        return ''
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt).strftime('%m/%d/%Y')
        except ValueError:
            continue
    return ''

#Extract data from MySQL (batch fetch)
def extract_data(**kwargs):
    start_time = time.time()
    print("Connecting to MySQL...")
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)
    print("Connected to MySQL. Extracting data...")

    # Fetch primary user data
    user_query = """
        SELECT *
        FROM userinfo_policy_address, plan_overview
        WHERE userinfo_policy_address.policy_id = plan_overview.policy_id
            AND plan_overview.brand = 'L713'
            AND userinfo_policy_address.cfname NOT LIKE '%test%'
            AND userinfo_policy_address.status = 'ACTIVE'
            AND userinfo_policy_address.boffice IS NULL
        ORDER BY userinfo_policy_address.clname ASC;
    """
    cursor.execute(user_query)
    rows = cursor.fetchall()
    print(f"User data fetched. Time taken: {time.time() - start_time:.2f} seconds")

    # Fetch all dependents in one query
    print("Fetching all dependents...")
    dependents_query = "SELECT * FROM dependents_in_policy"
    cursor.execute(dependents_query)
    dependents = cursor.fetchall()
    cursor.close()
    connection.close()
    print(f"Dependents data fetched. Time taken: {time.time() - start_time:.2f} seconds")

    # Map dependents to policies
    dependents_by_policy = {}
    for dependent in dependents:
        policy_id = dependent['policy_id']
        if policy_id not in dependents_by_policy:
            dependents_by_policy[policy_id] = []
        dependents_by_policy[policy_id].append(dependent)

    # Add dependents to each user record
    for record in rows:
        record['dependents'] = dependents_by_policy.get(record['policy_id'], [])

    print(f"Data extraction complete. Total time taken: {time.time() - start_time:.2f} seconds")
    kwargs['ti'].xcom_push(key='raw_data', value=rows)

# Step 2: Process and transform data
def transform_data(**kwargs):
    start_time = time.time()
    print("Transforming data...")
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='raw_data')
    if not raw_data:
        print("No raw data found!")
        return

    processed_data = []

    for record in raw_data:
        transformed_record = {
            'Effective': parse_date(record.get('effective_date', ''), ['%Y-%m-%d', '%m/%d/%Y']),
            'Last': (record.get('clname') or '').upper(),
            'Middle': (record.get('cmname') or '').upper(),
            'First': (record.get('cfname') or '').upper(),
            'SSN': record.get('cssn', ''),
            'Gender': 'M' if str(record.get('cgender')).strip() == '0' else 'F',
            'DOB': parse_date(record.get('cdob', ''), ['%Y-%m-%d', '%m/%d/%Y']),
            'Plan': record.get('plan_name_carrier', ''),
            'Tier': tier_mappings.get(record.get('tier', ''), ''),
            'Address1': (record.get('address1') or '').upper(),
            'Address2': (record.get('address2') or '').upper(),
            'City': record.get('city', ''),
            'State': record.get('state', ''),
            'Zip': record.get('zip', ''),
            'Phone': record.get('phone1', ''),
            'Email': (record.get('cemail') or '').upper(),
            'Term Date': '',
        }

        # Add dependents
        for i, dependent in enumerate(record.get('dependents', []), start=1):
            transformed_record[f'D{i} FirstName'] = (dependent.get('d_fname') or '').upper()
            transformed_record[f'D{i} MiddleName'] = (dependent.get('d_mname') or '').upper()
            transformed_record[f'D{i} LastName'] = (dependent.get('d_lname') or '').upper()
            transformed_record[f'D{i} DOB'] = parse_date(dependent.get('d_dob', ''), ['%Y-%m-%d', '%m/%d/%Y'])
            transformed_record[f'D{i} Gender'] = 'M' if str(dependent.get('d_gender')).strip() == '0' else 'F'
            transformed_record[f'D{i} SSN'] = dependent.get('d_ssn', '')
            transformed_record[f'D{i} Relationship'] = dependent.get('d_relate', '')

        processed_data.append(transformed_record)

    kwargs['ti'].xcom_push(key='transformed_data', value=processed_data)
    print(f"Data transformation complete. Time taken: {time.time() - start_time:.2f} seconds")

# Step 3: Save to Excel
def save_to_excel(**kwargs):
    start_time = time.time()
    print("Saving data to Excel...")
    data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
    if not data:
        print("No transformed data found!")
        return

    df = pd.DataFrame(data)
    df.to_excel(output_file_path, index=False)
    print(f"Data saved to Excel. Time taken: {time.time() - start_time:.2f} seconds")

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
    'mysql_to_excel_pipeline',
    default_args=default_args,
    description='A pipeline to generate eligibility data as an Excel file',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 28),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    save_task = PythonOperator(
        task_id='save_to_excel',
        python_callable=save_to_excel
    )

    extract_task >> transform_task >> save_task
