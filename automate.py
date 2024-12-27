from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def export_data_to_excel_with_sheets(status_filters, excel_file_path):
    # Use MySqlHook to get the connection
    mysql_hook = MySqlHook(mysql_conn_id='my_mysql_conn')
    
    # Get the connection object from the hook
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Open an ExcelWriter object
        with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
            for status_filter in status_filters:
                # SQL query to fetch data
                query = """
                    SELECT 
                        u.cfname AS first_name, 
                        u.cmname AS middle_name, 
                        u.clname AS last_name,
                        u.cemail AS email, 
                        u.cgender AS gender, 
                        ca.address1, 
                        ca.address2, 
                        ca.city, 
                        ca.state,
                        p.plan_name_system AS plan_name, 
                        pp.peffective_date, 
                        pp.pterm_date, 
                        pol.status AS policy_status,
                        pt.tier, 
                        ci.carrier_name
                    FROM 
                        userinfo u
                    INNER JOIN 
                        cust_addresses ca ON u.userid = ca.a_userid
                    INNER JOIN 
                        policies pol ON u.userid = pol.policy_userid
                    INNER JOIN 
                        plan_policies pp ON pol.policy_id = pp.policy_num
                    INNER JOIN 
                        plans p ON pp.plan_id = p.pid
                    INNER JOIN 
                        plan_tier pt ON p.pid = pt.pid_tier
                    INNER JOIN 
                        carrier_info ci ON p.cid = ci.cid
                    WHERE 
                        pol.status IN ({})
                        AND DATE(pol.effective_date) = CURDATE();
                """.format(', '.join(['%s'] * len(status_filter)))

                # Execute query with filter
                cursor.execute(query, status_filter)
                rows = cursor.fetchall()

                # Fetch column names
                columns = [col[0] for col in cursor.description]

                if rows:
                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=columns)
                    
                    # Use the filter names as sheet names
                    sheet_name = '_'.join(status_filter)
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
                    print(f"Data for {status_filter} exported to sheet '{sheet_name}'")
                else:
                    print(f"No data found for filter {status_filter}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


# Define the DAG
with DAG(
    'daily_policy_status_export',
    default_args=default_args,
    description='Daily export of policy data based on status at 8 PM',
    schedule_interval='*/2 * * * *',  
    start_date=datetime(2024, 11, 15),
    catchup=False,
) as dag:

    # File path for the Excel file
    excel_file_path = r''

    # Status filters to apply in the query
    status_filters = [
        ('ACTIVE', 'TERMED', 'WITHDRAWN'),
        ('ACTIVE', 'TERMED'),
        ('ACTIVE',)
    ]

    # Task: Export data to Excel with different sheets for each status filter
    export_status_data = PythonOperator(
        task_id='export_status_data',
        python_callable=export_data_to_excel_with_sheets,
        op_kwargs={
            'status_filters': status_filters,
            'excel_file_path': excel_file_path,
        },
    )

