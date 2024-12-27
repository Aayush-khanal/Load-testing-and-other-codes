import pandas as pd
import mysql.connector
from datetime import datetime

# Function to extract data and save it to multiple Excel sheets
def export_data_to_excel_with_sheets(status_filters, excel_dump):
    conn = mysql.connector.connect(
        host="",
        user="",
        password="",
        database=""
    )

    try:
        # Open an ExcelWriter object
        with pd.ExcelWriter(excel_dump, engine='xlsxwriter') as writer:
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

                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, status_filter)
                data = cursor.fetchall()

                if data:
                    # Convert to DataFrame
                    df = pd.DataFrame(data)
                    
                    # Use the filter names as sheet names
                    sheet_name = '_'.join(status_filter)
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
                    print(f"Data for {status_filter} exported to sheet '{sheet_name}'")
                else:
                    print(f"No data found for filter {status_filter}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the connection
        conn.close()


# Define status filters and Excel file name
status_filters = [
    ('ACTIVE', 'TERMED', 'WITHDRAWN'),
    ('ACTIVE', 'TERMED'),
    ('ACTIVE',)
]
excel_dump = 'policy_data_multiple_sheets.xlsx'

# Export data to Excel with multiple sheets
export_data_to_excel_with_sheets(status_filters, excel_dump)
