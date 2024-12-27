import mysql.connector
import pandas as pd
from openpyxl import Workbook

# MySQL Database Configuration
mysql_config = {
    'host': '',          
    'port': ,                                                                                                         
    'user': '',      
    'password': '',  
    'database': ''   
}

# List of tables to fetch column info
tables = ["group_info", "agent_info"]

# Function to fetch column names and datatypes
def fetch_table_info(table_name, connection):
    query = f"SHOW COLUMNS FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [{"Column Name": row[0], "Data Type": row[1]} for row in cursor.fetchall()]
    cursor.close()
    return columns

# Main Function
def main():
    try:
        # Connect to MySQL Database
        print("Connecting to the database...")
        connection = mysql.connector.connect(**mysql_config)
        print("Connection successful!")

        # Excel Writer
        output_file = "table_column_info.xlsx"
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for table in tables:
                print(f"Fetching column info for table: {table}")
                table_info = fetch_table_info(table, connection)

                # Convert to DataFrame
                df = pd.DataFrame(table_info)

                # Write to Excel sheet
                df.to_excel(writer, sheet_name=table, index=False)
                print(f"Data written to sheet: {table}")

        print(f"Column information successfully written to '{output_file}'.")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Database connection closed.")

# Run the script
if __name__ == "__main__":
    main()
