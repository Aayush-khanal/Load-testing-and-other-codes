import pandas as pd
import mysql.connector
from datetime import datetime

# MySQL Database Configuration
mysql_config = {
    'host': '',
    'port': ,
    'user': '',
    'password': '',
    'database': ''
}

# Load Data from MySQL
def load_data():
    connection = mysql.connector.connect(**mysql_config)
    query = """
    SELECT 
        agent_id, 
        agent_fname, 
        agent_lname, 
        agent_mname, 
        agent_email, 
        agent_ssn 
    FROM 
        agent_info
    """
    # Using pandas read_sql_query for compatibility with MySQL connector
    df = pd.read_sql_query(query, connection)
    connection.close()
    return df

# Preprocess and Standardize Data
def preprocess_data(df):
    # Ensure columns exist in the data before applying transformations
    df['agent_fname'] = df['agent_fname'].str.strip().str.lower()
    df['agent_lname'] = df['agent_lname'].str.strip().str.lower()
    df['agent_mname'] = df['agent_mname'].str.strip().str.lower()
    df['agent_email'] = df['agent_email'].str.strip().str.lower()
    return df

# Check for Duplicates and Process SSN
def find_duplicates(df):
    # Group by the specified columns to identify duplicates
    grouped = df.groupby(['agent_fname', 'agent_lname', 'agent_mname', 'agent_email'], dropna=False)
    
    duplicate_records = []
    for key, group in grouped:
        if len(group) > 1:  # Check if there are duplicates in the group
            ssn_values = group['agent_ssn'].dropna().unique()
            ssn_values = [str(ssn) for ssn in ssn_values if ssn is not None]  # Exclude None values
            if len(ssn_values) > 1:  # Multiple SSN values, use `//` to differentiate
                combined_ssn = " // ".join(ssn_values)
            else:
                combined_ssn = ssn_values[0] if len(ssn_values) > 0 else None

            combined_agent_ids = ",".join(map(str, group['agent_id'].tolist()))

            duplicate_records.append({
                'agent_fname': key[0],
                'agent_lname': key[1],
                'agent_mname': key[2],
                'agent_email': key[3],
                'agent_ssn': combined_ssn,
                'agent_ids': combined_agent_ids
            })
    
    duplicate_df = pd.DataFrame(duplicate_records)
    return duplicate_df

# Main Function
def main():
    print("Loading data from database...")
    df = load_data()
    if df.empty:
        print("No data retrieved from the database.")
        return

    print("Preprocessing data...")
    df = preprocess_data(df)

    print("Finding duplicates...")
    duplicates_df = find_duplicates(df)

    if duplicates_df.empty:
        print("No duplicates found.")
    else:
        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"duplicates_analysis_{timestamp}.xlsx"
        duplicates_df.to_excel(output_file, index=False)

        print(f"Results exported to {output_file}")

if __name__ == "__main__":
    main()
