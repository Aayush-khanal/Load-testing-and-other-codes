import pandas as pd
import mysql.connector
from rapidfuzz import fuzz, process
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
    query = "SELECT userid, cfname, clname, cmname, cdob, cemail FROM userinfo"
    df = pd.read_sql(query, connection)
    connection.close()
    return df

# Preprocess and Standardize Data
def preprocess_data(df):
    df['cfname'] = df['cfname'].str.strip().str.lower()
    df['clname'] = df['clname'].str.strip().str.lower()
    df['cmname'] = df['cmname'].str.strip().str.lower()
    df['cemail'] = df['cemail'].str.strip().str.lower()
    df['cdob'] = pd.to_datetime(df['cdob'], errors='coerce').dt.date
    return df

# Exact Match Function
def exact_match(df):
    exact_matches = df.duplicated(subset=['cfname', 'clname','cmname', 'cdob', 'cemail'], keep=False)
    exact_match_df = df[exact_matches]
    return exact_match_df

def fuzzy_match(df, name_threshold=90, email_threshold=85):
    matched_pairs = []
    n = len(df)
    
    for i in range(n):
        row1 = df.iloc[i]
        for j in range(i + 1, n): 
            row2 = df.iloc[j]
            
            # Calculate fuzzy similarities
            name_similarity = fuzz.token_sort_ratio(
                f"{row1['cfname']} {row1['cmname']} {row1['clname']}",
                f"{row2['cfname']} {row2['cmname']} {row2['clname']}"
            )
            email_similarity = fuzz.ratio(row1['cemail'], row2['cemail'])
            dob_match = row1['cdob'] == row2['cdob']

            # If above thresholds, mark as potential match
            if name_similarity >= name_threshold and email_similarity >= email_threshold and dob_match:
                matched_pairs.append({
                    'id_1': row1['userid'],
                    'id_2': row2['userid'],
                    'firstname_1': row1['cfname'],
                    'middlename_1': row1['cmname'],
                    'lastname_1': row1['clname'],
                    'dob_1': row1['cdob'],
                    'email_1': row1['cemail'],
                    'firstname_2': row2['cfname'],
                    'middlename_2': row2['cmname'],
                    'lastname_2': row2['clname'],
                    'dob_2': row2['cdob'],
                    'email_2': row2['cemail'],
                    'name_similarity': name_similarity,
                    'email_similarity': email_similarity
                })

    fuzzy_match_df = pd.DataFrame(matched_pairs)
    return fuzzy_match_df


# Main Function
def main():
    print("Loading data from database...")
    df = load_data()
    df = preprocess_data(df)

    print("Performing exact matching...")
    exact_match_df = exact_match(df)
    
    print("Performing fuzzy matching...")
    fuzzy_match_df = fuzzy_match(df)

    # Export to Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"userinfo_matching_results_{timestamp}.xlsx"

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        exact_match_df.to_excel(writer, sheet_name='Exact_Matches', index=False)
        fuzzy_match_df.to_excel(writer, sheet_name='Fuzzy_Matches', index=False)

    print(f"Results exported to {output_file}")

if __name__ == "__main__":
    main()
