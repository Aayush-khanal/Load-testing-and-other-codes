import mysql.connector
from pymongo import MongoClient

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
client = MongoClient(mongo_uri)

# Access the database and collection
db = client['qa_intern']
collection = db['plan_overview']  # Target collection

# Step 1: Extract data from MySQL
def extract_data():
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM plan_overview")  
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows

# Step 2: Transform data if needed 
def transform_data(data):
    return data

# Step 3: Load data into MongoDB
def load_data(data):
    collection.insert_many(data)

# Full pipeline
def pipeline():
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    print("Data migrated successfully!")

if __name__ == "__main__":
    pipeline()
