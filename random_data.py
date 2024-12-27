import mysql.connector
from mysql.connector import Error
from faker import Faker
import random
from datetime import datetime

fake = Faker()

def create_connection():
    try:
        connection = mysql.connector.connect(
            host="",
            user="",
            password="",
            database=""
        )
        if connection.is_connected():
            print("Connected to the database")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def insert_and_return_ids(connection, table_name, data, columns):
    try:
        cursor = connection.cursor()
        placeholders = ", ".join(["%s"] * len(columns))
        columns = ", ".join(columns)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.executemany(query, data)
        connection.commit()
        print(f"Inserted {len(data)} rows into {table_name}")
        
        # Fetch starting ID for auto-incremented rows
        cursor.execute("SELECT LAST_INSERT_ID()")
        first_id = cursor.fetchone()[0]
        return list(range(first_id, first_id + len(data)))
    except Error as e:
        print(f"Error while inserting into {table_name}: {e}")
        return []

def main():
    connection = create_connection()
    if not connection:
        return

    try:
        # Step 1: Insert into carrier_info
        carrier_info_data = [
            (fake.company(), datetime.now(), datetime.now())
            for _ in range(50)
        ]
        carrier_ids = insert_and_return_ids(connection, "carrier_info", carrier_info_data, [
            "carrier_name", "created_at", "updated_at"
        ])
        if not carrier_ids:
            print("Failed to insert carrier_info data. Exiting...")
            return

        # Step 2: Insert into userinfo
        userinfo_data = [
            (
                fake.first_name(), fake.last_name(), fake.first_name(),
                fake.email(), random.choice(['1', '0']),
                fake.date_of_birth(minimum_age=18, maximum_age=70),
                fake.ssn(), datetime.now(), datetime.now()
            )
            for _ in range(1000)
        ]
        user_ids = insert_and_return_ids(connection, "userinfo", userinfo_data, [
            "cfname", "clname", "cmname", "cemail", "cgender", "cdob", "cssn", "created_at", "updated_at"
        ])
        if not user_ids:
            print("Failed to insert userinfo data. Exiting...")
            return

        # Step 3: Insert into plans
        plans_data = [
            (
                random.choice(carrier_ids),  # Reference valid carrier IDs
                fake.catch_phrase(), fake.bs(),
                datetime.now(), datetime.now()
            )
            for _ in range(100)
        ]
        plan_ids = insert_and_return_ids(connection, "plans", plans_data, [
            "cid", "plan_name_system", "web_display_name", "created_at", "updated_at"
        ])
        if not plan_ids:
            print("Failed to insert plans data. Exiting...")
            return

        # Step 4: Insert into policies
        policies_data = [
            (
                random.choice(user_ids),  # Reference valid user IDs
                None,  # Placeholder for edate
                fake.date_this_decade(), fake.date_this_decade(),
                random.choice(["ACTIVE", "TERMED", "WITHDRAWN"]),
                datetime.now(), datetime.now()
            )
            for _ in range(500)
        ]
        policy_ids = insert_and_return_ids(connection, "policies", policies_data, [
            "policy_userid", "edate", "term_date", "effective_date", "status", "created_at", "updated_at"
        ])
        if not policy_ids:
            print("Failed to insert policies data. Exiting...")
            return

        # Step 5: Insert into plan_tier
        plan_tier_data = [
            (
                random.choice(plan_ids),  # Reference valid plan IDs
                random.choice(['IO', 'IS', 'IF', 'IC']),
                datetime.now(), datetime.now()
            )
            for _ in range(200)
        ]
        tier_ids = insert_and_return_ids(connection, "plan_tier", plan_tier_data, [
            "pid_tier", "tier", "created_at", "updated_at"
        ])
        if not tier_ids:
            print("Failed to insert plan_tier data. Exiting...")
            return

        # Step 6: Insert into plan_pricing
        plan_pricing_data = [
            (
                random.choice(tier_ids),  # Reference valid plan_tier IDs
                round(random.uniform(50.0, 500.0), 2),
                datetime.now(), datetime.now()
            )
            for _ in range(200)
        ]
        insert_and_return_ids(connection, "plan_pricing", plan_pricing_data, [
            "plan_id", "price_male_nons", "created_at", "updated_at"
        ])
        # Step 7: Insert into plan_policies
        plan_policies_data = [
            (
                random.choice(policy_ids),  # Reference valid policy IDs
                random.choice(plan_ids),   # Reference valid plan IDs
                fake.date_between(start_date='-1y', end_date='today'),  # Termination date
                fake.date_this_decade(),  # Effective date
                random.choice(['1', '2', '3']),  # pstatus
                datetime.now(), datetime.now()
            )
            for _ in range(500)
        ]
        insert_and_return_ids(connection, "plan_policies", plan_policies_data, [
            "policy_num", "plan_id", "pterm_date", "peffective_date", "pstatus", "created_at", "updated_at"
        ])

    finally:
        connection.close()

if __name__ == "__main__":
    main()
