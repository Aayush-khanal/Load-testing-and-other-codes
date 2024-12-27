from faker import Faker
import random
from datetime import datetime, timedelta
from Connection import db, cursor  
import time


# Initialize Faker for generating realistic dummy data
fake = Faker()


# Helper functions get random date in the last 10 years
def get_random_date():
    start_date = datetime.now() - timedelta(days=365 * 10)
    return start_date + timedelta(days=random.randint(0, 365 * 10))


def insert_userinfo(num_records):
    userinfo_data = []
    for _ in range(num_records):
        gender = random.choice(['0', '1'])  # 0 for male, 1 for female
        dob = get_random_date()
        ssn = fake.ssn()
        userinfo_data.append((
            fake.first_name(), fake.last_name(), fake.first_name(), fake.email(),
            gender, dob, ssn, datetime.now(), datetime.now()
        ))
    query = """
        INSERT INTO userinfo (cfname, clname, cmname, cemail, cgender, cdob, cssn, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, userinfo_data)


def insert_cust_addresses(user_ids):
    cust_address_data = []
    for user_id in user_ids:
        cust_address_data.append((
            user_id, fake.street_address(), fake.secondary_address(), fake.city(),
            fake.state_abbr(), fake.zipcode(), datetime.now(), datetime.now()
        ))
    query = """
        INSERT INTO cust_addresses (a_userid, address1, address2, city, state, zip, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, cust_address_data)


def insert_carrier_info(num_records):
    carrier_info_data = [(fake.company(), datetime.now(), datetime.now()) for _ in range(num_records)]
    query = """
        INSERT INTO carrier_info (carrier_name, created_at, updated_at)
        VALUES (%s, %s, %s)
    """
    cursor.executemany(query, carrier_info_data)


def insert_plans(carrier_ids):
    plans_data = []
    for carrier_id in carrier_ids:
        plans_data.append((
            carrier_id, fake.word(), fake.sentence(nb_words=3), datetime.now(), datetime.now()
        ))
    query = """
        INSERT INTO plans (cid, plan_name_system, web_display_name, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(query, plans_data)


def insert_plan_tier(plan_ids):
    plan_tier_data = [(plan_id, random.choice(["IO", "IS", "IC", "IF"]), datetime.now(), datetime.now()) for plan_id in plan_ids]
    query = """
        INSERT INTO plan_tier (pid_tier, tier, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(query, plan_tier_data)


def insert_plan_pricing(plan_tier_ids):
    plan_pricing_data = [(plan_tier_id, round(random.uniform(100.0, 500.0), 2), datetime.now(), datetime.now()) for plan_tier_id in plan_tier_ids]
    query = """
        INSERT INTO plan_pricing (plan_id, price_male_nons, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(query, plan_pricing_data)


def insert_policies(user_ids):
    policies_data = []
    for user_id in user_ids:
        status = random.choice(["ACTIVE", "TERMED", "WITHDRAWN"])  
        edate = get_random_date()
        uniux_edate = int (time.mktime(edate.timetuple())) #Converts to  unix
        effective_date = datetime.today()
        term_date = effective_date + timedelta(days=random.randint(1, 365)) if status != "ACTIVE" else None
        policies_data.append((user_id, uniux_edate, term_date, effective_date, status, datetime.now(), datetime.now()))
    query = """
        INSERT INTO policies (policy_userid, edate, term_date, effective_date, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, policies_data)


def insert_plan_policies(pid, plan_ids):
    plan_policies_data = []
    for policy_num, plan_id in zip(pid, plan_ids):
        pstatus = random.choice([1, 2, 3])  # 1 for active, 2 for termed, 3 for withdrawn
        pterm_date = get_random_date() if pstatus != 1 else None
        peffective_date = get_random_date()
        plan_policies_data.append((policy_num, plan_id, fake.date(), peffective_date, pstatus, datetime.now(), datetime.now()))
    query = """
        INSERT INTO plan_policies (policy_num, plan_id, pterm_date, peffective_date, pstatus, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, plan_policies_data)


# Insert dummy data in all tables
def insert_dummy_data(num_records=10):
    try:
        # Start transaction
        db.start_transaction()


        # Step 1: Insert userinfo and retrieve user_ids
        insert_userinfo(num_records)
        user_ids = [cursor.lastrowid - i for i in range(num_records)][::-1]  # Retrieve generated user IDs


        # Step 2: Insert customer addresses
        insert_cust_addresses(user_ids)


        # Step 3: Insert carriers and retrieve carrier_ids
        insert_carrier_info(num_records)
        carrier_ids = [cursor.lastrowid - i for i in range(num_records)][::-1]


        # Step 4: Insert plans and retrieve plan_ids
        insert_plans(carrier_ids)
        plan_ids = [cursor.lastrowid - i for i in range(num_records)][::-1]


        # Step 5: Insert plan tiers and retrieve plan_tier_ids
        insert_plan_tier(plan_ids)
        plan_tier_ids = [cursor.lastrowid - i for i in range(num_records)][::-1]


        # Step 6: Insert plan pricing
        insert_plan_pricing(plan_tier_ids)


        # Step 7: Insert policies and retrieve policy_ids
        insert_policies(user_ids)
        policy_ids = [cursor.lastrowid - i for i in range(num_records)][::-1]


        # Step 8: Insert plan policies
        insert_plan_policies(policy_ids, plan_ids)


        # Commit transaction
        db.commit()
        print("All records inserted successfully.")


    except Exception as e:
        # Rollback transaction in case of any error
        db.rollback()
        print("An error occurred:", e)


# Run the insertion function
insert_dummy_data()


# Close the connection
cursor.close()
db.close()