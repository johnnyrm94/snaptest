import sqlite3
import json
import pandas as pd

# Load 
file_path = r'snaptest\datasnap.json'
with open(file_path) as f:
    data = json.load(f)

conn = sqlite3.connect('datasnap.db')
cursor = conn.cursor()

# Create tables
create_table_queries = {
    'customers': """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            DOB TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone_number TEXT,
            language TEXT,
            income REAL,
            title TEXT,
            campaign INTEGER,
            FOREIGN KEY(campaign) REFERENCES marketing(id)
        );
    """,
    'applications': """
        CREATE TABLE IF NOT EXISTS applications (
            application_id TEXT PRIMARY KEY,
            customer_id TEXT,
            store TEXT,
            submit_date TEXT,
            approved INTEGER,
            approved_date TEXT,
            approved_amount REAL,
            dollars_used REAL,
            lease_grade TEXT,
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY(store) REFERENCES stores(store)
        );
    """,
    'stores': """
        CREATE TABLE IF NOT EXISTS stores (
            store TEXT PRIMARY KEY,
            start_dt TEXT,
            state TEXT,
            size TEXT,
            industry TEXT
        );
    """,
    'marketing': """
        CREATE TABLE IF NOT EXISTS marketing (
            id INTEGER PRIMARY KEY,
            name TEXT,
            spend REAL,
            start_date TEXT,
            end_date TEXT
        );
    """
}

for table_name, query in create_table_queries.items():
    cursor.execute(query)

# Insert data
def insert_data(table_name, data):
    if not data:
        print(f"No data to insert for table {table_name}")
        return
    placeholders = ', '.join(['?'] * len(data[0]))
    columns = ', '.join(data[0].keys())
    query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
    print(f"Inserting data into {table_name}: {query}")
    cursor.executemany(query, [tuple(d.values()) for d in data])
    print(f"Data inserted into {table_name}")

customers_data = [record['customers'] for record in data]
applications_data = [record['applications'] for record in data]
stores_data = [record['stores'] for record in data]
marketing_data = [record['marketing'] for record in data]

# Check duplicates
def remove_duplicates(data, key):
    ids = [d[key] for d in data]
    duplicates = set([x for x in ids if ids.count(x) > 1])
    if duplicates:
        print(f"Duplicate {key} values found: {duplicates}")
    unique_data = list({v[key]: v for v in data}.values())
    return unique_data

# Remove duplicates
unique_customers_data = remove_duplicates(customers_data, 'customer_id')
unique_applications_data = remove_duplicates(applications_data, 'application_id')
unique_stores_data = remove_duplicates(stores_data, 'store')
unique_marketing_data = remove_duplicates(marketing_data, 'id')

# Insert
insert_data('marketing', unique_marketing_data)
insert_data('stores', unique_stores_data)
insert_data('customers', unique_customers_data)
insert_data('applications', unique_applications_data)

conn.commit()

# tables into DF
customers_df = pd.read_sql_query("SELECT * FROM customers", conn)
applications_df = pd.read_sql_query("SELECT * FROM applications", conn)
stores_df = pd.read_sql_query("SELECT * FROM stores", conn)
marketing_df = pd.read_sql_query("SELECT * FROM marketing", conn)

# Print
print("Customers Data:")
print(customers_df)
print("\nApplications Data:")
print(applications_df)
print("\nStores Data:")
print(stores_df)
print("\nMarketing Data:")
print(marketing_df)

conn.close()
