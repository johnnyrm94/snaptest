import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('datasnap.db')

# Function to execute SQL query and return the result as a DataFrame
def execute_query(query, conn):
    return pd.read_sql_query(query, conn)

# Queries and corresponding sheet names for the Excel file
queries = [
    {
        "sheet_name": "Customers",
        "query": "SELECT * FROM customers"
    },
    {
        "sheet_name": "Customer Count",
        "query": "SELECT COUNT(*) AS customer_count FROM customers"
    },
    {
        "sheet_name": "Top 10 Customers by Income",
        "query": "SELECT * FROM customers ORDER BY income DESC LIMIT 10"
    },
    {
        "sheet_name": "Applications",
        "query": "SELECT * FROM applications"
    },
    {
        "sheet_name": "Application Count",
        "query": "SELECT COUNT(*) AS application_count FROM applications"
    },
    {
        "sheet_name": "Approved Applications",
        "query": "SELECT * FROM applications WHERE approved = 1"
    },
    {
        "sheet_name": "Applications by Customer",
        "query": """
            SELECT c.first_name, c.last_name, COUNT(a.application_id) AS application_count
            FROM customers c
            JOIN applications a ON c.customer_id = a.customer_id
            GROUP BY c.customer_id
            ORDER BY application_count DESC
            LIMIT 10
        """
    },
    {
        "sheet_name": "Stores",
        "query": "SELECT * FROM stores"
    },
    {
        "sheet_name": "Store Count",
        "query": "SELECT COUNT(*) AS store_count FROM stores"
    },
    {
        "sheet_name": "Top 10 Stores by Size",
        "query": "SELECT * FROM stores ORDER BY size DESC LIMIT 10"
    },
    {
        "sheet_name": "Marketing Campaigns",
        "query": "SELECT * FROM marketing"
    },
    {
        "sheet_name": "Campaign Count",
        "query": "SELECT COUNT(*) AS campaign_count FROM marketing"
    },
    {
        "sheet_name": "Top 10 Campaigns by Spend",
        "query": "SELECT * FROM marketing ORDER BY spend DESC LIMIT 10"
    },
    {
        "sheet_name": "Total Approved Dollars by Store",
        "query": """
            SELECT s.store, SUM(a.approved_amount) AS total_approved_dollars
            FROM stores s
            JOIN applications a ON s.store = a.store
            WHERE a.approved = 1
            GROUP BY s.store
            ORDER BY total_approved_dollars DESC
            LIMIT 10
        """
    },
    {
        "sheet_name": "Total Dollars Used by Campaign",
        "query": """
            SELECT m.name, SUM(a.dollars_used) AS total_dollars_used
            FROM marketing m
            JOIN customers c ON m.id = c.campaign
            JOIN applications a ON c.customer_id = a.customer_id
            GROUP BY m.id
            ORDER BY total_dollars_used DESC
            LIMIT 10
        """
    },
    {
        "sheet_name": "Total Applications by Store and Date",
        "query": """
            SELECT a.store, a.submit_date, COUNT(a.application_id) AS total_applications
            FROM applications a
            GROUP BY a.store, a.submit_date
            ORDER BY a.submit_date DESC, total_applications DESC
            LIMIT 10
        """
    },
    {
        "sheet_name": "KPIs - Total Dollars Used",
        "query": """
            SELECT store, date, SUM(total_dollars_used) AS total_dollars_used
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    },
    {
        "sheet_name": "KPIs - Total Approved Dollars",
        "query": """
            SELECT store, date, SUM(total_approved_dollars) AS total_approved_dollars
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    },
    {
        "sheet_name": "KPIs - Total Applications",
        "query": """
            SELECT store, date, SUM(total_applications) AS total_applications
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    },
    {
        "sheet_name": "KPIs - Total Approved Applications",
        "query": """
            SELECT store, date, SUM(total_approved_applications) AS total_approved_applications
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    },
    {
        "sheet_name": "KPIs - Total Campaigns",
        "query": """
            SELECT store, date, SUM(total_campaigns) AS total_campaigns
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    },
    {
        "sheet_name": "KPIs - Running Total Applications",
        "query": """
            SELECT store, date, SUM(running_total_applications) AS running_total_applications
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    },
    {
        "sheet_name": "KPIs - 30-day Rolling Avg Dollars Used",
        "query": """
            SELECT store, date, AVG(rolling_30_day_avg_dollars_used) AS rolling_30_day_avg_dollars_used
            FROM metrics_catalog_pandas_complete_dates
            GROUP BY store, date
            ORDER BY date DESC, store
        """
    }
]

# Create a Pandas Excel writer using openpyxl as the engine
with pd.ExcelWriter('reports.xlsx', engine='openpyxl') as writer:
    for q in queries:
        df = execute_query(q["query"], conn)
        df.to_excel(writer, sheet_name=q["sheet_name"], index=False)

# Close the database connection
conn.close()
