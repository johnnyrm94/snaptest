import sqlite3
import pandas as pd

conn = sqlite3.connect('datasnap.db')
cursor = conn.cursor()

applications_df = pd.read_sql_query("SELECT * FROM applications", conn)
customers_df = pd.read_sql_query("SELECT * FROM customers", conn)

merged_df = applications_df.merge(customers_df[['customer_id', 'campaign']], on='customer_id', how='left')

merged_df['submit_date'] = pd.to_datetime(merged_df['submit_date'])

merged_df['date'] = merged_df['submit_date'].dt.date

min_date = merged_df['date'].min()
max_date = merged_df['date'].max()

date_range = pd.date_range(start=min_date, end=max_date)

cursor.execute("DROP TABLE IF EXISTS date_range")
cursor.execute("CREATE TABLE date_range (date DATE PRIMARY KEY)")
cursor.executemany("INSERT INTO date_range (date) VALUES (?)", [(d.strftime('%Y-%m-%d'),) for d in date_range])

cursor.execute("""
    CREATE TABLE IF NOT EXISTS store_dates AS
    SELECT s.store, d.date
    FROM (SELECT DISTINCT store FROM applications) s
    CROSS JOIN date_range d
""")

conn.commit()

merged_df.to_sql('merged_temp', conn, if_exists='replace', index=False)

# Use SQL to compute the KPIs and create metrics_catalog_sql
cursor.execute("""
    CREATE TABLE IF NOT EXISTS metrics_catalog_sql AS
    WITH store_applications AS (
        SELECT 
            sd.store,
            sd.date,
            mt.dollars_used,
            mt.approved_amount,
            mt.application_id,
            mt.approved,
            mt.campaign
        FROM store_dates sd
        LEFT JOIN merged_temp mt ON sd.store = mt.store AND sd.date = mt.date
    ),
    aggregated_metrics AS (
        SELECT
            store,
            date,
            SUM(dollars_used) AS total_dollars_used,
            SUM(approved_amount) AS total_approved_dollars,
            COUNT(application_id) AS total_applications,
            SUM(CASE WHEN approved = 1 THEN 1 ELSE 0 END) AS total_approved_applications,
            COUNT(DISTINCT campaign) AS total_campaigns
        FROM store_applications
        GROUP BY store, date
    ),
    running_totals AS (
        SELECT
            store,
            date,
            total_dollars_used,
            total_approved_dollars,
            total_applications,
            total_approved_applications,
            total_campaigns,
            SUM(total_applications) OVER (PARTITION BY store ORDER BY date) AS running_total_applications,
            AVG(total_dollars_used) OVER (PARTITION BY store ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30_day_avg_dollars_used
        FROM aggregated_metrics
    )
    SELECT * FROM running_totals
""")

conn.commit()

metrics_catalog_sql_df = pd.read_sql_query("SELECT * FROM metrics_catalog_sql", conn)

print(metrics_catalog_sql_df.head())

conn.close()
