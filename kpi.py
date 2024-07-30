import sqlite3
import pandas as pd

conn = sqlite3.connect('datasnap.db')
applications_df = pd.read_sql_query("SELECT * FROM applications", conn)
customers_df = pd.read_sql_query("SELECT * FROM customers", conn)

merged_df = applications_df.merge(customers_df[['customer_id', 'campaign']], on='customer_id', how='left')
          
merged_df['submit_date'] = pd.to_datetime(merged_df['submit_date'])

merged_df['date'] = merged_df['submit_date'].dt.date

min_date = merged_df['date'].min()
max_date = merged_df['date'].max()

date_range_df = pd.DataFrame({'date': pd.date_range(start=min_date, end=max_date)})

stores = merged_df['store'].unique()

store_dates_df = pd.MultiIndex.from_product([stores, date_range_df['date']], names=['store', 'date']).to_frame(index=False)

store_dates_df['date'] = pd.to_datetime(store_dates_df['date'])
merged_df['date'] = pd.to_datetime(merged_df['date'])

# Merge the store-date combinations with the original data
complete_data_df = store_dates_df.merge(merged_df, on=['store', 'date'], how='left')

# KPIs 
metrics = complete_data_df.groupby(['store', 'date']).agg(
    total_dollars_used=pd.NamedAgg(column='dollars_used', aggfunc='sum'),
    total_approved_dollars=pd.NamedAgg(column='approved_amount', aggfunc='sum'),
    total_applications=pd.NamedAgg(column='application_id', aggfunc='count'),
    total_approved_applications=pd.NamedAgg(column='approved', aggfunc=lambda x: (x==1).sum()),
    total_campaigns=pd.NamedAgg(column='campaign', aggfunc=pd.Series.nunique)
).reset_index()

# Fill NaN 
metrics['total_dollars_used'] = metrics['total_dollars_used'].fillna(0)
metrics['total_approved_dollars'] = metrics['total_approved_dollars'].fillna(0)
metrics['total_applications'] = metrics['total_applications'].fillna(0)
metrics['total_approved_applications'] = metrics['total_approved_applications'].fillna(0)
metrics['total_campaigns'] = metrics['total_campaigns'].fillna(0)

# total of applications
metrics['running_total_applications'] = metrics.groupby('store')['total_applications'].cumsum()

#  30-day average 
metrics['rolling_30_day_avg_dollars_used'] = metrics.groupby('store')['total_dollars_used'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)

metrics.to_sql('metrics_catalog_pandas_complete_dates', conn, if_exists='replace', index=False)

conn.close()

print(metrics.head())
