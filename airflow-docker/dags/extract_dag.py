from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define file paths
csv_files_dir = 'C:\\Users\\dell\\Downloads\\Big Data\\csv_files'
json_file_path = 'C:\\Users\\dell\\Downloads\\Big Data\\json_file\\StoreSales.json'
output_dir = 'C:\\Users\\dell\\Downloads\\Big Data\\sql_database'

# Extraction functions
def extract_from_csv(file_path):
    return pd.read_csv(file_path)

def extract_from_json(file_path):
    with open(file_path) as f:
        return pd.json_normalize(json.load(f))

# Function to extract data from MySQL database and save each table as a CSV file
def extract_from_mysql_and_save_as_csv():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_xampp_conn')
    tables_df = mysql_hook.get_pandas_df("SHOW TABLES")
    os.makedirs(output_dir, exist_ok=True)
    
    for table in tables_df.iloc[:, 0]:  # assuming first column contains table names
        table_df = mysql_hook.get_pandas_df(f"SELECT * FROM {table}")
        
        # Example transformations
        if table == 'orders':
            table_df['OrderPriority'] = 'Standard'
            table_df['Year'] = pd.to_datetime(table_df['ORDERDATE']).dt.year
            table_df['Month'] = pd.to_datetime(table_df['ORDERDATE']).dt.month
            table_df['ShipMode'] = 'Air'
        elif table == 'customers':
            table_df['Segment'] = 'Retail'
            table_df['Market'] = 'US'
        elif table == 'orderdetails':
            table_df['SalesChannel'] = 'Online'
            table_df['Category'] = 'Electronics'
            table_df['SubCategory'] = 'Laptops'
        
        # Save table data to CSV
        csv_file_path = os.path.join(output_dir, f'{table}.csv')
        table_df.to_csv(csv_file_path, index=False)
        print(f"Saved {table} data to {csv_file_path}")

# Define the DAG
with DAG(
    'extract_dag',
    default_args=default_args,
    description='A DAG for extracting data from multiple sources',
    schedule_interval=None,
    catchup=False
) as dag:

    extract_csv1_task = PythonOperator(
        task_id='extract_csv1',
        python_callable=extract_from_csv,
        op_kwargs={'file_path': os.path.join(csv_files_dir, 'sales_data_sample.csv')},
        do_xcom_push=True
    )

    extract_csv2_task = PythonOperator(
        task_id='extract_csv2',
        python_callable=extract_from_csv,
        op_kwargs={'file_path': os.path.join(csv_files_dir, '50000 Sales Records.csv')},
        do_xcom_push=True
    )

    extract_json_task = PythonOperator(
        task_id='extract_json',
        python_callable=extract_from_json,
        op_kwargs={'file_path': json_file_path},
        do_xcom_push=True
    )

    extract_mysql_task = PythonOperator(
        task_id='extract_mysql',
        python_callable=extract_from_mysql_and_save_as_csv,
        do_xcom_push=False
    )

    # Define task dependencies
    [extract_csv1_task, extract_csv2_task, extract_json_task, extract_mysql_task]

