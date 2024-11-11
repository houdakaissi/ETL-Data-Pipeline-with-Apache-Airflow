from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import json

default_args = {'owner': 'airflow', 'start_date': datetime(2023, 1, 1), 'retries': 1}

# Task 1: Convert JSON to CSV format
def convert_json_to_csv(file_path, csv_file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    df = pd.json_normalize(data)
    df.to_csv(csv_file_path, index=False)
    print(f"Converted JSON to CSV and saved to {csv_file_path}")
    return df

# Task 2: Drop duplicates from each dataset
def drop_duplicates(df):
    return df.drop_duplicates()

# Task 3: Rename columns for df1
def rename_columns_df1(df1):
    return df1.rename(columns={
        'ORDERDATE': 'orderDate',
        'ORDERNUMBER': 'orderNumber',
        'SALES': 'Sales',
        'CITY': 'city',
        'STATE': 'state',
        'COUNTRY': 'country',
        'POSTALCODE': 'postalCode',
        'DEALSIZE': 'OrderPriority',
        'QUANTITYORDERED': 'quantityOrdered',
        'PRODUCTLINE': 'productName',
        'PRICEEACH': 'priceEach',
        'CUSTOMERNAME': 'customerName',
        'PRODUCTCODE': 'productCode',
        'customerNumber': 'customerNumber',
        'CONTACTLASTNAME': 'contactLastName',
        'CONTACTFIRSTNAME': 'contactFirstName',
        'PHONE': 'phone',
        'ADDRESSLINE1': 'addressLine1',
        'ADDRESSLINE2': 'addressLine2',
        'STATUS': 'status',
        'ORDERLINENUMBER': 'orderLineNumber'
    })

# Task 4: Rename columns for df2
def rename_columns_df2(df2):
    return df2.rename(columns={
        'Total Revenue': 'Sales',
        'Country': 'country',
        'Order Priority': 'OrderPriority',
        'Units Sold': 'quantityOrdered',
        'Item Type': 'productName',
        'Sales Channel': 'SalesChannel',
        'Order Date': 'orderDate',
        'Order ID': 'orderNumber',
        'Unit Price': 'priceEach'
    })

# Task 5: Rename columns for df_json_csv
def rename_columns_df_json_csv(df_json_csv):
    return df_json_csv.rename(columns={
        'sales': 'Sales',
        'Country': 'country',
        'Order ID': 'orderNumber',
        'Order Date': 'orderDate',
        'postal_code': 'postalCode',
        'order_priority': 'OrderPriority',
        'quantity': 'quantityOrdered',
        'product_name': 'productName',
        'Shipping Cost': 'priceEach',
        'Customer ID': 'customerNumber',
        'Customer Name': 'customerName',
        'City': 'city',
        'State': 'state',
        'Postal Code': 'postalCode',
        'Product ID': 'productCode',
        'Sub-Category': 'SubCategory',
        'Quantity': 'quantityOrdered',
        'Order Priority': 'OrderPriority'
    })

# Task 6: Merge df1 with df2
def merge_df1_df2(df1, df2):
    common_columns = ['orderNumber', 'Sales', 'country', 'OrderPriority', 'quantityOrdered', 'productName', 'priceEach']
    return pd.merge(df1, df2, on=common_columns, how='outer')

# Task 7: Merge merged_df1_df2 with df_json_csv
def merge_df1_df2_with_json_csv(merged_df1_df2, df_json_csv):
    common_columns = ['orderNumber', 'Sales', 'country', 'OrderPriority', 'orderDate', 'city', 'productCode', 'quantityOrdered', 'productName']
    return pd.merge(merged_df1_df2, df_json_csv, on=common_columns, how='outer')

# Task 8: Merge with customers
def merge_with_customers(final_merged_df, customers_df):
    merge_columns_customers = ['customerNumber', 'customerName', 'contactLastName', 'contactFirstName', 'phone', 'addressLine1', 'addressLine2', 'city', 'state', 'postalCode', 'country', 'Segment', 'Market']
    return pd.merge(final_merged_df, customers_df, on=merge_columns_customers, how='left')

# Task 9: Merge with products
def merge_with_products(final_merged_df, products_df):
    merge_columns_products = ['productCode', 'productName', 'MSRP']
    return pd.merge(final_merged_df, products_df, on=merge_columns_products, how='left')

# Task 10: Merge with orders
def merge_with_orders(final_merged_df, orders_df):
    merge_columns_orders = ['orderNumber', 'orderDate', 'status', 'OrderPriority', 'Year', 'Month', 'ShipMode']
    return pd.merge(final_merged_df, orders_df, on=merge_columns_orders, how='left')

# Task 11: Merge with orderdetails
def merge_with_orderdetails(final_merged_df, orderdetails_df):
    merge_columns_orderdetails = ['orderNumber', 'productCode', 'quantityOrdered', 'priceEach', 'orderLineNumber', 'SalesChannel', 'Category', 'SubCategory']
    return pd.merge(final_merged_df, orderdetails_df, on=merge_columns_orderdetails, how='left')

# Task 12: Save final merged data to CSV
def save_merged_data_to_csv(merged_data, output_path):
    merged_data.to_csv(output_path, index=False)
    print(f"Saved merged data to {output_path}")

with DAG('transform_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # Define each task with the updated detailed descriptions
    convert_json_task = PythonOperator(
        task_id='convert_json_to_csv',
        python_callable=convert_json_to_csv,
        op_kwargs={'file_path': 'C:\\path_to_json_file', 'csv_file_path': 'C:\\path_to_csv'}
    )

    drop_duplicates_task = PythonOperator(
        task_id='drop_duplicates',
        python_callable=drop_duplicates,
        op_args=[convert_json_task.output]
    )

    rename_columns_df1_task = PythonOperator(
        task_id='rename_columns_df1',
        python_callable=rename_columns_df1,
        op_args=[drop_duplicates_task.output]
    )

    rename_columns_df2_task = PythonOperator(
        task_id='rename_columns_df2',
        python_callable=rename_columns_df2,
        op_args=[drop_duplicates_task.output]
    )

    rename_columns_df_json_csv_task = PythonOperator(
        task_id='rename_columns_df_json_csv',
        python_callable=rename_columns_df_json_csv,
        op_args=[convert_json_task.output]
    )

    merge_df1_df2_task = PythonOperator(
        task_id='merge_df1_with_df2',
        python_callable=merge_df1_df2,
        op_args=[rename_columns_df1_task.output, rename_columns_df2_task.output]
    )

    merge_df1_df2_with_json_csv_task = PythonOperator(
        task_id='merge_df1_df2_with_json_csv',
        python_callable=merge_df1_df2_with_json_csv,
        op_args=[merge_df1_df2_task.output, rename_columns_df_json_csv_task.output]
    )

    merge_with_customers_task = PythonOperator(
        task_id='merge_with_customers',
        python_callable=merge_with_customers,
        op_args=[merge_df1_df2_with_json_csv_task.output, 'C:\\path_to_customers_csv']
    )

    merge_with_products_task = PythonOperator(
        task_id='merge_with_products',
        python_callable=merge_with_products,
        op_args=[merge_with_customers_task.output, 'C:\\path_to_products_csv']
    )

    merge_with_orders_task = PythonOperator(
        task_id='merge_with_orders',
        python_callable=merge_with_orders,
        op_args=[merge_with_products_task.output, 'C:\\path_to_orders_csv']
    )

    merge_with_orderdetails_task = PythonOperator(
        task_id='merge_with_orderdetails',
        python_callable=merge_with_orderdetails,
        op_args=[merge_with_orders_task.output, 'C:\\path_to_orderdetails_csv']
    )

    save_final_task = PythonOperator(
        task_id='save_final_merged_data',
        python_callable=save_merged_data_to_csv,
        op_args=[merge_with_orderdetails_task.output, 'C:\\path_to_final_csv']
    )

    # Define the dependencies between tasks to ensure the correct sequence
    convert_json_task >> drop_duplicates_task >> rename_columns_df1_task
    drop_duplicates_task >> rename_columns_df2_task
    convert_json_task >> rename_columns_df_json_csv_task
    rename_columns_df1_task >> merge_df1_df2_task
    rename_columns_df2_task >> merge_df1_df2_task
    merge_df1_df2_task >> merge_df1_df2_with_json_csv_task
    merge_df1_df2_with_json_csv_task >> merge_with_customers_task >> merge_with_products_task >> merge_with_orders_task >> merge_with_orderdetails_task >> save_final_task
