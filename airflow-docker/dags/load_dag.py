from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

# Function to load a CSV file into a PostgreSQL table
def load_csv_to_postgres(csv_file_path, table_name, **kwargs):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Get the PostgreSQL hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')

    # Convert the DataFrame to SQL and insert into the PostgreSQL table
    pg_hook.insert_rows(table=table_name, rows=df.values.tolist(), replace=True, target_fields=df.columns.tolist())

    print(f"Data from {csv_file_path} successfully loaded into {table_name}.")

# Function to add foreign keys to the tables
def add_foreign_keys(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create Foreign Key Constraints
    cursor.execute("""
        ALTER TABLE products
        ADD CONSTRAINT fk_productlines
        FOREIGN KEY (productLine)
        REFERENCES productlines (productLine);
    """)

    cursor.execute("""
        ALTER TABLE orders
        ADD CONSTRAINT fk_customers
        FOREIGN KEY (customerNumber)
        REFERENCES customers (customerNumber);
    """)

    cursor.execute("""
        ALTER TABLE orderdetails
        ADD CONSTRAINT fk_orders
        FOREIGN KEY (orderNumber)
        REFERENCES orders (orderNumber);
    """)

    cursor.execute("""
        ALTER TABLE orderdetails
        ADD CONSTRAINT fk_products
        FOREIGN KEY (productCode)
        REFERENCES products (productCode);
    """)

    cursor.execute("""
        ALTER TABLE employees
        ADD CONSTRAINT fk_offices
        FOREIGN KEY (officeCode)
        REFERENCES offices (officeCode);
    """)

    cursor.execute("""
        ALTER TABLE customers
        ADD CONSTRAINT fk_employees
        FOREIGN KEY (salesRepEmployeeNumber)
        REFERENCES employees (employeeNumber);
    """)

    cursor.execute("""
        ALTER TABLE payments
        ADD CONSTRAINT fk_customers_payments
        FOREIGN KEY (customerNumber)
        REFERENCES customers (customerNumber);
    """)

    cursor.execute("""
        ALTER TABLE orders
        ADD CONSTRAINT fk_order_priority
        FOREIGN KEY (OrderPriority)
        REFERENCES productlines (productLine);
    """)

    conn.commit()
    cursor.close()
    print("Foreign keys successfully added.")

with DAG('load_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # Task to load customers merged data into PostgreSQL
    load_customers_task = PythonOperator(
        task_id='load_customers_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\customers.csv', 'customers'],
        provide_context=True
    )

    # Task to load products merged data into PostgreSQL
    load_products_task = PythonOperator(
        task_id='load_products_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\products.csv', 'products'],
        provide_context=True
    )

    # Task to load orders merged data into PostgreSQL
    load_orders_task = PythonOperator(
        task_id='load_orders_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\orders.csv', 'orders'],
        provide_context=True
    )

    # Task to load orderdetails merged data into PostgreSQL
    load_orderdetails_task = PythonOperator(
        task_id='load_orderdetails_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\orderdetails.csv', 'orderdetails'],
        provide_context=True
    )

    # Task to load employees data into PostgreSQL
    load_employees_task = PythonOperator(
        task_id='load_employees_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\employees.csv', 'employees'],
        provide_context=True
    )

    # Task to load offices data into PostgreSQL
    load_offices_task = PythonOperator(
        task_id='load_offices_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\offices.csv', 'offices'],
        provide_context=True
    )

    # Task to load payments data into PostgreSQL
    load_payments_task = PythonOperator(
        task_id='load_payments_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\payments.csv', 'payments'],
        provide_context=True
    )

    # Task to load productlines data into PostgreSQL
    load_productlines_task = PythonOperator(
        task_id='load_productlines_to_postgres',
        python_callable=load_csv_to_postgres,
        op_args=['C:\\Users\\dell\\Downloads\\Big Data\\productlines.csv', 'productlines'],
        provide_context=True
    )

    # Task to add foreign key relationships
    add_foreign_keys_task = PythonOperator(
        task_id='add_foreign_keys',
        python_callable=add_foreign_keys,
        provide_context=True
    )

    # Task dependencies
    load_customers_task >> load_products_task >> load_orders_task >> load_orderdetails_task >> load_employees_task >> load_offices_task >> load_payments_task >> load_productlines_task >> add_foreign_keys_task
