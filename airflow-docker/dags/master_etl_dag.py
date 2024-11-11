from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

# Define the master DAG
with DAG('master_etl_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # Start task
    start_task = DummyOperator(task_id='start')

    # Trigger the extract DAG
    trigger_extract_dag = TriggerDagRunOperator(
        task_id='trigger_extract_dag',
        trigger_dag_id='extract_dag',
        conf={},
        wait_for_completion=True,
        trigger_rule='all_done'  # Proceed to the next task regardless of success or failure
    )

    # Trigger the transform DAG
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='transform_dag',
        conf={},
        wait_for_completion=True,
        trigger_rule='all_done'
    )

    # Trigger the load DAG
    trigger_load_dag = TriggerDagRunOperator(
        task_id='trigger_load_dag',
        trigger_dag_id='load_dag',
        conf={},
        wait_for_completion=True,
        trigger_rule='all_done'
    )

    # End task
    end_task = DummyOperator(task_id='end')

    # Define task dependencies
    start_task >> trigger_extract_dag >> trigger_transform_dag >> trigger_load_dag >> end_task
