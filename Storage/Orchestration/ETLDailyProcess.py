from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from KT_Cloud.Storage.ELTS.CustomerLifetimeValuebyRegionWeeklyELT import elt_process

# Define your Python functions here
def run_table_1():
    elt_process()


# More functions for other tasks as necessary

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize DAG
with DAG(
    'etl_orchestration',
    default_args=default_args,
    description='ETL Process Orchestration DAG',
    schedule_interval=None,  # Set to None for manual runs
    catchup=False,
) as dag:

    # Task 1 (Independent tasks that run first)
    task_1 = PythonOperator(
        task_id='run_table_1',
        python_callable=run_table_1,
    )
    

    


    # Define dependencies


    # You can add more tasks and dependencies following this pattern.
