from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ..ELTS.CustomerPurchaseFrequencyTotalSpendWeeklyELT import load_elt
from ..ELTS.topSellingArtistsWeeklyELT import load_and_transform_data
from ..ELTS.employeeSalePerformanceCustomerInteractionsWeeklyELT import load_employees_sales_customer_interactions_elt
from ..ELTS.CustomerAverageSpendWeeklyELT import load_average_purchase_value_elt
# from ELTS import X

# Define your Python functions here
def run_table_1():
    # Code to generate Table 1
    pass

def run_table_2():
    # Code to generate Table 2
    pass

def run_table_3():
    # Code to generate Table 3
    pass

# More functions for other tasks as necessary
def run_customer_purchase_frequency_total_spend():
    load_elt()
def run_top_sell_artists():
    load_and_transform_data()

def run_employees_sales_customer_interactions():
    load_employees_sales_customer_interactions_elt()

def run_customer_invoices_count():
    load_average_purchase_value_elt()

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
    
    task_3 = PythonOperator(
        task_id='run_table_3',
        python_callable=run_table_3,
    )
    
    # Add more independent tasks here
    task_5 = PythonOperator(
        task_id='run_table_5',
        python_callable=run_table_5,
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id='run_table_2',
        python_callable=run_table_2,
    )


    task_customer_purchase_frequency_total_spend(
        task_id='run_customer_purchase_frequency_total_spend',
        python_callable=run_customer_purchase_frequency_total_spend,
    )
    task_run_top_sell_artists(
        task_id='run_top_sell_artists',
        python_callable=run_top_sell_artists,
    )
    task_employees_sales_customer_interactions(
        task_id='run_employees_sales_customer_interactions',
        python_callable=run_employees_sales_customer_interactions,
    )
    task_average_purchase_value_elt(
        task_id='run_customer_invoices_count_etl',
        python_callable=run_customer_invoices_count,
    )
    # Define dependencies
    task_1 >> task_2
    task_3 >> task_4
    task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
