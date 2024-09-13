from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from KT_Cloud.Storage.ETLS.CustomerPurchaseFrequencyTotalSpendDailyETL import incremental_load
from KT_Cloud.Storage.ETLS.Revenue_Customer_GenreDailyETL import load_incremental_ETL
from KT_Cloud.Storage.ETLS.AlbumPopularityAndRevenueDailyETL import load_album_popularity_and_revenue_incremental_ETL
from KT_Cloud.Storage.ETLS.CustomerLoyaltyAndInvoiceSizeDailyETL import customer_loyaltyDailyETL
from KT_Cloud.Storage.ETLS.CustomerLifetimeValuebyRegionDailyETL import customer_ltvDailyETL


# from ETLS import X

# Define your Python functions here
def run_table_1():
    # Code to generate Table 1
    pass

def run_customer_loyaltyETL():
    customer_loyaltyDailyETL()

def run_customer_loyaltyDailyETL():
    customer_loyaltyDailyETL()

def run_customer_ltvDailyETL():
    customer_ltvDailyETL()
def run_album_popularity_and_revenue():
    load_album_popularity_and_revenue_incremental_ETL()

def run_customer_purchase_frequency_total_spend():
    incremental_load()
    
def run_revenue_customer_genre():
    load_incremental_ETL()

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

    
    # Add more independent tasks here
    customer_ltvDailyETL = PythonOperator(
        task_id='run_customer_ltvDailyETL',
        python_callable=run_customer_ltvDailyETL,
    )

    customer_loyaltyDailyETL = PythonOperator(
        task_id='run_customer_loyaltyDailyETL',
        python_callable=run_customer_loyaltyDailyETL,
    )

    task_album_popularity_and_revenue= PythonOperator(
        task_id='run_album_popularity_and_revenue',
        python_callable=run_album_popularity_and_revenue,
    )


    task_run_customer_purchase_frequency_total_spend=PythonOperator(
        task_id='run_customer_purchase_frequency_total_spend',
        python_callable=run_customer_purchase_frequency_total_spend,
    )
    
    task_revenue_customer_genre = PythonOperator(
        task_id='run_revenue_customer_genre',
        python_callable=run_revenue_customer_genre,
    )

    # Define dependencies
    # task_1 >> task_2
    # task_3 >> task_4
    # task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
