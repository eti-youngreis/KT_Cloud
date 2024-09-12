from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Storage.ETLS import GenreSalesWeeklyETL

# from ETLS import X
from ..ETLS.AlbumTotalTimeDownloadsWeekly import load
from ..ETLS.RevenuePerCustomerGenreWeekly import load as revenue_load



# Define your Python functions here
def run_genre_popularity_and_average_sales():
    load_genre_popularity_and_average_sales()

def run_table_2():
    # Code to generate Table 2
    pass


def run_table_3():
    # Code to generate Table 3
    pass
def  load_track_play_count():
    load_tk_1()

def  load_best_selling_albums():
    load_tk_2()
# More functions for other tasks as necessary
def run_album_totals():
    load()
    
def run_customer_genre_revenue():
    revenue_load()
    
# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Initialize DAG
with DAG(
    "etl_orchestration",
    default_args=default_args,
    description="ETL Process Orchestration DAG",
    schedule_interval=None,  # Set to None for manual runs
    catchup=False,
) as dag:

    # Task 1 (Independent tasks that run first)
    task_run_genre_popularity_and_average_sales = PythonOperator(
        task_id='run_genre_popularity_and_average_sales',
        python_callable = run_genre_popularity_and_average_sales,
    )

    task_3 = PythonOperator(
        task_id="run_table_3",
        python_callable=run_table_3,
    )

    # Add more independent tasks here
    track_play_count = PythonOperator(
        task_id='load_track_play_count',
        python_callable=load_track_play_count,
    )

    best_selling_albums = PythonOperator(
        task_id='load_best_selling_albums',
        python_callable=load_best_selling_albums,
    )
    
    task_album_totals = PythonOperator(
        task_id = 'task_album_totals_weekly',
        python_callable=run_album_totals
    )
    
    task_revenue_customer_genre = PythonOperator(
        task_id = 'task_revenue_customer_genre_weekly',
        python_callable=run_customer_genre_revenue
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id="run_table_2",
        python_callable=run_table_2,
    )
    
    # Define dependencies
    # task_1 >> task_2
    # task_3 >> task_4
    # task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
