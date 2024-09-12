from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from DB.ELTS.TrackPlayCountandRevenueContributionWeeklyELT import load as load_tk_1
from DB.ELTS.BestSellingAlbumsandTrackPopularitybyCountryWeeklyELT import load as load_tk_2
from DB.ELTS.GenrePopularityAndAverageSalesWeeklyELT import load_genre_popularity_and_average_sales
from DB.ELTS.SalesTrendsWeeklyELT import load_sales_trends
from DB.ELTS.popularityTrackByRegionWeekly import load_popularity_track

# from ELTS import X

# Define your Python functions here
def run_genre_popularity_and_average_sales():
    load_genre_popularity_and_average_sales()

def run_sales_trends():
    load_sales_trends()

def run_table_3():
    # Code to generate Table 3
    pass
def  load_track_play_count():
    load_tk_1()

def  load_best_selling_albums():
    load_tk_2()

def  load_popularity_track_by_region():
    load_popularity_track()
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
    task_genre_popularity_and_average_sales = PythonOperator(
        task_id='run_genre_popularity_and_average_sales',
        python_callable = run_genre_popularity_and_average_sales,
    )
    
    task_sales_trends = PythonOperator(
        task_id='run_sales_trends',
        python_callable = run_sales_trends,
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

    popularity_track = PythonOperator(
        task_id='load_popularity_track',
        python_callable=load_popularity_track_by_region,
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id='run_table_2',
        python_callable=run_table_2,
    )

    # Define dependencies
    # task_1 >> task_2
    # task_3 >> task_4
    # task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
