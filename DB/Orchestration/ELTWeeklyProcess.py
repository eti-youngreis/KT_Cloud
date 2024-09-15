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
from DB.ELTS.EmployeeCustomerSatisfactionAndAverageSalesValueWeeklyELT import load as load_weekly_employee_customer_satisfaction_sales
from DB.ELTS.RepeatCustomerAnalysisByArtistAndPurchaseFrequencyWeeklyELT import load as load_weekly_artist_repeat_customer_analysis
from DB.ELTS.CustomersInvoicesAvgWeeklyELT import load_average_purchase_value_elt

from DB.ELTS import AlbumPopularityWeeklyELT, PopularGenresWeeklyELT
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

def load_employee_customer_satisfaction_sales():
    load_weekly_employee_customer_satisfaction_sales()

def load_artist_repeat_customer_analysis():
    load_weekly_artist_repeat_customer_analysis()

def load_customers_Invoices_average_of_month():
    load_average_purchase_value_elt()
def run_album_popularity_and_revenue():
    AlbumPopularityWeeklyELT.album_popularity_full_elt()

def run_genres_popularity():
    PopularGenresWeeklyELT.popular_genres_by_city_full_elt()

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
    
    task_album_popularity_and_revenue = PythonOperator(
        task_id = 'run_album_popularity_and_revenue',
        python_callable = run_album_popularity_and_revenue
    )
    
    task_genres_popularity = PythonOperator(
        task_id = 'run_genres_popularity',
        python_callable = run_genres_popularity
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
    
    employee_customer_satisfaction_sales = PythonOperator(
        task_id='employee_customer_satisfaction_sales',
        python_callable=load_employee_customer_satisfaction_sales,
    )
    
    artist_repeat_customer_analysis = PythonOperator(
        task_id='artist_repeat_customer_analysis',
        python_callable=load_artist_repeat_customer_analysis,
    )

    customers_Invoices_average_of_month = PythonOperator(
        task_id='customers_Invoices_average_of_month',
        python_callable=load_customers_Invoices_average_of_month,
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
