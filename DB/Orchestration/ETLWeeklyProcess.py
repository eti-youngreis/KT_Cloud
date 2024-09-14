from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from DB.ETLS.EmployeeCustomerSatisfactionAndAverageSalesWeeklyETL import load as load_weekly_employee_customer_satisfaction_and_averagesales_value
from DB.ETLS.RepeatCustomerAnalysisByArtistAndPurchaseFrequencyWeeklyETL import load as load_weekly_artist_repeat_customer_analysis
from DB.ETLS.TrackLengthandDownloadFrequencyWeeklyETL import load_Track_Length_and_Download_Frequency
from DB.ETLS.TrackPopularityWeeklyETL import load_Track_Popularity
# from ETLS import X



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
    
def  load_employee_customer_satisfaction_and_averagesales_value():
    load_weekly_employee_customer_satisfaction_and_averagesales_value()
    
def  load_artist_repeat_customer_analysis():
    load_weekly_artist_repeat_customer_analysis()

def load_Track_Popularity_etl():
    load_Track_Popularity()


def load_Track_Length_and_Download_Frequency_etl():
    load_Track_Length_and_Download_Frequency()    
    
# More functions for other tasks as necessary

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
    
    employee_customer_satisfaction_and_averagesales_value = PythonOperator(
        task_id='employee_customer_satisfaction_and_averagesales_value',
        python_callable=load_employee_customer_satisfaction_and_averagesales_value,
    )

    artist_repeat_customer_analysis = PythonOperator(
        task_id='artist_repeat_customer_analysis',
        python_callable=load_artist_repeat_customer_analysis,
    )

    track_Popularity_task = PythonOperator(
        task_id='load_Track_Popularity_etl',
        python_callable=load_Track_Popularity_etl,
    )
    track_Length_and_Download_Frequency_task = PythonOperator(
        task_id='load_Track_Length_and_Download_Frequency_etl',
        python_callable=load_Track_Length_and_Download_Frequency_etl,
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
