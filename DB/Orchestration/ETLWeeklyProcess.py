from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ..ETLS import AlbumPopularityWeeklyETL, PopularGenresWeeklyETL
# from ETLS import X

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


def run_table_4():
    pass

def run_table_5():
    pass

def run_table_6():
    pass

def run_album_popularity_and_revenue():
    AlbumPopularityWeeklyETL.album_popularity_full_etl()

def run_genres_popularity():
    PopularGenresWeeklyETL.popular_genres_by_city_full_etl()

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
    schedule_interval='@weekly',  # Set to None for manual runs
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
    task_4 = PythonOperator(
        task_id='run_table_4',
        python_callable=run_table_4,
    )
    
    # Add more independent tasks here
    task_5 = PythonOperator(
        task_id='run_table_5',
        python_callable=run_table_5,
    )
    task_6 = PythonOperator(
        task_id='run_table_6',
        python_callable=run_table_6,
    )
    
    
    task_album_popularity_and_revenue = PythonOperator(
        task_id = 'run_album_popularity_and_revenue',
        python_callable = run_album_popularity_and_revenue
    )
    
    task_genres_popularity = PythonOperator(
        task_id = 'run_genres_popularity',
        python_callable = run_genres_popularity
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id='run_table_2',
        python_callable=run_table_2,
    )

    # Define dependencies
    task_1 >> task_2
    task_3 >> task_4
    task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
