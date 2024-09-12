from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
<<<<<<< HEAD
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from Storage.ETLS import GenreSalesWeeklyETL

# from ETLS import X
=======
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from DB.ETLS.TrackPlayCountandRevenueContributionWeeklyETL import load as load_tk_1
from DB.ETLS.BestSellingAlbumsandTrackPopularitybyCountryWeeklyETL import load as load_tk_2
from DB.ETLS.GenrePopularityAndAverageSalesWeeklyETL import load_genre_popularity_and_average_sales
# from ELTS import X
>>>>>>> 10bc377cee2c371bc2c9c3659ee433b2708fc569



# Define your Python functions here
<<<<<<< HEAD
def run_table_1():
    GenreSalesWeeklyETL.load

=======
def run_genre_popularity_and_average_sales():
    load_genre_popularity_and_average_sales()
>>>>>>> 10bc377cee2c371bc2c9c3659ee433b2708fc569

def run_table_2():
    # Code to generate Table 2
    pass


def run_table_3():
    # Code to generate Table 3
    pass
def  load_track_play_count():
    load_tk_1()

<<<<<<< HEAD

=======
def  load_best_selling_albums():
    load_tk_2()
>>>>>>> 10bc377cee2c371bc2c9c3659ee433b2708fc569
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
<<<<<<< HEAD
    task_1 = PythonOperator(
        task_id="run_table_1",
        python_callable=run_table_1,
=======
    task_run_genre_popularity_and_average_sales = PythonOperator(
        task_id='run_genre_popularity_and_average_sales',
        python_callable = run_genre_popularity_and_average_sales,
>>>>>>> 10bc377cee2c371bc2c9c3659ee433b2708fc569
    )

    task_3 = PythonOperator(
        task_id="run_table_3",
        python_callable=run_table_3,
    )

    # Add more independent tasks here
<<<<<<< HEAD
    task_5 = PythonOperator(
        task_id="run_table_5",
        python_callable=run_table_5,
=======
    track_play_count = PythonOperator(
        task_id='load_track_play_count',
        python_callable=load_track_play_count,
    )

    best_selling_albums = PythonOperator(
        task_id='load_best_selling_albums',
        python_callable=load_best_selling_albums,
>>>>>>> 10bc377cee2c371bc2c9c3659ee433b2708fc569
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
