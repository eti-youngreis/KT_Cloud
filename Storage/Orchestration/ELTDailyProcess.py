from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ELTS.TrackPlayCountandRevenueContributionDailyELT import (
    incremental_load as incrementel_load_tk_1,
)
from ELTS.BestSellingAlbumsandTrackPopularitybyCountryDailyELT import (
    incremental_load as incrementel_load_tk_2,
)
from ELTS.CustomerLifetimeValuebyRegionDailyELT import (
    customer_ltvDailyELT,
)
from ELTS.CustomerLoyaltyAndInvoieSizeDailyELT import (
    customer_loyaltyDailyELT,
)

# import DB.ELTS.TrackPlayCountandRevenueContributionDailyELT
# from ELTS import X


# Define your Python functions here
def run_table_1():
    # Code to generate Table 1
    pass


def run_customer_loyaltyDailyELT():
    customer_loyaltyDailyELT()


def run_customer_ltvDailyELT():
    customer_ltvDailyELT()


def load_track_play_count():
    incrementel_load_tk_1()


def load_best_selling_albums():
    incrementel_load_tk_2()


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
#
# Initialize DAG
with DAG(
    "ELT_orchestration",
    default_args=default_args,
    description="ELT Process Orchestration DAG",
    schedule_interval=None,  # Set to None for manual runs
    catchup=False,
) as dag:
    # Task 1 (Independent tasks that run first)
    task_1 = PythonOperator(
        task_id="run_table_1",
        python_callable=run_table_1,
    )

    customer_loyaltyDailyELT = PythonOperator(
        task_id="run_customer_loyaltyDailyELT",
        python_callable=run_customer_loyaltyDailyELT,
    )

    customer_ltvDailyELT = PythonOperator(
        task_id="run_customer_ltvDailyELT",
        python_callable=run_customer_ltvDailyELT,
    )
    # Add more independent tasks here
    track_play_count = PythonOperator(
        task_id="load_track_play_count",
        python_callable=load_track_play_count,
    )

    best_selling_albums = PythonOperator(
        task_id="load_best_selling_albums",
        python_callable=load_best_selling_albums,
    )

    # Define dependencies
    # task_1 >> task_2
    # task_3 >> task_4
    # task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
