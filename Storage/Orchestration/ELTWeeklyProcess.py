from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from KT_Cloud.Storage.ELTS.CustomerPurchaseFrequencyTotalSpendWeeklyELT import load_elt
from KT_Cloud.Storage.ELTS.topSellingArtistsWeeklyELT import load_and_transform_data
from KT_Cloud.Storage.ELTS.employeeSalePerformanceCustomerInteractionsWeeklyELT import load_employees_sales_customer_interactions_elt
from KT_Cloud.Storage.ELTS.AveragePurchaseValueWeeklyELT import load_average_purchase_value_elt
from KT_Cloud.Storage.ELTS.AlbumLength_DownloadsWeeklyELT import load_ELT_album_length_downloads
from KT_Cloud.Storage.ELTS.Revenue_Customer_GenreWeeklyELT import load_ELT_revenue_customer_genre
from KT_Cloud.Storage.ELTS.PopularGenresByCustomerSegmentWeeklyELT import (
    load_popular_genres_by_city_ELT,
)
from KT_Cloud.Storage.ELTS.AlbumPopularityAndRevenueWeeklyELT import (
    load_album_popularity_and_revenue_ELT,
)
from KT_Cloud.Storage.ELTS import GenreSalseWeeklyELT
from KT_Cloud.Storage.ELTS.CustomerLifetimeValuebyRegionWeeklyELT import customer_ltvWeeklyELT
from KT_Cloud.Storage.ELTS.CustomerLoyaltyAndInvoieSizeWeeklyELT import customer_loyaltyWeeklyELT
# from ELTS import X


# Define your Python functions here
def run_genre_salse_weekly():
    # Code to generate Table 1
    GenreSalseWeeklyELT.load()


def run_table_2():
    # Code to generate Table 2
    pass

def run_customer_loyaltyWeeklyELT():
    customer_loyaltyWeeklyELT()

def run_customer_ltvWeeklyELT():
    customer_ltvWeeklyELT()

def run_popular_genres_by_city():
    load_popular_genres_by_city_ELT()


def run_album_popularity_and_revenue():
    load_album_popularity_and_revenue_ELT()


# More functions for other tasks as necessary
def run_customer_purchase_frequency_total_spend():
    load_elt()


def run_top_sell_artists():
    load_and_transform_data()


def run_album_length_downloads():
    load_ELT_album_length_downloads()


def run_revenue_customer_genre():
    load_ELT_revenue_customer_genre()


def run_employees_sales_customer_interactions():
    load_employees_sales_customer_interactions_elt()


def run_customer_invoices_count():
    load_average_purchase_value_elt()


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

    customer_loyaltyWeeklyELT = PythonOperator(
        task_id="run_customer_loyaltyWeeklyELT",
        python_callable=run_customer_loyaltyWeeklyELT,
    )

    # Add more independent tasks here
    customer_ltvWeeklyELT = PythonOperator(
        task_id="run_customer_ltvWeeklyELT",
        python_callable=run_customer_ltvWeeklyELT,
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id="run_table_2",
        python_callable=run_table_2,
    )

    task_popular_genres_by_city = PythonOperator(
        task_id="run_popular_genres_by_city",
        python_callable=run_popular_genres_by_city,
    )

    task_album_popularity_and_revenue = PythonOperator(
        task_id="run_album_popularity_and_revenue",
        python_callable=run_album_popularity_and_revenue,
    )

    task_customer_purchase_frequency_total_spend = PythonOperator(
        task_id="run_customer_purchase_frequency_total_spend",
        python_callable=run_customer_purchase_frequency_total_spend,
    )
    task_run_top_sell_artists = PythonOperator(
        task_id="run_top_sell_artists",
        python_callable=run_top_sell_artists,
    )
    task_employees_sales_customer_interactions = PythonOperator(
        task_id="run_employees_sales_customer_interactions",
        python_callable=run_employees_sales_customer_interactions,
    )
    task_average_purchase_value_elt = PythonOperator(
        task_id="run_customer_invoices_count_etl",
        python_callable=run_customer_invoices_count,
    )

    task_album_length_downloads = PythonOperator(
        task_id="run_album_length_downloads",
        python_callable=run_album_length_downloads,
    )

    task_revenue_customer_genre = PythonOperator(
        task_id="run_revenue_customer_genre",
        python_callable=run_revenue_customer_genre,
    )


    task_genre_salse_weekly = PythonOperator(
        task_id="run_genre_salse_weekly",
        python_callable=run_genre_salse_weekly,
    )
    # Define dependencies
    # task_1 >> task_2
    # customer_loyaltyWeeklyELT >> task_4
    # task_5 >> task_6
    # You can add more tasks and dependencies following this pattern.
