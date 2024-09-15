from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ETLS.ProductsPurchasedTogetherWeeklyETL import load
from ETLS.topSellingArtistsWeeklyETL import load_top_selling_artists
from ETLS.employeeSalePerformanceCustomerInteractionsWeeklyETL import (
    load_employees_sales_customer_interactions,
)
from ETLS.AveragePurchaseValueWeeklyETL import load_customer_invoices_count_etl
from ETLS.AlbumLength_DownloadsWeeklyETL import load_ETL_album_length_downloads
from ETLS.Revenue_Customer_GenreWeeklyETL import load_ETL_revenue_customer_genre
from ETLS.AlbumPopularityAndRevenueWeeklyETL import (
    load_album_popularity_and_revenue_ETL,
)
from ETLS.CustomerLoyaltyAndInvoiceSizeWeeklyETL import customer_loyaltyWeeklyETL
from ETLS.CustomerLifetimeValuebyRegionWeeklyETL import customer_ltvWeeklyETL
from ETLS.PopularGenresbyCustomerSegmentWeeklyETL import load_popular_genres_by_city_ETL
from ELTS import SalesTrendsWeeklyELT
from ELTS import GenreSalseWeeklyELT
from ETLS.EmployeeCustomerSatisfactionWeeklyETL import load as load_employee_customer_satisfaction_sales
from ETLS.RepeatCustomerAnalysisWeeklyETL import load as load_repeat_customer_analysis
# from ETLS import X


# Define your Python functions here
def run_table_1():
    # Code to generate Table 1

    pass


def run_sales_trends_weekly():
    SalesTrendsWeeklyELT.load()

def run_genre_salse_weekly():
    GenreSalseWeeklyELT.load()
    
def run_customer_loyaltyWeeklyETL():
    customer_loyaltyWeeklyETL()



def run_customer_ltvWeeklyETL():
    customer_ltvWeeklyETL()

def run_customer_purchase_frequency_total_spend():
    load()

def run_top_sell_artists():
    load_top_selling_artists()

def run_album_length_downloads():
    load_ETL_album_length_downloads()

def run_revenue_customer_genre():
    load_ETL_revenue_customer_genre()

def run_album_popularity_and_revenue():
    load_album_popularity_and_revenue_ETL()

def run_popular_genres_by_city():
    load_popular_genres_by_city_ETL()

def run_employees_sales_customer_interactions():
    load_employees_sales_customer_interactions()

def run_customer_invoices_count_etl():
    load_customer_invoices_count_etl()
    
def run_employee_customer_satisfaction_sales_weekly():
    load_employee_customer_satisfaction_sales()

def run_repeat_customer_analysis_weekly():
    load_repeat_customer_analysis()

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
    task_1 = PythonOperator(
        task_id="run_table_1",
        python_callable=run_table_1,
    )

    customer_ltvWeeklyETL = PythonOperator(
        task_id="run_customer_ltvWeeklyETL",
        python_callable=run_customer_ltvWeeklyETL,
    )


    # Dependent tasks that run after Table 1, 3, 5
    customer_loyaltyWeeklyETL = PythonOperator(
        task_id="run_customer_loyaltyWeeklyETL",
        python_callable=run_customer_loyaltyWeeklyETL,
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
    task_top_sell_artists = PythonOperator(
        task_id="run_top_sell_artists",
        python_callable=run_top_sell_artists,
    )
    task_employees_sales_customer_interactions = PythonOperator(
        task_id="employees_sales_customer_interactions",
        python_callable=run_employees_sales_customer_interactions,
    )
    task_customer_invoices_count_etl = PythonOperator(
        task_id="customer_invoices_count_etl",
        python_callable=run_customer_invoices_count_etl,
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
        python_callable=run_sales_trends_weekly,
    )

    task_sales_trends_weekly = PythonOperator(
        task_id="run_sales_trends_weekly",
        python_callable=run_sales_trends_weekly,
    )
    
    task_employee_customer_satisfaction_sales = PythonOperator(
        task_id="run_employee_customer_satisfaction_sales",
        python_callable=run_employee_customer_satisfaction_sales_weekly,
    )
    
    task_repeat_customer_analysis = PythonOperator(
        task_id="run_repeat_customer_analysis",
        python_callable=run_repeat_customer_analysis_weekly,
    )

    # Define dependencies
    # task_1 >> task_2
    # task_3 >> task_4
    # task_5 >> task_6
    # task_album_length_downloads >> task_revenue_customer_genre

    # You can add more tasks and dependencies following this pattern.
