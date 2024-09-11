
from DB.ELTS.CustomerAverageSpendWeeklyELT import load as elt_load
from DB.ETLS.CustomerAverageSpendWeeklyETL import load as etl_load
import pandas as pd
import sqlite3
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


@pytest.fixture(scope="module")
def setup_databases():
    # Set up ELT database
    elt_conn = sqlite3.connect('chinook_elt.db')
    elt_load()

    # Set up ETL database
    etl_conn = sqlite3.connect('chinook.db')
    etl_load()

    yield elt_conn, etl_conn

    # Teardown
    elt_conn.close()
    etl_conn.close()


def test_customer_average_spend(setup_databases):
    elt_conn, etl_conn = setup_databases

    # Query ELT database
    elt_df = pd.read_sql_query(
        "SELECT * FROM CustomerAverageSpend ORDER BY CustomerId", elt_conn)

    # Query ETL database
    etl_df = pd.read_sql_query(
        "SELECT * FROM CustomerAverageSpend ORDER BY CustomerId", etl_conn)

    # Compare results
    pd.testing.assert_frame_equal(
        elt_df[['CustomerId', 'CustomerLifeTimeValue', 'AverageSpendPerInvoice']],
        etl_df[['CustomerId', 'CustomerLifeTimeValue', 'AverageSpendPerInvoice']],
        check_dtype=False
    )


def test_row_count(setup_databases):
    elt_conn, etl_conn = setup_databases

    elt_count = pd.read_sql_query(
        "SELECT COUNT(*) FROM CustomerAverageSpend", elt_conn).iloc[0, 0]
    etl_count = pd.read_sql_query(
        "SELECT COUNT(*) FROM CustomerAverageSpend", etl_conn).iloc[0, 0]

    assert elt_count == etl_count, "Row counts do not match between ELT and ETL results"


def test_total_lifetime_value(setup_databases):
    elt_conn, etl_conn = setup_databases

    elt_total = pd.read_sql_query(
        "SELECT SUM(CustomerLifeTimeValue) FROM CustomerAverageSpend", elt_conn).iloc[0, 0]
    etl_total = pd.read_sql_query(
        "SELECT SUM(CustomerLifeTimeValue) FROM CustomerAverageSpend", etl_conn).iloc[0, 0]

    assert pytest.approx(
        elt_total) == etl_total, "Total lifetime values do not match between ELT and ETL results"
