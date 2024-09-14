from typing import Tuple
import pytest
import pandas as pd
import sqlite3
from pandas import DataFrame


@pytest.fixture
def load_data():
    employees: DataFrame = pd.read_csv('DB/csvs/Employee.csv')
    customers: DataFrame = pd.read_csv('DB/csvs/Customer.csv')
    invoices: DataFrame = pd.read_csv('DB/csvs/Invoice.csv')
    return employees, customers, invoices


@pytest.fixture
def db_connection():
    conn = sqlite3.connect('chinook.db')
    yield conn
    conn.close()


def test_average_sales_and_retention_rate(load_data: Tuple[DataFrame, DataFrame, DataFrame], db_connection: sqlite3.Connection) -> None:
    employees, customers, invoices = load_data

    # Query to get the AverageSales and CustomerRetentionRate from EmployeeSalesCustomersRetentionRate
    query_result = pd.read_sql_query("""
        SELECT EmployeeId, AverageSales, CustomerRetentionRate 
        FROM EmployeeSalesCustomersRetentionRate
    """, db_connection)

    # Calculate expected average sales and retention rate from CSV data
    merged_data = employees.merge(
        customers, left_on='EmployeeId', right_on='SupportRepId')
    merged_data = merged_data.merge(invoices, on='CustomerId')

    expected_avg_sales = merged_data.groupby(
        'EmployeeId')['Total'].mean().reset_index()
    expected_avg_sales.columns = ['EmployeeId', 'ExpectedAverageSales']

    customer_purchase_counts = merged_data.groupby(
        ['EmployeeId', 'CustomerId']).size().reset_index(name='PurchaseCount')
    expected_retention_rate = customer_purchase_counts.groupby('EmployeeId').agg(
        ExpectedRetentionRate=(
            'PurchaseCount', lambda x: (x > 1).sum() / len(x))
    ).reset_index()
    # Merge and compare results
    comparison = query_result.merge(expected_avg_sales, on='EmployeeId')
    comparison = comparison.merge(expected_retention_rate, on='EmployeeId')

    for _, row in comparison.iterrows():
        assert abs(row['AverageSales'] - row['ExpectedAverageSales']) < 0.01, \
            f"AverageSales mismatch for EmployeeId {row['EmployeeId']}: " \
            f"AverageSales={row['AverageSales']}, " \
            f"ExpectedAverageSales={row['ExpectedAverageSales']}"

        assert abs(row['CustomerRetentionRate'] - row['ExpectedRetentionRate']) < 0.01, \
            f"CustomerRetentionRate mismatch for EmployeeId {row['EmployeeId']}: " \
            f"CustomerRetentionRate={row['CustomerRetentionRate']}, " \
            f"ExpectedRetentionRate={row['ExpectedRetentionRate']}"
