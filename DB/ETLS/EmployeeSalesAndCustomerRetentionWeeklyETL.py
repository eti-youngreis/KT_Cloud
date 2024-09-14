from datetime import datetime
import sqlite3
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as spark_func

USER_NAME = 'USER'
PROCCESS = 'EmployeeSalesCustomersRetentionRateWeeklyETL'


def load():
    """
    Loads and processes employee sales and customer retention data.

    This function performs the following steps:
    1. Initializes Spark session and SQLite connection
    2. Reads CSV files for employees, invoices, and customers
    3. Joins the dataframes to create a combined dataset
    4. Calculates customer retention rate and average sales per employee
    5. Adds metadata columns (CreatedAt, UpdatedAt, UpdatedBy)
    6. Saves the final results to a SQLite database table

    The function uses PySpark for data processing and Pandas for the final database insertion.
    """
    base_path = 'DB/csvs/'
    spark: SparkSession = SparkSession.builder.appName(
        "Employee Sales and Customer Retention ETL").getOrCreate()

    conn = sqlite3.connect('chinook.db')

    employees = spark.read.csv(
        path=f'{base_path}Employee.csv', header=True, inferSchema=True)

    invoices = spark.read.csv(
        path=f'{base_path}Invoice.csv', header=True, inferSchema=True)

    customers = spark.read.csv(
        path=f'{base_path}Customer.csv', header=True, inferSchema=True)

    employees_customers: DataFrame = employees.join(
        other=customers,
        on=employees.EmployeeId == customers.SupportRepId,
        how='inner')

    employees_customers_invoices: DataFrame = employees_customers.join(
        other=invoices, on='CustomerId', how='inner')

    # Define a window for customer purchases grouped by EmployeeId and CustomerId
    customer_purchase_window = Window.partitionBy('EmployeeId', 'CustomerId')

    # Add a column to identify repeat customers
    employees_customers_invoices = employees_customers_invoices.withColumn(
        'IsRepeatCustomer',
        spark_func.when(spark_func.count('InvoiceId').over(
            customer_purchase_window) > 1, 1).otherwise(0)
    )

    # Create a temporary table with aggregated customer data
    temp_table = employees_customers_invoices.groupBy('EmployeeId', 'CustomerId').agg(
        spark_func.any_value('IsRepeatCustomer').alias('IsRepeatCustomer'),
        spark_func.avg('Total').alias('Total')
    )

    # Calculate customer retention rate and average sales per employee
    employee_avg_sales_customer_retention_rate = temp_table.groupBy('EmployeeId').agg(
        (spark_func.sum('IsRepeatCustomer') /
         spark_func.countDistinct('CustomerId')).alias('CustomerRetentionRate'),
        spark_func.avg('Total').alias('AverageSales')
    )

    now = datetime.now()
    employee_avg_sales_customer_retention_rate = employee_avg_sales_customer_retention_rate \
        .withColumn('CreatedAt', spark_func.lit(now)) \
        .withColumn('UpdatedAt', spark_func.lit(now)) \
        .withColumn('UpdatedBy', spark_func.lit(f'{PROCCESS}:{USER_NAME}'))

    final_data_df: DataFrame = employee_avg_sales_customer_retention_rate.toPandas()

    final_data_df.to_sql(name='EmployeeSalesCustomersRetentionRate',
                         con=conn, if_exists='replace', index=False)

    conn.commit()

    conn.close()
