from datetime import datetime
import os
import sqlite3
from pandas import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as spark_func

USER_NAME = os.getlogin()
PROCCESS = 'CustomerAverageSpendWeeklyETL'


def load():

    base_path = 'DB/csvs/'
    spark: SparkSession = SparkSession.builder.appName(
        "Customer Sales ETL").getOrCreate()

    conn = sqlite3.connect('chinook.db')

    customers: DataFrame = spark.read.csv(
        path=f'{base_path}Customer.csv', header=True, inferSchema=True)

    invoices: DataFrame = spark.read.csv(
        path=f'{base_path}Invoice.csv', header=True, inferSchema=True)

    invoices_lines: DataFrame = spark.read.csv(
        path=f'{base_path}InvoiceLine.csv', header=True, inferSchema=True)

    customers_invoices: DataFrame = customers.join(
        other=invoices, on='CustomerId', how='inner')

    customers_invoices_lines: DataFrame = customers_invoices.join(
        other=invoices_lines, on='InvoiceId', how='inner')

    customer_analytics = customers_invoices_lines.groupby('CustomerId').agg(
        spark_func.sum('Total').alias('customer_lifetime_value'),
        spark_func.avg('Total').alias('average_spend_per_invoice'))
 
    final_data_df: DataFrame = customer_analytics.toPandas()

    final_data_df['created_at'] = datetime.now()
    final_data_df['updated_at'] = datetime.now()
    final_data_df['updated_by'] = f'{PROCCESS}:{USER_NAME}'

    final_data_df.to_sql(name='CustomerAverageSpendWeekly',
                         con=conn, if_exists='replace', index=False)

    conn.commit()

    conn.close()


# load()
conn = sqlite3.connect('chinook.db')
cursor = conn.cursor()
cursor.execute("SELECT * FROM CustomerAverageSpendWeekly LIMIT 10")
results = cursor.fetchall()
for row in results:
    print(row)

