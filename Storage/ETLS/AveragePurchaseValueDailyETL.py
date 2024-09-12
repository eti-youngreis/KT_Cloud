
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import sqlite3
import pandas as pd

def incremental_load_average_purchase_value_etl():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Average Purchase Value Over Time by Customer Type") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('./Chinook.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='customer_invices_avg';"
        table_exists = conn.execute(check_table_query).fetchone()
        latest_timestamp = None
        if table_exists:
            latest_timestamp_query = "SELECT MAX(updated_at) FROM customer_invices_avg"
            latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        print("latest_timestamp:", latest_timestamp)

        # EXTRACT (Loading CSVs from S3 or local storage)
        customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
  
        invoices_df = invoices_df.withColumn('InvoiceDate', F.to_date(F.col('InvoiceDate'), 'dd/MM/yyyy'))
        customers_df = customers_df.withColumn('created_at', F.to_date(F.col('created_at'), 'dd/MM/yyyy'))

        # TRANSFORM (Apply joins, groupings, and window functions)
        new_customers_df = customers_df.filter(F.col("created_at") > latest_timestamp)
        new_invoices_df = invoices_df.filter(F.col("InvoiceDate") > latest_timestamp)

        custumer_in_new_invoices_df = new_invoices_df[["CustomerId"]].distinct()
        customers_connect_to_invoices = custumer_in_new_invoices_df.join(customers_df, "CustomerId", "inner")
        customers_update = customers_connect_to_invoices.union(new_customers_df).distinct()

        customers_invoices = invoices_df.join(customers_update, 'CustomerId', "inner") \
            .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
            .groupBy("CustomerId", "InvoiceMonth") \
            .agg(F.avg("Total").alias("avgCustomerInvoices"))

        select_customer = None
        customer_ids_to_delete = [row['CustomerId'] for row in customers_invoices.select("CustomerId").distinct().collect()]
        if customer_ids_to_delete and table_exists:
            ids_placeholder = ', '.join('?' * len(customer_ids_to_delete))
            select_customer = conn.execute(f"SELECT CustomerId, created_at FROM 'customer_invices_avg' WHERE CustomerId IN ({ids_placeholder})", customer_ids_to_delete).fetchall()
            delete_query = f"DELETE FROM 'customer_invices_avg' WHERE CustomerId IN ({ids_placeholder})"
            conn.execute(delete_query, customer_ids_to_delete)

            schema = StructType([
                StructField("CustomerId", StringType(), True),
                StructField("created_at", StringType(), True)
            ])

            select_customer_df = spark.createDataFrame(select_customer, schema=schema)

            joined_df = customers_invoices.join(select_customer_df, on="CustomerId", how="left")

            transformed_data = joined_df.withColumn(
                "created_at", F.coalesce(F.col("created_at"), F.current_date())
            ).withColumn(
                "updated_at", F.current_date()
            ).withColumn(
                "updated_by", F.lit(f"process:shana_levovitz_{datetime.now().strftime('%Y-%m-%d')}")
            )

        else:
            transformed_data = customers_invoices.withColumn("created_at", F.current_date()) \
                .withColumn("updated_at", F.current_date()) \
                .withColumn("updated_by", F.lit(f"process:shana_levovitz_{datetime.now().strftime('%Y-%m-%d')}"))

        transformed_data = transformed_data.withColumn("created_at", F.date_format(F.col("created_at"), "yyyy-MM-dd")) \
            .withColumn("updated_at", F.date_format(F.col("updated_at"), "yyyy-MM-dd"))

        print("transformed_data:", transformed_data.show())
        transformed_data = transformed_data.toPandas()

        # LOAD (Save transformed data into SQLite using KT_DB)
        transformed_data.to_sql(
            'customer_invices_avg', conn, if_exists='append', index=False
        )
        conn.commit()
        print("customer_invices_avg", conn.execute(
            "SELECT * FROM 'customer_invices_avg'").fetchall())

    finally:
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load_average_purchase_value_etl()
