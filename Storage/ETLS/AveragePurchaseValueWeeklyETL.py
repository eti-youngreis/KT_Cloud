from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from pyspark.sql.types import StringType

# import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_average_purchase_value_etl():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Average Purchase Value Over Time by Customer Type") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('./Chinook.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
        invoiceLines_df = spark.read.csv(
            'C:/Users/shana/Desktop/ETL/InvoiceLine.csv', header=True, inferSchema=True)
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Calculate measures (e.g., total spend, average values)

        invoice_totals = invoiceLines_df \
            .withColumn("ItemTotal", F.col("UnitPrice") * F.col("Quantity")) \
            .groupBy("InvoiceId") \
            .agg(F.sum("ItemTotal").alias("InvoiceTotal"))

        customers_invoices = invoices_df \
            .join(invoice_totals, 'InvoiceId', "inner") \
            .join(customers_df, 'CustomerId', "inner") \
            .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
            .groupBy("CustomerId", "InvoiceMonth") \
            .agg(F.avg("InvoiceTotal").alias("avgCustomerInvoices"))  # חישוב ממוצע הקניות ללקוח בכל חודש

        transformed_data = customers_invoices.withColumn("created_at", F.date_format(F.current_date(), "yyyy-MM-dd")) \
            .withColumn("updated_at", F.date_format(F.current_date(), "yyyy-MM-dd")) \
            .withColumn("updated_by", F.lit(f"process:shana_levovitz_{datetime.now().strftime('%Y-%m-%d')}"))\
            
        transformed_data=transformed_data.toPandas()
        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion

        # Insert transformed data into a new table in SQLite using KT_DB
        transformed_data.to_sql(
            'customer_invices_avg', conn, if_exists='replace', index=False)
        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()
        print("customer_invices_avg", conn.execute(
            "SELECT * FROM 'customer_invices_avg'").fetchall())

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()


if __name__ == "__main__":
    load_average_purchase_value_etl()
