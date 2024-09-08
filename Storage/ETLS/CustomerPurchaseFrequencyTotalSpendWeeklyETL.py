from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime

def load():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Customer Purchase Analysis") \
        .getOrCreate()

    # Establish SQLite connection using sqlite3
    conn = sqlite3.connect('D:\\בוטקמפ\\s3\\KT_Cloud\\Customer.db')
    cursor = conn.cursor()

    try:
        # EXTRACT (Loading CSVs from local storage)
        customers_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Invoice.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, aggregations, and calculations)
        customer_invoices_df = customers_df.join(invoices_df, on="CustomerId", how="inner")
        customer_invoice_lines_df = customer_invoices_df.join(invoice_lines_df, on="InvoiceId", how="inner")

        # Aggregation to calculate TotalSpend and PurchaseFrequency
        aggregated_df = customer_invoice_lines_df.groupBy("CustomerId", "FirstName", "LastName") \
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSpend"),
                F.countDistinct("InvoiceId").alias("PurchaseFrequency")
            )

        # Adding timestamp columns for record creation and update
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_df = aggregated_df.withColumn("created_at", F.lit(current_datetime)) \
                                .withColumn("updated_at", F.lit(current_datetime)) \
                                .withColumn("updated_by", F.lit("process:user_name"))

        # LOAD (Save transformed data into SQLite using sqlite3)
        final_data_df = final_df.toPandas()
        if final_data_df.empty:
            print("DataFrame is empty. No data to write.")
        else:
            # Create the table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_purchase_summary (
                    CustomerId INTEGER,
                    FirstName TEXT,
                    LastName TEXT,
                    TotalSpend REAL,
                    PurchaseFrequency INTEGER,
                    created_at TEXT,
                    updated_at TEXT,
                    updated_by TEXT
                )
            """)
            # Write the DataFrame to the SQLite table
            final_data_df.to_sql('customer_purchase_summary', conn, if_exists='replace', index=False)
            conn.commit()

        # Query and print the contents of the table
        cursor.execute("SELECT * FROM customer_purchase_summary;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    finally:
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load()
