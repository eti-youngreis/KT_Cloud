from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime

def incremental_load():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Customer Purchase Analysis Incremental") \
        .getOrCreate()

    # Establish SQLite connection
    conn = sqlite3.connect('D:\\בוטקמפ\\s3\\KT_Cloud\\CustomerETL.db')
    cursor = conn.cursor()

    try:
        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_purchase_summary'")
        if cursor.fetchone() is None:
            # Create the table if it doesn't exist
            cursor.execute("""
                CREATE TABLE customer_purchase_summary (
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
            print("Table 'customer_purchase_summary' created.")

        # Get the last updated timestamp from the database
        cursor.execute("SELECT MAX(updated_at) FROM customer_purchase_summary")
        latest_timestamp = cursor.fetchone()[0]

        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'

        # Load CSV files
        customers_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Invoice.csv", header=True, inferSchema=True)

        # Join dataframes
        customer_invoices_df = customers_df.join(invoices_df, on="CustomerId", how="inner")
        customer_invoice_lines_df = customer_invoices_df.join(invoice_lines_df, on="InvoiceId", how="inner")

        # Filter based on the latest timestamp
        customer_invoice_lines_df = customer_invoice_lines_df.filter(customer_invoice_lines_df["updated_at"] > latest_timestamp)

        # Aggregation to calculate TotalSpend and PurchaseFrequency
        aggregated_df = customer_invoice_lines_df.groupBy("CustomerId", "FirstName", "LastName") \
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSpend"),
                F.countDistinct("InvoiceId").alias("PurchaseFrequency")
            )

        # Adding timestamp columns
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_df = aggregated_df.withColumn("created_at", F.lit(current_datetime)) \
                                .withColumn("updated_at", F.lit(current_datetime)) \
                                .withColumn("updated_by", F.lit("process:user_name"))

        # Load data into SQLite
        final_data_df = final_df.toPandas()
        if not final_data_df.empty:
            # Update existing records
            for index, row in final_data_df.iterrows():
                cursor.execute('''
                    UPDATE customer_purchase_summary
                    SET TotalSpend = ?, PurchaseFrequency = ?, updated_at = ?, updated_by = ?
                    WHERE CustomerId = ?
                ''', (row.TotalSpend, row.PurchaseFrequency, row.updated_at, row.updated_by, row.CustomerId))

            # Insert new records
            cursor.executemany('''
                INSERT INTO customer_purchase_summary (CustomerId, FirstName, LastName, TotalSpend, PurchaseFrequency, created_at, updated_at, updated_by)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM customer_purchase_summary WHERE CustomerId = ?
                )
            ''', [(row.CustomerId, row.FirstName, row.LastName, row.TotalSpend, row.PurchaseFrequency, row.created_at, row.updated_at, row.updated_by, row.CustomerId) for index, row in final_data_df.iterrows()])

            # Commit changes
            conn.commit()

        # Select and print the updated records from SQLite
        print("Updated customer_purchase_summary:")
        cursor.execute("SELECT * FROM customer_purchase_summary")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    finally:
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load()
