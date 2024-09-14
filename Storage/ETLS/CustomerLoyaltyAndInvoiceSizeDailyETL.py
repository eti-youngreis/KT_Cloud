from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, current_timestamp, lit, date_format, col
import sqlite3
import pandas as pd

def customer_loyaltyDailyETL():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyETL_Incremental").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")
    cursor = conn.cursor()

    # Check if customer_loyalty table exists
    cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name='customer_loyalty'
    """)
    table_exists = cursor.fetchone() is not None

    if not table_exists:
        # Create customer_loyalty table if it doesn't exist
        conn.execute("""
            CREATE TABLE customer_loyalty (
                CustomerId INTEGER PRIMARY KEY,
                FirstName TEXT,
                LastName TEXT,
                LoyaltyScore INTEGER,
                AverageInvoiceSize REAL,
                created_at TEXT,
                UpdatedAt TEXT,
                updated_by TEXT
            )
        """)

    # Get the latest updated_at timestamp
    if table_exists:
        cursor.execute("SELECT MAX(UpdatedAt) FROM customer_loyalty")
        latest_updated_at = cursor.fetchone()[0]
        if latest_updated_at is None:
            latest_updated_at = '1900-01-01 00:00:00'
    else:
        latest_updated_at = '1900-01-01 00:00:00'

    try:
        # Step 1: Extraction - Read CSV files into DataFrames
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # Step 2: Transformation - Calculate loyalty score and average invoice size
        # Filter invoices and invoice lines by the latest update date
        invoices_df = invoices_df.filter(col("InvoiceDate") > lit(latest_updated_at))

        # Calculate loyalty score
        loyalty_score_df = customers_df.join(invoices_df, "CustomerId") \
            .groupBy("CustomerId", "FirstName", "LastName") \
            .agg(count("InvoiceId").alias("LoyaltyScore"))

        # Calculate average invoice size
        average_invoice_size_df = invoices_df.join(invoice_lines_df, "InvoiceId") \
            .groupBy("CustomerId") \
            .agg(avg("Total").alias("AverageInvoiceSize"))

        # Combine loyalty score and average invoice size
        result_df = loyalty_score_df.join(average_invoice_size_df, "CustomerId")

        # Add metadata columns
        result_df = result_df.withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("UpdatedAt", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("updated_by", lit("process:your_user_name"))

        # Convert the Spark DataFrame to a pandas DataFrame for loading into SQLite
        new_data = result_df.toPandas()

        # Step 3: Incremental Load
        if not new_data.empty:
            # Delete the updated rows from the customer_loyalty table
            customer_ids = tuple(new_data['CustomerId'].values)
            if len(customer_ids) == 1:
                customer_ids = f"({customer_ids[0]})"
            cursor.execute(f"DELETE FROM customer_loyalty WHERE CustomerId IN {customer_ids}")

            # Insert new and updated rows
            new_data.to_sql('customer_loyalty', conn, if_exists='append', index=False)

            # Commit the transaction
            conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

customer_loyaltyDailyETL()
