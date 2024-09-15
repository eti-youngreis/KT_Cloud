from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, current_timestamp, lit, date_format
import sqlite3

def customer_loyaltyWeeklyETL():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyETL_FullLoad").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")

    try:
        # Drop and create customer_loyalty table
        conn.execute("DROP TABLE IF EXISTS customer_loyalty")
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

        # Step 1: Extraction - Read CSV files into DataFrames
        # Read customer data from CSV
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        # Read invoice line data from CSV
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        # Read invoice data from CSV
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # Step 2: Transformation - Calculate loyalty score and average invoice size

        # Join customer data with invoice data to calculate loyalty score
        # This aggregates the number of invoices per customer to determine loyalty score
        loyalty_score_df = customers_df.join(invoices_df, "CustomerId") \
            .groupBy("CustomerId", "FirstName", "LastName") \
            .agg(count("InvoiceId").alias("LoyaltyScore"))

        # Join invoice data with invoice line data to calculate average invoice size
        # This computes the average total amount of invoices for each customer
        average_invoice_size_df = invoices_df.join(invoice_lines_df, "InvoiceId") \
            .groupBy("CustomerId") \
            .agg(avg("Total").alias("AverageInvoiceSize"))

        # Combine the results of loyalty score and average invoice size into a single DataFrame
        result_df = loyalty_score_df.join(average_invoice_size_df, "CustomerId")

        # Add metadata columns to the DataFrame
        # This includes creation and update timestamps and the user who updated the data
        result_df = result_df.withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("UpdatedAt", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("updated_by", lit("process:your_user_name"))

        # Convert the Spark DataFrame to a pandas DataFrame for loading into SQLite
        new_data = result_df.toPandas()

        # Step 3: Full Load - Insert data into SQLite
        # Insert or append the data into the customer_loyalty table
        new_data.to_sql('customer_loyalty', conn, if_exists='append', index=False)

        # Commit the transaction to save changes
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

customer_loyaltyWeeklyETL()
