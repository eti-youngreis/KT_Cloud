from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, rank, current_timestamp, lit, date_format
from pyspark.sql.window import Window
import sqlite3
import pandas as pd

def customer_ltvWeeklyETL():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLTVByRegionETL").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_ltv.db")

    try:
        # Drop and Create customer_ltv table
        conn.execute("DROP TABLE IF EXISTS customer_ltv")
        conn.execute("""
            CREATE TABLE customer_ltv (
                CustomerId INTEGER,
                FirstName TEXT,
                LastName TEXT,
                Country TEXT,
                CustomerLTV REAL,
                Rank INTEGER,
                created_at TEXT,
                UpdatedAt TEXT,
                updated_by TEXT,
                PRIMARY KEY (CustomerId, Country)
            )
        """)

        # E - Extraction: Read CSV files
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # T - Transformation: Join customers with invoice data
        joined_df = customers_df.join(invoices_df, "CustomerId").join(invoice_lines_df, "InvoiceId")

        # Group by region (or use 'Country') and customer
        region_customer_df = joined_df.groupBy("Country", "CustomerId", "FirstName", "LastName") \
            .agg(spark_sum("Total").alias("CustomerLTV"))

        # Define window for ranking customers by LTV within each region (Country in this case)
        window_spec = Window.partitionBy("Country").orderBy(region_customer_df["CustomerLTV"].desc())

        # Apply ranking
        ranked_df = region_customer_df.withColumn("Rank", rank().over(window_spec))

        # Add metadata columns
        ranked_df = ranked_df.withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("UpdatedAt", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("updated_by", lit("process:user_name"))

        # Convert Spark DataFrame to pandas
        result_pandas = ranked_df.toPandas()

        # Perform full loading into SQLite
        result_pandas.to_sql('customer_ltv', conn, if_exists='append', index=False)

        # Commit the transaction
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

customer_ltvWeeklyETL()
