from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, rank, current_timestamp, lit, date_format
from pyspark.sql.window import Window
import sqlite3
import pandas as pd
from datetime import datetime

def customer_ltvDailyETL():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLTVByRegionETL").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_ltv.db")
    cursor = conn.cursor()

    try:
        # Check if customer_ltv table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='customer_ltv'
        """)
        table_exists = cursor.fetchone() is not None

        # If table does not exist, create it
        if not table_exists:
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

        # Retrieve the latest update timestamp if table exists
        if table_exists:
            cursor.execute("SELECT MAX(UpdatedAt) FROM customer_ltv")
            latest_updated_at = cursor.fetchone()[0]
            if latest_updated_at is None:
                latest_updated_at = '1900-01-01 00:00:00'
        else:
            latest_updated_at = '1900-01-01 00:00:00'

        # E - Extraction: Read CSV files, only get records updated after latest_updated_at
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # Rename UpdatedAt columns to avoid ambiguity
        invoices_df = invoices_df.withColumnRenamed("UpdatedAt", "InvoiceUpdatedAt")
        invoice_lines_df = invoice_lines_df.withColumnRenamed("UpdatedAt", "InvoiceLineUpdatedAt")

        # T - Transformation: Join customers with invoice data
        joined_df = customers_df.join(invoices_df, "CustomerId").join(invoice_lines_df, "InvoiceId")

        # Filter rows by latest_updated_at
        joined_df = joined_df.filter(joined_df["InvoiceUpdatedAt"] > lit(latest_updated_at))

        # Group by region (Country) and customer
        region_customer_df = joined_df.groupBy("Country", "CustomerId", "FirstName", "LastName") \
            .agg(spark_sum("Total").alias("CustomerLTV"))

        # Define window for ranking customers by LTV within each region (Country)
        window_spec = Window.partitionBy("Country").orderBy(region_customer_df["CustomerLTV"].desc())

        # Apply ranking
        ranked_df = region_customer_df.withColumn("Rank", rank().over(window_spec))

        # Add metadata columns
        ranked_df = ranked_df.withColumn("UpdatedAt", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("updated_by", lit("process:Efrat_Harush"))

        # Convert Spark DataFrame to pandas
        result_pandas = ranked_df.toPandas()

        # Load existing customer_ltv data into pandas DataFrame if exists
        if table_exists:
            existing_data = pd.read_sql_query("SELECT * FROM customer_ltv", conn)
        else:
            existing_data = pd.DataFrame()

        # Find rows to update by checking for matching CustomerId and Country
        updated_rows = pd.merge(result_pandas, existing_data, on=["CustomerId", "Country"], how="inner",
                                suffixes=('', '_existing'))

        # Delete existing rows that need updating
        if not updated_rows.empty:
            existing_data = existing_data[~existing_data[['CustomerId', 'Country']].isin(updated_rows[['CustomerId', 'Country']]).all(axis=1)]

        # Merge new and updated rows
        result_pandas = pd.concat([existing_data, result_pandas])

        # Insert new and updated rows with preserved created_at for updated rows
        for _, row in result_pandas.iterrows():
            created_at = row['created_at'] if 'created_at' in row and pd.notnull(row['created_at']) else datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute("""
                INSERT OR REPLACE INTO customer_ltv (CustomerId, FirstName, LastName, Country, CustomerLTV, Rank, created_at, UpdatedAt, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['CustomerId'], row['FirstName'], row['LastName'], row['Country'],
                row['CustomerLTV'], row['Rank'], created_at, row['UpdatedAt'], row['updated_by']
            ))

        # Commit the transaction
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

customer_ltvDailyETL()
