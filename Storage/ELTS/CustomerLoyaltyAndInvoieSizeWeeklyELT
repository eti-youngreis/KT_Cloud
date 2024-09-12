from pyspark.sql import SparkSession
import sqlite3
import pandas as pd

def elt_process():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyELT_FullLoad").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")

    try:
        # Drop and create raw data tables
        conn.execute("DROP TABLE IF EXISTS customers")
        conn.execute("DROP TABLE IF EXISTS invoice_lines")
        conn.execute("DROP TABLE IF EXISTS invoices")

        # Step 1: Extract and Load raw data into SQLite
        # Read customer data and load it into SQLite
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        customers_df.toPandas().to_sql('customers', conn, if_exists='replace', index=False)

        # Read invoice line data and load it into SQLite
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoice_lines_df.toPandas().to_sql('invoice_lines', conn, if_exists='replace', index=False)

        # Read invoice data and load it into SQLite
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)
        invoices_df.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)

        # Drop and create the final transformed customer_loyalty table
        conn.execute("DROP TABLE IF EXISTS customer_loyalty")
        conn.execute("""
            CREATE TABLE customer_loyalty (
                CustomerId INTEGER PRIMARY KEY,
                FirstName TEXT,
                LastName TEXT,
                LoyaltyScore INTEGER,
                AverageInvoiceSize REAL,
                CreatedAt TEXT,
                UpdatedAt TEXT,
                UpdatedBy TEXT
            )
        """)

        # Step 2: Transform - Use SQL for transformations within SQLite
        transformation_query = """
        INSERT INTO customer_loyalty (CustomerId, FirstName, LastName, LoyaltyScore, AverageInvoiceSize, CreatedAt, UpdatedAt, UpdatedBy)
        SELECT
            c.CustomerId,
            c.FirstName,
            c.LastName,
            COUNT(i.InvoiceId) AS LoyaltyScore,
            AVG(i.Total) AS AverageInvoiceSize,
            strftime('%Y-%m-%d %H:%M:%S', 'now') AS CreatedAt,
            strftime('%Y-%m-%d %H:%M:%S', 'now') AS UpdatedAt,
            'process:Efrat_Harush' AS UpdatedBy
        FROM
            customers c
        JOIN
            invoices i ON c.CustomerId = i.CustomerId
        JOIN
            invoice_lines il ON i.InvoiceId = il.InvoiceId
        GROUP BY
            c.CustomerId, c.FirstName, c.LastName
        ORDER BY
            c.CustomerId;
        """

        # Execute the transformation query
        conn.execute(transformation_query)

        # Commit the transaction to save the data
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

elt_process()