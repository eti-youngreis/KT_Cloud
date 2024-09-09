from pyspark.sql import SparkSession
import sqlite3
import pandas as pd

def elt_process():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLTVByRegionELT").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\p_database.db")

    try:
        # Drop and create raw data tables
        conn.execute("DROP TABLE IF EXISTS customers")
        conn.execute("DROP TABLE IF EXISTS invoice_lines")
        conn.execute("DROP TABLE IF EXISTS invoices")

        # E - Extraction: Read CSV files
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # L - Load: Convert Spark DataFrames to pandas and load raw data into SQLite
        customers_df.toPandas().to_sql('customers', conn, if_exists='replace', index=False)
        invoice_lines_df.toPandas().to_sql('invoice_lines', conn, if_exists='replace', index=False)
        invoices_df.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)

        # Drop and create the final transformed customer_ltv table
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
                updated_at TEXT,
                updated_by TEXT,
                PRIMARY KEY (CustomerId, Country)
            )
        """)

        # T - Transformation: Perform SQL queries for transformation within SQLite
        transformation_query = """
        INSERT INTO customer_ltv (CustomerId, FirstName, LastName, Country, CustomerLTV, Rank, created_at, updated_at, updated_by)
        SELECT
            c.CustomerId,
            c.FirstName,
            c.LastName,
            c.Country,
            SUM(il.Total) AS CustomerLTV,
            RANK() OVER (PARTITION BY c.Country ORDER BY SUM(il.Total) DESC) AS Rank,
            strftime('%Y-%m-%d %H:%M:%S', 'now') AS created_at,
            strftime('%Y-%m-%d %H:%M:%S', 'now') AS updated_at,
            'process:user_name' AS updated_by
        FROM
            customers c
        JOIN
            invoices i ON c.CustomerId = i.CustomerId
        JOIN
            invoice_lines il ON i.InvoiceId = il.InvoiceId
        GROUP BY
            c.Country, c.CustomerId, c.FirstName, c.LastName
        ORDER BY
            c.Country, Rank;
        """

        # Execute the transformation query
        conn.execute(transformation_query)

        # Commit the transaction
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

elt_process()
