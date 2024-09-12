from pyspark.sql import SparkSession
import sqlite3
import pandas as pd


def incremental_elt_process():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyELT_IncrementalLoad").getOrCreate()

    try:
        # Open SQLite connection
        conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")
        cursor = conn.cursor()

        # Check if the 'AlbumPopularityAndRevenue' table exists
        cursor.execute("""
             SELECT name FROM sqlite_master WHERE type='table' AND name='customer_loyalty';
         """)
        table_exists = cursor.fetchone()

        # If the table exists, drop it
        if table_exists:
            # Step 1: Get the latest processed updated_at from the customer_loyalty table
            latest_updated_at_query = "SELECT MAX(UpdatedAt) FROM customer_loyalty"
            cursor = conn.cursor()
            cursor.execute(latest_updated_at_query)
            latest_updated_at = cursor.fetchone()[0]

        if not table_exists or latest_updated_at is None:
            latest_updated_at = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initiallyaded initially

        # Step 2: Extract - Load raw data, but filter to only include updated records
        # Read customer data and filter by updated_at
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        new_customers_df = customers_df.filter(customers_df["UpdatedAt"] > latest_updated_at)

        # Read invoice line data and filter by updated_at
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        new_invoice_lines_df = invoice_lines_df.filter(invoice_lines_df["UpdatedAt"] > latest_updated_at)

        # Read invoice data and filter by updated_at
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)
        new_invoices_df = invoices_df.filter(invoices_df["UpdatedAt"] > latest_updated_at)

        # Step 3: Load the filtered data into SQLite (append mode)
        new_customers_df.toPandas().to_sql('customers', conn, if_exists='append', index=False)
        new_invoice_lines_df.toPandas().to_sql('invoice_lines', conn, if_exists='append', index=False)
        new_invoices_df.toPandas().to_sql('invoices', conn, if_exists='append', index=False)

        if not table_exists:
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
        # Step 4: Transform - Apply the same SQL transformation logic within SQLite
        transformation_query = """
            INSERT OR REPLACE INTO customer_loyalty (CustomerId, FirstName, LastName, LoyaltyScore, AverageInvoiceSize, CreatedAt, UpdatedAt, UpdatedBy)
            SELECT
                c.CustomerId,
                c.FirstName,
                c.LastName,
                COUNT(i.InvoiceId) AS LoyaltyScore,
                AVG(i.Total) AS AverageInvoiceSize,
                COALESCE(cl.CreatedAt, strftime('%Y-%m-%d %H:%M:%S', 'now')) AS CreatedAt,
                strftime('%Y-%m-%d %H:%M:%S', 'now') AS UpdatedAt,
                'process:Efrat_Harush' AS UpdatedBy
            FROM 
                customers c
            JOIN
                invoices i ON c.CustomerId = i.CustomerId
            JOIN
                invoice_lines il ON i.InvoiceId = il.InvoiceId
            LEFT JOIN
                customer_loyalty cl ON c.CustomerId = cl.CustomerId
            WHERE
                c.UpdatedAt > ?
            GROUP BY
                c.CustomerId, c.FirstName, c.LastName
            ORDER BY
                c.CustomerId;
        """

        # Execute the transformation query using the latest updated_at as a parameter
        conn.execute(transformation_query, (latest_updated_at,))
        # Commit the transaction to save the data
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()


incremental_elt_process()
