from pyspark.sql import SparkSession
import sqlite3
import pandas as pd


def customer_loyaltyDailyELT():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyELT_IncrementalLoad").getOrCreate()

    try:
        # Open SQLite connection
        conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")
        cursor = conn.cursor()

        # Check if the 'customer_loyalty' table exists
        cursor.execute("""
             SELECT name FROM sqlite_master WHERE type='table' AND name='customer_loyalty';
         """)
        table_exists = cursor.fetchone()

        # If the table exists, get the latest processed updated_at from the customer_loyalty table
        if table_exists:
            latest_updated_at_query = "SELECT MAX(UpdatedAt) FROM customer_loyalty"
            cursor.execute(latest_updated_at_query)
            latest_updated_at = cursor.fetchone()[0]

        if not table_exists or latest_updated_at is None:
            latest_updated_at = '1900-01-01 00:00:00'  # Default to a very old timestamp

        # Step 2: Extract - Load raw data, filtering updated records
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        new_customers_df = customers_df.filter(customers_df["UpdatedAt"] > latest_updated_at)

        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        new_invoice_lines_df = invoice_lines_df.filter(invoice_lines_df["UpdatedAt"] > latest_updated_at)

        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)
        new_invoices_df = invoices_df.filter(invoices_df["UpdatedAt"] > latest_updated_at)

        # Load filtered data into SQLite (append mode)
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
                    created_at TEXT,
                    UpdatedAt TEXT,
                    updated_by TEXT
                )
            """)

        # Create a temporary table to store the creation date of existing rows
        conn.execute("""
            CREATE TEMPORARY TABLE temp_loyalty_creation_dates AS
            SELECT CustomerId, created_at FROM customer_loyalty;
        """)

        # Step 4: Transform - Insert or update customer loyalty data
        transformation_query = """
            INSERT OR REPLACE INTO customer_loyalty (CustomerId, FirstName, LastName, LoyaltyScore, AverageInvoiceSize, created_at, UpdatedAt, updated_by)
            SELECT
                c.CustomerId,
                c.FirstName,
                c.LastName,
                COUNT(i.InvoiceId) AS LoyaltyScore,
                AVG(i.Total) AS AverageInvoiceSize,
                COALESCE(tlcd.created_at, strftime('%Y-%m-%d %H:%M:%S', 'now')) AS created_at,
                strftime('%Y-%m-%d %H:%M:%S', 'now') AS UpdatedAt,
                'process:Efrat_Harush' AS UpdatedBy
            FROM 
                customers c
            JOIN
                invoices i ON c.CustomerId = i.CustomerId
            JOIN
                invoice_lines il ON i.InvoiceId = il.InvoiceId
            LEFT JOIN
                temp_loyalty_creation_dates tlcd ON c.CustomerId = tlcd.CustomerId
            WHERE
                c.UpdatedAt > ?
            GROUP BY
                c.CustomerId, c.FirstName, c.LastName
            ORDER BY
                c.CustomerId;
        """

        # Execute the transformation query using the latest updated_at
        conn.execute(transformation_query, (latest_updated_at,))
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()


customer_loyaltyDailyELT()
