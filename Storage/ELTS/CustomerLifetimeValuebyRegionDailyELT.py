import sqlite3
import pandas as pd
from pyspark.sql import SparkSession

def customer_ltvDailyELT():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyELT_IncrementalLoad").getOrCreate()
    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_ltv.db")
    cursor = conn.cursor()

    try:
        # Check if the 'customer_ltv' table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='customer_ltv';
        """)
        table_exists = cursor.fetchone()

        if table_exists:
            # Get the latest processed updated_at from the customer_ltv table
            latest_updated_at_query = "SELECT MAX(UpdatedAt) FROM customer_ltv"
            cursor.execute(latest_updated_at_query)
            latest_updated_at = cursor.fetchone()[0]
        else:
            latest_updated_at = None

        if not table_exists or latest_updated_at is None:
            latest_updated_at = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially

        # Extract data
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        new_customers_df = customers_df.filter(customers_df["UpdatedAt"] > latest_updated_at)

        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        new_invoice_lines_df = invoice_lines_df.filter(invoice_lines_df["UpdatedAt"] > latest_updated_at)

        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)
        new_invoices_df = invoices_df.filter(invoices_df["UpdatedAt"] > latest_updated_at)

        # Load the filtered data into temporary SQLite tables
        new_customers_df.toPandas().to_sql('customers', conn, if_exists='replace', index=False)
        new_invoice_lines_df.toPandas().to_sql('invoice_lines', conn, if_exists='replace', index=False)
        new_invoices_df.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)

        # Create the customer_ltv table if it doesn't exist
        if not table_exists:
            create_table_query = """
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
            );
            """
            conn.execute(create_table_query)

        # Create a temporary table to store the existing records with their 'created_at' field
        cursor.execute("DROP TABLE IF EXISTS temp_customer_ltv;")
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_customer_ltv AS
            SELECT * FROM customer_ltv WHERE 1=0;
        """)
        cursor.execute("""
            INSERT INTO temp_customer_ltv (CustomerId, FirstName, LastName, Country, CustomerLTV, Rank, created_at, UpdatedAt, updated_by)
            SELECT CustomerId, FirstName, LastName, Country, CustomerLTV, Rank, created_at, UpdatedAt, updated_by
            FROM customer_ltv
            WHERE (CustomerId, Country) IN (
                SELECT CustomerId, Country FROM customers
            );
        """)

        # Delete old records for updated customers from the customer_ltv table
        delete_query = """
        DELETE FROM customer_ltv
        WHERE (CustomerId, Country) IN (
            SELECT CustomerId, Country FROM customers
        );
        """
        cursor.execute(delete_query)

        # Insert updated records into customer_ltv table, preserving 'created_at'
        insert_query = """
        INSERT INTO customer_ltv (CustomerId, FirstName, LastName, Country, CustomerLTV, Rank, created_at, UpdatedAt, updated_by)
        SELECT
            c.CustomerId,
            c.FirstName,
            c.LastName,
            c.Country,
            SUM(i.Total) AS CustomerLTV,
            RANK() OVER (PARTITION BY c.Country ORDER BY SUM(i.Total) DESC) AS Rank,
            COALESCE(tcl.created_at, datetime('now')) AS created_at,  -- Preserve created_at or use current timestamp
            datetime('now') AS UpdatedAt,
            'process:Efrat_Harush' AS updated_by
        FROM
            customers c
        JOIN
            invoices i ON c.CustomerId = i.CustomerId
        JOIN
            invoice_lines il ON i.InvoiceId = il.InvoiceId
        LEFT JOIN
            temp_customer_ltv tcl ON c.CustomerId = tcl.CustomerId AND c.Country = tcl.Country
        GROUP BY
            c.Country, c.CustomerId, c.FirstName, c.LastName
        ORDER BY
            c.Country, Rank;
        """
        cursor.execute(insert_query)
        conn.commit()

    finally:
        # Drop temporary tables
        cursor.execute("DROP TABLE IF EXISTS customers;")
        cursor.execute("DROP TABLE IF EXISTS invoice_lines;")
        cursor.execute("DROP TABLE IF EXISTS invoices;")
        cursor.execute("DROP TABLE IF EXISTS temp_customer_ltv;")

        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

customer_ltvDailyELT()
