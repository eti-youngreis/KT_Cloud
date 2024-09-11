from pyspark.sql import SparkSession, functions as F
import sqlite3
from pyspark.sql.types import StringType

def load_customers_average_spend_lifetime_value_elt():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT - Customer Average Spend and Lifetime Value") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('./ELT_customer_data.db')  # Connect to SQLite database

    try:
        # -----------------------------
        # E - Extract
        # -----------------------------
        # Read the relevant CSV files
        customers_df = spark.read.csv('D:\\בוטקאמפ\\vastProject\\csv\\Customer.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv('D:\\בוטקאמפ\\vastProject\\csv\\Invoice.csv', header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv('D:\\בוטקאמפ\\vastProject\\csv\\InvoiceLine.csv', header=True, inferSchema=True)

        # Remove the BirthDate casting since that column does not exist
        # -----------------------------
        # L - Load
        # -----------------------------
        # Load raw data into SQLite (using Pandas to convert Spark DataFrames)
        customers_df = customers_df.toPandas()
        customers_df.to_sql('Customers', conn, if_exists='replace', index=False)
        invoices_df = invoices_df.toPandas()
        invoices_df.to_sql('Invoices', conn, if_exists='replace', index=False)
        invoice_lines_df = invoice_lines_df.toPandas()
        invoice_lines_df.to_sql('InvoiceLines', conn, if_exists='replace', index=False)

        conn.execute("DROP TABLE IF EXISTS customer_spend_lifetime_value_etl;")

        # -----------------------------
        # T - Transform
        # -----------------------------
        # Write a SQL query to perform transformations (joining and aggregation)
        transform_query = """
        CREATE TABLE customer_spend_lifetime_value_etl AS 
        SELECT c.CustomerId, c.FirstName, c.LastName,
               AVG(il.UnitPrice * il.Quantity) AS AvgSpendPerInvoice,
               SUM(il.UnitPrice * il.Quantity) AS LifetimeValue,
               CURRENT_DATE AS created_at,
               CURRENT_DATE AS updated_at,
               'process:yehudit_schwartzman_' || CURRENT_DATE AS updated_by
        FROM customers c
        JOIN invoices i ON c.CustomerId = i.CustomerId
        JOIN invoicelines il ON i.InvoiceId = il.InvoiceId
        GROUP BY c.CustomerId
        """

        # Execute the transformation query to create the final table
        conn.execute(transform_query)

        # Commit the transformation
        conn.commit()

        # Print results to verify the transformed data
        print("customer_spend_lifetime_value_etl:", conn.execute("SELECT * FROM 'customer_spend_lifetime_value_etl'").fetchall())
        print()
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    load_customers_average_spend_lifetime_value_elt()
