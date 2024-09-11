from pyspark.sql import SparkSession
import sqlite3  # Assuming you're using sqlite3
import pandas as pd

def load():

    conn = sqlite3.connect('../../../Customers_ELT.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        customers = pd.read_csv("../../../Customer.csv")
        invoices = pd.read_csv("../../../Invoice.csv")
        invoice_lines = pd.read_csv("../../../InvoiceLine.csv")


        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Load raw data into SQLite
        customers.to_sql('Customers', conn, if_exists='replace', index=False)
        invoices.to_sql('Invoices', conn, if_exists='replace', index=False)
        invoice_lines.to_sql('Invoices_Line', conn, if_exists='replace', index=False)

        # TRANSFORM (Perform transformations with SQL queries using KT_DB functions)
        # -------------------------------------------------------------------------
        drop_query="""DROP TABLE IF EXISTS customer_loyalty_and_invoice_size_ELT"""
        conn.execute(drop_query)
        conn.commit()

        transform_query = """
            CREATE TABLE customer_loyalty_and_invoice_size_ELT AS 
            SELECT 
                C.CustomerId,
                C.FirstName,
                C.LastName,
                COUNT(I.InvoiceId) AS loyalty_score,
                AVG(IL.total_spend) AS avg_invoice_size,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at,
                'process:SL' AS updated_by
            FROM 
                Customers C
            LEFT JOIN Invoices I ON C.CustomerId = I.CustomerId
            LEFT JOIN (
                SELECT InvoiceId, SUM(UnitPrice * Quantity) AS total_spend
                FROM Invoices_Line
                GROUP BY InvoiceId
            ) IL ON I.InvoiceId = IL.InvoiceId
            GROUP BY 
                C.CustomerId, C.FirstName, C.LastName;
        """

        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()

    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection