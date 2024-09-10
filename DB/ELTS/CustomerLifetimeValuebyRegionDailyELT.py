from pyspark.sql import SparkSession
import sqlite3
import pandas as pd

def load():
    # Connect to the Chinook database
    conn = sqlite3.connect('../../../Customers_ELT.db')

    try:
        # EXTRACT (Load tables from the Chinook database)
        # -----------------------------------------------
        customers = pd.read_csv("../../../Customer.csv")
        invoices = pd.read_csv("../../../Invoice.csv")
        invoice_lines = pd.read_csv("../../../InvoiceLine.csv")

        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Load raw data into SQLite
        customers.to_sql('Customers', conn, if_exists='replace', index=False)
        invoices.to_sql('Invoices', conn, if_exists='replace', index=False)
        invoice_lines.to_sql('InvoiceLines', conn, if_exists='replace', index=False)

        # TRANSFORM (Calculate Customer Lifetime Value by Region)
        # -------------------------------------------------------------------------
        # Drop existing table if it exists
        drop_query = """DROP TABLE IF EXISTS customer_lifetime_value_by_region_ELT"""
        conn.execute(drop_query)
        conn.commit()

        # Transformation query to calculate Customer Lifetime Value by Region
        transform_query = """
            CREATE TABLE customer_lifetime_value_by_region_ELT AS 
            SELECT 
                C.CustomerId,
                C.FirstName,
                C.LastName,
                C.Country AS Region,
                SUM(IL.UnitPrice * IL.Quantity) AS LifetimeValue,
                RANK() OVER (PARTITION BY C.Country ORDER BY SUM(IL.UnitPrice * IL.Quantity) DESC) AS CustomerRank,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at,
                'process:SL' AS updated_by
            FROM 
                Customers C
            JOIN Invoices I ON C.CustomerId = I.CustomerId
            JOIN InvoiceLines IL ON I.InvoiceId = IL.InvoiceId
            GROUP BY 
                C.CustomerId, C.FirstName, C.LastName, C.Country;
        """

        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()

    finally:
        # Close the SQLite connection
        conn.close()


