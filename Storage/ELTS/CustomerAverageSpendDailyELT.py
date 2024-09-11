from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import pandas as pd

def incremental_load_average_purchase_value_elt():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template without KT_DB") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Step 2: Establish SQLite connection using sqlite3
    conn = sqlite3.connect('./Chinook.db')  # Connect to the SQLite database
    try:
        check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='customer_invoice_avg_elt';"
        table_exists = conn.execute(check_table_query).fetchone()
        latest_timestamp = None
        if table_exists:
            latest_timestamp_query = "SELECT MAX(updated_at) FROM customer_invoice_avg_elt"
            latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        print("latest_timestamp:", latest_timestamp)
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table (e.g., Customers, Invoices, InvoiceLines)
        customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
  
        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        # new_customers_df = customers_df.filter(F.col("created_at") > latest_timestamp)
        # new_invoices_df = invoices_df.filter(F.col("InvoiceDate") > latest_timestamp)
        # invoices_df = invoices_df.withColumn('InvoiceDate', F.to_date(F.col('InvoiceDate'), 'dd/MM/yyyy'))
        # customers_df = customers_df.withColumn('created_at', F.to_date(F.col('created_at'), 'dd/MM/yyyy'))

        customers_pd = customers_df.toPandas()
        invoices_pd = invoices_df.toPandas()
        # Load raw data into SQLite
        customers_pd['created_at'] = pd.to_datetime(customers_pd['created_at'], format='%d/%m/%Y %H:%M')
        invoices_pd['InvoiceDate'] = pd.to_datetime(invoices_pd['InvoiceDate'], format='%d/%m/%Y %H:%M')
        customers_pd.to_sql('Customers_ELT', conn, if_exists='replace', index=False)
        invoices_pd.to_sql('Invoices_ELT', conn, if_exists='replace', index=False)
        # TRANSFORM (Perform transformations with SQL queries)
        # ----------------------------------------------------
        # Join tables and calculate total purchase and average purchase per customer
        # Apply the transformations using SQLite SQL queries
        # drop_query="""DROP TABLE IF EXISTS customer_invoice_avg_elt"""
        # conn.execute(drop_query)
        # conn.commit()
        create_table_time_query = """
            CREATE TABLE IF NOT EXISTS tableTime AS
            SELECT ci.CustomerId, ci.created_at 
            FROM customer_invoice_avg_elt ci
            WHERE EXISTS (
                SELECT 1
                FROM Customers_ELT ce
                JOIN Invoices_ELT i ON ce.CustomerId = i.CustomerId
                WHERE ((i.InvoiceDate > ? OR ce.created_at > ?)
                AND ci.customerId = ce.customerId)
                group by ce.CustomerId
            )
        """


        print("SELECT :",conn.execute("""SELECT c.CustomerId,c.created_at,i.InvoiceDate FROM Customers_ELT c
                JOIN Invoices_ELT i ON c.CustomerId = i.CustomerId
                where i.InvoiceDate > ? or c.created_at > ?
                group by c.CustomerId """
        ,(latest_timestamp,latest_timestamp,)).fetchall())
        # print("SELECT :",conn.execute("""SELECT InvoiceDate FROM Invoices_ELT 
        #                 where InvoiceDate > ? """
        # ,(latest_timestamp,)).fetchall())
        
        conn.execute(create_table_time_query, (latest_timestamp, latest_timestamp))
        conn.commit()
        conn.execute("""DELETE FROM customer_invoice_avg_elt
            WHERE EXISTS (
                SELECT 1
                FROM tableTime t
                WHERE customer_invoice_avg_elt.CustomerId = t.CustomerId
            )""")

        conn.commit()
        print("customer_invoice_avg_after_del:", conn.execute("SELECT * FROM 'customer_invoice_avg_elt'").fetchall())
        print("tableTime:", conn.execute("SELECT * FROM 'tableTime'").fetchall())

        transform_query = """
            INSERT INTO customer_invoice_avg_elt (CustomerId,InvoiceMonth, avg_spend, created_at, updated_at, updated_by)
            SELECT c.CustomerId, strftime('%m', i.InvoiceDate) AS InvoiceMonth,
                   AVG(i.Total) AS avg_spend,
                   COALESCE(t.created_at, CURRENT_DATE) AS created_at,
                   CURRENT_DATE AS updated_at,
                   'process:shana_levovitz_' || CURRENT_DATE AS updated_by
            FROM Customers_ELT c
            RIGHT JOIN Invoices_ELT i ON c.CustomerId = i.CustomerId
            LEFT JOIN tableTime t ON c.CustomerId = t.CustomerId
            WHERE i.InvoiceDate > ? OR c.created_at > ?
            GROUP BY c.CustomerId, InvoiceMonth
        """
        conn.execute(transform_query, (latest_timestamp, latest_timestamp))

        # Commit the changes to the database
        conn.execute("""DROP TABLE IF EXISTS tableTime""")
        conn.commit()
        print("customer_invoice_avg:", conn.execute("SELECT * FROM 'customer_invoice_avg_elt'").fetchall())
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session
        
if __name__ == "__main__":
    incremental_load_average_purchase_value_elt()