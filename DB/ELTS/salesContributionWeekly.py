import os
from pyspark.sql import SparkSession
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Chinook_ELT_Track_Analysis") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method

    try:
        # EXTRACT (Loading CSVs from local storage)
        # ----------------------------------------
        track_df = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)
        customer_df = spark.read.csv('C:/Users/leabe/Documents/data/Customer.csv', header=True, inferSchema=True)
        invoice_df = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
        invoice_line_df = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)
        media_type_df = spark.read.csv('C:/Users/leabe/Documents/data/MediaType.csv', header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite using KT_DB without transformation)
        # -----------------------------------------------------------------------
        track_pd = track_df.toPandas()
        customer_pd = customer_df.toPandas()
        invoice_pd = invoice_df.toPandas()
        invoice_line_pd = invoice_line_df.toPandas()
        media_type_pd = media_type_df.toPandas()

        # Insert raw data into SQLite tables
        KT_DB.insert_dataframe(conn, 'Track', track_pd)
        KT_DB.insert_dataframe(conn, 'Customer', customer_pd)
        KT_DB.insert_dataframe(conn, 'Invoice', invoice_pd)
        KT_DB.insert_dataframe(conn, 'InvoiceLine', invoice_line_pd)
        KT_DB.insert_dataframe(conn, 'MediaType', media_type_pd)

        # TRANSFORMATION (Perform transformations using SQL queries)
        # -----------------------------------------------------------
        # Example 1: Join tables to analyze sales distribution by MediaType and Time of Day (hour)

        transform_query = """
            CREATE TABLE SalesByMediaTypeAndTime AS
            SELECT 
                mt.Name AS MediaType,
                strftime('%H', i.InvoiceDate) AS HourOfDay,
                SUM(il.UnitPrice * il.Quantity) AS TotalSales
            FROM 
                Invoice i
            JOIN 
                InvoiceLine il ON i.InvoiceId = il.InvoiceId
            JOIN 
                Track t ON il.TrackId = t.TrackId
            JOIN 
                MediaType mt ON t.MediaTypeId = mt.MediaTypeId
            GROUP BY 
                mt.Name, strftime('%H', i.InvoiceDate)
            ORDER BY 
                mt.Name, HourOfDay;
        """

        # Execute the transformation query using KT_DB's execute() method
        KT_DB.execute(conn, transform_query)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()