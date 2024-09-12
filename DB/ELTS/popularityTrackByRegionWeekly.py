import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, current_date, lit
from pyspark.sql.window import Window
import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_popularity_track():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Populary Track by region ELT") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method

    try:

        # read from csv
        # main table
        track_table = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)

        # Load other related tables
        customer_table = spark.read.csv('C:/Users/leabe/Documents/data/Customer.csv', header=True, inferSchema=True)
        invoice_table = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
        invoice_line_table = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite using KT_DB without transformation)
        # -----------------------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        track_df = track_table.toPandas()
        customer_df = customer_table.toPandas()
        invoice_df = invoice_table.toPandas()
        invoice_line_df = invoice_line_table.toPandas()

        # Assuming KT_DB has a method to insert Pandas DataFrames into SQLite
        KT_DB.insert_dataframe(conn, 'track-main_table', track_df)
        KT_DB.insert_dataframe(conn, 'customer_table', customer_df)
        KT_DB.insert_dataframe(conn, 'invoice_table', invoice_df)
        KT_DB.insert_dataframe(conn, 'invoice_line_table', invoice_line_df)  


        transform_query = """
            SELECT t.TrackId, t.Name, t.AlbumId, t.Country, SUM(il.InvoiceLineId) AS Popularity,
                CURRENT_DATE() AS created_at, CURRENT_DATE() AS updated_at,
                CONCAT('process:', '{}') AS updated_by
            FROM track_table t
            JOIN invoice_line_table il ON t.TrackId = il.TrackId
            JOIN invoice_table i ON il.InvoiceId = i.InvoiceId
            JOIN customer_table c ON i.CustomerId = c.CustomerId
            GROUP BY t.TrackId, t.Name, t.AlbumId, t.Country
        """

        # Execute the transformation query using KT_DB's execute() method
        KT_DB.execute(conn, transform_query)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)
        
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()


