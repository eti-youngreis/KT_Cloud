import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_date, lit
from pyspark.sql.window import Window
import KT_DB

def load_popularity_track_daily_incremental():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Popularity Track by region ELT - Daily Incremental") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')

    try:
        # Read incremental data from CSV files
        track_table_incremental = spark.read.csv('C:/Users/leabe/Documents/data/Track_incremental.csv', header=True, inferSchema=True)
        invoice_line_table_incremental = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine_incremental.csv', header=True, inferSchema=True)

        # Load incremental data into SQLite tables
        track_incremental_df = track_table_incremental.toPandas()
        invoice_line_incremental_df = invoice_line_table_incremental.toPandas()

        KT_DB.insert_dataframe(conn, 'track_incremental_table', track_incremental_df)
        KT_DB.insert_dataframe(conn, 'invoice_line_incremental_table', invoice_line_incremental_df)

        # Transformation query for daily incremental data
        transform_query_daily = """
            SELECT t.TrackId, t.Name, t.AlbumId, t.Country, SUM(il.InvoiceLineId) AS Popularity,
                CURRENT_DATE() AS created_at, CURRENT_DATE() AS updated_at,
                CONCAT('process:', '{}') AS updated_by
            FROM track_incremental_table t
            JOIN invoice_line_incremental_table il ON t.TrackId = il.TrackId
            GROUP BY t.TrackId, t.Name, t.AlbumId, t.Country
        """

        # Execute the daily incremental transformation query
        KT_DB.execute(conn, transform_query_daily)

        # Commit the changes to the database
        KT_DB.commit(conn)
        
    finally:
        # Close the SQLite connection and stop Spark session
        KT_DB.close(conn)
        spark.stop()

# Call the function to load daily incremental data
load_popularity_track_daily_incremental()
