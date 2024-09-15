import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_date, lit
from pyspark.sql.window import Window
import KT_DB

def load_sales_distribution_daily_incremental():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Sales Distribution by Media Type and Time of Day - Daily Incremental") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')

    try:
        # Read incremental data from CSV files
        invoice_line_incremental = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine_incremental.csv', header=True, inferSchema=True)
        track_incremental = spark.read.csv('C:/Users/leabe/Documents/data/Track_incremental.csv', header=True, inferSchema=True)
        media_type_incremental = spark.read.csv('C:/Users/leabe/Documents/data/MediaType_incremental.csv', header=True, inferSchema=True)

        # Load incremental data into SQLite tables
        invoice_line_incremental_df = invoice_line_incremental.toPandas()
        track_incremental_df = track_incremental.toPandas()
        media_type_incremental_df = media_type_incremental.toPandas()

        KT_DB.insert_dataframe(conn, 'InvoiceLine_incremental', invoice_line_incremental_df)
        KT_DB.insert_dataframe(conn, 'Track_incremental', track_incremental_df)
        KT_DB.insert_dataframe(conn, 'MediaType_incremental', media_type_incremental_df)

        # Transformation query for daily incremental data
        transform_query_daily = """
            CREATE TABLE SalesByMediaTypeAndTimeDaily AS
            SELECT 
                mt.Name AS MediaType,
                strftime('%H', i.InvoiceDate) AS HourOfDay,
                SUM(il.UnitPrice * il.Quantity) AS TotalSales
            FROM 
                InvoiceLine_incremental il
            JOIN 
                Track_incremental t ON il.TrackId = t.TrackId
            JOIN 
                MediaType_incremental mt ON t.MediaTypeId = mt.MediaTypeId
            JOIN 
                Invoice i ON il.InvoiceId = i.InvoiceId
            GROUP BY 
                mt.Name, strftime('%H', i.InvoiceDate)
            ORDER BY 
                mt.Name, HourOfDay;
        """

        # Execute the daily incremental transformation query
        KT_DB.execute(conn, transform_query_daily)

        # Commit the changes to the database
        KT_DB.commit(conn)
        
    finally:
        # Close the SQLite connection and stop Spark session
        KT_DB.close(conn)
        spark.stop()

# Call the function to load daily incremental data for Sales Distribution by Media Type and Time of Day
load_sales_distribution_daily_incremental()
