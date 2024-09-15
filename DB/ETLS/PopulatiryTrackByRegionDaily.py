import os
import sqlite3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def load_track_popularity_daily_increment():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Daily Incremental Track Popularity by Region") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')
    
    try:
        # Check if the table exists
        check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='TrackPopularityByRegion';"
        table_exists = conn.execute(check_table_query).fetchone()
        latest_timestamp = None
        if table_exists:
            latest_timestamp_query = "SELECT MAX(updated_at) FROM TrackPopularityByRegion"
            latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]
        
        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        
        # EXTRACT (Loading CSVs from S3 or local storage)
        track_df = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)
        customer_df = spark.read.csv('C:/Users/leabe/Documents/data/Customer.csv', header=True, inferSchema=True)
        invoice_df = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
        invoice_line_df = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)
        
        # Transform date columns
        invoice_df = invoice_df.withColumn('InvoiceDate', F.to_date(F.col('InvoiceDate'), 'dd/MM/yyyy'))
        
        # TRANSFORM (Apply joins, groupings)
        joined_df = track_df.join(invoice_line_df, track_df.TrackId == invoice_line_df.TrackId) \
            .join(invoice_df, invoice_line_df.InvoiceId == invoice_df.InvoiceId) \
            .join(customer_df, invoice_df.CustomerId == customer_df.CustomerId) \
            .filter(F.col('InvoiceDate') > latest_timestamp)
        
        track_popularity_by_region = joined_df.groupBy('TrackId', 'Name', 'AlbumId', 'Country') \
            .agg(F.sum('InvoiceLineId').alias('Popularity'))
        
        # Add metadata
        current_user = os.getenv('USER')  # Set your actual user name
        track_popularity_by_region = track_popularity_by_region.withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"process:{current_user}"))

        # LOAD (Save transformed data into SQLite using SQLite3)
        final_data_df = track_popularity_by_region.toPandas()
        
        if table_exists:
            ids_placeholder = ', '.join('?' * len(final_data_df['TrackId']))
            delete_query = f"DELETE FROM TrackPopularityByRegion WHERE TrackId IN ({ids_placeholder})"
            conn.execute(delete_query, final_data_df['TrackId'].tolist())
        
        final_data_df.to_sql('TrackPopularityByRegion', conn, if_exists='append', index=False)
        conn.commit()
    
    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load_track_popularity_daily_increment()
