import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, current_date, lit
from pyspark.sql.window import Window
import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_popularity_track_by_region():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Populary Track by region") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/chinook.db')  # Assuming KT_DB has a connect() method

    try:

        # read from csv
        # main table
        track_df = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)

        # Load other related tables
        customer_df = spark.read.csv('C:/Users/leabe/Documents/data/Customer.csv', header=True, inferSchema=True)
        invoice_df = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
        invoice_line_df = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)

        joined_df = track_df.join(invoice_line_df, track_df.TrackId==invoice_line_df.TrackId) \
            .join(invoice_df, invoice_line_df.InvoiceId==invoice_df.InvoiceId) \
            .join(customer_df, invoice_df.CustomerId==customer_df.CustomerId)

        track_popularity_by_region = joined_df.groupBy('TrackId', 'Name', 'AlbumId', 'Country') \
            .agg(sum('InvoiceLineId').alias('Popularity'))

        # Add metadata

        current_user = os.getenv('USER')
        track_popularity_by_region = track_popularity_by_region.withColumn("created_at", current_date()) \
            .withColumn("updated_at", current_date()) \
            .withColumn("updated_by", lit(f"process:{current_user}"))

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        final_data_df = track_popularity_by_region.toPandas()

        KT_DB.insert_dataframe(conn, 'TrackPopularityByRegion', final_data_df)
        KT_DB.commit(conn)
        
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()


