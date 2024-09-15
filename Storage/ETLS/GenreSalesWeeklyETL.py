from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime
import os

BASE_URL = "C:/Users/jimmy/Desktop/תמר לימודים  יד/bootcamp/db_files"

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Product Genre Popularity and Average Sales Price ETL") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection
    conn = sqlite3.connect(os.path.join(BASE_URL, "genres_table.db"))
    
    try:
        # EXTRACT: Load CSV files from local storage into Spark DataFrames
        genre_table = spark.read.csv(os.path.join(BASE_URL, "Genre.csv"), header=True, inferSchema=True)
        track_table = spark.read.csv(os.path.join(BASE_URL, "Track.csv"), header=True, inferSchema=True)
        invoice_line_table = spark.read.csv(os.path.join(BASE_URL, "InvoiceLine.csv"), header=True, inferSchema=True)
        
        # TRANSFORM: Perform data transformations and aggregations
        joined_data = genre_table.alias('g') \
            .join(track_table.alias('t'), F.col('g.GenreId') == F.col('t.GenreId'), 'inner') \
            .join(invoice_line_table.alias('il'), F.col('t.TrackId') == F.col('il.TrackId'), 'inner')

        aggregated_data = joined_data.groupBy('g.GenreId', 'g.Name') \
            .agg(
                F.sum(F.col('il.UnitPrice') * F.col('il.Quantity')).alias('Total_Sales'),
                F.avg(F.col('il.UnitPrice')).alias('Average_Sales_Price')
            )

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = aggregated_data \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Tamar Gavrielov'))

        # Sort the data by 'GenreId'
        final_data_df = transformed_data.select(
            'GenreId',
            'Name',
            'Total_Sales',
            'Average_Sales_Price',
            'created_at',
            'updated_at',
            'updated_by'
        ).orderBy('GenreId')

        final_data_df.show()

        # Load data into SQLite database
        final_data_df = final_data_df.toPandas()
        final_data_df.to_sql('genre_popularity', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Clean up resources
        conn.close()  # Ensure SQLite connection is properly closed
        spark.stop()  # Stop the Spark session

if __name__ == '__main__':
    load()
