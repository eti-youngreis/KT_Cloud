from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime

def load_genre_popularity_and_average_sales():
    # Step 1: Initialize Spark session
    # Create a Spark session to enable Spark SQL functionality
    spark = SparkSession.builder \
        .appName("Product Genre Popularity and Average Sales Price ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    # Connect to SQLite database (or create it if it does not exist)
    conn = sqlite3.connect("genres_table.db")

    # Define the path to the CSV files
    path = "D:\\Users\\גילי\\Documents\\בוטקמפ\\csv files\\"
    
    try:
        # EXTRACT: Load CSV files from local storage into Spark DataFrames
        # Read CSV files into DataFrames with schema inference and header row
        genre_table = spark.read.csv(path + "Genre.csv", header=True, inferSchema=True)
        track_table = spark.read.csv(path + "Track.csv", header=True, inferSchema=True)
        invoice_line_table = spark.read.csv(path + "InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM: Perform data transformations and aggregations
        # Join genre, track, and invoice_line tables
        joined_data = genre_table.alias('g') \
            .join(track_table.alias('t'), F.col('g.GenreId') == F.col('t.GenreId'), 'inner') \
            .join(invoice_line_table.alias('il'), F.col('t.TrackId') == F.col('il.TrackId'), 'inner')

        # Calculate total sales and average sales price for each genre
        aggregated_data = joined_data.groupBy('g.GenreId', 'g.Name') \
            .agg(
                F.sum(F.col('il.UnitPrice') * F.col('il.Quantity')).alias('Total_Sales'),
                F.avg(F.col('il.UnitPrice')).alias('Average_Sales_Price')
            )

        # Add metadata columns to the DataFrame
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = aggregated_data \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Gili Bolak'))

        # Select the final columns for output
        final_data_df = transformed_data.select(
            'GenreId',
            'Name',
            'Total_Sales',
            'Average_Sales_Price',
            'created_at',
            'updated_at',
            'updated_by'
        )

        # Load data into SQLite database
        # Convert Spark DataFrame to Pandas DataFrame and write to SQLite table
        final_data_df = final_data_df.toPandas()
        final_data_df.to_sql('genre_popularity', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Clean up resources
        # Close the SQLite connection and stop the Spark session
        conn.close()  # Ensure connection is properly closed
        spark.stop()  # Stop the Spark session