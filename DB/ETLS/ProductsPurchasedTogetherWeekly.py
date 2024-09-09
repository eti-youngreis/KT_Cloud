from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime
# import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    # --------------------------------
    # Creating a Spark session to enable distributed data processing with PySpark.
    spark = SparkSession.builder \
        .appName('ETL Template with KT_DB') \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # -----------------------------------------------
    # Establishing a connection to the SQLite database where results will be saved.
    # Uncomment and use KT_DB connection if KT_DB is the preferred library.
    # conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('/TrackCombinations.db')  # Direct connection to the SQLite database.

    # Define the path to the directory containing the CSV files.
    path_to_tables = 'C:\\Users\\USER\\Desktop\\3.9.2024\\Emploee_Tables\\'

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------
        # Load the necessary tables from CSV files into Spark DataFrames.
        # This includes invoice line items, album details, and track details.
        invoice_line_table = spark.read.csv(f'{path_to_tables}\\InvoiceLine.csv', header=True, inferSchema=True)
        album_table = spark.read.csv(f'{path_to_tables}\\Album.csv', header=True, inferSchema=True)
        track_table = spark.read.csv(f'{path_to_tables}\\Track.csv', header=True, inferSchema=True)
        
        # TRANSFORM (Join tables and identify common product bundles)
        # -----------------------------------------------------------
        # Join the invoice line table with track and album tables to enrich data with album and track information.
        joined_invoice = invoice_line_table.alias('il') \
            .join(track_table.alias('t'), F.col('il.TrackId') == F.col('t.TrackId'), 'inner') \
            .join(album_table.alias('a'), F.col('a.AlbumId') == F.col('t.AlbumId'), 'inner')
        
        # Select relevant columns for further analysis.
        invoice_details = joined_invoice.select(
            F.col('il.InvoiceId').alias('InvoiceId'),
            F.col('il.TrackId').alias('TrackId'),
            F.col('t.AlbumId').alias('AlbumId'),
            F.col('a.Title').alias('AlbumTitle'),
            F.col('t.Name').alias('TrackName')
        )

        # Identifying pairs of tracks/albums purchased together using a self-join on the invoice details.
        # The condition (TrackId1 < TrackId2) ensures that each pair is unique and avoids duplicate pairings.
        invoice_pairs = invoice_details.alias('id1') \
            .join(invoice_details.alias('id2'), 
                  (F.col('id1.InvoiceId') == F.col('id2.InvoiceId')) & 
                  (F.col('id1.TrackId') < F.col('id2.TrackId')),
                  'inner'
            ) \
            .select(
                F.col('id1.TrackId').alias('TrackId1'),
                F.col('id2.TrackId').alias('TrackId2'),
                F.col('id1.AlbumId').alias('AlbumId1'),
                F.col('id2.AlbumId').alias('AlbumId2')
            )

        # Group by the track pairs to count how often each combination occurs, indicating frequently purchased bundles.
        track_combinations = invoice_pairs.groupBy(
            'TrackId1', 'TrackId2', 'AlbumId1', 'AlbumId2'
        ).count() \
        .withColumnRenamed('count', 'Frequency') \
        .orderBy(F.desc('Frequency'))

        # Add metadata columns to the DataFrame
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        track_combinations = track_combinations \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Shani_Strauss'))

        # LOAD (Saving results back to SQLite)
        # ------------------------------------
        # Convert the resulting Spark DataFrame to a Pandas DataFrame to save it into the SQLite database.
        track_combinations_pd = track_combinations.toPandas()

        # Save the results to the SQLite table named 'TrackCombinations'.
        # If the table exists, it will be replaced with the new data.
        track_combinations_pd.to_sql('TrackCombinations', conn, if_exists='replace', index=False)
        
    except Exception as e:
        # Print any error that occurs during the ETL process.
        print(f"An error occurred during ETL: {e}")
    finally:
        # Ensure the connection to the SQLite database is closed properly.
        conn.close()
        # Stop the Spark session to free up resources.
        spark.stop()
        
# Run the ETL function to execute the data processing pipeline.
if __name__ == "__main__":
    load()
