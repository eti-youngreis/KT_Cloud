from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime

# ETL process to load and transform music-related data and save results to a SQLite database
def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName('ETL Template with KT_DB') \
        .getOrCreate()

    # Step 2: Establish SQLite connection (KT_DB assumed to be a library for SQLite operations)
    # conn = KT_DB.connect('/path_to_sqlite.db')  # Example using KT_DB for connection
    conn = sqlite3.connect('TrackCombinations.db')  # Connection to SQLite database
    path = "D:\\בוטקאמפ\\Vast Project\\EmployeeTables\\"
    
    try:
        # EXTRACT (Loading CSV files from local storage)
        # -----------------------------------------------
        invoice_line_table = spark.read.csv(f'{path}InvoiceLine.csv', header=True, inferSchema=True)
        album_table = spark.read.csv(f'{path}Album.csv', header=True, inferSchema=True)
        track_table = spark.read.csv(f'{path}Track.csv', header=True, inferSchema=True)

        # TRANSFORM (Applying joins, grouping, and other transformations)
        # --------------------------------------------------------
        # Joining the invoice_line table with track and album tables
        joined_invoice = invoice_line_table.alias('il') \
            .join(track_table.alias('t'), F.col('il.TrackId') == F.col('t.TrackId'), 'inner') \
            .join(album_table.alias('a'), F.col('a.AlbumId') == F.col('t.AlbumId'), 'inner')
        
        # Selecting relevant columns for further analysis
        invoice_details = joined_invoice.select(
            F.col('il.InvoiceId').alias('InvoiceId'),
            F.col('il.TrackId').alias('TrackId'),
            F.col('t.AlbumId').alias('AlbumId'),
            F.col('a.Title').alias('AlbumTitle'),
            F.col('t.Name').alias('TrackName')
        )
        
        # Identifying pairs of tracks purchased together using a self-join
        invoice_pairs = invoice_details.alias('id1') \
            .join(invoice_details.alias('id2'),
                  (F.col('id1.InvoiceId') == F.col('id2.InvoiceId')) &
                  (F.col('id1.TrackId') < F.col('id2.TrackId')),  # Ensuring unique pairs
                  'inner'
            ) \
            .select(
                F.col('id1.TrackId').alias('TrackId1'),
                F.col('id2.TrackId').alias('TrackId2'),
                F.col('id1.AlbumId').alias('AlbumId1'),
                F.col('id2.AlbumId').alias('AlbumId2')
            )
        
        # Grouping by track pairs and counting the occurrences (frequency of purchases together)
        track_combinations = invoice_pairs.groupBy(
            'TrackId1', 'TrackId2', 'AlbumId1', 'AlbumId2'
        ).count() \
        .withColumnRenamed('count', 'Frequency') \
        .orderBy(F.desc('Frequency'))
        
        # Adding metadata columns such as creation time and updated info
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        track_combinations = track_combinations \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('rachel_krashinski'))

        # LOAD (Saving the transformed data back to SQLite)
        # -------------------------------------------------
        # Convert the Spark DataFrame to Pandas for saving into SQLite
        track_combinations_pd = track_combinations.toPandas()
        # Save the DataFrame to SQLite under the table name 'TrackCombinations'
        track_combinations_pd.to_sql('TrackCombinations', conn, if_exists='replace', index=False)
    
    except Exception as e:
        print(f"An error occurred during ETL: {e}")
    
    finally:
        # Closing the SQLite connection
        conn.close()
        # Stopping the Spark session
        spark.stop()

if __name__ == "__main__":
    # Run the ETL function
    load()
