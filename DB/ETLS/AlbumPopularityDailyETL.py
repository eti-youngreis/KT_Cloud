from numpy import greater
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import sqlite3
import getpass  # For getting the username for metadata





def album_popularity_incremental_etl():
    """
    ETL process to calculate Album Popularity and Revenue.
    The process extracts data from CSV files, transforms the data by calculating total revenue
    and track count for each album, and then loads the transformed data into a SQLite database.

    Requirements:
    - Main Table: Albums
    - Measures:
      - Total Revenue: Sum of revenue generated from all tracks in the album.
      - Track Count: Total number of tracks in the album.
    - Metadata:
      - created_at: The timestamp when the record is created.
      - updated_at: The timestamp when the record is updated (for incremental loading).
      - updated_by: The username or process responsible for the update.
    """
    
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('DB/etl_db.db')  
    cursor = conn.cursor()
    

    try:
        # Check if final table exists, if not create it
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='AlbumPopularityAndRevenue'")
        if cursor.fetchone() is None:
            cursor.execute('''
                CREATE TABLE AlbumPopularityAndRevenue (
                    AlbumId INTEGER PRIMARY KEY,
                    Title TEXT,
                    ArtistId INTEGER,
                    TrackCount INTEGER,
                    TotalRevenue REAL,
                    created_at TEXT,
                    updated_at TEXT,
                    updated_by TEXT
                )
            ''')
            print("Table 'AlbumPopularityAndRevenue' created.")

        # Get the latest processed timestamp
        cursor.execute("SELECT MAX(updated_at) FROM AlbumPopularityAndRevenue")
        latest_timestamp = cursor.fetchone()[0] or '1900-01-01 00:00:00'
        
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table: Albums, Tracks, and InvoiceLines
        albums = spark.read.csv("DB\csvs\Album.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("DB\csvs\Track.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("DB\csvs\InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the albums with tracks
        albums_tracks = albums.join(tracks, albums["AlbumId"] == tracks["AlbumId"], "inner").drop(
            tracks["AlbumId"])
        #join albums and tracks with invoice lines
        # Filter only the updated records
        albums_tracks_invoice_lines = albums_tracks.join(
                invoice_lines, 
                "TrackId", 
                "inner"
            ).drop(
                invoice_lines["TrackId"]).drop(
                    invoice_lines["UnitPrice"]).withColumn("max_updated_at", F.greatest(albums.updated_at ,tracks.updated_at, invoice_lines.updated_at)).drop(
                        albums["updated_at"]).drop(
                            tracks["updated_at"]).drop(
                                invoice_lines["updated_at"]).filter(
                        F.col('max_updated_at') > latest_timestamp)
       

        # Calculate Total Revenue for each album
        total_revenue_per_album = albums_tracks_invoice_lines.groupBy("AlbumId") \
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalRevenue")  # Calculate total revenue
            )

        # Calculate Track Count for each album
        track_count_per_album = albums_tracks.groupBy("AlbumId") \
            .agg(
                F.count("TrackId").alias("TrackCount")  # Count the number of tracks per album
            )

        # Join total revenue and track count results
        album_popularity = albums.join(total_revenue_per_album.join(track_count_per_album, 'AlbumId', 'inner'),'AlbumId','inner')


        # Add metadata columns
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated_by = getpass.getuser()  # Get the username of the person running the script
        

        
        # add created_at column 
        # create view of updated records   
        album_popularity.createOrReplaceTempView("album_popularity_view")
        # select all created_at from prev records to update
        created_time_of_records_df = spark.sql('SELECT AlbumId, created_at from AlbumPopularityAndRevenue WHERE AlbumId IN (SELECT AlbumId FROM album_popularity_view)')
        # add to df
        album_popularity = album_popularity.join(created_time_of_records_df,"AlbumId","left")\
            .select([album_popularity["*"]] +
                    [created_time_of_records_df["created_at"] if "created_at" in created_time_of_records_df.columns 
                     else F.lit(current_datetime).alias("created_at")])
        
        # add updated_at and updated_by columns    
        album_popularity = album_popularity \
            .withColumn("updated_at", F.lit(current_datetime)) \
            .withColumn("updated_by", F.lit(updated_by))
        
        
        # LOAD
        
        # delete from final table if exists all rcords to update
        cursor.execute('DELETE from AlbumPopularityAndRevenue WHERE AlbumId in (SELECT AlbumId FROM album_popularity_view)')
        
        # Insert the updated data
        album_popularity_df = album_popularity.toPandas()
        album_popularity_df.to_sql('AlbumPopularityAndRevenue', conn, if_exists='append', index=False)



        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()
    
if __name__ == "__main__":
    album_popularity_incremental_etl()
