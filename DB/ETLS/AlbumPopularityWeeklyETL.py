from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import sqlite3
import getpass  # For getting the username for metadata





def load():
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
    conn = sqlite3.connect('DB/ETLS/etl_db.db')  # Assuming KT_DB has a connect() method

    try:
        cursor = conn.cursor()
        # Check if the 'album_popularity_revenue' table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='AlbumPopularityAndRevenue';
        """)
        table_exists = cursor.fetchone()

        # If the table exists, drop it
        if table_exists:
            print("Table 'AlbumPopularityAndRevenue' exists, dropping it.")
            cursor.execute("DROP TABLE AlbumPopularityAndRevenue;")
            conn.commit()
        else:
            print("AlbumPopularityAndRevenue' does not exist.")
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table: Albums, Tracks, and InvoiceLines
        albums = spark.read.csv("DB/ETLS/csv_files/Album.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("DB/ETLS/csv_files/Track.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("DB/ETLS/csv_files/InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the albums with tracks
        albums_tracks = albums.join(tracks, albums["AlbumId"] == tracks["AlbumId"], "inner").drop(
            tracks["AlbumId"])
        #join albums and tracks with invoice lines
        albums_tracks_invoice_lines = albums_tracks.join(
                invoice_lines, 
                "TrackId", 
                "inner"
            ).drop(
                invoice_lines["TrackId"]).drop(
                    invoice_lines["UnitPrice"])
       

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
        album_metrics = albums.join(total_revenue_per_album.join(track_count_per_album, 'AlbumId', 'inner'),'AlbumId','inner')

        # Add metadata columns
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated_by = getpass.getuser()  # Get the username of the person running the script

        album_metrics = album_metrics \
            .withColumn("created_at", F.lit(current_datetime)) \
            .withColumn("updated_at", F.lit(current_datetime)) \
            .withColumn("updated_by", F.lit(updated_by))
        
        # LOAD

    
        # Create table if not exists
        cursor.execute('''CREATE TABLE IF NOT EXISTS AlbumPopularityAndRevenue (
                      AlbumId INTEGER,
                      Title TEXT,
                      ArtistId INTEGER,
                      TotalRevenue REAL,
                      TrackCount INTEGER,
                      created_at TEXT,
                      updated_at TEXT,
                      updated_by TEXT)''')

        # Insert data
        for row in album_metrics.collect():
            cursor.execute('''INSERT INTO AlbumPopularityAndRevenue 
                          (AlbumId, Title, ArtistId, TotalRevenue, TrackCount, created_at, updated_at, updated_by)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                          (row['AlbumId'], row['Title'], row['ArtistId'], row['TotalRevenue'],
                           row['TrackCount'], row['created_at'], row['updated_at'], row['updated_by']))



        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()
    
if __name__ == "__main__":
    load()
