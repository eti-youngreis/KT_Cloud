from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import sqlite3
import getpass  # For getting the username for metadata
import pandas as pd





def album_popularity_full_elt():
    """
    ELT process to calculate Album Popularity and Revenue.
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
    conn = sqlite3.connect('DB/etl_db.db')  # Assuming KT_DB has a connect() method

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
        # Load the tables CSV as a Pandas DataFrame
        albums = pd.read_csv("DB\csvs\Album.csv")
        tracks = pd.read_csv("DB\csvs\Track.csv")
        invoice_lines = pd.read_csv("DB\csvs\InvoiceLine.csv")
        
        # LOAD (Save the raw data into SQLite using KT_DB without transformation)
        # -----------------------------------------------------------------------
        
        # Insert the full CSV data into corresponding raw tables in SQLite
        
        albums.to_sql('albums', conn, if_exists='replace', index=False)
        tracks.to_sql('tracks', conn, if_exists='replace', index=False)
        invoice_lines.to_sql('invoice_lines', conn, if_exists='replace', index=False)
    


        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the albums with tracks
        
        transform_query = """
        CREATE TABLE AlbumPopularityAndRevenue AS
        SELECT al.AlbumId,
            al.Title,
            al.ArtistId,
            SUM(tr.UnitPrice * il.Quantity) AS TotalRevenue,
            COUNT(tr.TrackId) AS TrackCount,
            DATETIME('now') AS created_at,
            DATETIME('now') AS updated_at,
            'process:user_name' AS updated_by  
        FROM albums al
        JOIN tracks tr ON al.AlbumId = tr.AlbumId
        JOIN invoice_lines il ON il.TrackId = tr.TrackId
        GROUP BY al.AlbumId;
    """

        cursor = conn.cursor()
        cursor.execute(transform_query)
        



        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()
    
if __name__ == "__main__":
    album_popularity_full_elt()
