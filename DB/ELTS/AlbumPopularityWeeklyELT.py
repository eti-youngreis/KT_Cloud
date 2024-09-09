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
        
        # LOAD (Save the raw data into SQLite using KT_DB without transformation)
        # -----------------------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS albums (
                      AlbumId INTEGER,
                      Title TEXT,
                      ArtistId INTEGER)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS tracks (
                      TrackId INTEGER,
                      Name TEXT,
                      AlbumId INTEGER,
                      MediaTypeId INTEGER,
                      GenreId INTEGER,
                      Composer TEXT,
                      Milliseconds INTEGER,
                      Bytes INTEGER,
                      UnitPrice DOUBLE)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS invoice_lines (
                      InvoiceLineId INTEGER,
                      InvoiceId INTEGER,
                      TrackID INTEGER,
                      UnitPrice DOUBLE,
                      Quantity INTEGER)''')
        
        
        # Insert data to 'albums' table
        for row in albums.collect():
            cursor.execute('''INSERT INTO albums 
                              (AlbumId, Title, ArtistId) 
                              VALUES (?, ?, ?)''',
                           (row['AlbumId'], row['Title'], row['ArtistId']))
        
        # Insert data into 'tracks' table
        for row in tracks.collect():
            cursor.execute('''INSERT INTO tracks
                              (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (row['TrackId'], row['Name'], row['AlbumId'], row['MediaTypeId'], row['GenreId'],
                            row['Composer'], row['Milliseconds'], row['Bytes'], row['UnitPrice']))
        
        # Insert data into 'invoice_lines' table
        for row in invoice_lines.collect():
            cursor.execute('''INSERT INTO invoice_lines
                              (InvoiceLineId, InvoiceId, TrackId, UnitPrice, Quantity) 
                              VALUES (?, ?, ?, ?, ?)''',
                           (row['InvoiceLineId'], row['InvoiceId'], row['TrackId'], row['UnitPrice'], row['Quantity']))
    


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
    load()
