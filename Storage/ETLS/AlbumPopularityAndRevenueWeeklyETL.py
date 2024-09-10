from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime

def load_album_popularity_and_revenue():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Album Popularity and Revenue ETL") \
        .getOrCreate()

    # EXTRACT (Loading CSVs)
    albums = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Album.csv", header=True, inferSchema=True)
    invoice_lines = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/InvoiceLine.csv", header=True, inferSchema=True)
    tracks = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Track.csv", header=True, inferSchema=True)

    # Rename columns to avoid ambiguity
    albums = albums.withColumnRenamed("AlbumId", "AlbumId_Albums")
    tracks = tracks.withColumnRenamed("AlbumId", "AlbumId_Tracks").withColumnRenamed("UnitPrice", "Track_UnitPrice")
    invoice_lines = invoice_lines.withColumnRenamed("TrackId", "TrackId_InvoiceLines").withColumnRenamed("UnitPrice", "Invoice_UnitPrice")

    # TRANSFORM
    # Join albums with tracks to get album-related information
    album_tracks = albums.join(tracks, albums["AlbumId_Albums"] == tracks["AlbumId_Tracks"], "inner")

    # Join the result with invoice_lines to get revenue details
    album_invoice_lines = album_tracks.join(invoice_lines, album_tracks["TrackId"] == invoice_lines["TrackId_InvoiceLines"], "inner")

    # Calculate total revenue and track count per album
    album_revenue = album_invoice_lines.groupBy("AlbumId_Albums", "Title", "ArtistId") \
        .agg(
            F.sum("Invoice_UnitPrice").alias("TotalRevenue"),
            F.count("TrackId").alias("TrackCount")
        )

    # Rename columns back to original names if needed
    album_revenue = album_revenue.withColumnRenamed("AlbumId_Albums", "AlbumId")

    # Metadata
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    album_revenue = album_revenue.withColumn("created_at", F.lit(now)) \
        .withColumn("updated_at", F.lit(now)) \
        .withColumn("updated_by", F.lit("process:user_name"))

    # Load data into SQLite using sqlite3
    conn = sqlite3.connect('C:/Users/user1/Desktop/0909/Album.db')
    cursor = conn.cursor()

    # Drop table if exists
    cursor.execute('DROP TABLE IF EXISTS AlbumPopularityAndRevenue')

    # Create table
    cursor.execute('''CREATE TABLE AlbumPopularityAndRevenue (
                      AlbumId INTEGER,
                      Title TEXT,
                      ArtistId INTEGER,
                      TotalRevenue REAL,
                      TrackCount INTEGER,
                      created_at TEXT,
                      updated_at TEXT,
                      updated_by TEXT)''')

    # Insert data
    for row in album_revenue.collect():
        cursor.execute('''INSERT INTO AlbumPopularityAndRevenue 
                          (AlbumId, Title, ArtistId, TotalRevenue, TrackCount, created_at, updated_at, updated_by)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                          (row['AlbumId'], row['Title'], row['ArtistId'], row['TotalRevenue'],
                           row['TrackCount'], row['created_at'], row['updated_at'], row['updated_by']))

    conn.commit()
    conn.close()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    load_album_popularity_and_revenue()
