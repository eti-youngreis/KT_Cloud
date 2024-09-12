import pandas as pd
from pyspark.sql import SparkSession
import sqlite3
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

def add_created_at_column(file_path):

    df = pd.read_csv(file_path)

    if 'created_at' not in df.columns:
        print(df.columns)
        # הוספת עמודת created_at
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['created_at'] = current_time
        df['updated_at'] = current_time


        new_file_path = file_path
        df.to_csv(new_file_path, index=False)

        print(f"File saved with 'created_at' column: {new_file_path}")

def load_album_popularity_and_revenue_incremental_ETL():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Album Popularity and Revenue ETL") \
        .getOrCreate()

    # connect SQLite
    conn = sqlite3.connect('C:/Users/user1/Desktop/0909/Album.db')
    cursor = conn.cursor()

    try:

        cursor.execute("SELECT MAX(updated_at) FROM AlbumPopularityAndRevenue")
        latest_timestamp = cursor.fetchone()[0]

        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        print(latest_timestamp)

        # read the csv files
        albums_path = "C:/Users/user1/Downloads/KAN-134_attachments/Album.csv"
        invoice_lines_path = "C:/Users/user1/Downloads/KAN-134_attachments/InvoiceLine.csv"
        tracks_path = "C:/Users/user1/Downloads/KAN-134_attachments/Track.csv"

        # add created_at and updated_at
        add_created_at_column(albums_path)
        add_created_at_column(invoice_lines_path)
        add_created_at_column(tracks_path)

        # EXTRACT (Loading CSVs)
        albums = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Album.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/InvoiceLine.csv", header=True,
                                       inferSchema=True)
        tracks = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Track.csv", header=True, inferSchema=True)

        # filter data by updated_at
        albums = albums.withColumn("updated_at", to_timestamp(col("updated_at"), "dd/MM/yyyy HH:mm"))
        invoice_lines = invoice_lines.withColumn("updated_at", to_timestamp(col("updated_at"), "dd/MM/yyyy HH:mm"))
        tracks = tracks.withColumn("updated_at", to_timestamp(col("updated_at"), "dd/MM/yyyy HH:mm"))



        # Rename columns to avoid ambiguity
        albums = albums.withColumnRenamed("AlbumId", "AlbumId_Albums").withColumnRenamed("updated_at", "updated_at_albums")
        tracks = tracks.withColumnRenamed("AlbumId", "AlbumId_Tracks").withColumnRenamed("UnitPrice", "Track_UnitPrice").withColumnRenamed("updated_at", "updated_at_tracks")
        invoice_lines = invoice_lines.withColumnRenamed("TrackId", "TrackId_InvoiceLines").withColumnRenamed(
            "UnitPrice", "Invoice_UnitPrice").withColumnRenamed("updated_at", "updated_at_invoice_lines")



        # Join albums with tracks to get album-related information
        album_tracks = albums.join(
            tracks,
            (albums["AlbumId_Albums"] == tracks["AlbumId_Tracks"]) &
            ((albums["updated_at_albums"] > latest_timestamp) |
             (tracks["updated_at_tracks"] > latest_timestamp)),
            "inner"
        )

        # Join the result with invoice_lines to get revenue details
        album_invoice_lines = album_tracks.join(
            invoice_lines,
            (album_tracks["TrackId"] == invoice_lines["TrackId_InvoiceLines"]) &
            ((album_tracks["updated_at_albums"] > latest_timestamp) |
             (invoice_lines["updated_at_invoice_lines"] > latest_timestamp)),
            "inner"
        )

        # Calculate total revenue and track count per album
        album_revenue = album_invoice_lines.groupBy("AlbumId_Albums", "Title", "ArtistId") \
            .agg(
            F.sum("Invoice_UnitPrice").alias("TotalRevenue"),
            F.count("TrackId").alias("TrackCount")
        )

        print(f"Number of rows : {album_revenue.count()}")

        # Rename columns back to original names if needed
        album_revenue = album_revenue.withColumnRenamed("AlbumId_Albums", "AlbumId")

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        album_revenue = album_revenue.withColumn("created_at", F.lit(now)) \
            .withColumn("updated_at", F.lit(now)) \
            .withColumn("updated_by", F.lit("process:user_name"))

        rows = album_revenue.collect()

        for row in rows:
            cursor.execute('''
                        UPDATE AlbumPopularityAndRevenue
                        SET Title = ?,
                            ArtistId = ?,
                            TotalRevenue = ?,
                            TrackCount = ?,
                            updated_at = ?
                        WHERE AlbumId = ?
                    ''', (row.Title, row.ArtistId, row.TotalRevenue, row.TrackCount, row.updated_at, row.AlbumId))

        cursor.executemany('''
                    INSERT INTO AlbumPopularityAndRevenue (AlbumId, Title, ArtistId, TotalRevenue, TrackCount, created_at, updated_at)
                    SELECT ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM AlbumPopularityAndRevenue WHERE AlbumId = ?
                    )
                ''', [
            (row.AlbumId, row.Title, row.ArtistId, row.TotalRevenue, row.TrackCount, row.created_at, row.updated_at,
             row.AlbumId) for row in rows])

        conn.commit()

    finally:

        conn.close()


if __name__ == "__main__":
    load_album_popularity_and_revenue_incremental_ETL()