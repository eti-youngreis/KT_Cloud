import os
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load():
    try:
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("ETL with SQLite") \
            .getOrCreate()

        # Step 2: Establish SQLite connection using sqlite3
        conn = sqlite3.connect('D:\\b\\Album.db')
        cursor = conn.cursor()

        try:
            # EXTRACT (Loading CSVs from local storage)
            print("Loading CSV files...")
            albums_df = spark.read.csv("D:\\csvFiles\\Album.csv", header=True, inferSchema=True)
            tracks_df = spark.read.csv("D:\\csvFiles\\Track.csv", header=True, inferSchema=True)
            invoice_lines_df = spark.read.csv("D:\\csvFiles\\InvoiceLine.csv", header=True, inferSchema=True)

            # TRANSFORM (Apply joins, groupings, and window functions)
            print("Transforming data...")

            albums_df = albums_df.withColumnRenamed("AlbumId", "AlbumId_Albums")
            tracks_df = tracks_df.withColumnRenamed("AlbumId", "AlbumId_Tracks")

            album_lengths_df = albums_df.join(
                tracks_df, albums_df["AlbumId_Albums"] == tracks_df["AlbumId_Tracks"], "left"
            ).groupBy("AlbumId_Albums", "Title", "ArtistId") \
            .agg(F.sum("Milliseconds").alias("TotalAlbumLength"))
            
            album_downloads_df = albums_df.join(
                tracks_df, albums_df["AlbumId_Albums"] == tracks_df["AlbumId_Tracks"], "left"
            ).join(
                invoice_lines_df, tracks_df["TrackId"] == invoice_lines_df["TrackId"], "left"
            ).groupBy("AlbumId_Albums", "Title", "ArtistId") \
            .agg(F.count("InvoiceLineId").alias("TotalDownloads"))

            final_albums_df = album_lengths_df.join(
                album_downloads_df,
                ["AlbumId_Albums", "Title", "ArtistId"],
                "inner"
            ).withColumn("created_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp()) \
            .withColumn("updated_by", F.lit("process:user_name"))            
            final_albums_pd = final_albums_df.toPandas()

            # LOAD (Save transformed data into SQLite)
            print("Loading data into SQLite...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS albums_summary (
                    AlbumId INTEGER,
                    Title TEXT,
                    ArtistId INTEGER,
                    TotalAlbumLength INTEGER,
                    TotalDownloads INTEGER,
                    created_at TEXT,
                    updated_at TEXT,
                    updated_by TEXT
                )
            ''')

            final_albums_pd.to_sql('albums_summary', conn, if_exists='replace', index=False)

            conn.commit()

            # SELECT query to retrieve data
            print("Executing SELECT query...")
            cursor.execute("SELECT * FROM albums_summary ORDER BY AlbumId_Albums LIMIT 10")
            rows = cursor.fetchall()

            for row in rows:
                print(row)

        except Exception as e:
            print(f"Error during ETL process: {e}")

        finally:
            # Step 3: Close the SQLite connection and stop Spark session
            cursor.close()
            conn.close()
            spark.stop()

    except Exception as e:
        print(f"Error initializing Spark or SQLite: {e}")

load()