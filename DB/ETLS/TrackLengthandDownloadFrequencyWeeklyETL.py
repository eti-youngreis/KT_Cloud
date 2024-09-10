from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
import os

# Set environment variables for Spark and Java
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark-3.5.2-bin-hadoop3\spark-3.5.2-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\רוט\AppData\Local\Programs\Python\Python312\python.exe"
os.environ["PATH"] += f";{os.environ['JAVA_HOME']}\\bin"

def load_Track_Length_and_Download_Frequency():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder.appName(
        "Track_Length_and_Download_Frequency ETL").getOrCreate()

    # Path to the SQLite database
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\track_db.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # EXTRACT (Load CSV files into Spark DataFrames)
        tracks_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Track.csv', header=True, inferSchema=True)
        albums_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Album.csv', header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\InvoiceLine.csv', header=True, inferSchema=True)

        # TRANSFORM (Join tables and calculate metrics)
        window_album = Window.partitionBy("AlbumId")

        # Join tracks and albums DataFrames, then calculate average track length
        tracks_albums_df = tracks_df.join(albums_df, tracks_df.AlbumId == albums_df.AlbumId) \
            .drop(albums_df.AlbumId)  # Drop duplicate AlbumId column
        
        tracks_albums_df = tracks_albums_df \
            .withColumn("AverageTrackLength", F.avg("Milliseconds").over(window_album))

        # Join tracks and invoice lines DataFrames, then calculate download frequency
        track_downloads_df = tracks_df.join(invoice_lines_df, "TrackId") \
            .drop(invoice_lines_df.TrackId)
        
        window_track = Window.partitionBy("TrackId")

        # Calculate Download Quantity using Window
        track_downloads_df = track_downloads_df \
        .withColumn("DownloadFrequency", F.sum("Quantity").over(window_track)) \

        # track_downloads_df = track_downloads_df \
        #     .withColumn("DownloadFrequency", F.count("InvoiceLineId").over(window_track))
        print("tracks_albums_df.columns:")
        print(tracks_albums_df.columns)
        print("track_downloads_df.columns:")
        print(track_downloads_df.columns)
        # שמירה על עמודות ספציפיות בלבד
        tracks_albums_df = tracks_albums_df.select("AlbumId","AverageTrackLength")
        track_downloads_df = track_downloads_df.select("TrackId", "DownloadFrequency")
        # Combine final data and add metadata columns
        final_data = tracks_df \
            .join(tracks_albums_df, "AlbumId", "left") \
            .join(track_downloads_df, "TrackId", "left")
        print("final_data.columns:")    
        print(final_data.columns)
        # Select only relevant columns
        final_data = final_data.select(
            tracks_df.TrackId.alias("TrackId"),
            tracks_df.Name,
            tracks_df.AlbumId,
            tracks_df.MediaTypeId,
            tracks_df.GenreId,
            tracks_df.Composer,
            tracks_df.Milliseconds,
            tracks_df.Bytes,
            tracks_df.UnitPrice,
            tracks_albums_df.AverageTrackLength,
            track_downloads_df.DownloadFrequency,
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            F.lit("sarit_roth").alias("updated_by")
        )

        # LOAD (Save transformed data into SQLite database)
        final_data_df = final_data.toPandas()
        print(final_data_df.columns)

        try:
            cursor.execute('DROP TABLE IF EXISTS final_table')
            # Create table in SQLite if it doesn't exist
            cursor.execute('''
                    CREATE TABLE IF NOT EXISTS final_table (
                        TrackID INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        AlbumID INTEGER,
                        MediaTypeId INTEGER,
                        GenreID INTEGER,
                        Composer TEXT,
                        Milliseconds INTEGER,
                        Bytes INTEGER,
                        UnitPrice REAL,
                        AverageTrackLength REAL,
                        DownloadFrequency INTEGER,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        updated_by TEXT
                    )
                ''')

            # Insert transformed data into final_table
            final_data_df.to_sql('final_table', conn,
                                 if_exists='replace', index=False)

            # Commit the changes to the database
            conn.commit()

        except Exception as e:
            print(f"An error occurred during database operations: {e}")

    finally:
        # Step 3: Clean up resources
        cursor.close()  # Close the cursor
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session


# Execute the function
load_Track_Length_and_Download_Frequency()
