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

    # Step 2: Establish SQLite connection using sqlite3
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\track_db.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # EXTRACT (Loading CSVs from local storage)
        tracks_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Track.csv', header=True, inferSchema=True)
        albums_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Album.csv', header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\InvoiceLine.csv', header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        tracks_df.toPandas().to_sql('tracks', conn, if_exists='replace', index=False)
        invoice_lines_df.toPandas().to_sql('invoice_lines', conn,
                                           if_exists='replace', index=False)
        albums_df.toPandas().to_sql('albums', conn, if_exists='replace', index=False)

        # TRANSFORM (Perform transformations with SQL queries)
        # ----------------------------------------------------
        # Join tables and calculate total purchase and average purchase per customer
        # Apply the transformations using SQLite SQL queries
        drop_query = """DROP TABLE IF EXISTS track_popularity_ELT"""
        conn.execute(drop_query)
        conn.commit()
        transformation_query = '''
         CREATE TABLE IF NOT EXISTS track_transformation_elt AS
            SELECT 
                t.TrackId,
                t.Name,
                t.AlbumId,
                t.MediaTypeId,
                t.GenreId,
                t.Composer,
                t.Milliseconds,
                t.Bytes,
                t.UnitPrice,
                avg_lengths.AverageTrackLength,
                download_stats.DownloadQuantity,
                download_stats.DownloadFrequency,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at,
                'sarit_roth' AS updated_by
            FROM 
                tracks t
            LEFT JOIN (
                SELECT
                    AlbumId,
                    AVG(Milliseconds) AS AverageTrackLength
                FROM
                    tracks
                GROUP BY
                    AlbumId
            ) AS avg_lengths ON t.AlbumId = avg_lengths.AlbumId
            LEFT JOIN (
                SELECT
                    TrackId,
                    SUM(Quantity) AS DownloadQuantity,
                    COUNT(InvoiceLineId) AS DownloadFrequency
                FROM
                    invoice_lines
                GROUP BY
                    TrackId
            ) AS download_stats ON t.TrackId = download_stats.TrackId;

        '''
        cursor = conn.cursor()
        # Execute the transformation query
        cursor.execute(transformation_query)
        # Commit the changes to the database
        conn.commit()

    except Exception as e:
        print(f"An error occurred during database operations: {e}")
    finally:
        # Clean up resources
        cursor.close()
        conn.close()
        spark.stop()


def compare_etl_elt_tables():
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\track_db.db'
    conn = sqlite3.connect(db_path)

    try:
        cursor = conn.cursor()

        # SQL query to compare the two tables (excluding timestamp columns)
        comparison_query = '''
        SELECT 
            COUNT(*) AS TotalMismatch,
            SUM(CASE WHEN tt.TrackId IS NULL THEN 1 ELSE 0 END) AS MissingInTrackTransformation,
            SUM(CASE WHEN tf.TrackId IS NULL THEN 1 ELSE 0 END) AS MissingInFinalTable
        FROM track_transformation_elt tt
        FULL OUTER JOIN final_table tf ON tt.TrackId = tf.TrackId
        WHERE 
            tt.Name <> tf.Name OR
            tt.AlbumId <> tf.AlbumId OR
            tt.MediaTypeId <> tf.MediaTypeId OR
            tt.GenreId <> tf.GenreId OR
            tt.Composer <> tf.Composer OR
            tt.Milliseconds <> tf.Milliseconds OR
            tt.Bytes <> tf.Bytes OR
            tt.UnitPrice <> tf.UnitPrice
        '''

        # Execute the comparison query
        cursor.execute(comparison_query)
        result = cursor.fetchone()

        # Display results
        total_mismatch, missing_in_tt, missing_in_tf = result
        print(f"Total Mismatches: {total_mismatch}")
        print(f"Missing in track_transformation_elt: {missing_in_tt}")
        print(f"Missing in final_table: {missing_in_tf}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up resources
        cursor.close()
        conn.close()


# Execute the function
if __name__ == "__main__":
    load_Track_Length_and_Download_Frequency()
    compare_etl_elt_tables()
