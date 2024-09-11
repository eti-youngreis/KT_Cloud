from pyspark.sql import SparkSession
import sqlite3
import os

# Set environment variables for Spark and Java
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark-3.5.2-bin-hadoop3\spark-3.5.2-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\רוט\AppData\Local\Programs\Python\Python312\python.exe"
os.environ["PATH"] += f";{os.environ['JAVA_HOME']}\\bin"


def load_Track_Popularity():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder.appName("Track_Popularity Weekly ELT").getOrCreate()

    # Step 2: Establish SQLite connection using sqlite3
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\Track_Popularity_db.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table (e.g., Customers, Invoices, InvoiceLines)
        tracks_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Track.csv', header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\InvoiceLine.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Invoice.csv', header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        tracks_df.toPandas().to_sql('tracks', conn, if_exists='replace', index=False)
        invoice_lines_df.toPandas().to_sql('invoice_lines', conn,
                                           if_exists='replace', index=False)
        invoices_df.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)

        # TRANSFORM (Perform transformations with SQL queries)
        # ----------------------------------------------------
        # Join tables and calculate total purchase and average purchase per customer
        # Apply the transformations using SQLite SQL queries
        drop_query = """DROP TABLE IF EXISTS track_popularity_ELT"""
        conn.execute(drop_query)
        conn.commit()
        transformation_query = '''
        CREATE TABLE IF NOT EXISTS track_popularity_ELT AS
        SELECT t.TrackId,t.Name,t.AlbumId,t.MediaTypeId,t.GenreId,t.Composer,t.Milliseconds,t.Bytes,t.UnitPrice,
            SUM(il.Quantity) AS CopiesSold,
            RANK() OVER (ORDER BY SUM(il.Quantity) DESC) AS PopularityRank,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'sarit_roth' AS updated_by
        FROM tracks t
        INNER JOIN invoice_lines il ON t.TrackId = il.TrackId
        INNER JOIN invoices i ON il.InvoiceId = i.InvoiceId
        GROUP BY t.TrackId;
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

# Execute the function

def testing():
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\Track_Popularity_db.db'
    conn = sqlite3.connect(db_path)

    try:
        cursor = conn.cursor()

        # SQL query to compare the two tables (excluding timestamp columns)
        comparison_query = '''
        SELECT 
            COUNT(*) AS TotalMismatch,
            SUM(CASE WHEN tp.TrackId IS NULL THEN 1 ELSE 0 END) AS MissingInTrackPopularity,
            SUM(CASE WHEN te.TrackId IS NULL THEN 1 ELSE 0 END) AS MissingInTrackPopularity_ELT
        FROM track_popularity tp
        FULL OUTER JOIN track_popularity_ELT te ON tp.TrackId = te.TrackId
        WHERE 
            tp.Name <> te.Name OR
            tp.AlbumId <> te.AlbumId OR
            tp.MediaTypeId <> te.MediaTypeId OR
            tp.GenreId <> te.GenreId OR
            tp.Composer <> te.Composer OR
            tp.Milliseconds <> te.Milliseconds OR
            tp.Bytes <> te.Bytes OR
            tp.UnitPrice <> te.UnitPrice OR
            tp.CopiesSold <> te.CopiesSold OR
            tp.PopularityRank <> te.PopularityRank
        '''

        # Execute the comparison query
        cursor.execute(comparison_query)
        result = cursor.fetchone()

        # Display results
        total_mismatch, missing_in_tp, missing_in_te = result
        print(f"Total Mismatches: {total_mismatch}")
        print(f"Missing in track_popularity: {missing_in_tp}")
        print(f"Missing in track_popularity_ELT: {missing_in_te}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up resources
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_Track_Popularity()
    testing()
    
