from pyspark.sql import SparkSession
import sqlite3
import traceback

user_name = "Shani_K"  # שם המשתמש שלך

def load_track_play_count_and_revenue_contribution_elt(spark):
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database
    
    try:
        # EXTRACT (שליפה של קבצי CSV)
        track_table = spark.read.csv("./csv/Track.csv", header=True, inferSchema=True)
        invoiceLine_table = spark.read.csv("./csv/InvoiceLine.csv", header=True, inferSchema=True)

        # LOAD (טעינת הנתונים למסד הנתונים SQLite)
        track_table_pd = track_table.toPandas()
        invoiceLine_table_pd = invoiceLine_table.toPandas()

        track_table_pd.to_sql('Track', conn, if_exists='replace', index=False)
        invoiceLine_table_pd.to_sql('InvoiceLine', conn, if_exists='replace', index=False)

        # TRANSFORM (ביצוע הטרנספורמציות בתוך מסד הנתונים עצמו, באמצעות שאילתות SQL)
        drop_query = """DROP TABLE IF EXISTS track_play_count_and_revenue_contribution_elt"""
        conn.execute(drop_query)
        conn.commit()
        
        transform_query = """
        CREATE TABLE track_play_count_and_revenue_contribution_elt AS
        SELECT 
            t.TrackID,
            t.Name,
            t.AlbumID,
            SUM(inv.Quantity) AS total_play_count,
            SUM(inv.Quantity * inv.UnitPrice) AS revenue_contribution,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'process:user_name' AS updated_by
        FROM Track t
        JOIN InvoiceLine inv ON t.TrackID = inv.TrackID
        GROUP BY t.TrackID
        """

        # ביצוע השאילתה במסד הנתונים
        conn.execute(transform_query)
        conn.commit()

        # הצגת התוצאה
        print("track_play_count_and_revenue_contribution_elt:", conn.execute("SELECT * FROM track_play_count_and_revenue_contribution_elt").fetchall())

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    load_track_play_count_and_revenue_contribution_elt()
