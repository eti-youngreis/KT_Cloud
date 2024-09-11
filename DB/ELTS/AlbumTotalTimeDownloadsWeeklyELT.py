from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3

def load():
    
    current_user = "User" # when IAM is implemented, get current user for session details
    
    elt_table_name = 'album_total_time_downloads_elt'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    conn = sqlite3.connect(base_path + 'database.db')
    
    try:
        conn = sqlite3.connect(base_path + "database.db")
        
        # extract data from csv files
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")

        invoice_line_table = spark.read.option("header", "true").csv(
            base_path + "InvoiceLine.csv"
        )
                                 
        track_table.toPandas().to_sql('tracks', con = conn, if_exists='replace', index=False)
        
        invoice_line_table.toPandas().to_sql('invoiceLines', con = conn, if_exists='replace', index = False)
        
        conn.execute(f"drop table if exists {elt_table_name}")
        conn.execute(f"""
                    CREATE TABLE {elt_table_name} (
                    AlbumId INTEGER,
                    total_album_downloads INTEGER,
                    total_album_length INTEGER,
                    created_at DATE,
                    updated_at DATE,
                    updated_by TEXT
                );""")
        transformation_query = f"""
                INSERT INTO {elt_table_name} (AlbumId, total_album_downloads, total_album_length, created_at, updated_at, updated_by)
                SELECT 
                    t.AlbumId, 
                    SUM(il.Quantity) AS total_album_downloads, 
                    temp.total_album_length,
                    CURRENT_DATE AS created_at,  
                    CURRENT_DATE AS updated_at, 
                    'WeeklyAlbumTotalsELT:{current_user}' AS updated_by    
                FROM invoiceLines il
                right JOIN tracks t ON il.TrackId = t.TrackId
                JOIN (
                    SELECT AlbumId, 
                        SUM(Milliseconds) AS total_album_length
                    FROM tracks
                    GROUP BY AlbumId
                ) AS temp ON t.AlbumId = temp.AlbumId
                GROUP BY t.AlbumId, temp.total_album_length;
                """
        conn.execute(transformation_query)
        conn.commit()
        
    finally:
        conn.close()
        spark.stop()
        
if __name__ == "__main__":
    load()