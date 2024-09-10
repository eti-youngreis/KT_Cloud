import sqlite3
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, lit


def get_last_processed_timestamp(cursor):
    cursor.execute("SELECT MAX(updated_at) FROM Best_Selling_Albums_And_Track_Popularity_By_Country")
    result = cursor.fetchone()
    return result[0] if result[0] is not None else '1900-01-01 00:00:00'

def incremental_load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Best-Selling Albums and Track Popularity by Country - ELT") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('DB/ETLS/elt_db_2.db')
    cursor = conn.cursor()

    last_processed_timestamp = get_last_processed_timestamp(cursor)
    print(f"Last processed timestamp: {last_processed_timestamp}")

    try:
        # EXTRACT (Load CSVs from local storage into DataFrames)
        Invoices = spark.read.csv('DB/ETLS/data/Invoice.csv', header=True, inferSchema=True)
        InvoiceLines = spark.read.csv('DB/ETLS/data/InvoiceLine.csv', header=True, inferSchema=True)
        Tracks = spark.read.csv('DB/ETLS/data/Track.csv', header=True, inferSchema=True)
        Albums = spark.read.csv('DB/ETLS/data/Album.csv', header=True, inferSchema=True)
        Customers = spark.read.csv('DB/ETLS/data/Customer.csv', header=True, inferSchema=True)

        Tracks = Tracks.filter(col("created_at") > last_processed_timestamp)
        InvoiceLines = InvoiceLines.filter(col("created_at") > last_processed_timestamp)
        Invoices = Invoices.filter(col("created_at") > last_processed_timestamp)
        Albums = Albums.filter(col("created_at") > last_processed_timestamp)
        Customers = Customers.filter(col("created_at") > last_processed_timestamp)



        # LOAD (Load extracted data into SQLite database)
        Invoices.toPandas().to_sql('Invoices', conn, if_exists='replace', index=False)
        InvoiceLines.toPandas().to_sql('InvoiceLines', conn, if_exists='replace', index=False)
        Tracks.toPandas().to_sql('Tracks', conn, if_exists='replace', index=False)
        Albums.toPandas().to_sql('Albums', conn, if_exists='replace', index=False)
        Customers.toPandas().to_sql('Customers', conn, if_exists='replace', index=False)

        # TRANSFORM (SQL transformation in the database)
        query = """
        WITH AlbumSales AS (
            SELECT 
                c.Country, 
                a.AlbumId, 
                a.Title AS AlbumTitle, 
                t.TrackId, 
                t.Name AS TrackName, 
                SUM(il.Quantity) AS total_play_count
            FROM 
                InvoiceLines il
            JOIN 
                Tracks t ON il.TrackId = t.TrackId
            JOIN 
                Albums a ON t.AlbumId = a.AlbumId
            JOIN 
                Invoices i ON il.InvoiceId = i.InvoiceId
            JOIN 
                Customers c ON i.CustomerId = c.CustomerId
            GROUP BY 
                c.Country, a.AlbumId, t.TrackId
        ),
        BestSellingAlbums AS (
            SELECT 
                Country, 
                AlbumId, 
                AlbumTitle,
                SUM(total_play_count) AS total_album_sales
            FROM 
                AlbumSales
            GROUP BY 
                Country, AlbumId, AlbumTitle
        ),
        BestTracks AS (
            SELECT 
                a.Country, 
                a.AlbumId, 
                a.AlbumTitle, 
                a.TrackId, 
                a.TrackName, 
                a.total_play_count,
                RANK() OVER (PARTITION BY a.Country, a.AlbumId ORDER BY a.total_play_count DESC) AS track_rank
            FROM 
                AlbumSales a
        )
        SELECT 
            b.Country, 
            b.AlbumId, 
            b.AlbumTitle, 
            bt.TrackId, 
            bt.TrackName, 
            bt.total_play_count, 
            b.total_album_sales
        FROM 
            BestSellingAlbums b
        JOIN 
            BestTracks bt ON b.Country = bt.Country AND b.AlbumId = bt.AlbumId
        WHERE 
            bt.track_rank = 1
        ORDER BY 
            b.Country, b.total_album_sales DESC;
        """
        
        # Execute query to transform the data
        transformed_data = pd.read_sql_query(query, conn)

        # Add metadata columns
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_name = "tamar_koledetzky"  # Replace with a dynamic method if needed
        
        transformed_data['created_at'] = current_datetime
        transformed_data['updated_at'] = current_datetime
        transformed_data['updated_by'] = user_name

        # Store the transformed data back into the SQLite database
        transformed_data.to_sql('Best_Selling_Albums_And_Track_Popularity_By_Country', conn, if_exists='append', index=False)
         # Query and print the contents of the table
        cursor.execute("SELECT * FROM Best_Selling_Albums_And_Track_Popularity_By_Country LIMIT 10;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

        # Load the data back into Spark DataFrame for visualization
        # transformed_df = spark.createDataFrame(transformed_data)

        # Display the result using show()
        # transformed_df.show(truncate=False)

    finally:
        # Step 3: Close SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load()
