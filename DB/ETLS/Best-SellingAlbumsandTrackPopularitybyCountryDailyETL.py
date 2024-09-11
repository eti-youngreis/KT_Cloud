import sqlite3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Best-Selling Albums and Track Popularity by Country") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('DB/ETLS/etl_db_2.db')
    cursor = conn.cursor()

    try:
        # EXTRACT (Load CSVs from local storage)
        Invoices = spark.read.csv('DB/ETLS/data/Invoice.csv', header=True, inferSchema=True)
        InvoiceLines = spark.read.csv('DB/ETLS/data/InvoiceLine.csv', header=True, inferSchema=True)
        Tracks = spark.read.csv('DB/ETLS/data/Track.csv', header=True, inferSchema=True)
        Albums = spark.read.csv('DB/ETLS/data/Album.csv', header=True, inferSchema=True)
        Customers = spark.read.csv('DB/ETLS/data/Customer.csv', header=True, inferSchema=True)

        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Tracks = Tracks.withColumn("created_at", F.lit(current_datetime))
        # InvoiceLines = InvoiceLines.withColumn("created_at", F.lit(current_datetime))
        # Invoices = Invoices.withColumn("created_at", F.lit(current_datetime))
        # Albums = Albums.withColumn("created_at", F.lit(current_datetime))
        # Customers = Customers.withColumn("created_at", F.lit(current_datetime))

        cursor.execute("SELECT MAX(updated_at) FROM Best_Selling_Albums_And_Track_Popularity_By_Country")
        last_update_time = cursor.fetchone()[0]
        if last_update_time:
            last_update_time = datetime.strptime(last_update_time, '%Y-%m-%d %H:%M:%S')
            print(f"Last update time: {last_update_time}")
        else:
            print("No previous data found, performing full load.")


        if last_update_time:
            Invoices = Invoices.filter(F.col("created_at") > last_update_time)
            InvoiceLines = InvoiceLines.filter(F.col("created_at") > last_update_time)
            Tracks = Tracks.filter(F.col("created_at") > last_update_time)
            Albums = Albums.filter(F.col("created_at") > last_update_time)
            Customers = Customers.filter(F.col("created_at") > last_update_time)

        # TRANSFORM (Join tables and group by Album and Country)
        invoice_lines_tracks_df = InvoiceLines.alias("il"
        ).join(Tracks.alias("t"), on=F.col("il.TrackId") == F.col("t.TrackId"), how="inner"
        ).join(Albums.alias("a"), on=F.col("t.AlbumId") == F.col("a.AlbumId"), how="inner"
        ).join(Invoices.alias("i"), on=F.col("il.InvoiceId") == F.col("i.InvoiceId"), how="inner"
        ).join(Customers.alias("c"), on=F.col("i.CustomerId") == F.col("c.CustomerId"), how="inner"
        ).select(
            F.col("il.TrackId"),
            F.col("t.Name").alias("TrackName"),
            F.col("il.Quantity"),
            F.col("c.Country"),
            F.col("a.AlbumId"),
            F.col("a.Title").alias("AlbumTitle")
        )

        # Group by Album and Country
        # album_sales_by_country = invoice_lines_tracks_df.groupBy("Country", "AlbumId", "AlbumTitle") \
        #     .agg(
        #         F.sum("Quantity").alias("total_sales")
        #     )
        # Group by Country and AlbumId to get total sales per album in each country
        album_sales_by_country = invoice_lines_tracks_df.groupBy("Country", "AlbumId", "AlbumTitle") \
            .agg(
                F.sum("Quantity").alias("total_album_sales")
            )

        # Window specification to rank albums by total sales within each country
        album_window_spec = Window.partitionBy("Country").orderBy(F.desc("total_album_sales"))

        # Rank the albums by their sales within each country
        ranked_albums = album_sales_by_country.withColumn(
            "album_rank", F.rank().over(album_window_spec)
        )

        # Filter to keep only the best-selling album in each country
        best_selling_album_per_country = ranked_albums.filter(F.col("album_rank") == 1)

        # Join the best-selling albums with the original data to get the most popular track per album and country
        best_selling_tracks = best_selling_album_per_country.join(
            invoice_lines_tracks_df,
            on=["Country", "AlbumId"]
        ).groupBy("Country", "AlbumId", "TrackId", "TrackName", "total_album_sales") \
            .agg(
                F.sum("Quantity").alias("total_track_sales")
            )

        # Window specification to rank tracks within the best-selling albums by their sales
        track_window_spec = Window.partitionBy("Country", "AlbumId").orderBy(F.desc("total_track_sales"))

        # Rank the tracks by their sales
        ranked_tracks = best_selling_tracks.withColumn(
            "track_rank", F.rank().over(track_window_spec)
        )

        # Filter to keep only the most popular track from the best-selling album in each country
        most_popular_track_per_country = ranked_tracks.filter(F.col("track_rank") == 1)

        # Show the final result
        most_popular_track_per_country.select(
            "Country", "AlbumId", "TrackId", "TrackName", "total_album_sales", "total_track_sales"
        ).show(truncate=False)
        # Group by Album, Track, and Country to get the most popular tracks
        # track_sales_by_album_and_country = invoice_lines_tracks_df.groupBy("Country", "AlbumId", "TrackId", "TrackName") \
        #     .agg(
        #         F.sum("Quantity").alias("total_play_count")
        #     )

        # # Use window function to rank tracks within each album by popularity within each country
        # window_spec = Window.partitionBy("Country", "AlbumId").orderBy(F.desc("total_play_count"))

        # ranked_tracks = track_sales_by_album_and_country.withColumn(
        #     "track_rank", F.rank().over(window_spec)
        # )

        # # Filter to get the most popular track per album and country
        # most_popular_tracks = ranked_tracks.filter(F.col("track_rank") == 1)

        # # Show the result
        # most_popular_tracks.select(
        #     "AlbumId", "TrackId", "TrackName", "Country", "total_play_count"
        # ).show(truncate=False)

        # Add metadata columns
        # current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_name = "tamar_koledetzky"  # Replace this with a method to fetch the current user's name if needed

        final_data_with_metadata = most_popular_track_per_country.withColumn("created_at", F.lit(current_datetime)) \
            .withColumn("updated_at", F.lit(current_datetime)) \
            .withColumn("updated_by", F.lit(user_name))

        # LOAD (Save transformed data into SQLite)
        final_data_df = final_data_with_metadata.toPandas()

        # if final_data_df.empty:
        #     print("DataFrame is empty. No data to write.")
        # else:
        #     # Create table if not exists
        #     create_table_sql = """
        #     CREATE TABLE IF NOT EXISTS Best_Selling_Albums_And_Track_Popularity_By_Country (  
        #         Country TEXT,
        #         AlbumId INT,
        #         AlbumTitle TEXT,
        #         TrackId INT,
        #         TrackName TEXT,
        #         total_play_count INT,
        #         created_at TEXT,
        #         updated_at TEXT,
        #         updated_by TEXT
        #     )
        #     """
        #     cursor.execute(create_table_sql)

            # Insert transformed data into SQLite
        final_data_df.to_sql('Best_Selling_Albums_And_Track_Popularity_By_Country', conn, if_exists='append', index=False)
        # Commit the changes to the database
        conn.commit()
        # Query and print the contents of the table
        cursor.execute("SELECT * FROM Best_Selling_Albums_And_Track_Popularity_By_Country LIMIT 10;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load()
