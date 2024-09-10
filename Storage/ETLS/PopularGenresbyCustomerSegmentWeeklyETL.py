from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime

def load_popular_genres_by_city():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Popular Genres by City ETL") \
        .getOrCreate()

    # EXTRACT (Loading CSVs)
    customers = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Customer.csv", header=True, inferSchema=True)
    invoices = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Invoice.csv", header=True, inferSchema=True)
    invoice_lines = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/InvoiceLine.csv", header=True, inferSchema=True)
    tracks = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Track.csv", header=True, inferSchema=True)
    genres = spark.read.csv("C:/Users/user1/Downloads/KAN-134_attachments/Genre.csv", header=True, inferSchema=True)

    # Rename columns to avoid ambiguity
    invoice_lines = invoice_lines.withColumnRenamed("TrackId", "TrackId_InvoiceLines").withColumnRenamed("Quantity", "Invoice_Quantity")
    tracks = tracks.withColumnRenamed("TrackId", "TrackId_Tracks").withColumnRenamed("AlbumId", "AlbumId_Tracks").withColumnRenamed("UnitPrice", "Track_UnitPrice")
    genres = genres.withColumnRenamed("GenreId", "GenreId_Genres").withColumnRenamed("Name", "GenreName")

    # TRANSFORM
    # Join customers with invoices to get customer purchases
    customer_invoices = customers.join(invoices, customers["CustomerId"] == invoices["CustomerId"], "inner")

    # Join the result with invoice_lines and tracks to get track and genre information
    customer_tracks = customer_invoices.join(invoice_lines, customer_invoices["InvoiceId"] == invoice_lines["InvoiceId"], "inner") \
                                       .join(tracks, invoice_lines["TrackId_InvoiceLines"] == tracks["TrackId_Tracks"], "inner") \
                                       .join(genres, tracks["GenreId"] == genres["GenreId_Genres"], "inner")

    # Group by City and Genre and sum the quantity to get genre popularity
    genre_popularity = customer_tracks.groupBy("City", "GenreName") \
                                      .agg(F.sum("Invoice_Quantity").alias("TrackCount"))

    # Rank genres by popularity within each city using window function
    window_spec = Window.partitionBy("City").orderBy(F.desc("TrackCount"))
    genre_popularity = genre_popularity.withColumn("Rank", F.rank().over(window_spec))

    # Filter to get only the top 5 genres per city
    top_genres = genre_popularity.filter(genre_popularity["Rank"] <= 5)

    # Metadata
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    top_genres = top_genres.withColumn("created_at", F.lit(now_str)) \
                           .withColumn("updated_at", F.lit(now_str)) \
                           .withColumn("updated_by", F.lit("process:user_name"))

    # Load data into SQLite using sqlite3
    conn = sqlite3.connect('C:/Users/user1/Desktop/0909/Genre.db')
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS PopularGenresByCity')

    # Create table if not exists
    cursor.execute('''CREATE TABLE  PopularGenresByCity (
                      City TEXT,
                      GenreName TEXT,
                      TrackCount INTEGER,
                      Rank INTEGER,
                      created_at TEXT,
                      updated_at TEXT,
                      updated_by TEXT)''')

    # Insert data
    for row in top_genres.collect():
        cursor.execute('''INSERT INTO PopularGenresByCity 
                          (City, GenreName, TrackCount, Rank, created_at, updated_at, updated_by)
                          VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (row['City'], row['GenreName'], row['TrackCount'], row['Rank'],
                           row['created_at'], row['updated_at'], row['updated_by']))

    conn.commit()
    conn.close()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    load_popular_genres_by_city()
