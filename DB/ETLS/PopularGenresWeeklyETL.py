from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import sqlite3
import getpass  # For getting the username for metadata


def load():
    """
    ETL process to calculate Most Popular Genres by Customer Segment.
    """

    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('DB/etl_db.db')  # Assuming KT_DB has a connect() method

    try:
        cursor = conn.cursor()
        # Check if the 'album_popularity_revenue' table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='PopularGenresByCustomerSegment';
        """)
        table_exists_flag = cursor.fetchone()

        # If the table exists, drop it
        if table_exists_flag:
            print("Table 'PopularGenresByCustomerSegment' exists, dropping it.")
            cursor.execute("DROP TABLE PopularGenresByCustomerSegment;")
            conn.commit()
        else:
            print("PopularGenresByCustomerSegment' does not exist.")
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table: Albums, Tracks, and InvoiceLines

        customers = spark.read.csv("DB\ETLS\csv_files\Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("DB\ETLS\csv_files\Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("DB\ETLS\csv_files\InvoiceLine.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("DB\ETLS\csv_files\Track.csv", header=True, inferSchema=True)
        genres = spark.read.csv("DB\ETLS\csv_files\Genre.csv", header=True, inferSchema=True)

        # exchange name to avoid multiply
        genres = genres.withColumnRenamed("Name", "GenreName")

        # TRANSFORM (Apply joins, groupings, and window functions)
        # -------------------------------------------------------

        # join all tables

        customers_with_genres = customers.join(invoices, "CustomerId", "inner").drop(customers["CustomerId"]).join(
            invoice_lines, "InvoiceId", "inner"
        ).drop(invoices["InvoiceId"]).join(
            tracks, "TrackId", "inner"
        ).drop(invoice_lines["TrackId"]).join(
            genres, "GenreId", "inner"
        ).drop(tracks["GenreId"]).drop(tracks["Name"]).select(
            "City", "GenreName", "Quantity"
        )

        # count Genres popularity group by customer Cities
        agg_customers_with_genres = customers_with_genres.groupBy("City", "GenreName").agg(
            F.sum("Quantity").alias("GenreCount")
        )

        # window function to rank genres' popularity
        window_spec = Window.partitionBy("City").orderBy(F.desc("GenreCount"))
        genres_rank = agg_customers_with_genres.withColumn("rank", F.row_number().over(window_spec))

        # filter only the popular genres

        genres_rank_filtered = genres_rank.filter(F.col("rank") <= 5)
        top_genres = genres_rank_filtered.groupBy("City").agg(
            F.max(F.when(F.col("rank") == 1, F.col("GenreName"))).alias("most_popular_genre"),
            F.max(F.when(F.col("rank") == 2, F.col("GenreName"))).alias("sec_popular_genre"),
            F.max(F.when(F.col("rank") == 3, F.col("GenreName"))).alias("third_popular_genre"),
            F.max(F.when(F.col("rank") == 4, F.col("GenreName"))).alias("fourth_popular_genre"),
            F.max(F.when(F.col("rank") == 5, F.col("GenreName"))).alias("fifth_popular_genre")
        )

        # Add metadata columns
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated_by = getpass.getuser()  # Get the username of the person running the script

        top_genres = top_genres \
            .withColumn("created_at", F.lit(current_datetime)) \
            .withColumn("updated_at", F.lit(current_datetime)) \
            .withColumn("updated_by", F.lit(updated_by))

        # LOAD
        # -----------------------------------------------------------

        # Create table if not exists
       
        cursor.execute('''CREATE TABLE IF NOT EXISTS PopularGenresByCustomerSegment (
                      City TEXT NOT NULL PRIMARY KEY,
                      most_popular_genre TEXT,
                      sec_popular_genre TEXT,
                      third_popular_genre TEXT,
                      fourth_popular_genre TEXT,
                      fifth_popular_genre TEXT,
                      created_at TEXT,
                      updated_at TEXT,
                      updated_by TEXT)''')
        
        # Insert data
        for row in top_genres.collect():
            cursor.execute('''INSERT INTO PopularGenresByCustomerSegment
                          (City, most_popular_genre,
                            sec_popular_genre,
                            third_popular_genre,
                            fourth_popular_genre,
                            fifth_popular_genre, created_at, updated_at, updated_by)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (
                               row['City'], row['most_popular_genre'], row['sec_popular_genre'],
                               row['third_popular_genre'], row['fourth_popular_genre'], row['fifth_popular_genre'], row['created_at'],
                               row['updated_at'], row['updated_by']))

        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()


load()
