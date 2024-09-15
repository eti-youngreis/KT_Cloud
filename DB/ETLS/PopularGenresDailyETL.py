from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import sqlite3
import getpass  # For getting the username for metadata


def popular_genres_by_city_incremental_etl():
    """
    ETL process to calculate Most Popular Genres by Customer Segment.
    """

    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('DB/etl_db.db')  
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='PopularGenresByCity'")
        if cursor.fetchone() is None:
            cursor.execute('''
            CREATE TABLE PopularGenresByCity (
                City TEXT NOT NULL PRIMARY KEY,
                most_popular_genre TEXT,
                sec_popular_genre TEXT,
                third_popular_genre TEXT,
                fourth_popular_genre TEXT,
                fifth_popular_genre TEXT,
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
            ''')
            print("Table 'PopularGenresByCity' created.")

        # Step 4: Get the latest processed timestamp
        cursor.execute("SELECT MAX(updated_at) FROM PopularGenresByCity")
        latest_timestamp = cursor.fetchone()[0] or '1900-01-01 00:00:00'
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table: Albums, Tracks, and InvoiceLines

        customers = spark.read.csv("DB\csvs\Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("DB\csvs\Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("DB\csvs\InvoiceLine.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("DB\csvs\Track.csv", header=True, inferSchema=True)
        genres = spark.read.csv("DB\csvs\Genre.csv", header=True, inferSchema=True)

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
        ).drop(tracks["GenreId"]).drop(tracks["Name"]).withColumn("max_updated_at", F.greatest(customers.updated_at ,invoices.updated_at, invoice_lines.updated_at, tracks.updated_at, genres.updated_at)).drop(
                        customers["updated_at"]).drop(
                            tracks["updated_at"]).drop(
                                invoice_lines["updated_at"]).drop(
                                    invoices["updated_at"]).drop(
                                        genres["updated_at"]).filter(
                        F.col('max_updated_at') > latest_timestamp).select(
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
        

        
        # add created_at column 
        # create view of updated records   
        top_genres.createOrReplaceTempView("genres_popularity_view")
        # select all created_at from prev records to update
        created_time_of_records_df = spark.sql('SELECT City, created_at from PopularGenresByCity WHERE City IN (SELECT City FROM genres_popularity_view)')
        # add to df
        top_genres = top_genres.join(created_time_of_records_df,"AlbumId","left")\
            .select([top_genres["*"]] +
                    [created_time_of_records_df["created_at"] if "created_at" in created_time_of_records_df.columns 
                     else F.lit(current_datetime).alias("created_at")])
        
        # add updated_at and updated_by columns    
        top_genres = top_genres \
            .withColumn("updated_at", F.lit(current_datetime)) \
            .withColumn("updated_by", F.lit(updated_by))
        
        
        # LOAD
        # -----------------------------------------------------------
        
        # delete from final table if exists all rcords to update
        cursor.execute('DELETE from PopularGenresByCity WHERE City in (SELECT City FROM genres_popularity_view)')
        
        # Insert the updated data
        top_genres_df = top_genres.toPandas()
        top_genres_df.to_sql('PopularGenresByCity', conn, if_exists='append', index=False)




        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()


if __name__  == "__main__":
    popular_genres_by_city_incremental_etl()
