from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Product Genre Popularity and Average Sales Price ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method
    path = 'D:\Users\גילי\Documents\בוטקמפ\csv files'

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the Genres, Tracks, and InvoiceLines tables
        genres_df = spark.read.csv(path + "/Genre.csv", header=True, inferSchema=True)
        tracks_df = spark.read.csv(path + "/Track.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(path + "/InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM (Calculate total sales and average sales price by genre)
        # --------------------------------------------------------
        # Join Tracks with InvoiceLines to get sales data
        tracks_sales_df = tracks_df.join(invoice_lines_df, "TrackId")

        # Join the above with Genres to get genre data
        genre_sales_df = genres_df.join(tracks_sales_df, "GenreId")

        # Calculate Total Sales and Average Sales Price for each genre
        genre_sales_agg_df = genre_sales_df.groupBy("GenreId", "Name") \
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSales"),
                F.avg("UnitPrice").alias("AverageSalesPrice")
            )

        # Add metadata columns
        final_genre_sales_df = genre_sales_agg_df.withColumn("created_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp()) \
            .withColumn("updated_by", F.lit("user_name"))  # Replace "user_name" with the actual user
        
        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_genre_sales_df.toPandas()

        # Insert transformed data into a new table in SQLite using KT_DB
        KT_DB.insert_dataframe(conn, 'GenreSales', final_data_df)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()

