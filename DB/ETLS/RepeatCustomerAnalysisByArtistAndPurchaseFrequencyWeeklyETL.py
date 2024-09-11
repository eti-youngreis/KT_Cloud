from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import os

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Artist Repeat Customer Analysis ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using sqlite3
    db_path = r"KT_Cloud\DB\ETLS\dbs\artist_repeat_customer_analysis.db"

    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))

    conn = sqlite3.connect(db_path)

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        invoices = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\InvoiceLine.csv", header=True, inferSchema=True)
        tracks = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Track.csv", header=True, inferSchema=True)
        albums = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Album.csv", header=True, inferSchema=True)
        artists = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Artist.csv", header=True, inferSchema=True)
        customers = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Customer.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        
        # Alias the tables
        invoices = invoices.alias("inv")
        invoice_lines = invoice_lines.alias("il")
        tracks = tracks.alias("tr")
        albums = albums.alias("al")
        artists = artists.alias("ar")
        customers = customers.alias("cust")

        # Join invoices with invoice_lines, tracks, albums, and artists
        sales_data = invoices.join(invoice_lines, "InvoiceID", "inner") \
            .join(tracks, invoice_lines["TrackID"] == tracks["TrackID"], "inner") \
            .join(albums, tracks["AlbumID"] == albums["AlbumID"], "inner") \
            .join(artists, albums["ArtistID"] == artists["ArtistID"], "inner") \
            .select("inv.InvoiceID", "inv.CustomerID", "ar.ArtistID", "ar.Name", "il.Quantity")
        
        sales_data.show()

        # Define a window specification partitioned by ArtistID and CustomerID
        customer_artist_window = Window.partitionBy("ar.ArtistID", "inv.CustomerID")

        # Calculate purchase count per customer per artist using window function
        sales_data = sales_data.withColumn("PurchaseCount", F.count("inv.InvoiceID").over(customer_artist_window))

        # Filter for customers who have purchased more than once
        repeat_customers = sales_data.filter(F.col("PurchaseCount") > 1).select("ArtistID", "Name", "CustomerID", "PurchaseCount").distinct()

        # Define a window specification partitioned by ArtistID
        artist_window = Window.partitionBy("ar.ArtistID")

        # Calculate the count of repeat customers per artist
        repeat_customer_count = repeat_customers.withColumn("RepeatCustomerCount", F.count("CustomerID").over(artist_window))

        # Select final columns
        final_data = repeat_customer_count.select("ArtistID", "Name", "CustomerID", "PurchaseCount", "RepeatCustomerCount")

        # Show the final data
        final_data.show()

        # Add metadata columns
        current_time = datetime.now()
        user_name = "DailyETL:Yehudit"

        final_data = final_data \
            .withColumn("created_at", F.lit(current_time)) \
            .withColumn("updated_at", F.lit(current_time)) \
            .withColumn("updated_by", F.lit(user_name))

        # LOAD (Save transformed data into SQLite using sqlite3)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_data.toPandas()

        # Create table if not exists and insert data into SQLite
        final_data_df.to_sql(name=r"artist_repeat_customer_analysis", con=conn, if_exists='replace', index=False)
        
        # Commit the changes to the database
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load()