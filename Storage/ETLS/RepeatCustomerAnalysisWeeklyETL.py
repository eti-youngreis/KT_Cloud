from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# import KT_DB  # Assuming KT_DB is the library for SQLite operations
import sqlite3

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Repeat customer analysis Weekly ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/db/repeat_customer_analysis.db')
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------

        # Load other related tables
        customers = spark.read.csv("D:/boto3 project/csv files/Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("D:/boto3 project/csv files/Invoice.csv", header=True, inferSchema=True)
        invoice_line = spark.read.csv("D:/boto3 project/csv files/InvoiceLine.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("D:/boto3 project/csv files/Track.csv", header=True, inferSchema=True)
        artists = spark.read.csv("D:/boto3 project/csv files/Artist.csv", header=True, inferSchema=True)
        albums = spark.read.csv("D:/boto3 project/csv files/Album.csv", header=True, inferSchema=True)
        
        # Rename ambiguous columns
        artists = artists.withColumnRenamed("Name", "ArtistName")  # Rename Name to ArtistName
        customers = customers.withColumnRenamed("FirstName", "CustomerFirstName")\
                                .withColumnRenamed("LastName", "CustomerLastName")  # Differentiate customer names
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------

        # Join InvoiceLines with Tracks to get the TrackID in the invoice lines
        invoice_track_df = invoice_line.join(tracks, "TrackId")
        # Join Tracks with Albums to get AlbumID, and join with Artists to get ArtistID and ArtistName
        invoice_track_album_artist_df = invoice_track_df.join(albums, "AlbumId")\
                                                .join(artists, "ArtistId")
        
        # Join the result with Invoices to get the CustomerId and other invoice details
        full_df = invoice_track_album_artist_df.join(invoices, "InvoiceId")\
            .join(customers, "CustomerId")
        # Transformation: Group by artist and customer, count number of purchases per customer for each artist
        customer_purchase_count = full_df.groupBy("ArtistId", "CustomerId", "ArtistName")\
                                        .agg(F.count("InvoiceId").alias("PurchaseCount"))
        
        # Apply window function to rank customers by number of purchases for each artist
        window_spec = Window.partitionBy("ArtistId").orderBy(F.desc("PurchaseCount"))
        customer_purchase_count = customer_purchase_count.withColumn("Rank", F.rank().over(window_spec))
    
        customer_purchase_count = customer_purchase_count.withColumn("created_at", F.current_timestamp())\
                            .withColumn("updated_at", F.current_timestamp())\
                            .withColumn("updated_by", F.lit("Efrat"))
                    
        # Convert to pandas dataframe and write to SQLite
        customer_purchase_count.toPandas().to_sql('repeat_customer_analysis', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  
        spark.stop()

if __name__ == "__main__":
    load()