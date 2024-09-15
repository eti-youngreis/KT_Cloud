from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# import KT_DB  # Assuming KT_DB is the library for SQLite operations
import sqlite3

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Repeat customer analysis Daily ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/db/repeat_customer_analysis.db')
    try:
        cursor = conn.cursor()
        latest_timestamp_query = "SELECT MAX(updated_at) FROM repeat_customer_analysis"
        cursor.execute(latest_timestamp_query)
        latest_timestamp = cursor.fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------

        # Load other related tables
        customers = spark.read.csv("D:/boto3 project/csv files/Customer_with_created_at.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("D:/boto3 project/csv files/Invoice_with_created_at.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("D:/boto3 project/csv files/InvoiceLine_with_created_at.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("D:/boto3 project/csv files/Track_with_created_at.csv", header=True, inferSchema=True)
        artists = spark.read.csv("D:/boto3 project/csv files/Artist_with_created_at.csv", header=True, inferSchema=True)
        albums = spark.read.csv("D:/boto3 project/csv files/Album_with_created_at.csv", header=True, inferSchema=True)
        
        # Rename ambiguous columns
        artists = artists.withColumnRenamed("Name", "ArtistName")  # Rename Name to ArtistName
        customers = customers.withColumnRenamed("FirstName", "CustomerFirstName")\
                                .withColumnRenamed("LastName", "CustomerLastName")  # Differentiate customer names
        # Filter for new data based on the latest timestamp
        customers = customers.filter(customers["updated_at"] > latest_timestamp)
        invoices = invoices.filter(invoices["updated_at"] > latest_timestamp)
        invoice_lines = invoice_lines.filter(invoice_lines["updated_at"] > latest_timestamp)
        tracks = tracks.filter(tracks["updated_at"] > latest_timestamp)
        artists = artists.filter(artists["updated_at"] > latest_timestamp)
        albums = albums.filter(albums["updated_at"] > latest_timestamp)
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------

        # Join InvoiceLines with Tracks to get the TrackID in the invoice lines
        invoice_track_df = invoice_lines.join(tracks, "TrackId")
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