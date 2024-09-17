from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import os

def incremental_load():
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("Daily Artist Repeat Customer Analysis ETL") \
            .getOrCreate()

        # Step 2: Establish SQLite connection
        db_path = r"KT_Cloud\DB\ETLS\dbs\artist_repeat_customer_analysis.db"
        if not os.path.exists(os.path.dirname(db_path)):
            os.makedirs(os.path.dirname(db_path))

        conn = sqlite3.connect(db_path)

        try:
            # Step 3: Get the latest processed timestamp from the target table
            cursor = conn.cursor()
            latest_timestamp_query = "SELECT MAX(updated_at) FROM artist_repeat_customer_analysis"
            cursor.execute(latest_timestamp_query)
            latest_timestamp = cursor.fetchone()[0]

            # Handle case where no data exists yet (initial load)
            if latest_timestamp is None:
                latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially

            # EXTRACT (Loading CSVs from local storage with incremental filtering)
            # --------------------------------------------------------------------
            invoices = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Invoice_with_created_at.csv", header=True, inferSchema=True)
            invoice_lines = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\InvoiceLine_with_created_at.csv", header=True, inferSchema=True)
            tracks = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Track_with_created_at.csv", header=True, inferSchema=True)
            albums = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Album_with_created_at.csv", header=True, inferSchema=True)
            artists = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Artist_with_created_at.csv", header=True, inferSchema=True)

            # Filter for new data based on the latest timestamp
            # new_invoices = invoices.filter(invoices["updated_at"] > latest_timestamp)
            # new_invoice_lines = invoice_lines.join(new_invoices, "InvoiceID", "inner")
            # new_tracks = tracks.filter(tracks["updated_at"] > latest_timestamp)
            # new_albums = albums.filter(albums["updated_at"] > latest_timestamp)
            # new_artists = artists.filter(artists["updated_at"] > latest_timestamp)
            # new_customers = customers.filter(customers["updated_at"] > latest_timestamp)

            # TRANSFORM (Apply joins, groupings, and window functions)
            # --------------------------------------------------------
            # Alias the tables with more descriptive names
            invoices = invoices.alias("invoice")
            invoice_lines = invoice_lines.alias("invoice_line")
            tracks = tracks.alias("track")
            albums = albums.alias("album")
            artists = artists.alias("artist")

            # Join invoices with invoice_lines, tracks, albums, and artists
            sales_data = invoices.join(invoice_lines, "InvoiceID", "inner") \
                .join(tracks, invoice_lines["TrackID"] == tracks["TrackID"], "inner") \
                .join(albums, tracks["AlbumID"] == albums["AlbumID"], "inner") \
                .join(artists, albums["ArtistID"] == artists["ArtistID"], "inner") \
                .select("invoice.InvoiceID", "invoice.CustomerID", "artist.ArtistID", "artist.Name", "invoice_line.Quantity")

            # Define a window specification partitioned by ArtistID and CustomerID
            customer_artist_window = Window.partitionBy("artist.ArtistID", "invoice.CustomerID")

            # Calculate purchase count per customer per artist using window function
            sales_data = sales_data.withColumn("PurchaseCount", F.count("invoice.InvoiceID").over(customer_artist_window))
            # Filter for customers who have purchased more than once
            repeat_customers_df = sales_data.filter(F.col("PurchaseCount") > 1).select("ArtistID", "Name", "CustomerID", "PurchaseCount").distinct()
            
            # Define a window specification partitioned by ArtistID
            artist_window = Window.partitionBy("artist.ArtistID")
            
            # Calculate the count of repeat customers per artist
            repeat_customer_count = repeat_customers_df.withColumn("RepeatCustomerCount", F.count("CustomerID").over(artist_window))
            
            # Select final columns
            final_data = repeat_customer_count.select("ArtistID", "Name", "CustomerID", "PurchaseCount", "RepeatCustomerCount")
            
            # Add metadata columns
            current_time = datetime.now()
            user_name = "DailyETL:Yehudit"

            # LOAD (Delete and insert new data to SQLite)
            # --------------------------------------------
            final_data_df = final_data.toPandas()

            # Create a temporary table with the new data
            final_data_df.to_sql('temp_artist_repeat_customer', conn, if_exists='replace', index=False)

            # Delete existing records that are in the new dataset
            delete_query = """
            DELETE FROM artist_repeat_customer_analysis
            WHERE ArtistID IN (SELECT ArtistID FROM temp_artist_repeat_customer)
            """
            cursor.execute(delete_query)

            # Insert all records from the temporary table
            insert_query = """
            INSERT INTO artist_repeat_customer_analysis
            (ArtistID, Name, CustomerID, PurchaseCount, RepeatCustomerCount, created_at, updated_at, updated_by)
            SELECT temp.ArtistID, temp.Name, temp.CustomerID, temp.PurchaseCount, temp.RepeatCustomerCount,
                 COALESCE(main.created_at, ?), ?, ?
            FROM temp_artist_repeat_customer temp
            LEFT JOIN artist_repeat_customer_analysis main
            ON temp.ArtistID = main.ArtistID AND temp.CustomerID = main.CustomerID
            """
            cursor.execute(insert_query, (current_time, current_time, user_name))

            # Remove the temporary table
            cursor.execute("DROP TABLE temp_artist_repeat_customer")

            # Commit the changes to the database
            conn.commit()
            
        finally:
            # Step 4: Close the SQLite connection and stop Spark session
            conn.close()
            spark.stop()

if __name__ == "__main__":
        incremental_load()