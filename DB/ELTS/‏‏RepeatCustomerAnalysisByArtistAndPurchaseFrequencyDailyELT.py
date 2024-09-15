from pyspark.sql import SparkSession
import sqlite3
from datetime import datetime
import os

def load():
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("Artist Repeat Customer Analysis ELT") \
            .getOrCreate()

        # Step 2: Establish SQLite connection using sqlite3
        db_path = r"KT_Cloud\DB\ELTS\dbs\artist_repeat_customer_analysis_elt.db"

        if not os.path.exists(os.path.dirname(db_path)):
            os.makedirs(os.path.dirname(db_path))

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            # Get the latest processed timestamp from the target table
            latest_timestamp_query = "SELECT MAX(updated_at) FROM artist_repeat_customer_analysis"
            cursor.execute(latest_timestamp_query)
            latest_timestamp = cursor.fetchone()[0]

            # Handle case where no data exists yet (initial load)
            if latest_timestamp is None:
                latest_timestamp = '1900-01-01 00:00:00'  # Default for initial load

            # EXTRACT (Loading CSVs from local storage)
            # -----------------------------------------------
            invoices = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Invoice_with_created_at.csv", header=True, inferSchema=True)
            invoice_lines = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\InvoiceLine_with_created_at.csv", header=True, inferSchema=True)
            tracks = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Track_with_created_at.csv", header=True, inferSchema=True)
            albums = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Album_with_created_at.csv", header=True, inferSchema=True)
            artists = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Artist_with_created_at.csv", header=True, inferSchema=True)
            customers = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Customer_with_created_at.csv", header=True, inferSchema=True)

            # LOAD (Save raw data into SQLite using sqlite3)
            # ----------------------------------------------------
            invoices.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="invoices_elt", con=conn, if_exists='replace', index=False)
            invoice_lines.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="invoice_lines_elt", con=conn, if_exists='replace', index=False)
            tracks.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="tracks_elt", con=conn, if_exists='replace', index=False)
            albums.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="albums_elt", con=conn, if_exists='replace', index=False)
            artists.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="artists_elt", con=conn, if_exists='replace', index=False)
            customers.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="customers_elt", con=conn, if_exists='replace', index=False)

            conn.commit()

            # TRANSFORM (Use SQL queries to transform the data)
            # --------------------------------------------------------
            query = """
            CREATE TABLE TEMP_CTE AS
            WITH sales_data AS (
                SELECT inv.InvoiceID, inv.CustomerID, ar.ArtistID, ar.Name, il.Quantity
                FROM invoices_elt inv
                JOIN invoice_lines_elt il ON inv.InvoiceID = il.InvoiceID
                JOIN tracks_elt tr ON il.TrackID = tr.TrackID
                JOIN albums_elt al ON tr.AlbumID = al.AlbumID
                JOIN artists_elt ar ON al.ArtistID = ar.ArtistID
                WHERE (inv.updated_at > ? OR ar.updated_at > ? OR cust.updated_at > ?)
            ),
            customer_artist_purchases AS (
                SELECT ArtistID, CustomerID, Name, COUNT(InvoiceID) AS PurchaseCount
                FROM sales_data
                GROUP BY ArtistID, CustomerID
                HAVING PurchaseCount > 1
            ),
            repeat_customer_analysis AS (
                SELECT ArtistID, Name, CustomerID, PurchaseCount,
                       COUNT(CustomerID) OVER (PARTITION BY ArtistID) AS RepeatCustomerCount
                FROM customer_artist_purchases
            ),
            CREATED_AT_FROM_TARGET_TABLE AS (
                SELECT ArtistID, CustomerID, created_at
                FROM artist_repeat_customer_analysis
            )
            SELECT
                rca.ArtistID, rca.Name, rca.CustomerID, rca.PurchaseCount, rca.RepeatCustomerCount,
                IFNULL(CAFTT.created_at, datetime('now')) AS created_at,
                datetime('now') AS updated_at,
                'DailyELT:Yehudit' AS updated_by
            FROM repeat_customer_analysis rca
            LEFT JOIN CREATED_AT_FROM_TARGET_TABLE CAFTT
            ON rca.ArtistID = CAFTT.ArtistID AND rca.CustomerID = CAFTT.CustomerID
            """

            cursor.execute(query, (latest_timestamp, latest_timestamp, latest_timestamp))

            delete_query = """
            DELETE FROM artist_repeat_customer_analysis
            WHERE (ArtistID, CustomerID) IN (SELECT ArtistID, CustomerID FROM TEMP_CTE)
            """
            cursor.execute(delete_query)

            insert_query = """
            INSERT INTO artist_repeat_customer_analysis 
            SELECT * FROM TEMP_CTE
            """
            cursor.execute(insert_query)

            conn.commit()

        finally:
            # Step 3: Close the SQLite connection and stop Spark session
            conn.close()
            spark.stop()

if __name__ == "__main__":
        load()