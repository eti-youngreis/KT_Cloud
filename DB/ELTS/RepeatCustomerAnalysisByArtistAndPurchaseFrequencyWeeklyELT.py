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

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        invoices = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\InvoiceLine.csv", header=True, inferSchema=True)
        tracks = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Track.csv", header=True, inferSchema=True)
        albums = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Album.csv", header=True, inferSchema=True)
        artists = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Artist.csv", header=True, inferSchema=True)
        customers = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Customer.csv", header=True, inferSchema=True)

        # LOAD (Save raw data into SQLite using sqlite3)
        # ----------------------------------------------------
        invoices.toPandas().to_sql(name="invoices_elt", con=conn, if_exists='replace', index=False)
        invoice_lines.toPandas().to_sql(name="invoice_lines_elt", con=conn, if_exists='replace', index=False)
        tracks.toPandas().to_sql(name="tracks_elt", con=conn, if_exists='replace', index=False)
        albums.toPandas().to_sql(name="albums_elt", con=conn, if_exists='replace', index=False)
        artists.toPandas().to_sql(name="artists_elt", con=conn, if_exists='replace', index=False)
        customers.toPandas().to_sql(name="customers_elt", con=conn, if_exists='replace', index=False)

        conn.commit()

        # TRANSFORM (Use SQL queries to transform the data)
        # --------------------------------------------------------
        
        # 1. Join the necessary tables: invoices, invoice_lines, tracks, albums, and artists
        query_sales_data = """
        SELECT inv.InvoiceID, inv.CustomerID, ar.ArtistID, ar.Name, il.Quantity
        FROM invoices_elt inv
        JOIN invoice_lines_elt il ON inv.InvoiceID = il.InvoiceID
        JOIN tracks_elt tr ON il.TrackID = tr.TrackID
        JOIN albums_elt al ON tr.AlbumID = al.AlbumID
        JOIN artists_elt ar ON al.ArtistID = ar.ArtistID
        """
        sales_data = conn.execute(query_sales_data).fetchall()

        # 2. Create a temporary table for sales data
        conn.execute("DROP TABLE IF EXISTS sales_data")
        conn.execute("""
        CREATE TABLE sales_data (
            InvoiceID INTEGER,
            CustomerID INTEGER,
            ArtistID INTEGER,
            Name TEXT,
            Quantity INTEGER
        )
        """)

        # Insert the joined data into the sales_data table
        conn.executemany("""
        INSERT INTO sales_data (InvoiceID, CustomerID, ArtistID, Name, Quantity)
        VALUES (?, ?, ?, ?, ?)
        """, sales_data)

        conn.commit()

        # 3. Calculate purchase count per customer per artist
        query_purchase_count = """
        CREATE VIEW IF NOT EXISTS customer_artist_purchases AS
        SELECT ArtistID, CustomerID, Name, COUNT(InvoiceID) AS PurchaseCount
        FROM sales_data
        GROUP BY ArtistID, CustomerID
        HAVING PurchaseCount > 1
        """
        conn.execute(query_purchase_count)

        # 4. Calculate the count of repeat customers per artist
        query_repeat_customers = """
        CREATE VIEW IF NOT EXISTS repeat_customer_analysis AS
        SELECT ArtistID, Name, CustomerID, PurchaseCount,
               COUNT(CustomerID) OVER (PARTITION BY ArtistID) AS RepeatCustomerCount
        FROM customer_artist_purchases
        """
        conn.execute(query_repeat_customers)

        # 5. Add metadata columns (created_at, updated_at, updated_by)
        current_time = datetime.now()
        user_name = "DailyELT:Yehudit"

        # Select the final data with metadata
        final_query = f"""
        SELECT ArtistID, Name, CustomerID, PurchaseCount, RepeatCustomerCount,
               '{current_time}' AS created_at,
               '{current_time}' AS updated_at,
               '{user_name}' AS updated_by
        FROM repeat_customer_analysis
        """
        final_data = conn.execute(final_query).fetchall()

        # Create the final table
        conn.execute("DROP TABLE IF EXISTS artist_repeat_customer_analysis")
        conn.execute("""
        CREATE TABLE artist_repeat_customer_analysis (
            ArtistID INTEGER,
            Name TEXT,
            CustomerID INTEGER,
            PurchaseCount INTEGER,
            RepeatCustomerCount INTEGER,
            created_at TEXT,
            updated_at TEXT,
            updated_by TEXT
        )
        """)

        # Insert the final data into the artist_repeat_customer_analysis table
        conn.executemany("""
        INSERT INTO artist_repeat_customer_analysis (ArtistID, Name, CustomerID, PurchaseCount, RepeatCustomerCount, created_at, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, final_data)

        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load()
