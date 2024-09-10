from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/etl_db.db')

    try:
        cursor = conn.cursor()
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        customers = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/InvoiceLine.csv", header=True, inferSchema=True)
        tracks = spark.read.csv("KT_Cloud\Storage\ETLS\csv files\Track.csv", header=True, inferSchema=True)
        artists = spark.read.csv("KT_Cloud\Storage\ETLS\csv files\Artist.csv", header=True, inferSchema=True)
        albums = spark.read.csv("KT_Cloud\Storage\ETLS\csv files\Album.csv", header=True, inferSchema=True)
        # Load other related tables

        # LOAD (Save the raw data into SQLite using KT_DB without transformation)
        # -----------------------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        invoices.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)
        invoice_lines.toPandas().to_sql('invoice_lines', conn, if_exists='replace', index=False)
        tracks.toPandas().to_sql('tracks', conn, if_exists='replace', index=False)
        albums.toPandas().to_sql('albums', conn, if_exists='replace', index=False)
        artists.toPandas().to_sql('artists', conn, if_exists='replace', index=False)
        customers.toPandas().to_sql('customers', conn, if_exists='replace', index=False)



        # TRANSFORM (Perform transformations with SQL queries using KT_DB functions)
        # -------------------------------------------------------------------------
        # Example Transformation 1: Join tables and calculate total spend and average
        query_sales_data = """
            SELECT inv.CustomerId, inv.InvoiceId, il.Quantity, tr.TrackId, art.Name
            FROM invoices inv
            JOIN invoice_lines il ON inv.InvoiceId = il.InvoiceId
            JOIN tracks tr ON il.TrackId = tr.TrackId
            JOIN albums al ON tr.AlbumId = al.AlbumId
            JOIN artists art ON al.ArtistId = art.ArtistId;
            """

        sales = cursor.execute(query_sales_data).fetchall()
        cursor.execute('''CREATE TABLE IF NOT EXISTS sales_data (
                    InvoiceID INTEGER,
                    CustomerID INTEGER,
                    ArtistID INTEGER,
                    Name TEXT,
                    Quantity INTEGER)'''
        )
        
        # Insert data to 'sales_data' table
        conn.executemany("""
            INSERT INTO sales_data (InvoiceID, CustomerID, ArtistID, Name, Quantity)
            VALUES (?, ?, ?, ?, ?)
            """, sales)
        # Execute the transformation query using KT_DB's execute() method

        repeat_customer_query = """
                CREATE VIEW IF NOT EXISTS repeat_customer_analysis AS
                SELECT ArtistID, Name, CustomerID, PurchaseCount,
                COUNT(CustomerID) OVER (PARTITION BY ArtistID) AS RepeatCustomerCount
                FROM (
                    SELECT ArtistID, CustomerID, Name, COUNT(InvoiceID) AS PurchaseCount
                    FROM sales_data
                    GROUP BY ArtistID, CustomerID
                    HAVING COUNT(InvoiceID) > 1
                ) AS customer_artist_purchases;
                """
        cursor.execute(repeat_customer_query)
        
        time = datetime.now()
        user_name = "Efrat"
        query = f"""
            SELECT ArtistID, Name, CustomerID, PurchaseCount, RepeatCustomerCount,
            '{time}' AS created_at,
            '{time}' AS updated_at,
            '{user_name}' AS updated_by
            FROM repeat_customer_analysis
            """

        final_data = conn.execute(query).fetchall()

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

        
        
        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()
        
if __name__ == "__main__":
    load()
