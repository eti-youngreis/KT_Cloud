from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import pandas

def load_incremental_ETL():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Revenue per Customer and Genre ETL incremental") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('D:\\b\\CustomerRevenueETL.db')
    cursor = conn.cursor()

    try:
        # Step 3: Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_genre_revenue'")
        if cursor.fetchone() is None:
            # Create the table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_genre_revenue (
                CustomerId INTEGER,
                FirstName TEXT,
                LastName TEXT,
                RockRevenue REAL,
                MetalRevenue REAL,
                LatinRevenue REAL,
                ReggaeRevenue REAL,
                PopRevenue REAL,
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
            ''')
            print("Table 'customer_genre_revenue' created.")
        
        # Get the latest processed timestamp from the target table
        cursor.execute("SELECT MAX(updated_at) FROM customer_genre_revenue")
        latest_timestamp = cursor.fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially

                # EXTRACT: פילטור אינקרמנטלי לכל הטבלאות
        customers_df = spark.read.csv("D:\\csvFiles\\Customer_with_created_at.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("D:\\csvFiles\\Invoice_with_created_at.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("D:\\csvFiles\\InvoiceLine_with_created_at.csv", header=True, inferSchema=True)
        tracks_df = spark.read.csv("D:\\csvFiles\\Track_with_created_at.csv", header=True, inferSchema=True)
        genres_df = spark.read.csv("D:\\csvFiles\\Genre_with_created_at.csv", header=True, inferSchema=True)

        # Perform FULL OUTER JOIN to include all records
        joined_df = customers_df.alias("c") \
            .join(invoices_df.alias("i"), F.col("c.CustomerId") == F.col("i.CustomerId"), "outer") \
            .join(invoice_lines_df.alias("il"), F.col("i.InvoiceId") == F.col("il.InvoiceId"), "outer") \
            .join(tracks_df.alias("t"), F.col("il.TrackId") == F.col("t.TrackId"), "outer") \
            .join(genres_df.alias("g"), F.col("t.GenreId") == F.col("g.GenreId"), "outer")

        # Filter records that have been updated since the latest timestamp
        updated_records_df = joined_df.filter(
            (F.col("c.updated_at") > latest_timestamp) |
            (F.col("i.updated_at") > latest_timestamp) |
            (F.col("il.updated_at") > latest_timestamp) |
            (F.col("t.updated_at") > latest_timestamp) |
            (F.col("g.updated_at") > latest_timestamp)
        )

        # Aggregation - Calculate the revenue for each genre with explicit references to the genre name column
        transformed_data = updated_records_df.groupBy(
            F.col("c.CustomerId"), F.col("c.FirstName"), F.col("c.LastName")
        ).agg(
            F.sum(F.when(F.col("g.Name") == "Rock", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("RockRevenue"),
            F.sum(F.when(F.col("g.Name") == "Metal", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("MetalRevenue"),
            F.sum(F.when(F.col("g.Name") == "Latin", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("LatinRevenue"),
            F.sum(F.when(F.col("g.Name") == "Reggae", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("ReggaeRevenue"),
            F.sum(F.when(F.col("g.Name") == "Pop", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("PopRevenue")
        )

        # Add metadata columns
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        transformed_data = transformed_data.withColumn("created_at", F.lit(current_time)) \
                                            .withColumn("updated_at", F.lit(current_time)) \
                                            .withColumn("updated_by", F.lit("process:user_name"))
        
        # Convert to Pandas DataFrame for SQLite insertion
        print(f"Total transformed records: {transformed_data.count()}")
        final_df = transformed_data.toPandas()
        
        if not final_df.empty:
                
            final_df.to_sql('temp_table', conn, if_exists='replace', index=False)

            delete_query = '''DELETE FROM customer_genre_revenue WHERE CustomerId IN (SELECT CustomerId FROM temp_table)'''
            cursor.execute(delete_query)

            insert_query = '''INSERT INTO customer_genre_revenue SELECT * FROM temp_table '''
            cursor.execute(insert_query)

            conn.commit()

            # Delete temp_table
            cursor.execute('DROP TABLE IF EXISTS temp_table')
            
        # SELECT query to retrieve data
        print("Executing SELECT query...")
        cursor.execute("SELECT CustomerId, created_at, updated_at FROM customer_genre_revenue ORDER BY CAST(CustomerId AS INTEGER)")
        rows = cursor.fetchall()

        for row in rows:
            print(row)

    
    finally:
        # Step 6: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

def add_created_at_column(file_path):
    # Step 1: Read the CSV file
    df = pandas.read_csv(file_path)

    # Step 2: Add created_at column
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # df.loc['created_at'] = current_time
    df.loc[:10,'updated_at'] = current_time

    # Step 3: Save the updated CSV file
    df.to_csv(file_path, index=False)

    print(f"Created_at column added to {file_path}")

def main():
    # Add created_at column to CSV files
    # add_created_at_column("D:\\csvFiles\\Customer_with_created_at.csv")
    # add_created_at_column("D:\\csvFiles\\Invoice_with_created_at.csv")
    # add_created_at_column("D:\\csvFiles\\InvoiceLine_with_created_at.csv")
    add_created_at_column("D:\\csvFiles\\Track_with_created_at.csv")
    # add_created_at_column("D:\\csvFiles\\Genre_with_created_at.csv")
    load_incremental_ETL()

if __name__ == "__main__":
    main()
