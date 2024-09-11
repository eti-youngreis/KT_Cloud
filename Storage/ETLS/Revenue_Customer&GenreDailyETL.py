from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime
import pandas as pd

def load_incremental_ETL():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Revenue per Customer and Genre ETL incremental") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('D:\\b\\CustomerRevenueETL.db')
    cursor = conn.cursor()

    try:
        # Step 3: Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_genre_revenue'")
        if cursor.fetchone() is None:
            cursor.execute('''
            CREATE TABLE customer_genre_revenue (
                CustomerId INTEGER PRIMARY KEY,
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
        
        # Step 4: Get the latest processed timestamp
        cursor.execute("SELECT MAX(updated_at) FROM customer_genre_revenue")
        latest_timestamp = cursor.fetchone()[0] or '1900-01-01 00:00:00'

        # EXTRACT: Load only the new or updated records
        def load_df(file_path):
            return spark.read.csv(file_path, header=True, inferSchema=True)

        # Read CSV files
        customers_df = load_df("D:\\csvFiles\\Customer_with_created_at.csv")
        invoices_df = load_df("D:\\csvFiles\\Invoice_with_created_at.csv")
        invoice_lines_df = load_df("D:\\csvFiles\\InvoiceLine_with_created_at.csv")
        tracks_df = load_df("D:\\csvFiles\\Track_with_created_at.csv")
        genres_df = load_df("D:\\csvFiles\\Genre_with_created_at.csv")

        # Filter new or updated records
        def filter_df(df, timestamp_col):
            return df.filter(F.col(timestamp_col) > latest_timestamp)

        new_customers_df = filter_df(customers_df, "updated_at")
        new_invoices_df = filter_df(invoices_df, "updated_at")
        new_invoice_lines_df = filter_df(invoice_lines_df, "updated_at")
        new_tracks_df = filter_df(tracks_df, "updated_at")
        new_genres_df = filter_df(genres_df, "updated_at")

        # Join and transform data
        joined_df = new_customers_df.alias("c") \
            .join(new_invoices_df.alias("i"), F.col("c.CustomerId") == F.col("i.CustomerId")) \
            .join(new_invoice_lines_df.alias("il"), F.col("i.InvoiceId") == F.col("il.InvoiceId")) \
            .join(new_tracks_df.alias("t"), F.col("il.TrackId") == F.col("t.TrackId")) \
            .join(new_genres_df.alias("g"), F.col("t.GenreId") == F.col("g.GenreId"))

        transformed_data = joined_df.groupBy(
            F.col("c.CustomerId"), F.col("c.FirstName"), F.col("c.LastName")
        ).agg(
            F.sum(F.when(F.col("g.Name") == "Rock", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("RockRevenue"),
            F.sum(F.when(F.col("g.Name") == "Metal", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("MetalRevenue"),
            F.sum(F.when(F.col("g.Name") == "Latin", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("LatinRevenue"),
            F.sum(F.when(F.col("g.Name") == "Reggae", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("ReggaeRevenue"),
            F.sum(F.when(F.col("g.Name") == "Pop", F.col("il.UnitPrice") * F.col("il.Quantity")).otherwise(0)).alias("PopRevenue")
        )
        
        # Convert to Pandas DataFrame for SQLite insertion
        print(f"Total transformed records: {transformed_data.count()}")
        print(transformed_data.toPandas())
        final_df = transformed_data.toPandas()

        # Step 5: Update or insert records
        if not final_df.empty:
           for index, row in final_df.iterrows():
            cursor.execute("SELECT created_at FROM customer_genre_revenue WHERE CustomerId = ?", (row.CustomerId,))
            existing_record = cursor.fetchone()

            if existing_record:
                cursor.execute('''
                    UPDATE customer_genre_revenue
                    SET RockRevenue = ?, MetalRevenue = ?, LatinRevenue = ?, ReggaeRevenue = ?, PopRevenue = ?, updated_at = ?, updated_by = ?
                    WHERE CustomerId = ?
                ''', (row.RockRevenue, row.MetalRevenue, row.LatinRevenue, row.ReggaeRevenue, row.PopRevenue, row.updated_at, row.updated_by, row.CustomerId))
            else:
                # For new records, insert the current created_at
                cursor.execute('''
                    INSERT INTO customer_genre_revenue (CustomerId, FirstName, LastName, RockRevenue, MetalRevenue, LatinRevenue, ReggaeRevenue, PopRevenue, created_at, updated_at, updated_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row.CustomerId, row.FirstName, row.LastName, row.RockRevenue, row.MetalRevenue, row.LatinRevenue, row.ReggaeRevenue, row.PopRevenue, row.created_at, row.updated_at, row.updated_by))

            conn.commit()

        # SELECT query to verify results
        print("Executing SELECT query...")
        cursor.execute("SELECT * FROM customer_genre_revenue ORDER BY CustomerId LIMIT 10")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    finally:
        conn.close()
        spark.stop()

def add_created_at_column(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Add created_at column
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.loc['updated_at'] = current_time

    # Save the updated CSV file
    df.to_csv(file_path, index=False)
    print(f"Created_at column added to {file_path}")

def main():
    # Add created_at column to CSV files
    csv_files = [
        "D:\\csvFiles\\Customer_with_created_at.csv",
        "D:\\csvFiles\\Invoice_with_created_at.csv",
        "D:\\csvFiles\\InvoiceLine_with_created_at.csv",
        "D:\\csvFiles\\Track_with_created_at.csv",
        "D:\\csvFiles\\Genre_with_created_at.csv"
    ]
    for file_path in csv_files:
        add_created_at_column(file_path)
    
    # Load incremental ETL
    load_incremental_ETL()

if __name__ == "__main__":
    main()
