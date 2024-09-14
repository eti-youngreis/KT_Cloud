import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_ETL_revenue_customer_genre():
    try:
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("Revenue per Customer and Genre") \
            .getOrCreate()

        # Step 2: Establish SQLite connection using sqlite3
        conn = sqlite3.connect('D:\\b\\CustomerRevenueETL.db')
        cursor = conn.cursor()

        try:
            # EXTRACT (Loading CSVs from local storage)
            print("Loading CSV files...")
            customers_df = spark.read.csv("D:\\csvFiles\\Customer.csv", header=True, inferSchema=True)
            invoices_df = spark.read.csv("D:\\csvFiles\\Invoice.csv", header=True, inferSchema=True)
            invoice_lines_df = spark.read.csv("D:\\csvFiles\\InvoiceLine.csv", header=True, inferSchema=True)
            tracks_df = spark.read.csv("D:\\csvFiles\\Track.csv", header=True, inferSchema=True)
            genres_df = spark.read.csv("D:\\csvFiles\\Genre.csv", header=True, inferSchema=True)

            # TRANSFORM (Apply joins, groupings, and window functions)
            print("Transforming data...")

            # Join customer data with invoice lines and genres
            joined_df = customers_df.alias('c').join(
                invoices_df.alias('i'), F.col('c.CustomerId') == F.col('i.CustomerId'), "inner"
            ).join(
                invoice_lines_df.alias('il'), F.col('i.InvoiceId') == F.col('il.InvoiceId'), "inner"
            ).join(
                tracks_df.alias('t'), F.col('il.TrackId') == F.col('t.TrackId'), "inner"
            ).join(
                genres_df.alias('g'), F.col('t.GenreId') == F.col('g.GenreId'), "inner"
            )

            # Group by customer and create columns for each genre
            customer_genre_revenue_df = joined_df.groupBy(
                F.col("c.CustomerId"), F.col("c.FirstName"), F.col("c.LastName")
            ).agg(
                F.sum(F.when(F.col("g.Name") == "Rock", F.col("il.UnitPrice")).otherwise(0)).alias("RockRevenue"),
                F.sum(F.when(F.col("g.Name") == "Metal", F.col("il.UnitPrice")).otherwise(0)).alias("MetalRevenue"),
                F.sum(F.when(F.col("g.Name") == "Latin", F.col("il.UnitPrice")).otherwise(0)).alias("LatinRevenue"),
                F.sum(F.when(F.col("g.Name") == "Reggae", F.col("il.UnitPrice")).otherwise(0)).alias("ReggaeRevenue"),
                F.sum(F.when(F.col("g.Name") == "Pop", F.col("il.UnitPrice")).otherwise(0)).alias("PopRevenue")
            )

            # Add created_at, updated_at, and updated_by columns
            customer_genre_revenue_df = customer_genre_revenue_df.withColumn(
                "created_at", F.current_timestamp()
            ).withColumn(
                "updated_at", F.current_timestamp()
            ).withColumn(
                "updated_by", F.lit("process:user_name")
            )

            # Convert to Pandas DataFrame for SQLite insertion
            final_revenue_pd = customer_genre_revenue_df.toPandas()

            # Drop the old table if exists
            cursor.execute('''DROP TABLE IF EXISTS customer_genre_revenue''')

            # LOAD (Create the table with the necessary columns, including created_at, updated_at, updated_by)
            print("Loading data into SQLite...")

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

            cursor.execute('''DROP TABLE IF EXISTS customer_genre_revenue''')
            # Insert the transformed data into SQLite
            final_revenue_pd.to_sql('customer_genre_revenue', conn, if_exists='replace', index=False)

            conn.commit()

            # SELECT query to retrieve data
            print("Executing SELECT query...")
            cursor.execute("SELECT * FROM customer_genre_revenue order by CustomerId LIMIT 10")
            rows = cursor.fetchall()
            for row in rows:
                print(row)

        except Exception as e:
            print(f"Error during ETL process: {e}")

        finally:
            # Step 3: Close the SQLite connection and stop Spark session
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"Error initializing Spark or SQLite: {e}")

load_ETL_revenue_customer_genre()