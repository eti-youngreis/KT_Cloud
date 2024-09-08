import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def load():
    try:
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("Revenue per Customer and Genre") \
            .getOrCreate()

        # Step 2: Establish SQLite connection using sqlite3
        conn = sqlite3.connect('D:\\b\\CustomerRevenue.db')
        cursor = conn.cursor()

        try:
            # EXTRACT (Loading CSVs from local storage)
            print("Loading CSV files...")
            customers_df = spark.read.csv("D:\\csv_files\\Customer.csv", header=True, inferSchema=True)
            invoices_df = spark.read.csv("D:\\csv_files\\Invoice.csv", header=True, inferSchema=True)
            invoice_lines_df = spark.read.csv("D:\\csv_files\\InvoiceLine.csv", header=True, inferSchema=True)
            tracks_df = spark.read.csv("D:\\csv_files\\Track.csv", header=True, inferSchema=True)
            genres_df = spark.read.csv("D:\\csv_files\\Genre.csv", header=True, inferSchema=True)

            # TRANSFORM (Apply joins, groupings, and window functions)
            print("Transforming data...")

            # Join customer data with invoice lines and genres
            joined_df = customers_df.join(
                invoices_df, customers_df["CustomerId"] == invoices_df["CustomerId"], "inner"
            ).join(
                invoice_lines_df, invoices_df["InvoiceId"] == invoice_lines_df["InvoiceId"], "inner"
            ).join(
                tracks_df, invoice_lines_df["TrackId"] == tracks_df["TrackId"], "inner"
            ).join(
                genres_df, tracks_df["GenreId"] == genres_df["GenreId"], "inner"
            )

            # Group by customer, genre, and invoice date
            customer_genre_revenue_df = joined_df.groupBy(
                "CustomerId", "FirstName", "LastName", "GenreId", "Name", "InvoiceDate"
            ).agg(
                F.sum("UnitPrice").alias("TotalRevenue")
            )

            # Define window for calculating rolling revenue over time
            window_spec = Window.partitionBy("CustomerId", "GenreId").orderBy("InvoiceDate").rowsBetween(-1, 0)

            # Calculate rolling revenue over time
            rolling_revenue_df = customer_genre_revenue_df.withColumn(
                "RollingRevenue", F.sum("TotalRevenue").over(window_spec)
            ).withColumn(
                "created_at", F.current_timestamp()
            ).withColumn(
                "updated_at", F.current_timestamp()
            ).withColumn(
                "updated_by", F.lit("process:user_name")
            )

            # Convert to Pandas DataFrame for SQLite insertion
            final_revenue_pd = rolling_revenue_df.toPandas()

            # LOAD (Save transformed data into SQLite)
            print("Loading data into SQLite...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customer_genre_revenue (
                    CustomerId INTEGER,
                    FirstName TEXT,
                    LastName TEXT,
                    GenreId INTEGER,
                    GenreName TEXT,
                    InvoiceDate TEXT,
                    TotalRevenue REAL,
                    RollingRevenue REAL,
                    created_at TEXT,
                    updated_at TEXT,
                    updated_by TEXT
                )
            ''')

            final_revenue_pd.to_sql('customer_genre_revenue', conn, if_exists='replace', index=False)

            conn.commit()

            # SELECT query to retrieve data
            print("Executing SELECT query...")
            cursor.execute("SELECT * FROM customer_genre_revenue LIMIT 10")
            rows = cursor.fetchall()

            for row in rows:
                print(row)

        except Exception as e:
            print(f"Error during ETL process: {e}")

        finally:
            # Step 3: Close the SQLite connection and stop Spark session
            cursor.close()
            conn.close()
            spark.stop()

    except Exception as e:
        print(f"Error initializing Spark or SQLite: {e}")

load()
