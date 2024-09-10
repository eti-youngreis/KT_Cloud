# .עובדת - ETL שאילתה 2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime, timedelta

def load():
    # Step 1: Initialize Spark session
    # Create a Spark session to enable Spark SQL functionality
    spark = SparkSession.builder \
        .appName("Sales Trends Analysis") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    # Connect to SQLite database (or create it if it does not exist)
    conn = sqlite3.connect("sales_trends.db")

    # Define the path to the CSV files
    path = "D:\\Users\\גילי\\Documents\\בוטקמפ\\csv files\\"
    
    try:
        # EXTRACT: Load CSV files from local storage into Spark DataFrames
        # Read CSV files into DataFrames with schema inference and header row
        employees_df = spark.read.csv(path + "Employee.csv", header=True, inferSchema=True)
        customers_df = spark.read.csv(path + "Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv(path + "Invoice.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(path + "InvoiceLine.csv", header=True, inferSchema=True)
        tracks_df = spark.read.csv(path + "Track.csv", header=True, inferSchema=True)
        genres_df = spark.read.csv(path + "Genre.csv", header=True, inferSchema=True)

        # TRANSFORM: Perform data transformations and aggregations
        # Set the date range for the analysis (last 180 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        
        # Join the tables to create the sales_data DataFrame
        sales_data = (
            invoices_df.alias("i")
            .join(invoice_lines_df.alias("il"), "InvoiceId")
            .join(tracks_df.alias("t"), "TrackId")
            .join(genres_df.alias("g"), "GenreId")
            .join(customers_df.alias("c"), "CustomerId")
            .join(employees_df.alias("e"), employees_df["EmployeeId"] == customers_df["SupportRepId"])
            .select(
                F.col("e.EmployeeId"),
                F.col("e.FirstName"),
                F.col("e.LastName"),
                F.col("g.GenreId"),
                F.col("g.Name").alias("GenreName"),
                F.col("il.UnitPrice"),
                F.col("il.Quantity"),
                F.col("i.InvoiceDate"),
                F.col("i.InvoiceId")
            )
            .filter((F.col("i.InvoiceDate") >= start_date) & (F.col("i.InvoiceDate") <= end_date))
        )

        # Define the window specification by EmployeeId, GenreId and ordered by InvoiceDate
        window_spec = Window.partitionBy("EmployeeId", "GenreId").orderBy("InvoiceDate")

        # Group by EmployeeId, GenreId and InvoiceDate to get total sales
        sales_data_grouped = sales_data.groupBy("EmployeeId", "GenreId", "InvoiceDate").agg(
            F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("Total_Sales")
        )

        # Calculate the rolling sales totals using the window specification
        sales_trends = sales_data_grouped.withColumn("Rolling_Sales", F.sum("Total_Sales").over(window_spec))

        # Add metadata columns to the DataFrame
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sales_trends = sales_trends \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Gili Bolak'))

        # Select the final columns for output
        final_sales_trends = sales_trends.select(
            'EmployeeId',
            'GenreId',
            'InvoiceDate',
            'Total_Sales',
            'Rolling_Sales',
            'created_at',
            'updated_at',
            'updated_by'
        )

        # Load data into SQLite database
        # Convert Spark DataFrame to Pandas DataFrame and write to SQLite table
        final_sales_trends_df = final_sales_trends.toPandas()
        final_sales_trends_df.to_sql('sales_trends', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Clean up resources
        # Close the SQLite connection and stop the Spark session
        conn.close()  # Ensure connection is properly closed
        spark.stop()  # Stop the Spark session
