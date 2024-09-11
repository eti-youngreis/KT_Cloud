from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3  # Using SQLite for database operations
from datetime import datetime
import os

BASE_URL = "C:/Users/jimmy/Desktop/תמר לימודים  יד/bootcamp/db_files"

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Full ETL with SQLite") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect(os.path.join(BASE_URL, "employee_genre_sales.db"))

    try:
        # EXTRACT: Load CSV files from local storage into Spark DataFrames
        employees = spark.read.csv(os.path.join(BASE_URL, "Employee.csv"), header=True, inferSchema=True)
        invoices = spark.read.csv(os.path.join(BASE_URL, "Invoice.csv"), header=True, inferSchema=True)
        invoice_lines = spark.read.csv(os.path.join(BASE_URL, "InvoiceLine.csv"), header=True, inferSchema=True)
        tracks = spark.read.csv(os.path.join(BASE_URL, "Track.csv"), header=True, inferSchema=True)
        genres = spark.read.csv(os.path.join(BASE_URL, "Genre.csv"), header=True, inferSchema=True)
        customers = spark.read.csv(os.path.join(BASE_URL, "Customer.csv"), header=True, inferSchema=True)

        # TRANSFORM: Apply joins, groupings, and window functions
        joined_data = employees.alias('e') \
            .join(customers.alias('c'), F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'left') \
            .join(invoices.alias('i'), F.col('c.CustomerId') == F.col('i.CustomerId'), 'left') \
            .join(invoice_lines.alias('il'), F.col('i.InvoiceId') == F.col('il.InvoiceId'), 'left') \
            .join(tracks.alias('t'), F.col('il.TrackId') == F.col('t.TrackId'), 'left') \
            .join(genres.alias('g'), F.col('t.GenreId') == F.col('g.GenreId'), 'left')

        # Aggregate data (e.g., total sales and average sales price per genre)
        aggregated_data = joined_data.groupBy('e.EmployeeId', 'e.FirstName', 'e.LastName', 'g.Name') \
            .agg(
                F.sum(F.col('il.UnitPrice') * F.col('il.Quantity')).alias('Total_Sales'),
                F.avg(F.col('il.UnitPrice')).alias('Average_Sales_Price')
            ).orderBy('e.EmployeeId','g.Name')

        # Add metadata columns
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = aggregated_data \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Tamar Gavrielov'))
        transformed_data.show()
        # Apply window function (if necessary, e.g., ranking or partitioning)
        window_spec = Window.partitionBy().orderBy(F.desc("Total_Sales"))
        final_data = transformed_data.withColumn("rank", F.rank().over(window_spec))
        # LOAD: Overwrite the SQLite table with the new transformed data
        final_data_df = final_data.toPandas()

        # Insert transformed data into the SQLite database (overwrite mode)
        final_data_df.to_sql('employee_genre_sales_elt', conn, if_exists='replace', index=False)

        # Commit the changes to the database
        conn.commit()

    finally:
        # Step 4: Close the SQLite connection and stop the Spark session
        conn.close()  # Close SQLite connection
        spark.stop()  # Stop the Spark session


if __name__ == "__main__":
    load()
