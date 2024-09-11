from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sqlite3
import pandas as pd
import os
from datetime import datetime, timedelta


BASE_URL = "C:/Users/jimmy/Desktop/תמר לימודים  יד/bootcamp/db_files"


def load_to_sqlite(df, table_name, conn):
    """Helper function to load a Spark DataFrame into SQLite."""
    df.toPandas().to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()


def extract_transform_load_genre_popularity():
    # Initialize Spark session
    spark = SparkSession.builder.appName("Product Genre Popularity ETL").getOrCreate()

    # Establish SQLite connection
    conn = sqlite3.connect(os.path.join(BASE_URL, "sqllite_data.db"))

    try:
        # Extract
        genres = spark.read.csv(
            os.path.join(BASE_URL, "Genre.csv"), header=True, inferSchema=True
        )
        tracks = spark.read.csv(
            os.path.join(BASE_URL, "Track.csv"), header=True, inferSchema=True
        )
        invoice_lines = spark.read.csv(
            os.path.join(BASE_URL, "InvoiceLine.csv"), header=True, inferSchema=True
        )

        # Transform
        genre_track_sales = (
            genres.alias("g")
            .join(tracks.alias("t"), F.col("g.GenreId") == F.col("t.GenreId"), "inner")
            .join(
                invoice_lines.alias("il"),
                F.col("t.TrackId") == F.col("il.TrackId"),
                "inner",
            )
            .select(
                F.col("g.GenreId"),
                F.col("g.Name").alias("GenreName"),
                F.col("il.UnitPrice"),
                F.col("il.Quantity"),
            )
        )

        result_df = (
            genre_track_sales.groupBy("GenreId", "GenreName")
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("Total_Sales"),
                F.avg(F.col("UnitPrice")).alias("Average_Sales_Price"),
            )
            .withColumn("created_at", F.lit(datetime.now()))
            .withColumn("updated_at", F.lit(datetime.now()))
            .withColumn("updated_by", F.lit("Tamar Gavrielov"))
        )

        result_df.show()
        # Load
        load_to_sqlite(
            result_df, "product_genre_popularity_and_average_sales_price", conn
        )

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close SQLite connection and stop Spark session
        conn.close()
        spark.stop()


def extract_transform_load_sales_trends():
    # Initialize Spark session
    spark = SparkSession.builder.appName("Sales Trends ETL").getOrCreate()

    # Establish SQLite connection
    conn = sqlite3.connect(os.path.join(BASE_URL, "sqllite_data.db"))

    try:
        # Extract
        employees = spark.read.csv(
            os.path.join(BASE_URL, "Employee.csv"), header=True, inferSchema=True
        )
        invoices = spark.read.csv(
            os.path.join(BASE_URL, "Invoice.csv"), header=True, inferSchema=True
        )
        invoice_lines = spark.read.csv(
            os.path.join(BASE_URL, "InvoiceLine.csv"), header=True, inferSchema=True
        )
        tracks = spark.read.csv(
            os.path.join(BASE_URL, "Track.csv"), header=True, inferSchema=True
        )
        genres = spark.read.csv(
            os.path.join(BASE_URL, "Genre.csv"), header=True, inferSchema=True
        )

        customers = spark.read.csv(
            os.path.join(BASE_URL, "Customer.csv"), header=True, inferSchema=True
        )
        # Define the end date and start date (past 6 months)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=6 * 30)  # Approx. 6 months

        # Convert end_date and start_date to string format suitable for comparison
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")

        # Transform
        sales_data = (
            employees.alias("e")
            .join(
                customers.alias("c"),
                F.col("e.EmployeeId") == F.col("c.SupportRepId"),
                "inner",
            )
            .join(
                invoices.alias("i"),
                F.col("c.CustomerId") == F.col("i.CustomerId"),
                "inner",
            )
            .join(
                invoice_lines.alias("il"),
                F.col("i.InvoiceId") == F.col("il.InvoiceId"),
                "inner",
            )
            .join(
                tracks.alias("t"),
                F.col("il.TrackId") == F.col("t.TrackId"),
                "inner",
            )
            .join(
                genres.alias("g"),
                F.col("t.GenreId") == F.col("g.GenreId"),
                "inner",
            )
            .filter(F.col("i.InvoiceDate").between(start_date_str, end_date_str))
            .select(
                F.col("e.EmployeeId"),
                F.col("e.FirstName"),
                F.col("e.LastName"),
                F.col("g.Name").alias("GenreName"),
                F.col("il.UnitPrice"),
                F.col("il.Quantity"),
                F.col("i.InvoiceDate"),
            )
        )

        # Calculate total sales per employee and genre
        sales_trends = (
            sales_data.groupBy("EmployeeId", "FirstName", "LastName", "GenreName")
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("Total_Sales"),
                F.count("*").alias("Number_of_Sales"),
            )
            .withColumn("created_at", F.lit(datetime.now()))
            .withColumn("updated_at", F.lit(datetime.now()))
            .withColumn("updated_by", F.lit("Tamar Gavrielov"))
        )

        # Define window spec for calculating rolling sales trends
        window_spec = Window.partitionBy("EmployeeId", "GenreName").orderBy(
            "InvoiceDate"
        )

        # Calculate rolling sales totals
        sales_trends_with_rolling = sales_data.withColumn(
            "Rolling_Total_Sales",
            F.sum(F.col("UnitPrice") * F.col("Quantity")).over(window_spec),
        ).dropDuplicates()

        sales_trends.show()
        sales_trends_with_rolling.show()
        # Load
        load_to_sqlite(sales_trends, "sales_trends", conn)
        load_to_sqlite(sales_trends_with_rolling, "sales_trends_with_rolling", conn)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close SQLite connection and stop Spark session
        conn.close()
        spark.stop()


# def load():
#     # Step 1: Initialize Spark session
#     spark = SparkSession.builder.appName("ETL Template with SQLite").getOrCreate()

#     # Step 2: Establish SQLite connection
#     conn = sqlite3.connect(os.path.join(BASE_URL, "sqllite_data.db"))

#     try:
#         # EXTRACT (Loading CSVs from local storage)
#         genre = spark.read.csv(
#             os.path.join(BASE_URL, "Genre.csv"), header=True, inferSchema=True
#         )
#         invoice = spark.read.csv(
#             os.path.join(BASE_URL, "Invoice.csv"), header=True, inferSchema=True
#         )
#         invoice_line = spark.read.csv(
#             os.path.join(BASE_URL, "InvoiceLine.csv"), header=True, inferSchema=True
#         )
#         employee = spark.read.csv(
#             os.path.join(BASE_URL, "Employee.csv"), header=True, inferSchema=True
#         )
#         track = spark.read.csv(
#             os.path.join(BASE_URL, "Track.csv"), header=True, inferSchema=True
#         )
#         customer = spark.read.csv(
#             os.path.join(BASE_URL, "Customer.csv"), header=True, inferSchema=True
#         )

#         # TRANSFORM (Apply joins, groupings, and window functions)
#         # --------------------------------------------------------
#         track_genre_invoice_line = (
#             genre.alias("g")
#             .join(track.alias("t"), F.col("g.GenreId") == F.col("t.GenreId"), "inner")
#             .join(
#                 invoice_line.alias("il"),
#                 F.col("t.TrackId") == F.col("il.TrackId"),
#                 "inner",
#             )
#             .select(
#                 F.col("g.GenreId"),
#                 F.col("g.Name").alias("GenreName"),
#                 F.col("il.UnitPrice"),
#                 F.col("il.Quantity"),
#             )
#         )

#         product_genre_popularity_and_average_sales_price = (
#             track_genre_invoice_line.groupBy("GenreId", "GenreName").agg(
#                 F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("Total_Sales"),
#                 F.avg(F.col("UnitPrice")).alias("Average_Sales_Price"),
#             )
#         )

#         product_genre_popularity_and_average_sales_price = (
#             product_genre_popularity_and_average_sales_price.withColumn(
#                 "created_at", F.lit(datetime.now())
#             )
#             .withColumn("updated_at", F.lit(datetime.now()))
#             .withColumn("updated_by", F.lit("Tamar Gavrielov"))
#         )

#         # Join employee, customer, invoice, invoice_line, track, and genre

#         six_months_ago = datetime.now() - timedelta(days=180)
#         filtered_df = joined_df.filter(F.col("InvoiceDate") >= F.lit(six_months_ago))

#         sales_trends_by_employee_and_product_category = filtered_df.groupBy("EmployeeId", "GenreId") \
#                             .agg(
#                                 F.sum("UnitPrice" * "Quantity").alias("Total_Sales"),
#                                 F.count("InvoiceID").alias("Number_of_Transactions")
#                             )

#         result_df = result_df.withColumn("created_at", F.lit(datetime.now())) \
#                             .withColumn("updated_at", F.lit(datetime.now())) \
#                             .withColumn("updated_by", F.lit("process:user_name"))


#         # LOAD (Save transformed data into SQLite)
#         # ----------------------------------------------------
#         # Convert Spark DataFrame to Pandas DataFrame
#         final_script_1 = product_genre_popularity_and_average_sales_price.toPandas()

#         # Insert transformed data into a new table in SQLite
#         final_script_1.to_sql(
#             "product_genre_popularity_and_average_sales_price",
#             conn,
#             if_exists="replace",
#             index=False,
#         )

#         # Commit the changes
#         conn.commit()

#     except Exception as e:
#         print(f"An error occurred: {e}")

#     finally:
#         # Close the SQLite connection and stop Spark session
#         conn.close()
#         spark.stop()
def load():
     extract_transform_load_genre_popularity()
     extract_transform_load_sales_trends()
if __name__ == "__main__":
    load()



