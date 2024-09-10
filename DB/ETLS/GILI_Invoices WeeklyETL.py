from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Sales Trends by Employee and Product Category ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Replace with the correct path

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the relevant tables
        employees_df = spark.read.csv("/path_to_csv/Employee.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("/path_to_csv/Invoice.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("/path_to_csv/InvoiceLine.csv", header=True, inferSchema=True)
        tracks_df = spark.read.csv("/path_to_csv/Track.csv", header=True, inferSchema=True)
        genres_df = spark.read.csv("/path_to_csv/Genre.csv", header=True, inferSchema=True)

        # TRANSFORM (Calculate sales trends by employee and genre)
        # --------------------------------------------------------
        # Step 1: Filter invoices from the past 6 months
        recent_invoices_df = invoices_df.filter(F.col("InvoiceDate") >= F.add_months(F.current_date(), -6))

        # Step 2: Join Invoices with InvoiceLines, Tracks, and Genres
        invoice_sales_df = recent_invoices_df.join(invoice_lines_df, "InvoiceId") \
            .join(tracks_df, "TrackId") \
            .join(genres_df, "GenreId")

        # Step 3: Join with Employees table to track sales by employee
        employee_sales_df = invoice_sales_df.join(employees_df, invoice_sales_df["SupportRepId"] == employees_df["EmployeeId"])

        # Step 4: Group by Employee and Genre, and calculate total sales
        employee_genre_sales_df = employee_sales_df.groupBy("EmployeeId", "LastName", "FirstName", "GenreId", "Name") \
            .agg(
                F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSales")
            )

        # Step 5: Apply window functions to calculate sales trends over time
        window_spec = Window.partitionBy("EmployeeId", "GenreId").orderBy("InvoiceDate")
        employee_genre_trends_df = employee_sales_df.withColumn("SalesTrend", F.sum("UnitPrice").over(window_spec))

        # Add metadata columns
        final_employee_genre_sales_df = employee_genre_sales_df.withColumn("created_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp()) \
            .withColumn("updated_by", F.lit("user_name"))  # Replace "user_name" with the actual user

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_employee_genre_sales_df.toPandas()

        # Insert transformed data into a new table in SQLite using KT_DB
        KT_DB.insert_dataframe(conn, 'EmployeeSalesTrends', final_data_df)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()
