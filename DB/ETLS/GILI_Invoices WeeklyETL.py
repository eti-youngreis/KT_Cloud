from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Sales Trends Analysis") \
    .getOrCreate()

# Load CSV data into DataFrames
path = "D:\\Users\\גילי\\Documents\\בוטקמפ\\csv files\\"
employees_df = spark.read.csv(path + "Employee.csv", header=True, inferSchema=True)
customers_df = spark.read.csv(path + "Customer.csv", header=True, inferSchema=True)
invoices_df = spark.read.csv(path + "Invoice.csv", header=True, inferSchema=True)
invoice_lines_df = spark.read.csv(path + "InvoiceLine.csv", header=True, inferSchema=True)
tracks_df = spark.read.csv(path + "Track.csv", header=True, inferSchema=True)
genres_df = spark.read.csv(path + "Genre.csv", header=True, inferSchema=True)


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

# Show the resulting DataFrame
sales_trends.show()