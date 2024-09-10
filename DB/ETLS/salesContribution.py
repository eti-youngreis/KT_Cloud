import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, current_date, lit

# create sparkSession
spark = SparkSession.builder.appName("Chinook_ETL_Track_Analysis").getOrCreate()

# read from csv
track_df = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)
customer_df = spark.read.csv('C:/Users/leabe/Documents/data/Customer.csv', header=True, inferSchema=True)
invoice_df = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
invoice_line_df = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)

joined_df = track_df.join(invoice_line_df, track_df.TrackId==invoice_line_df.TrackId) \
    .join(invoice_df, invoice_line_df.InvoiceId==invoice_df.InvoiceId) \
    .join(customer_df, invoice_df.CustomerId==customer_df.CustomerId)

sales_contribution = joined_df.groupBy("TrackId", "Name", "AlbumId") \
    .agg(sum("UnitPrice").alias("SalesContribution"))

# Add metadata

sales_contribution = sales_contribution.withColumn("created_at", current_date()) \
    .withColumn("updated_at", current_date()) \
    .withColumn("updated_by", lit(f"process:{current_user}"))

# Save results to SQLite

sales_contribution.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:path/to/sales_contribution.db") \
    .option("dbtable", "SalesContribution.db") \
    .mode("overwrite") \
    .save()

spark.stop()
