import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, hour, count, sum, current_date, lit
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load_sales_distribution_by_media_type():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Sales Distribution by Media Type") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method
    
    try:
        # Read from CSV
        invoice_df = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
        invoice_line_df = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)
        track_df = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)
        media_type_df = spark.read.csv('C:/Users/leabe/Documents/data/MediaType.csv', header=True, inferSchema=True)

        # Join the data
        joined_df = invoice_df.join(invoice_line_df, invoice_df.InvoiceId == invoice_line_df.InvoiceId) \
            .join(track_df, invoice_line_df.TrackId == track_df.TrackId) \
            .join(media_type_df, track_df.MediaTypeId == media_type_df.MediaTypeId)

        # Extract hour from invoice date
        joined_df = joined_df.withColumn('Hour', hour(col('InvoiceDate')))

        # Group by Media Type and Hour
        sales_distribution = joined_df.groupBy('MediaType', 'Hour') \
            .agg(count('InvoiceLineId').alias('Frequency'), sum('Total').alias('TotalSales'))

        # Add metadata
        current_user = os.getenv('USER')
        sales_distribution = sales_distribution.withColumn("created_at", current_date()) \
            .withColumn("updated_at", current_date()) \
            .withColumn("updated_by", lit(f"process:{current_user}"))

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        final_data_df = sales_distribution.toPandas()
        KT_DB.insert_dataframe(conn, 'SalesDistributionByMediaType', final_data_df)
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()
