# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as spark_sum, count, coalesce, lit, current_timestamp
# import sqlite3

# def incremental_etl():
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("Customer ETL") \
#         .config("spark.driver.extraClassPath", "/path/to/sqlite-jdbc-driver.jar") \
#         .getOrCreate()

#     conn = sqlite3.connect('../../../Customers_ETL.db')
#     cursor = conn.cursor()

#     try:
#         # EXTRACT (Load CSVs using PySpark)
#         customers_df = spark.read.csv("../../../Customer.csv", header=True, inferSchema=True)
#         invoices_df = spark.read.csv("../../../Invoice.csv", header=True, inferSchema=True)
#         invoice_lines_df = spark.read.csv("../../../InvoiceLine.csv", header=True, inferSchema=True)

#         # Check if the 'customer_loyalty' table exists in SQLite
#         cursor.execute("""
#             SELECT name FROM sqlite_master WHERE type='table' AND name='customer_loyalty_and_invoice_size_ETL';
#         """)
#         table_exists = cursor.fetchone()

#         # If the table exists, get the latest processed updated_at
#         if table_exists:
#             latest_updated_at_query = "SELECT MAX(updated_at) FROM customer_loyalty_and_invoice_size_ETL"
#             cursor.execute(latest_updated_at_query)
#             last_updated = cursor.fetchone()[0]
#         else:
#             last_updated = None

#         if not last_updated:
#             last_updated = '1900-01-01 00:00:00'  # Default to a very old timestamp

#         # Convert last_updated to Spark timestamp
#         last_updated = spark.sql(f"SELECT TO_TIMESTAMP('{last_updated}')").collect()[0][0]
#         print(f"Last Updated: {last_updated}")

#         # Filter the data for rows that have been created or updated after the last load
#         customers_filtered_df = customers_df.filter(customers_df['UpdatedAt'] > last_updated)
#         invoices_filtered_df = invoices_df.filter(invoices_df['UpdatedAt'] > last_updated)
#         invoice_lines_filtered_df = invoice_lines_df.filter(invoice_lines_df['UpdatedAt'] > last_updated)

#         # Skip if no new data
#         if customers_filtered_df.count() == 0 and invoices_filtered_df.count() == 0 and invoice_lines_filtered_df.count() == 0:
#             print("No new data to process.")
#             return

#         # TRANSFORM
#         # Calculate total spend per invoice
#         invoice_lines_filtered_df = invoice_lines_filtered_df.withColumn(
#             'total_spend', col('UnitPrice') * col('Quantity')
#         )

#         # Join customer, invoice, and invoice line data
#         customer_invoices_df = customers_filtered_df.join(invoices_filtered_df, 'CustomerId', how='left')
#         customer_invoice_lines_df = customer_invoices_df.join(
#             invoice_lines_filtered_df, 'InvoiceId', how='left'
#         )

#         # If table exists, read historical data from SQLite using Spark's JDBC
#         if table_exists:
#             historical_data_df = spark.read \
#                 .format("jdbc") \
#                 .option("url", "jdbc:sqlite:/path/to/Customers_ETL.db") \
#                 .option("dbtable", "customer_loyalty_and_invoice_size_ETL") \
#                 .load()
#         else:
#             # Create an empty historical DataFrame if table doesn't exist
#             historical_data_df = spark.createDataFrame([], schema=customer_invoice_lines_df.schema)

#         # Aggregation: Calculating loyalty_score and avg_invoice_size
#         customer_summary_df = customer_invoice_lines_df.groupBy(
#             'CustomerId', 'FirstName', 'LastName'
#         ).agg(
#             count('InvoiceId').alias('loyalty_score'),  # Number of invoices
#             spark_sum('total_spend').alias('total_spend')  # Total spend
#         )

#         # Merge with historical data
#         transformed_df = customer_summary_df.join(
#             historical_data_df, on='CustomerId', how='left'
#         )

#         # Update loyalty_score and avg_invoice_size
#         transformed_df = transformed_df.withColumn(
#             'loyalty_score',
#             coalesce(col('loyalty_score_x'), lit(0)) + col('loyalty_score')
#         ).withColumn(
#             'avg_invoice_size',
#             col('total_spend') / col('loyalty_score')
#         ).withColumn(
#             'created_at',
#             coalesce(col('created_at'), current_timestamp())
#         ).withColumn(
#             'updated_at', current_timestamp()
#         ).withColumn(
#             'updated_by', lit('process:ETL')
#         )

#         # Final columns for loading into the database
#         final_df = transformed_df.select(
#             'CustomerId', 'FirstName', 'LastName', 'loyalty_score', 
#             'avg_invoice_size', 'created_at', 'updated_at', 'updated_by'
#         )

#         # LOAD
#         # Remove existing rows for customers being updated in the SQLite DB
#         cursor.execute("""
#             DELETE FROM customer_loyalty_and_invoice_size_ETL
#             WHERE CustomerId IN ({})
#         """.format(",".join(["?"] * final_df.count())), final_df.select('CustomerId').rdd.flatMap(lambda x: x).collect())

#         # Load transformed data into the SQLite database using JDBC
#         final_df.write \
#             .format("jdbc") \
#             .option("url", "jdbc:sqlite:/path/to/Customers_ETL.db") \
#             .option("dbtable", "customer_loyalty_and_invoice_size_ETL") \
#             .mode('append') \
#             .save()

#         # Commit the changes to the SQLite database
#         conn.commit()

#     finally:
#         # Close the SQLite connection
#         conn.close()
#         # Stop the Spark session
#         spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum, current_timestamp, lit, date_format, col
import sqlite3
import pandas as pd

def get_last_updated_at():
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(UpdatedAt) FROM customer_loyalty")
        latest_updated_at = cursor.fetchone()[0]
        if latest_updated_at is None:
            latest_updated_at = '1900-01-01 00:00:00'
        return latest_updated_at
    finally:
        conn.close()

def delete_old_records(customer_ids):
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\customer_loyalty.db")
    cursor = conn.cursor()
    try:
        if len(customer_ids) > 0:
            if len(customer_ids) == 1:
                customer_ids = f"({customer_ids[0]})"
            else:
                customer_ids = tuple(customer_ids)
            cursor.execute(f"DELETE FROM customer_loyalty WHERE CustomerId IN {customer_ids}")
            conn.commit()
    finally:
        conn.close()

def load():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyAndInvoiceSize").getOrCreate()

    # Get the latest update timestamp
    latest_updated_at = get_last_updated_at()

    try:
        # Step 1: Extraction - Read CSV files into DataFrames
        customers_df = spark.read.csv("../../../Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("../../../Invoice.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("../../../InvoiceLine.csv", header=True, inferSchema=True)
        # Convert latest_updated_at to timestamp for filtering
        latest_updated_at = lit(pd.to_datetime(latest_updated_at))

        # Filter invoices and invoice lines by the latest update date
        invoices_df = invoices_df.filter(col("UpdatedAt") > latest_updated_at)
        invoice_lines_df = invoice_lines_df.filter(col("UpdatedAt") > latest_updated_at)

        # Step 2: Transformation - Calculate loyalty score and average invoice size
        # Calculate loyalty score
        loyalty_score_df = invoices_df.groupBy("CustomerId") \
            .agg(count("InvoiceId").alias("LoyaltyScore"))

        # Calculate average invoice size
        invoice_totals_df = invoice_lines_df.groupBy("InvoiceId") \
            .agg(sum(col("UnitPrice") * col("Quantity")).alias("TotalAmount"))

        average_invoice_size_df = invoices_df.join(invoice_totals_df, "InvoiceId") \
            .groupBy("CustomerId") \
            .agg(avg("TotalAmount").alias("AverageInvoiceSize"))

        # Combine loyalty score and average invoice size
        result_df = loyalty_score_df.join(average_invoice_size_df, "CustomerId", "left")

        # Add metadata columns
        result_df = result_df.withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("UpdatedAt", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("updated_by", lit("process:your_user_name"))

        # Convert the Spark DataFrame to a pandas DataFrame for loading into SQLite
        new_data = result_df.toPandas()

        # Step 3: Incremental Load
        if not new_data.empty:
            # Extract customer IDs from the new data
            customer_ids = new_data['CustomerId'].tolist()
            delete_old_records(customer_ids)

            # Load new and updated rows into SQLite
            conn = sqlite3.connect('../../../Customers_ETL.db')
            new_data.to_sql('customer_loyalty_and_invoice_size_ETL', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()

    finally:
        # Stop the Spark session
        spark.stop()

# Call the incremental load function
load()
