from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, current_timestamp, lit, date_format
import sqlite3
import pandas as pd

def load():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyETL_Incremental").getOrCreate()

    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\database.db")

    try:
        # Step 1: Extract - Read CSV files into DataFrames
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # Step 2: Transformation - Calculate loyalty score and average invoice size
        loyalty_score_df = customers_df.join(invoices_df, "CustomerId") \
            .groupBy("CustomerId", "FirstName", "LastName") \
            .agg(count("InvoiceId").alias("LoyaltyScore"))

        average_invoice_size_df = invoices_df.join(invoice_lines_df, "InvoiceId") \
            .groupBy("CustomerId") \
            .agg(avg("Total").alias("AverageInvoiceSize"))

        result_df = loyalty_score_df.join(average_invoice_size_df, "CustomerId")

        # Add metadata and convert timestamps to string
        result_df = result_df.withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("updated_by", lit("process:your_user_name"))

        # Load existing data from SQLite
        query = "SELECT * FROM customer_loyalty"
        existing_data = pd.read_sql_query(query, conn)

        # Convert Spark DataFrame to pandas for comparison
        new_data = result_df.toPandas()

        # Step 3: Perform incremental update - check for existing records
        for index, row in new_data.iterrows():
            # Check if the CustomerId exists in the existing data
            if row['CustomerId'] in existing_data['CustomerId'].values:
                # Update the existing row
                conn.execute("""
                    UPDATE customer_loyalty
                    SET LoyaltyScore = ?, AverageInvoiceSize = ?, updated_at = ?, updated_by = ?
                    WHERE CustomerId = ?
                """, (row['LoyaltyScore'], row['AverageInvoiceSize'], row['updated_at'], row['updated_by'], row['CustomerId']))
            else:
                # Insert the new row
                row.to_frame().T.to_sql('customer_loyalty', conn, if_exists='append', index=False)

        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

load()
