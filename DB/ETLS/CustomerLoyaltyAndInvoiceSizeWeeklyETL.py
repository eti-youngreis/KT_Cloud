from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sqlite3  # Assuming you're using sqlite3
import pandas as pd

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLoyaltyAndInvoiceSize").getOrCreate()

    try:
        customers = spark.read.csv("../../../Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("../../../Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("../../../InvoiceLine.csv", header=True, inferSchema=True)

        # Calculate Loyalty Score: Number of purchases (invoices per customer)
        loyalty_score = invoices.groupBy("CustomerId").agg(F.count("*").alias("loyalty_score"))

        # Calculate total amount spent per invoice
        invoice_totals = invoice_lines.groupBy("InvoiceId").agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("total_amount"))

        # Calculate average invoice size per customer by joining with Invoices
        avg_invoice_size = invoices.join(invoice_totals, "InvoiceId") \
            .groupBy("CustomerId") \
            .agg(F.avg("total_amount").alias("avg_invoice_size"))
        
        # Join loyalty score and average invoice size with customers
        result = customers.join(loyalty_score, "CustomerId").join(avg_invoice_size, "CustomerId")

         # Add metadata columns
        result = result.withColumn("created_at", F.current_timestamp()) \
                   .withColumn("updated_at", F.current_timestamp()) \
                   .withColumn("updated_by", F.lit("DailyETL:SL"))
        
        # Select only the desired columns
        final_result = result.select("CustomerId", "FirstName", "LastName", "loyalty_score", "avg_invoice_size", "created_at", "updated_at", "updated_by")
        
        final_df = final_result.toPandas()

        conn = sqlite3.connect('../../../Customers_ETL.db')
        final_df.to_sql('customer_loyalty_and_invoice_size_ETL', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        spark.stop()

    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()



