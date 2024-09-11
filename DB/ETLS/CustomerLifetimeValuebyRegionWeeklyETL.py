from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sqlite3  # Assuming you're using sqlite3
import pandas as pd

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Customer Lifetime Value by Region") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('../../../Customers_ETL.db')

    try:
        customers = spark.read.csv("../../../Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("../../../Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("../../../InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM (Join tables and calculate LTV)
        # -----------------------------------------------
        # Step 1: Join Customers with Invoices and InvoiceLines
        customer_invoices = customers.join(invoices, "CustomerId")
        full_data = customer_invoices.join(invoice_lines, "InvoiceId")

        # Step 2: Calculate total amount spent by each customer (LTV)
        ltv_data = full_data.groupBy("CustomerId", "Country") \
            .agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("LTV"))

        # Step 3: Use window functions to rank customers by LTV within each region
        window_spec = Window.partitionBy("Country").orderBy(F.desc("LTV"))
        ltv_ranked = ltv_data.withColumn("LTV_Rank", F.rank().over(window_spec))

        # Add metadata (optional)
        ltv_ranked = ltv_ranked.withColumn("created_at", F.current_timestamp()) \
                               .withColumn("updated_at",F.current_timestamp())\
                               .withColumn("updated_by", F.lit("DailyETL:SL"))
        
        # LOAD (Save the result to the SQLite database)
        # ----------------------------------------------------
        final_df = ltv_ranked.toPandas()
        final_df.to_sql('customer_lifetime_value_by_region', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        spark.stop()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()