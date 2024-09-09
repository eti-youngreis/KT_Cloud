from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3

def load_average_purchase_value():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Average Purchase Value Over Time by Customer Type") \
        .getOrCreate()
    
    # Load CSV files into Spark DataFrames
    customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
    invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
    invoiceLines_df = spark.read.csv('C:/Users/shana/Desktop/ETL/InvoiceLine.csv', header=True, inferSchema=True)
    
    # Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')
    
    try:
        # Calculate invoice totals
        invoice_totals = invoiceLines_df \
            .withColumn("ItemTotal", F.col("UnitPrice") * F.col("Quantity")) \
            .groupBy("InvoiceId") \
            .agg(F.sum("ItemTotal").alias("InvoiceTotal"))
        
        # Aggregate average customer invoices per month
        customers_invoices = invoices_df \
            .join(invoice_totals, 'InvoiceId', "inner") \
            .join(customers_df, 'CustomerId', "inner") \
            .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
            .groupBy("CustomerId", "InvoiceMonth") \
            .agg(F.avg("InvoiceTotal").alias("avgCustomerInvoices"))
        
        # Apply window function to rank customers by spending
        window_spec = Window.partitionBy().orderBy(F.desc("avgCustomerInvoices"))
        transformed_data = customers_invoices.withColumn("rank", F.rank().over(window_spec))
        transformed_data.show(50)

        # Save transformed data to SQLite
        transformed_data.toPandas().to_sql('customer_invoice_avg', conn, if_exists='replace', index=False)

        # Print results
        print("customer_invoice_avg:", conn.execute("SELECT * FROM 'customer_invoice_avg'").fetchall())
    
    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load_average_purchase_value()
