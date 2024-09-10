from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime

def load():
    # Step 1: Initialize Spark session
    # Create a Spark session to enable Spark SQL functionality
    spark = SparkSession.builder \
        .appName("ETL - Customer Average Spend and Lifetime Value") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    # Connect to SQLite database (or create it if it does not exist)
    conn = sqlite3.connect("./ETL_customer_data.db")

    # Define the path to the CSV files
    path = "D:\\בוטקאמפ\\vastProject\\csv\\"

    try:
        # EXTRACT: Load CSV files from local storage into Spark DataFrames
        # Read CSV files into DataFrames with schema inference and header row
        customer_table = spark.read.csv(path + "Customer.csv", header=True, inferSchema=True)
        invoice_table = spark.read.csv(path + "Invoice.csv", header=True, inferSchema=True)
        invoice_line_table = spark.read.csv(path + "InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM: Perform data transformations and aggregations
        # Join customer and invoice tables on CustomerID
        customer_invoice = customer_table.alias('c') \
            .join(invoice_table.alias('i'), 'CustomerID', 'inner') \
            .join(invoice_line_table.alias('il'), 'InvoiceID', 'inner')

        # Calculate Average Spend per Invoice
        average_spend = customer_invoice.groupBy('CustomerID') \
            .agg(F.avg('Total').alias('average_spend_per_invoice'))

        # Calculate Customer Lifetime Value
        lifetime_value = customer_invoice.groupBy('CustomerID') \
            .agg(F.sum('Total').alias('customer_lifetime_value'))

        # Merge the results into one DataFrame
        transformed_data = average_spend.join(lifetime_value, 'CustomerID')

        # Add metadata columns
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = transformed_data \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('user_name'))  # Change 'user_name' to the desired name

        # Select the final columns for output
        final_data_df = transformed_data.select(
            'CustomerID',
            'average_spend_per_invoice',
            'customer_lifetime_value',
            'created_at',
            'updated_at',
            'updated_by'
        )

        # Load data into SQLite database
        # Convert Spark DataFrame to Pandas DataFrame and write to SQLite table
        final_data_df = final_data_df.toPandas()
        final_data_df.to_sql('customer_lifetime_value', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Clean up resources
        # Close the SQLite connection and stop the Spark session
        conn.close()  # Ensure connection is properly closed
        spark.stop()  # Stop the Spark session

# Call the function to execute the ETL process
if __name__ == "__main__":
    load()
