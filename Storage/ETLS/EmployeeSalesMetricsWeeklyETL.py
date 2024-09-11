from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from datetime import datetime

def load():
    # Step 1: Initialize Spark session
    # Create a Spark session to enable Spark SQL functionality
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    # Connect to SQLite database (or create it if it does not exist)
    conn = sqlite3.connect("employee_tbl.db")

    # Define the path to the CSV files
    path = "D:\\בוטקאמפ\\Vast Project\\EmployeeTables\\"
    
    try:
        # EXTRACT: Load CSV files from local storage into Spark DataFrames
        # Read CSV files into DataFrames with schema inference and header row
        employee_table = spark.read.csv(path + "Employee.csv", header=True, inferSchema=True)
        invoice_table = spark.read.csv(path + "Invoice.csv", header=True, inferSchema=True)
        customer_table = spark.read.csv(path + "Customer.csv", header=True, inferSchema=True)

        # TRANSFORM: Perform data transformations and aggregations
        # Join employee, customer, and invoice tables
        joined_employee_customer = employee_table.alias('e') \
            .join(customer_table.alias('c'), F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
            .join(invoice_table.alias('i'), F.col('c.CustomerId') == F.col('i.CustomerId'), 'inner')

        # Calculate the number of successful sales and total invoices for each employee
        aggregated_data = joined_employee_customer.groupBy('e.EmployeeId', 'e.FirstName', 'e.LastName') \
            .agg(
                F.sum(F.when(F.col('i.Total') > 0, 1).otherwise(0)).alias('successful_sales'),
                F.count('i.InvoiceId').alias('total_invoices')
            )

        # Calculate the number of invoices per customer for each employee
        invoice_count_per_customer = joined_employee_customer.groupBy('e.EmployeeId', 'c.CustomerId') \
            .agg(F.count('i.InvoiceId').alias('invoice_count'))

        # Calculate the average number of invoices per customer for each employee
        average_invoice_count = invoice_count_per_customer.groupBy('e.EmployeeId') \
            .agg(F.avg('invoice_count').alias('average_invoice_count'))

        # Merge aggregated data with average invoice count and calculate conversion rate
        transformed_data = aggregated_data.join(average_invoice_count, on='EmployeeId') \
            .withColumn('conversion_rate', F.col('successful_sales') / F.col('total_invoices'))

        # Add metadata columns to the DataFrame
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = transformed_data \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('rachel_krashinski'))

        # Select the final columns for output
        final_data_df = transformed_data.select(
            'EmployeeId',
            'FirstName',
            'LastName',
            'average_invoice_count',
            'conversion_rate',
            'created_at',
            'updated_at',
            'updated_by'
        )

        # Load data into SQLite database
        # Convert Spark DataFrame to Pandas DataFrame and write to SQLite table
        final_data_df = final_data_df.toPandas()
        final_data_df.to_sql('employee_data', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Clean up resources
        # Close the SQLite connection and stop the Spark session
        conn.close()  # Ensure connection is properly closed
        spark.stop()  # Stop the Spark session

# Call the function to execute the ETL process
if __name__ == "__main__":
    load()
