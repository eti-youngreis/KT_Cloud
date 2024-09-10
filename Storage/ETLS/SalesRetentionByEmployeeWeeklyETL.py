from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
       .appName("Average Sales per Employee and Customer Retention Rate") \
       .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect("sales_data.db")

    # Define the path to the CSV files
    path = "D:\\בוטקאמפ\\vastProject\\csv\\"

    try:
        # EXTRACT: Load CSV files into Spark DataFrames
        employee_table = spark.read.csv(path + "Employee.csv", header=True, inferSchema=True)
        customer_table = spark.read.csv(path + "Customer.csv", header=True, inferSchema=True)
        invoice_table = spark.read.csv(path + "Invoice.csv", header=True, inferSchema=True)
        invoice_line_table = spark.read.csv(path + "InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM: Perform data transformations and aggregations
        # Join employee, customer, and sales data
        employee_customer_sales = employee_table.alias('e') \
            .join(customer_table.alias('c'), F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
            .join(invoice_table.alias('i'), F.col('c.CustomerID') == F.col('i.CustomerID'), 'inner') \
            .join(invoice_line_table.alias('il'), F.col('i.InvoiceId') == F.col('il.InvoiceId'), 'inner')

        # Calculate Total Sales for each invoice line by multiplying UnitPrice and Quantity
        employee_customer_sales = employee_customer_sales \
            .withColumn('TotalSales', F.col('il.UnitPrice') * F.col('il.Quantity'))

        # Calculate Average Sales per Employee
        average_sales = employee_customer_sales.groupBy('e.EmployeeId') \
            .agg(F.avg('TotalSales').alias('average_sales_per_employee'))

        # Calculate Customer Retention Rate using a window function
        window_spec = Window.partitionBy('c.CustomerId').orderBy('i.InvoiceDate')

        # Mark repeat customers, specifying the table/alias for InvoiceId
        repeat_customers = employee_customer_sales \
            .withColumn('repeat_customer', (F.count('i.InvoiceId').over(window_spec) > 1).cast('int'))

        # Calculate retention rate per employee
        retention_rate = repeat_customers.groupBy('e.EmployeeId') \
            .agg(F.sum('repeat_customer').alias('repeat_customer_count'),
                 F.countDistinct('c.CustomerId').alias('total_customers')) \
            .withColumn('retention_rate', F.col('repeat_customer_count') / F.col('total_customers'))

        # Merge the results into one DataFrame
        final_data = average_sales.join(retention_rate, 'EmployeeId')

        # Add metadata columns
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_data = final_data \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('user_name'))  # Change 'user_name' to your name

        # Select the final columns for output
        final_data_df = final_data.select(
            'EmployeeId',
            'average_sales_per_employee',
            'retention_rate',
            'created_at',
            'updated_at',
            'updated_by'
        )

        # Load data into SQLite database
        final_data_df = final_data_df.toPandas()
        final_data_df.to_sql('employee_sales_retention', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Step 3: Clean up resources
        conn.close()  # Ensure connection is properly closed
        spark.stop()  # Stop the Spark session

# Call the function to execute the ETL process
if __name__ == "__main__":
    load()
