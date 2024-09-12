from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3

from datetime import datetime
# import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName('ETL Template with KT_DB') \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('employee.db')
    path_to_tables = 'C:\\Users\\USER\\Desktop\\3.9.2024\\Emploee_Tables\\'
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)

        employee_table = spark.read.csv(f'{path_to_tables}\\Employee.csv', header=True, inferSchema=True)

        # Load other related tables
        invoice_table = spark.read.csv(f'{path_to_tables}\\Invoice.csv', header=True, inferSchema=True)
        customer_table = spark.read.csv(f'{path_to_tables}\\Customer.csv', header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the main table with related_table_1
        joined_employee_customer = employee_table.alias('e') \
        .join(customer_table.alias('c'), F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
        .join(invoice_table.alias('i'), F.col('i.CustomerId') == F.col('c.CustomerId'))

        
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