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
       
        # Current time for 'created_at' and 'updated_at' fields
        current_time = datetime.now()

    # Define aliases for the tables
        employee_alias = employee_table.alias('e')
        customer_alias = customer_table.alias('c')
        invoice_alias = invoice_table.alias('i')

        # Join the tables
        joined_employee_customer = employee_alias \
            .join(customer_alias, F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
            .join(invoice_alias, F.col('i.CustomerId') == F.col('c.CustomerId'))

        # Define the window spec by partitioning over 'EmployeeId'
        employee_window = Window.partitionBy('e.EmployeeId')

        # Calculate successful_sales and total_invoices using window functions
        transformed_data = joined_employee_customer \
            .withColumn('successful_sales', F.sum(F.when(F.col('i.Total') > 0, 1).otherwise(0)).over(employee_window)) \
            .withColumn('total_invoices', F.count('i.InvoiceId').over(employee_window)) \
            .withColumn('invoice_count', F.count('i.InvoiceId').over(Window.partitionBy('e.EmployeeId', 'c.CustomerId'))) \
            .withColumn('average_invoice_count', F.avg('invoice_count').over(employee_window)) \
            .withColumn('conversion_rate', F.col('successful_sales') / F.col('total_invoices')) \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Shani_Strauss')) \
            .dropDuplicates(['e.EmployeeId'])  # Drop duplicates to maintain one row per EmployeeId

        # Select the relevant columns to match the original output format
        final_data_df = transformed_data.select(
            'e.EmployeeId', 
            'e.FirstName', 
            'e.LastName', 
            'successful_sales', 
            'total_invoices', 
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