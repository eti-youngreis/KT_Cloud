from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName('ETL Template with KT_DB') \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('/employee.db')
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

        
        # Calculate measures (e.g., total spend, average values)
        aggregated_data = joined_employee_customer.groupBy('e.EmployeeId', 'e.FirstName', 'e.LastName') \
            .agg(
                F.sum(F.when(F.col('i.Total') > 0, 1).otherwise(0)).alias('successful_sales'),
                F.count('i.InvoiceId').alias('total_invoices')
            )

        # Apply window function (e.g., rank customers by spend)
        window_spec = Window.partitionBy().orderBy(F.desc('total'))
        transformed_data = aggregated_data.withColumn('rank', F.rank().over(window_spec))

        # Calculate metadata (e.g., date of the last transaction)
        final_data = transformed_data.withColumn('last_transaction_date', F.max('transaction_date').over(window_spec))

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_data.toPandas()

        # Insert transformed data into a new table in SQLite using KT_DB
        KT_DB.insert_dataframe(conn, 'final_table', final_data_df)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()
