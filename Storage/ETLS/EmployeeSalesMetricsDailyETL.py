from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import pandas as pd
from datetime import datetime

def get_last_updated_at(conn):
    query = "SELECT MAX(updated_at) FROM employee_data"
    last_updated_at = pd.read_sql(query, conn).iloc[0, 0]
    return last_updated_at if last_updated_at is not pd.NA else '1970-01-01 00:00:00'

def incremental_load():
    
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName('ETL Template with Incremental Load') \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('employee_tbl.db')
    path = "D:\\בוטקאמפ\\Vast Project\\EmployeeTables\\"
    
    try:
        # Get the last `updated_at` value
        last_updated_at = get_last_updated_at(conn)
        
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        employee_table = spark.read.csv(f'{path}\\Employee.csv', header=True, inferSchema=True)
        invoice_table = spark.read.csv(f'{path}\\Invoice.csv', header=True, inferSchema=True)
        customer_table = spark.read.csv(f'{path}\\Customer.csv', header=True, inferSchema=True)
        
        # Add `updated_at` column with the current timestamp
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        employee_table = employee_table.withColumn('updated_at', F.lit(current_time))
        invoice_table = invoice_table.withColumn('updated_at', F.lit(current_time))
        customer_table = customer_table.withColumn('updated_at', F.lit(current_time))
        
        # FILTER new/updated data based on `updated_at`
        employee_table = employee_table.filter(F.col('updated_at') > last_updated_at)
        invoice_table = invoice_table.filter(F.col('updated_at') > last_updated_at)
        customer_table = customer_table.filter(F.col('updated_at') > last_updated_at)
        
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        joined_employee_customer = employee_table.alias('e') \
            .join(customer_table.alias('c'), F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
            .join(invoice_table.alias('i'), F.col('i.CustomerId') == F.col('c.CustomerId'))
        
        aggregated_data = joined_employee_customer.groupBy('e.EmployeeId', 'e.FirstName', 'e.LastName') \
            .agg(
                F.sum(F.when(F.col('i.Total') > 0, 1).otherwise(0)).alias('successful_sales'),
                F.count('i.InvoiceId').alias('total_invoices')
            )
        
        invoice_count_per_customer = joined_employee_customer.groupBy('e.EmployeeId', 'c.CustomerId') \
            .agg(F.count('i.InvoiceId').alias('invoice_count'))
        
        average_invoice_count = invoice_count_per_customer.groupBy('e.EmployeeId') \
            .agg(F.avg('invoice_count').alias('average_invoice_count'))
        
        transformed_data = aggregated_data.join(average_invoice_count, on='EmployeeId') \
            .withColumn('conversion_rate', F.col('successful_sales') / F.col('total_invoices')) \
            .withColumn('created_at', F.lit(current_time)) \
            .withColumn('updated_at', F.lit(current_time)) \
            .withColumn('updated_by', F.lit('Shani_Strauss'))
        
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
        
        # Convert Spark DataFrame to Pandas DataFrame for SQLite operations
        final_data_df = final_data_df.toPandas()
        
        # Load existing data from SQLite
        existing_data = pd.read_sql('SELECT * FROM employee_data', conn)
        
        # Merge new data with existing data
        updated_data = existing_data.set_index('EmployeeId').combine_first(final_data_df.set_index('EmployeeId')).reset_index()
        
        # Update `updated_at` only for existing records
        updated_data['updated_at'] = updated_data.apply(
            lambda row: final_data_df.loc[final_data_df['EmployeeId'] == row['EmployeeId'], 'updated_at'].values[0] if row['EmployeeId'] in final_data_df['EmployeeId'].values else row['updated_at'],
            axis=1
        )
        # Save the updated data back to SQLite
        updated_data.to_sql('employee_data', conn, if_exists='replace', index=False)
        conn.commit()
        
    finally:
        # Step 3: Clean up resources
        conn.close()
        spark.stop()
        
# Call the function to execute the ETL process
if __name__ == "__main__":
    incremental_load()