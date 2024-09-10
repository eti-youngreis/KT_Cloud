from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import sqlite3

def load_employee_sales_and_customer_etl():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Employee Sales Performance and Customer Interactions") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')

    try:
        # Check if the table exists
        check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='employee_sales_and_count';"
        table_exists = conn.execute(check_table_query).fetchone()
        latest_timestamp = None
        if table_exists:
            latest_timestamp_query = "SELECT MAX(updated_at) FROM employee_sales_and_count"
            latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        print("latest_timestamp:", latest_timestamp)

        # EXTRACT (Loading CSVs from S3 or local storage)
        employees_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
        customers_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Customer.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)

        invoices_df = invoices_df.withColumn('InvoiceDate', F.to_date(F.col('InvoiceDate'), 'dd/MM/yyyy'))
        customers_df = customers_df.withColumn('created_at', F.to_date(F.col('created_at'), 'dd/MM/yyyy'))
        employees_df = employees_df.withColumn('created_at', F.to_date(F.col('created_at'), 'dd/MM/yyyy'))
        # Transform columns to string
        # TRANSFORM (Apply joins, groupings, and window functions)
        new_employees_df = employees_df.filter(F.col("created_at") > latest_timestamp)
        new_customers_df = customers_df.filter(F.col("created_at") > latest_timestamp)
        new_invoices_df = invoices_df.filter(F.col("InvoiceDate") > latest_timestamp)
        
        print("new_invoices_df:", new_invoices_df.show())
        
        custumer_in_new_invoices_df = new_invoices_df[["CustomerId"]].distinct()
        customers_connect_to_invoices = custumer_in_new_invoices_df.join(customers_df, "CustomerId", "inner")
        customers_update = customers_connect_to_invoices.union(new_customers_df).distinct()
        print("customers_update:", customers_update.show())

        employees_new_custommer_df = customers_update[["SupportRepId"]].distinct()
        employees_connect_to_customer = employees_new_custommer_df.join(employees_df, employees_df['EmployeeId'] == employees_new_custommer_df['SupportRepId'], "inner").drop('SupportRepId')
        employees_update = employees_connect_to_customer.union(new_employees_df).distinct()
        print("employees_update:", employees_update.show())

        employees_update = employees_update.alias("emp")
        customers_update = customers_update.alias("cust")
        employees_df = employees_df.alias("emp_df")
        customers_df = customers_df.alias("cust_df")
        invoices_df = invoices_df.alias("inv")

        # Perform join with clear names
        employees_join_customers = employees_update.join(
            customers_df, employees_update['EmployeeId'] == customers_df['SupportRepId'], "inner"
        ).select(
            F.col("emp.EmployeeId"),
            F.col("emp.FirstName").alias("EmployeeFirstName"),
            F.col("cust_df.FirstName").alias("CustomerFirstName"),
            F.col("emp.LastName").alias("EmployeeLastName"),
            F.col("cust_df.LastName").alias("CustomerLastName"),
            F.col("cust_df.CustomerId")
        )

        customers_invoices = invoices_df.join(employees_join_customers, 'CustomerId', "inner")
        employees_sales_df = customers_invoices.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
            .agg(F.sum(customers_invoices["Total"]).alias("TotalSales"))
        employees_customers_df = employees_join_customers.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
            .agg(F.count("CustomerId").alias("CustomersInteractions"))

        final_employee_combined_df = employees_sales_df.join(employees_customers_df,
                                                             ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"]
                                                             ).withColumn("created_at", F.current_date().cast(StringType())) \
            .withColumn("updated_at", F.current_date().cast(StringType())) \
            .withColumn("updated_by", F.lit(f"process:yael_karo_{datetime.now().strftime('%Y-%m-%d')}")).toPandas()

        # LOAD (Save transformed data into SQLite)
        employee_ids_to_delete = final_employee_combined_df['EmployeeId'].tolist()
        if employee_ids_to_delete and table_exists:
            ids_placeholder = ', '.join('?' * len(employee_ids_to_delete))
            delete_query = f"DELETE FROM 'employee_sales_and_count' WHERE EmployeeId IN ({ids_placeholder})"
            conn.execute(delete_query, employee_ids_to_delete)
        final_employee_combined_df.to_sql(
            'employee_sales_and_count', conn, if_exists='append', index=False
        )
        conn.commit()
        print("employee_sales_and_count", conn.execute(
            "SELECT * FROM 'employee_sales_and_count'").fetchall())

    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()


if __name__ == "__main__":
    load_employee_sales_and_customer_etl()
