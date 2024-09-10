from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import os

def incremental_load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Employee Customer Satisfaction and Sales ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    db_path = r"KT_Cloud\DB\ETLS\dbs\employee_customer_satisfaction_and_averagesales_value.db"
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))

    conn = sqlite3.connect(db_path)

    try:
        # Step 3: Get the latest processed timestamp from the target table
        cursor = conn.cursor()
        latest_timestamp_query = "SELECT MAX(updated_at) FROM employee_customer_satisfaction_and_averagesales_value"
        cursor.execute(latest_timestamp_query)
        latest_timestamp = cursor.fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially

        # EXTRACT (Loading CSVs from local storage with incremental filtering)
        # --------------------------------------------------------------------
        employees = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Employee_with_created_at.csv", header=True, inferSchema=True)
        customers = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Customer_with_created_at.csv", header=True, inferSchema=True)
        invoices = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Invoice_with_created_at.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\InvoiceLine_with_created_at.csv", header=True, inferSchema=True)

        # Filter for new data based on the latest timestamp
        new_employees = employees.filter(employees["updated_at"] > latest_timestamp)
        new_customers = customers.filter(customers["updated_at"] > latest_timestamp)
        new_invoices = invoices.filter(invoices["updated_at"] > latest_timestamp)
        new_invoice_lines = invoice_lines.join(new_invoices, "InvoiceID", "inner")

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        
        # Alias the tables
        new_invoices = new_invoices.alias("invoice")
        new_customers = new_customers.alias("customer")
        new_employees = new_employees.alias("employee")
        
        customer_invoices = new_invoices.join(new_customers, new_invoices["CustomerID"] == new_customers["CustomerID"], "inner") \
            .select("invoice.InvoiceID", "customer.CustomerID", "customer.SupportRepId")
    
        # Calculate Customer Satisfaction
        repeat_customers = customer_invoices.groupBy("customer.SupportRepId", "customer.CustomerID") \
            .agg(F.count("invoice.InvoiceID").alias("InvoiceCount")) \
            .filter(F.col("InvoiceCount") > 1)
    
        customer_satisfaction = repeat_customers.groupBy("customer.SupportRepId") \
            .agg(F.countDistinct("customer.CustomerID").alias("CustomerSatisfaction"))
    
        # Calculate Average Sales Value
        sales_data = new_invoices.join(new_invoice_lines, "InvoiceID", "inner")
    
        avg_sales = sales_data.groupBy("InvoiceID", "invoice.Total") \
            .agg(F.sum("Quantity").alias("QuantitySum")) \
            .withColumn("AverageSalesValue", F.col("invoice.Total") / F.col("QuantitySum"))
    
        # Join all metrics
        employee_metrics = new_employees.alias("employee") \
            .join(customer_satisfaction.alias("satisfaction"), new_employees["EmployeeId"] == customer_satisfaction["SupportRepId"], "left") \
            .select("employee.EmployeeId", "employee.LastName", "employee.FirstName", "satisfaction.CustomerSatisfaction")
    
        employee_metrics_with_customers = employee_metrics \
            .join(new_customers.alias("customer"), employee_metrics["EmployeeId"] == new_customers["customer.SupportRepId"], "left")\
            .select("employee.EmployeeId", "employee.LastName", "employee.FirstName", "customer.CustomerId", "satisfaction.CustomerSatisfaction")
    
        final_data = employee_metrics_with_customers.join(new_invoices.alias("invoice"), employee_metrics_with_customers["customer.CustomerId"] == new_invoices["invoice.CustomerId"], "left") \
            .select("employee.EmployeeId", "employee.LastName", "employee.FirstName", "satisfaction.CustomerSatisfaction", "customer.CustomerId", "invoice.InvoiceID")
    
        final_result = final_data.join(avg_sales.alias("sales"), "InvoiceID", "left")
        # Fill nulls for employees who have no repeat customers or no sales
        employee_metrics = final_result.fillna({"CustomerSatisfaction": 0, "AverageSalesValue": 0})

        # Add metadata columns
        current_time = datetime.now()
        user_name = "DailyETL:Yehudit"

        final_data = final_result \
            .withColumn("created_at", F.lit(current_time)) \
            .withColumn("updated_at", F.lit(current_time)) \
            .withColumn("updated_by", F.lit(user_name))

        # LOAD (Append new transformed data to SQLite)
        # --------------------------------------------
        final_data_df = final_data.toPandas()

        # Append new data to the existing table
        final_data_df.to_sql(name="employee_customer_satisfaction_and_averagesales_value", con=conn, if_exists='append', index=False)
        
        # Commit the changes to the database
        conn.commit()

    finally:
        # Step 4: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load()