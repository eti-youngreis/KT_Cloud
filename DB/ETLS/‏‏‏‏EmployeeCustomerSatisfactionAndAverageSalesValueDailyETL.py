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
        # new_employees = employees.filter(employees["updated_at"] > latest_timestamp)
        # new_customers = customers.filter(customers["updated_at"] > latest_timestamp)
        # new_invoices = invoices.filter(invoices["updated_at"] > latest_timestamp)
        # new_invoice_lines = invoice_lines.join(new_invoices, "InvoiceID", "inner")

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        
        # Alias the tables
        invoices = invoices.alias("invoice")
        customers = customers.alias("customer")
        employees = employees.alias("employee")
        
        customer_invoices = invoices.join(customers, invoices["CustomerID"] == customers["CustomerID"], "inner") \
            .select("invoice.InvoiceID", "customer.CustomerID", "customer.SupportRepId")
    
        # Calculate Customer Satisfaction
        repeat_customers = customer_invoices.groupBy("customer.SupportRepId", "customer.CustomerID") \
            .agg(F.count("invoice.InvoiceID").alias("InvoiceCount")) \
            .filter(F.col("InvoiceCount") > 1)
    
        customer_satisfaction = repeat_customers.groupBy("customer.SupportRepId") \
            .agg(F.countDistinct("customer.CustomerID").alias("CustomerSatisfaction"))
    
        # Calculate Average Sales Value
        sales_data = invoices.join(invoice_lines, "InvoiceID", "inner")
    
        avg_sales = sales_data.groupBy("InvoiceID", "invoice.Total") \
            .agg(F.sum("Quantity").alias("QuantitySum")) \
            .withColumn("AverageSalesValue", F.col("invoice.Total") / F.col("QuantitySum"))
    
        # Join all metrics
        employee_metrics = employees.alias("employee") \
            .join(customer_satisfaction.alias("satisfaction"), employees["EmployeeId"] == customer_satisfaction["SupportRepId"], "left") \
            .select("employee.EmployeeId", "employee.LastName", "employee.FirstName", "satisfaction.CustomerSatisfaction")
    
        employee_metrics_with_customers = employee_metrics \
            .join(customers.alias("customer"), employee_metrics["EmployeeId"] == customers["customer.SupportRepId"], "left")\
            .select("employee.EmployeeId", "employee.LastName", "employee.FirstName", "customer.CustomerId", "satisfaction.CustomerSatisfaction")
    
        final_data = employee_metrics_with_customers.join(invoices.alias("invoice"), employee_metrics_with_customers["customer.CustomerId"] == invoices["invoice.CustomerId"], "left") \
            .select("employee.EmployeeId", "employee.LastName", "employee.FirstName", "satisfaction.CustomerSatisfaction", "customer.CustomerId", "invoice.InvoiceID")
    
        final_result = final_data.join(avg_sales.alias("sales"), "InvoiceID", "left")
        # Fill nulls for employees who have no repeat customers or no sales
        employee_metrics = final_result.fillna({"CustomerSatisfaction": 0, "AverageSalesValue": 0})

        # Add metadata columns
        current_time = datetime.now()
        user_name = "DailyETL:Yehudit"

        # LOAD (Delete and insert new data to SQLite)
        # --------------------------------------------
        final_data_df = employee_metrics.toPandas()

        # Create a temporary table with the new data
        final_data_df.to_sql('temp_employee_metrics', conn, if_exists='replace', index=False)

        # Delete existing records that are in the new dataset
        delete_query = """
        DELETE FROM employee_customer_satisfaction_and_averagesales_value
        WHERE EmployeeId IN (SELECT EmployeeId FROM temp_employee_metrics)
        """
        cursor.execute(delete_query)

        # Insert all records from the temporary table
        insert_query = """
        INSERT INTO employee_customer_satisfaction_and_averagesales_value
        (EmployeeId, LastName, FirstName, CustomerSatisfaction, AverageSalesValue, created_at, updated_at, updated_by)
        SELECT temp.EmployeeId, temp.LastName, temp.FirstName, temp.CustomerSatisfaction, temp.AverageSalesValue,
             COALESCE(main.created_at, ?), ?, ?
        FROM temp_employee_metrics temp
        LEFT JOIN employee_customer_satisfaction_and_averagesales_value main
        ON temp.EmployeeId = main.EmployeeId
        """
        cursor.execute(insert_query, (current_time, current_time, user_name))

        # Remove the temporary table
        cursor.execute("DROP TABLE temp_employee_metrics")

        # Commit the changes to the database
        conn.commit()

    finally:
        # Step 4: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load()