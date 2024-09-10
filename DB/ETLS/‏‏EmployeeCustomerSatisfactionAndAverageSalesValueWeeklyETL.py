from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime
import os

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Employee Customer Satisfaction and Sales ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using sqlite3
    db_path = r"C:\Users\1\Downloads\final_vast\vast\dbs\etl_db.db"  # נתיב מוחלט עבור Windows
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))

    conn = sqlite3.connect(db_path)

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        employees = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Employee.csv", header=True, inferSchema=True)
        customers = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\InvoiceLine.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        
        # Alias the tables
        invoices = invoices.alias("inv")
        customers = customers.alias("cust")
        employees = employees.alias("emp")
        
        customer_invoices = invoices.join(customers, invoices["CustomerID"] == customers["CustomerID"], "inner") \
            .select("inv.InvoiceID", "cust.CustomerID", "cust.SupportRepId")
        
        # Calculate Customer Satisfaction
        repeat_customers = customer_invoices.groupBy("cust.SupportRepId", "cust.CustomerID") \
            .agg(F.count("inv.InvoiceID").alias("InvoiceCount")) \
            .filter(F.col("InvoiceCount") > 1)
        
        customer_satisfaction = repeat_customers.groupBy("cust.SupportRepId") \
            .agg(F.countDistinct("cust.CustomerID").alias("CustomerSatisfaction"))
        
        customer_satisfaction.show()
        
        # Join Invoices with InvoiceLines to calculate Average Sales Value
        sales_data = invoices.join(invoice_lines, "InvoiceID", "inner")
        
        avg_sales = sales_data.groupBy("InvoiceID", "Total") \
            .agg(F.sum("Quantity").alias("QuantitySum")) \
            .withColumn("AverageSalesValue", F.col("Total") / F.col("QuantitySum"))
        
        avg_sales.show()
        
        # Aliasing DataFrames to avoid ambiguous column names
        employee_metrics = employees.alias("emp") \
            .join(customer_satisfaction.alias("cs"), employees["EmployeeId"] == customer_satisfaction["SupportRepId"], "left") \
            .select("emp.EmployeeId", "emp.LastName", "emp.FirstName", "cs.CustomerSatisfaction")
        
        # Now join with customers without ambiguity
        employee_metrics_with_customers = employee_metrics \
            .join(customers.alias("cust"), employee_metrics["EmployeeId"] == customers["cust.SupportRepId"], "left")\
            .select("emp.EmployeeId", "emp.LastName", "emp.FirstName", "cust.CustomerId", "cs.CustomerSatisfaction")
        
        employee_metrics_with_customers.show()
        
        # Use full qualified column names to avoid ambiguity
        final_data = employee_metrics_with_customers.join(invoices.alias("inv"), employee_metrics_with_customers["cust.CustomerId"] == invoices["inv.CustomerId"], "left") \
            .select("emp.EmployeeId", "emp.LastName", "emp.FirstName", "cs.CustomerSatisfaction", "cust.CustomerId", "inv.InvoiceID")
        
        final_data.show()
        
        # Perform the join on the invoice ID as before
        final_result = final_data.join(avg_sales.alias("sales"), "InvoiceID", "left")
        
        # Fill nulls for employees who have no repeat customers or no sales
        employee_metrics = final_result.fillna({"CustomerSatisfaction": 0, "AverageSalesValue": 0})

        # Show the result
        final_result.show()

        # Add metadata columns
        current_time = datetime.now()
        user_name = "DailyETL:Yehudit"

        final_data = final_result \
            .withColumn("created_at", F.lit(current_time)) \
            .withColumn("updated_at", F.lit(current_time)) \
            .withColumn("updated_by", F.lit(user_name))

        # LOAD (Save transformed data into SQLite using sqlite3)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_data.toPandas()

        # Create table if not exists and insert data into SQLite
        final_data_df.to_sql(name=r"KT_Cloud\DB\ETLS\dbs\employee_customer_satisfaction_and_averagesales_value", con = conn, if_exists='replace', index=False)
        
        # Commit the changes to the database
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load()
