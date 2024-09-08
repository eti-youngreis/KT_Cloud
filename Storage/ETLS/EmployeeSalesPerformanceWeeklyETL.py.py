from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load_employees():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Employee Sales Performance and Customer Interactions Template with KT_DB") \
        .getOrCreate()
    
    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database
    
    try:
        # Load CSV files into Spark DataFrames
        employees_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
        customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
        invoiceLines_df = spark.read.csv('C:/Users/shana/Desktop/ETL/InvoiceLine.csv', header=True, inferSchema=True)
     
        # Join Employees with Customers who they support
        employees_join_customers = employees_df.join(customers_df, employees_df['EmployeeId'] == customers_df['SupportRepId'], "inner") \
            .select(
                employees_df["EmployeeId"],
                employees_df["FirstName"].alias("EmployeeFirstName"),
                customers_df["FirstName"].alias("CustomerFirstName"),
                employees_df["LastName"].alias("EmployeeLastName"),
                customers_df["LastName"].alias("CustomerLastName"),
                customers_df["CustomerId"]
            )
        
        # Join customers with their invoices
        customers_invoices = invoices_df.join(invoiceLines_df, 'InvoiceId', "inner").join(employees_join_customers, 'CustomerId', "inner")
 
        # Aggregate sales performance per employee
        employees_sales_df = customers_invoices.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
                                .agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSales"))
        
        # Count customer interactions per employee
        employees_customers_df = employees_join_customers.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
                                    .agg(F.count("CustomerId").alias("CustomersInteractions"))
        
        # Combine sales and customer interactions data
        final_employee_sales_df = employees_sales_df.join(employees_customers_df, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"]) \
                                      .withColumn("created_at", F.current_date()) \
                                      .withColumn("updated_at", F.current_date()) \
                                      .withColumn("updated_by", F.lit(f"process:shoshana_levovitz_{F.current_date()}")).toPandas()
        
        final_employee_customers_df = employees_customers_df.join(employees_customers_df, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName", "CustomersInteractions"]) \
                                      .withColumn("created_at", F.current_date()) \
                                      .withColumn("updated_at", F.current_date()) \
                                      .withColumn("updated_by", F.lit(f"process:shoshana_levovitz_{F.current_date()}")).toPandas()

        # Save employee sales and customer data to SQLite
        final_employee_sales_df.to_sql('employee_sales', conn, if_exists='replace', index=False)
        final_employee_customers_df.to_sql('employee_customers', conn, if_exists='replace', index=False)

        # Calculate invoice totals
        invoice_totals = invoiceLines_df \
                    .withColumn("ItemTotal", F.col("UnitPrice") * F.col("Quantity")) \
                    .groupBy("InvoiceId") \
                    .agg(F.sum("ItemTotal").alias("InvoiceTotal"))
        
        # Aggregate average customer invoices per month
        customers_invoices = invoices_df \
            .join(invoice_totals, 'InvoiceId', "inner") \
            .join(customers_df, 'CustomerId', "inner") \
            .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
            .groupBy("CustomerId", "InvoiceMonth") \
            .agg(F.avg("InvoiceTotal").alias("avgCustomerInvoices"))  # Calculate average monthly spending per customer
        
        # Apply window function to rank customers by spending
        window_spec = Window.partitionBy().orderBy(F.desc("avgCustomerInvoices"))
        transformed_data = customers_invoices.withColumn(
            "rank", F.rank().over(window_spec))
        transformed_data.show(200)

        # Save transformed data to SQLite
        transformed_data = transformed_data.toPandas()
        transformed_data.to_sql('customer_invoice_avg', conn, if_exists='replace', index=False)

        # Commit the changes to the database
        conn.commit()
        
        # Print results
        print("customer_invoice_avg:", conn.execute("SELECT * FROM 'customer_invoice_avg'").fetchall())
        print("employee_sales:", conn.execute("SELECT * FROM 'employee_sales'").fetchall())
        print("employee_customers:", conn.execute("SELECT * FROM 'employee_customers'").fetchall())
    
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    load_employees()

