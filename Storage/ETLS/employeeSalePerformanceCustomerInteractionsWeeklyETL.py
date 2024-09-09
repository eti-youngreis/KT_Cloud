from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3

def load_employees_sales_customer_interactions():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Employee Sales Performance and Customer Interactions") \
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
                employees_df["LastName"].alias("EmployeeLastName"),
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
        final_employee_df = employees_sales_df.join(employees_customers_df, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"]) \
                                      .withColumn("created_at", F.current_date()) \
                                      .withColumn("updated_at", F.current_date()) \
                                      .withColumn("updated_by", F.lit(f"process:shoshana_levovitz_{F.current_date()}")).toPandas()
        
        # Save combined employee data to SQLite
        final_employee_df.to_sql('employee_performance', conn, if_exists='replace', index=False)

        # Commit the changes to the database
        conn.commit()
        
        # Print results
        print("employee_performance:", conn.execute("SELECT * FROM 'employee_performance'").fetchall())
    
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    load_employees_sales_customer_interactions()
