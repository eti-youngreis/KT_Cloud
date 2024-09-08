from pyspark.sql import SparkSession
import sqlite3


def load_employees():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT - Employee Sales Performance and Customer Interactions") \
        .getOrCreate()
    
    # Step 2: Load CSV files into Spark DataFrames
    employees_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
    customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
    invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
    invoiceLines_df = spark.read.csv('C:/Users/shana/Desktop/ETL/InvoiceLine.csv', header=True, inferSchema=True)
    
    # Step 3: Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database
    
    try:
        # Step 4: Load raw data into SQLite

        employees_df.toPandas().to_sql('raw_employees', conn, if_exists='replace', index=False)
        customers_df.toPandas().to_sql('raw_customers', conn, if_exists='replace', index=False)
        invoices_df.toPandas().to_sql('raw_invoices', conn, if_exists='replace', index=False)
        invoiceLines_df.toPandas().to_sql('raw_invoice_lines', conn, if_exists='replace', index=False)

        # Step 5: Perform transformations using SQL queries
        query_employee_sales = """
        CREATE TABLE employee_sales AS
        SELECT
            e.EmployeeId,
            e.FirstName AS EmployeeFirstName,
            e.LastName AS EmployeeLastName,
            SUM(il.UnitPrice * il.Quantity) AS TotalSales
        FROM raw_employees e
        INNER JOIN raw_customers c ON e.EmployeeId = c.SupportRepId
        INNER JOIN raw_invoices i ON c.CustomerId = i.CustomerId
        INNER JOIN raw_invoice_lines il ON i.InvoiceId = il.InvoiceId
        GROUP BY e.EmployeeId, e.FirstName, e.LastName
        """
        conn.execute(query_employee_sales)

        query_customer_interactions = """
        CREATE TABLE employee_customers AS
        SELECT
            e.EmployeeId,
            e.FirstName AS EmployeeFirstName,
            e.LastName AS EmployeeLastName,
            COUNT(c.CustomerId) AS CustomersInteractions
        FROM raw_employees e
        INNER JOIN raw_customers c ON e.EmployeeId = c.SupportRepId
        GROUP BY e.EmployeeId, e.FirstName, e.LastName
        """
        conn.execute(query_customer_interactions)

        query_invoice_totals = """
        CREATE TABLE invoice_totals AS
        SELECT
            InvoiceId,
            SUM(UnitPrice * Quantity) AS InvoiceTotal
        FROM raw_invoice_lines
        GROUP BY InvoiceId
        """
        conn.execute(query_invoice_totals)

        query_customer_invoice_avg = """
        CREATE TABLEW customer_invoice_avg AS
        SELECT
            c.CustomerId,
            strftime('%m', i.InvoiceDate) AS InvoiceMonth,
            AVG(it.InvoiceTotal) AS avgCustomerInvoices
        FROM raw_customers c
        INNER JOIN raw_invoices i ON c.CustomerId = i.CustomerId
        INNER JOIN invoice_totals it ON i.InvoiceId = it.InvoiceId
        GROUP BY c.CustomerId, InvoiceMonth
        """
        conn.execute(query_customer_invoice_avg)

        query_rank_customers = """
        CREATE TABLEW ranked_customers AS
        SELECT *,
            RANK() OVER (ORDER BY avgCustomerInvoices DESC) AS rank
        FROM customer_invoice_avg
        """
        conn.execute(query_rank_customers)

        # Step 6: Print results
        print("employee_sales:", conn.execute("SELECT * FROM employee_sales").fetchall())
        print("employee_customers:", conn.execute("SELECT * FROM employee_customers").fetchall())
        print("ranked_customers:", conn.execute("SELECT * FROM ranked_customers").fetchall())
    
    finally:
        # Step 7: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    load_employees()
