from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import sqlite3
from datetime import datetime

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/etl_db.db')
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        employees = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Employee.csv", header=True, inferSchema=True)
        employees = employees.drop('HireDate').drop('BirthDate')
        customers = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Invoice.csv", header=True, inferSchema=True)
        invoice_line = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/InvoiceLine.csv", header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite)
        # ------------------------------------
        employees.toPandas().to_sql('employees', conn, if_exists='replace', index=False)
        customers.toPandas().to_sql('customers', conn, if_exists='replace', index=False)
        invoices.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)
        invoice_line.toPandas().to_sql('invoice_lines', conn, if_exists='replace', index=False)

        # TRANSFORM (Perform transformations with SQL queries using SQLite)
        # --------------------------------------------------------------
        # 1. Join the necessary tables and create a view
        query_customer_invoices = """
        CREATE VIEW IF NOT EXISTS customer_invoices AS
        SELECT inv.InvoiceId, cust.CustomerId, cust.SupportRepId, il.UnitPrice * il.Quantity AS TotalValue
        FROM invoices inv
        JOIN customers cust ON inv.CustomerId = cust.CustomerId
        JOIN invoice_lines il ON inv.InvoiceId = il.InvoiceId
        """
        conn.execute(query_customer_invoices)

        # 2. Calculate customer satisfaction (repeat customers)
        query_customer_satisfaction = """
        CREATE VIEW IF NOT EXISTS customer_satisfaction AS
        SELECT SupportRepId, COUNT(DISTINCT CustomerId) AS CustomerSatisfaction
        FROM (
            SELECT SupportRepId, CustomerId, COUNT(InvoiceId) AS InvoiceCount
            FROM customer_invoices
            GROUP BY SupportRepId, CustomerId
            HAVING InvoiceCount > 1
        )
        GROUP BY SupportRepId
        """
        conn.execute(query_customer_satisfaction)

        # 3. Calculate average sales value
        query_average_sales = """
        CREATE VIEW IF NOT EXISTS average_sales AS
        SELECT SupportRepId, AVG(TotalValue) AS AverageSalesValue
        FROM customer_invoices
        GROUP BY SupportRepId
        """
        conn.execute(query_average_sales)

        # 4. Combine the results
        query_final_results = """
        CREATE VIEW IF NOT EXISTS employee_performance AS
        SELECT e.EmployeeId, e.FirstName, e.LastName, 
            COALESCE(cs.CustomerSatisfaction, 0) AS CustomerSatisfaction, 
            COALESCE(avs.AverageSalesValue, 0) AS AverageSalesValue
        FROM employees e
        LEFT JOIN customer_satisfaction cs ON e.EmployeeId = cs.SupportRepId
        LEFT JOIN average_sales avs ON e.EmployeeId = avs.SupportRepId
        """
        conn.execute(query_final_results)

        # 5. Add metadata columns (created_at, updated_at, updated_by)
        current_time = datetime.now()
        user_name = "Efrat"

        # Select the final data with metadata
        final_query = f"""
        SELECT EmployeeId, FirstName, LastName, CustomerSatisfaction, AverageSalesValue,
            '{current_time}' AS created_at,
            '{current_time}' AS updated_at,
            '{user_name}' AS updated_by
        FROM employee_performance
        """
        final_data = conn.execute(final_query).fetchall()

        # Create the final table
        conn.execute("DROP TABLE IF EXISTS employee_customer_satisfaction_sales")
        conn.execute("""
        CREATE TABLE employee_customer_satisfaction_sales (
            EmployeeId INTEGER,
            FirstName TEXT,
            LastName TEXT,
            CustomerSatisfaction INTEGER,
            AverageSalesValue REAL,
            created_at TEXT,
            updated_at TEXT,
            updated_by TEXT
        )
        """)

        # Insert the final data into the employee_customer_satisfaction_sales table
        conn.executemany("""
        INSERT INTO employee_customer_satisfaction_sales (EmployeeId, FirstName, LastName, CustomerSatisfaction, AverageSalesValue, created_at, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, final_data)

        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load()
