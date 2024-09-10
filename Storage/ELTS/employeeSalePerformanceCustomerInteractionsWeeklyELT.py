from pyspark.sql import SparkSession,functions as F
import sqlite3
from pyspark.sql.types import StringType


def load_employees_sales_customer_interactions_elt():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT - Employee Sales Performance and Customer Interactions") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database

    try:
        # -----------------------------
        # E - Extract
        # -----------------------------
        # Read the relevant CSV files
        employees_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
        customers_df = spark.read.csv("C:/Users/shana/Desktop/ETL/Customer.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)

        employees_df = employees_df.withColumn('HireDate', F.col('HireDate').cast(StringType()))
        employees_df = employees_df.withColumn('BirthDate', F.col('BirthDate').cast(StringType()))
        # -----------------------------
        # L - Load
        # -----------------------------
        # Load raw data into SQLite (using Pandas to convert Spark DataFrames)
        employees_df=employees_df.toPandas()
        employees_df.to_sql('Employees', conn, if_exists='replace', index=False)
        customers_df=customers_df.toPandas()
        customers_df.to_sql('Customers', conn, if_exists='replace', index=False)
        invoices_df=invoices_df.toPandas()
        invoices_df.to_sql('Invoices', conn, if_exists='replace', index=False)

        conn.execute("DROP TABLE IF EXISTS employee_performance_etl;")

        # -----------------------------
        # T - Transform
        # -----------------------------
        # Write a SQL query to perform transformations (joining and aggregation)
        transform_query = """
         CREATE TABLE employee_performance_etl AS 
         SELECT e.*,count(DISTINCT c.CustomerId) AS CustomersInteractions, SUM(i.Total) AS TotalSales,CURRENT_DATE AS created_at,
            CURRENT_DATE AS updated_at,
            'process:shoshana_levovitz_' || CURRENT_DATE AS updated_by
         FROM employees e JOIN customers c
         ON e.EmployeeId=c.SupportRepId 
         JOIN invoices i ON c.CustomerId = i.CustomerId
         GROUP BY e.EmployeeId
        """

        # Execute the transformation query to create the final table
        conn.execute(transform_query)

        # Commit the transformation
        conn.commit()

        # Print results to verify the transformed data
        print("employee_performance:", conn.execute("SELECT * FROM 'employee_performance_etl'").fetchall())

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    load_employees_sales_customer_interactions_elt()
