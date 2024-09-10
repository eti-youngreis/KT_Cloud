from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_employee_sales_and_customer_elt():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT - Employee Sales Performance and Customer Interactions") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('./Chinook.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        employees_df = spark.read.csv(
            'C:/Users/Owner/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
        # Load other related tables
        customers_df = spark.read.csv(
            'C:/Users/Owner/Desktop/ETL/Customer.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv(
            'C:/Users/Owner/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)

        employees_df = employees_df.withColumn('HireDate', F.col('HireDate').cast(StringType()))
        employees_df = employees_df.withColumn('BirthDate', F.col('BirthDate').cast(StringType()))
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the main table with related_table_1
        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion

        # Insert transformed data into a new table in SQLite using KT_DB
        # final_employee_combined_df.to_sql(
        #     'employee__sales_and_count_count', conn, if_exists='replace', index=False)
        # Commit the changes to the database using KT_DB's commit() function
        employees_pd=employees_df.toPandas()
        customers_pd = customers_df.toPandas()
        invoices_pd = invoices_df.toPandas()
        # Load raw data into SQLite
        employees_pd.to_sql('Employees', conn, if_exists='replace', index=False)
        customers_pd.to_sql('Customers', conn, if_exists='replace', index=False)
        invoices_pd.to_sql('Invoices', conn, if_exists='replace', index=False)
        # TRANSFORM (Perform transformations with SQL queries)
        # ----------------------------------------------------
        # Join tables and calculate total purchase and average purchase per customer
        # Apply the transformations using SQLite SQL queries
        drop_query="""DROP TABLE IF EXISTS employee_sales_and_customer_elt"""
        conn.execute(drop_query)
        # conn.commit()
        transform_query = """
            CREATE TABLE employee_sales_and_customer_elt AS
            SELECT e.*,count(DISTINCT c.CustomerId) AS CustomersInteractions,
            SUM(i.Total) AS TotalSales,
            CURRENT_DATE AS created_at,
            CURRENT_DATE AS updated_at,
            'process:yael_karo_' || CURRENT_DATE AS updated_by
            FROM employees e JOIN customers c
            ON e.EmployeeId=c.SupportRepId
            JOIN invoices i ON c.CustomerId = i.CustomerId
            GROUP BY e.EmployeeId
        """

        # Execute the transformation query
        conn.execute(transform_query)
        conn.commit()
        print("employee_sales_and_customer_elt", conn.execute(
            "SELECT * FROM 'employee_sales_and_customer_elt'").fetchall())

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()


if __name__ == "__main__":
    load_employee_sales_and_customer_elt()
