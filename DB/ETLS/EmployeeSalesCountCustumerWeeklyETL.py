from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_employee_sales_and_customer_etl():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Employee Sales Performance and Customer Interactions") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('./Chinook.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        employees_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
        customers_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Customer.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
        invoiceLines_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/InvoiceLine.csv', header=True, inferSchema=True)
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the main table with related_table_1
        employees_join_customers = employees_df.join(customers_df, employees_df['EmployeeId'] == customers_df['SupportRepId'], "inner").select(
            employees_df["EmployeeId"],
            employees_df["FirstName"].alias("EmployeeFirstName"),
            customers_df["FirstName"].alias("CustomerFirstName"),
            employees_df["LastName"].alias("EmployeeLastName"),
            customers_df["LastName"].alias("CustomerLastName"),
            customers_df["CustomerId"]

        )

        customers_invoices = invoices_df.join(invoiceLines_df, 'InvoiceId', "inner").join(
            employees_join_customers, 'CustomerId', "inner")
        employees_sales_df = customers_invoices.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
            .agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSales"))
        employees_customers_df = employees_join_customers.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
            .agg(F.count("CustomerId").alias("CustomersInteractions"))

        final_employee_combined_df = employees_sales_df.join(employees_customers_df,
                                                             ["EmployeeId", "EmployeeFirstName",
                                                                 "EmployeeLastName"]
                                                             ).withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"process:yael_karo_{F.current_date()}")).toPandas()
        # Apply window function (e.g., rank customers by spend)

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion

        # Insert transformed data into a new table in SQLite using KT_DB
        final_employee_combined_df.to_sql(
            'employee_sales_and_count', conn, if_exists='replace', index=False)
        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()
        print("employee_sales_and_count", conn.execute(
            "SELECT * FROM 'employee_sales_and_count'").fetchall())

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()


if __name__ == "__main__":
    load_employee_sales_and_customer_etl()
