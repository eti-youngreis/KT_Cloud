from pyspark.sql import SparkSession
import sqlite3

def load():
    # Step 1: Initialize Spark session - This creates a Spark session for processing large datasets
    spark = SparkSession.builder \
        .appName("ELT Template with SQLite") \
        .getOrCreate()

    # Step 2: Establish SQLite connection - This connects to an SQLite database file, creating it if it doesn't exist
    conn = sqlite3.connect('employee_tbl.db')

    # Define the path to the folder containing the CSV files
    path = "D:\\בוטקאמפ\\Vast Project\\EmployeeTables\\"
    
    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main data tables from CSV files into Spark DataFrames
        # InferSchema=True automatically infers the column data types
        employee_table = spark.read.csv(path + "Employee.csv", header=True, inferSchema=True)
        invoice_table = spark.read.csv(path + "Invoice.csv", header=True, inferSchema=True)
        customer_table = spark.read.csv(path + "Customer.csv", header=True, inferSchema=True)

        # Remove any columns with the timestamp data type to simplify the schema
        employee_table = employee_table.drop(*[c for c, dt in employee_table.dtypes if dt == "timestamp"])
        invoice_table = invoice_table.drop(*[c for c, dt in invoice_table.dtypes if dt == "timestamp"])
        customer_table = customer_table.drop(*[c for c, dt in customer_table.dtypes if dt == "timestamp"])

        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames for SQLite insertion
        employee_table_df = employee_table.toPandas()
        invoice_table_df = invoice_table.toPandas()
        customer_table_df = customer_table.toPandas()

        # Insert the DataFrames into SQLite as tables. if_exists='replace' means that the table will be overwritten if it already exists
        employee_table_df.to_sql('employee_table', conn, if_exists='replace', index=False)
        invoice_table_df.to_sql('invoice_table', conn, if_exists='replace', index=False)
        customer_table_df.to_sql('customer_table', conn, if_exists='replace', index=False)

        # TRANSFORM (Perform transformations using SQL queries in SQLite)
        # -------------------------------------------------------------------------
        # Drop the table if it exists to ensure a clean transformation process
        conn.execute("DROP TABLE IF EXISTS final_table")

        # SQL transformation: 
        # 1. Join the employee, customer, and invoice tables
        # 2. Calculate the number of invoices per employee, the total invoices, and conversion rates
        transform_query = """
            CREATE TABLE final_table AS
            WITH invoice_count_per_customer AS (
                SELECT e.EmployeeId,
                       c.CustomerId,
                       COUNT(i.InvoiceId) AS invoice_count
                FROM employee_table e
                JOIN customer_table c ON e.EmployeeId = c.SupportRepId
                JOIN invoice_table i ON c.CustomerId = i.CustomerId
                GROUP BY e.EmployeeId, c.CustomerId
            ),
            aggregated_data AS (
                SELECT e.EmployeeId,
                       e.FirstName,
                       e.LastName,
                       COUNT(i.InvoiceId) AS total_invoices,
                       SUM(CASE WHEN i.Total > 0 THEN 1 ELSE 0 END) AS successful_sales
                FROM employee_table e
                JOIN customer_table c ON e.EmployeeId = c.SupportRepId
                JOIN invoice_table i ON c.CustomerId = i.CustomerId
                GROUP BY e.EmployeeId, e.FirstName, e.LastName
            )
            SELECT a.EmployeeId,
                   a.FirstName,
                   a.LastName,
                   AVG(i.invoice_count) AS average_invoice_count,
                   (a.successful_sales * 1.0 / a.total_invoices) AS conversion_rate,
                   CURRENT_TIMESTAMP AS created_at,
                   CURRENT_TIMESTAMP AS updated_at,
                   'rachel_krashinski' AS updated_by
            FROM aggregated_data a
            LEFT JOIN invoice_count_per_customer i ON a.EmployeeId = i.EmployeeId
            GROUP BY a.EmployeeId, a.FirstName, a.LastName, a.total_invoices, a.successful_sales
        """

        # Execute the transformation query to create a new table in SQLite
        conn.execute(transform_query)

        # Commit the changes to the database to ensure data is saved
        conn.commit()

    finally:
        # Step 3: Clean up resources - close SQLite connection and stop the Spark session
        conn.close()
        spark.stop()

# Call the function to execute the ELT process
if __name__ == "__main__":
    load()
    
