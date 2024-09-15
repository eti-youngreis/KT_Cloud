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
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/etl_db1.db')
    try:
        cursor = conn.cursor()
        latest_timestamp_query = "SELECT MAX(updated_at) FROM employee_customer_satisfaction_sales"
        cursor.execute(latest_timestamp_query)
        latest_timestamp = cursor.fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially
        # EXTRACT (Loading CSVs from S3 or local storage)
        # EXTRACT (Loading CSVs from S3 or local storage)
        employees = spark.read.csv("D:/boto3 project/csv files/Employee_with_created_at.csv", header=True, inferSchema=True)
        employees = employees.drop('HireDate').drop('BirthDate')

        customers = spark.read.csv("D:/boto3 project/csv files/Customer_with_created_at.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("D:/boto3 project/csv files/Invoice_with_created_at.csv", header=True, inferSchema=True)
        invoice_line = spark.read.csv("D:/boto3 project/csv files/InvoiceLine_with_created_at.csv", header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite)
        employees.toPandas().to_sql('employees', conn, if_exists='replace', index=False)
        customers.toPandas().to_sql('customers', conn, if_exists='replace', index=False)
        invoices.toPandas().to_sql('invoices', conn, if_exists='replace', index=False)
        invoice_line.toPandas().to_sql('invoice_lines', conn, if_exists='replace', index=False)

        # TRANSFORM and FINAL QUERY (Combine multiple steps into a single query)
        current_time = datetime.now()
        user_name = "Efrat"

        create_temp_table_query = f"""
            CREATE TEMP TABLE TempDeletedRowsInfo AS
            SELECT e.EmployeeId, e.FirstName, e.LastName, 
                COALESCE(cs.CustomerSatisfaction, 0) AS CustomerSatisfaction, 
                COALESCE(AVG(il.UnitPrice * il.Quantity), 0) AS AverageSalesValue,
                '{current_time}' AS created_at,
                '{current_time}' AS updated_at,
                '{user_name}' AS updated_by
            FROM employees e
            LEFT JOIN customers cust ON e.EmployeeId = cust.SupportRepId
            LEFT JOIN invoices inv ON cust.CustomerId = inv.CustomerId
            LEFT JOIN invoice_lines il ON inv.InvoiceId = il.InvoiceId
            LEFT JOIN (
                SELECT cust.SupportRepId, COUNT(DISTINCT cust.CustomerId) AS CustomerSatisfaction
                FROM invoices inv
                JOIN customers cust ON inv.CustomerId = cust.CustomerId
                GROUP BY cust.SupportRepId
            ) cs ON e.EmployeeId = cs.SupportRepId
            WHERE e.updated_at > '{latest_timestamp}' 
                OR cust.updated_at > '{latest_timestamp}' 
                OR inv.updated_at > '{latest_timestamp}' 
                OR il.updated_at > '{latest_timestamp}'
            GROUP BY e.EmployeeId, e.FirstName, e.LastName, cs.CustomerSatisfaction
        """


        
        # Delete rows from  the temporary table
        delete_query = """
        DELETE FROM employee_customer_satisfaction_sales
        WHERE EmployeeId IN (
            SELECT EmployeeId FROM TempDeletedRowsInfo
        );
        """
        
        # Insert to AlbumPopularityAndRevenue the updates from temp table
        insert_query = """
        INSERT INTO employee_customer_satisfaction_sales (EmployeeId, FirstName, LastName, CustomerSatisfaction, AverageSalesValue, created_at, updated_at, updated_by)
        SELECT EmployeeId, FirstName, LastName, CustomerSatisfaction, AverageSalesValue, created_at, updated_at, updated_by
        FROM TempDeletedRowsInfo;
        """

        create_temp_table_query = create_temp_table_query.strip().replace('\n', '').replace('  ', ' ')
        delete_query = delete_query.strip().replace('\n', '').replace('  ', ' ')
        insert_query = insert_query.strip().replace('\n', '').replace('  ', ' ')
        print("striped")
        cursor.execute(create_temp_table_query)
        conn.commit()
        cursor.execute(delete_query)
        conn.commit()
        cursor.execute(insert_query)
        conn.commit()
        cursor.execute("SELECT * FROM employee_customer_satisfaction_sales")

        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()
    


if __name__ == "__main__":
    load()
