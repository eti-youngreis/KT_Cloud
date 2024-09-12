from pyspark.sql import SparkSession
import sqlite3
from datetime import datetime
import os

def load():
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("Employee Customer Satisfaction and Sales ELT") \
            .getOrCreate()

        # Step 2: Establish SQLite connection using sqlite3
        db_path = r"KT_Cloud\DB\ELTS\dbs\employee_customer_satisfaction_sales_elt.db"

        if not os.path.exists(os.path.dirname(db_path)):
            os.makedirs(os.path.dirname(db_path))

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            # Get the latest processed timestamp from the target table
            latest_timestamp_query = "SELECT MAX(updated_at) FROM employee_customer_satisfaction_sales"
            cursor.execute(latest_timestamp_query)
            latest_timestamp = cursor.fetchone()[0]

            # Handle case where no data exists yet (initial load)
            if latest_timestamp is None:
                latest_timestamp = '1900-01-01 00:00:00'  # Default for initial load

            # EXTRACT (Loading CSVs from local storage)
            # -----------------------------------------------
            employees = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Employee_with_created_at.csv", header=True, inferSchema=False)
            employees = employees.drop('HireDate')
            employees = employees.drop('BirthDate')
            customers = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Customer_with_created_at.csv", header=True, inferSchema=True)
            invoices = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\Invoice_with_created_at.csv", header=True, inferSchema=True)
            invoice_lines = spark.read.csv(r"KT_Cloud\DB\ELTS\dbs\InvoiceLine_with_created_at.csv", header=True, inferSchema=True)

            # LOAD (Save raw data into SQLite using sqlite3)
            # ----------------------------------------------------
            employees.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="employees_elt", con=conn, if_exists='replace', index=False)
            customers.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="customers_elt", con=conn, if_exists='replace', index=False)
            invoices.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="invoices_elt", con=conn, if_exists='replace', index=False)
            invoice_lines.filter(f"updated_at > '{latest_timestamp}'").toPandas().to_sql(name="invoice_lines_elt", con=conn, if_exists='replace', index=False)

            conn.commit()

            # TRANSFORM (Use SQL queries to transform the data)
            # --------------------------------------------------------
            query = """
            CREATE TABLE TEMP_CTE AS
            WITH customer_invoices AS (
                SELECT inv.InvoiceId, cust.CustomerId, cust.SupportRepId, il.UnitPrice * il.Quantity AS TotalValue
                FROM invoices_elt inv
                FULL OUTER JOIN customers_elt cust ON inv.CustomerId = cust.CustomerId
                FULL OUTER JOIN invoice_lines_elt il ON inv.InvoiceId = il.InvoiceId
                WHERE (inv.updated_at > ? OR cust.CustomerId IS NOT NULL)
                AND (cust.updated_at > ? OR cust.CustomerId IS NOT NULL)
                AND (il.updated_at > ? OR il.InvoiceLineId IS NOT NULL)
            ),
            customer_satisfaction AS (
                SELECT SupportRepId, COUNT(DISTINCT CustomerId) AS CustomerSatisfaction
                FROM (
                    SELECT SupportRepId, CustomerId, COUNT(InvoiceId) AS InvoiceCount
                    FROM customer_invoices
                    GROUP BY SupportRepId, CustomerId
                    HAVING InvoiceCount > 1
                )
                GROUP BY SupportRepId
            ),
            average_sales AS (
                SELECT SupportRepId, AVG(TotalValue) AS AverageSalesValue
                FROM customer_invoices
                GROUP BY SupportRepId
            ),
            CREATED_AT_FROM_TARGET_TABLE AS (
                SELECT EmployeeId, created_at
                FROM employee_customer_satisfaction_sales
            )
            SELECT
                e.EmployeeId, e.FirstName, e.LastName,
                COALESCE(cs.CustomerSatisfaction, 0) AS CustomerSatisfaction,
                COALESCE(avs.AverageSalesValue, 0) AS AverageSalesValue,
                IFNULL(CAFTT.created_at, datetime('now')) AS created_at,
                datetime('now') AS updated_at,
                'DailyELT:Yehudit' AS updated_by
            FROM employees_elt e
            LEFT JOIN customer_satisfaction cs ON e.EmployeeId = cs.SupportRepId
            LEFT JOIN average_sales avs ON e.EmployeeId = avs.SupportRepId
            LEFT JOIN CREATED_AT_FROM_TARGET_TABLE CAFTT
            ON e.EmployeeId = CAFTT.EmployeeId
            """

            cursor.execute(query, (latest_timestamp, latest_timestamp, latest_timestamp))

            delete_query = """
            DELETE FROM employee_customer_satisfaction_sales
            WHERE EmployeeId IN (SELECT EmployeeId FROM TEMP_CTE)
            """
            cursor.execute(delete_query)

            insert_query = """
            INSERT INTO employee_customer_satisfaction_sales 
            SELECT * FROM TEMP_CTE
            """
            cursor.execute(insert_query)

            conn.commit()

        finally:
            # Step 3: Close the SQLite connection and stop Spark session
            conn.close()
            spark.stop()

if __name__ == "__main__":
        load()