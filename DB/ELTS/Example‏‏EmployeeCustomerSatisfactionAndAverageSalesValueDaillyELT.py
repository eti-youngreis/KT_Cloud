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

    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        employees = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Employee.csv", header=True, inferSchema=False)
        employees = employees.drop('HireDate')
        employees = employees.drop('BirthDate')
        customers = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\Invoice.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv(r"KT_Cloud\DB\ETLS\dbs\InvoiceLine.csv", header=True, inferSchema=True)

        # Get the latest processed timestamp from the target table
        latest_timestamp_query = "SELECT MAX(created_at) FROM final_table"
        latest_timestamp = KT_DB.execute_and_fetch(conn, latest_timestamp_query)

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # Default for initial load

        # LOAD (Save raw data into SQLite using sqlite3)
        # ----------------------------------------------------
        employees.filter(updated > latest_timestamp).toPandas().to_sql(name="employees_elt", con=conn, if_exists='replace', index=False)
        customers.filter(updated > latest_timestamp).toPandas().to_sql(name="customers_elt", con=conn, if_exists='replace', index=False)
        invoices.filter(updated > latest_timestamp).toPandas().to_sql(name="invoices_elt", con=conn, if_exists='replace', index=False)
        invoice_lines.filter(updated > latest_timestamp).toPandas().to_sql(name="invoice_lines_elt", con=conn, if_exists='replace', index=False)

        conn.commit()

        # TRANSFORM (Use SQL queries to transform the data)
        # --------------------------------------------------------
        
        # 1. Join the necessary tables and create a view
        query = """
            CREATE TABLE TEMP_CTE AS
            WITH customer_invoices AS(
                SELECT inv.InvoiceId, cust.CustomerId, cust.SupportRepId, il.UnitPrice * il.Quantity AS TotalValue
                FROM invoices_elt inv
                FULL OUTER JOIN customers_elt cust ON inv.CustomerId = cust.CustomerId
                FULL OUTER JOIN invoice_lines_elt il ON inv.InvoiceId = il.InvoiceId
                WHERE (inv.updated > ${latest_timestamp} OR  cust.id IS NOT NULL) # all new invoices plus old invoices that belongs to new customers
                AND (cust.updated > ${latest_timestamp} OR cust.id IS NOT NULL) # all new customers plus old customers with new invoices
                AND (il.updated > ${latest_timestamp} OR il.id IS NOT NULL)
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
            CREATED_AT_FROM_TARGET_TABLE AS(
                SELECT ID, CREATEDAT
                FROM TARGET_TABLE
            )
            SELECT       
            e.EmployeeId, e.FirstName, e.LastName, 
            COALESCE(cs.CustomerSatisfaction, 0) AS CustomerSatisfaction, 
            COALESCE(avs.AverageSalesValue, 0) AS AverageSalesValue,
            IFNULL(CAFTT.CREATED_AT, GETDATE()) AS CREATED_AT,
            GETDATE() AS UPDATED_AT,
            'WeeklyELT:Yehudit' AS UPDATED_BY
            FROM employees_elt e
            LEFT JOIN customer_satisfaction cs ON e.EmployeeId = cs.SupportRepId
            LEFT JOIN average_sales avs ON e.EmployeeId = avs.SupportRepId
            LEFT JOIN CREATED_AT_FROM_TARGET_TABLE CAFTT
            ON e.EmployeeId = CAFTT.ID
        """

        conn.execute(query)

        delete_query="""DELETE FROM employee_customer_satisfaction_sales
        USING TEMP_CTE
        WHERE id = id"""
        conn.execute(delete_query)

        insert_query="""insert into employee_customer_satisfaction_sales select * from TEMP_CTE"""
        conn.execute(insert_query)
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

        
if __name__ == "__main__":
    load()