from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


# import KT_DB  # Assuming KT_DB is the library for SQLite operations
import sqlite3

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Employee Customer Satisfaction and Sales Weekly ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/db/employee_customer_satisfaction.db')
    try:
        cursor = conn.cursor()
        latest_timestamp_query = "SELECT MAX(created_at) FROM employee_customer_satisfaction"
        cursor.execute(latest_timestamp_query)
        latest_timestamp = cursor.fetchone()[0]
        
        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        employees = spark.read.csv("D:/boto3 project/csv files/Employee_with_created_at.csv", header=True, inferSchema=True)
        employees = employees.withColumnRenamed("FirstName", "EmployeeFirstName")\
                        .withColumnRenamed("LastName", "EmployeeLastName")
        # Load other related tables
        customers = spark.read.csv("D:/boto3 project/csv files/Customer_with_created_at.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("D:/boto3 project/csv files/Invoice_with_created_at.csv", header=True, inferSchema=True)
        invoice_lines = spark.read.csv("D:/boto3 project/csv files/InvoiceLine_with_created_at.csv", header=True, inferSchema=True)
        
        # Filter for new data based on the latest timestamp
        new_employees= employees.filter(employees["created_at"] > latest_timestamp)
        new_customers = customers.filter(customers["created_at"] > latest_timestamp)
        new_invoices = invoices.filter(invoices["created_at"] > latest_timestamp)
        new_invoice_lines = invoice_lines.filter(invoice_lines["created_at"] > latest_timestamp)
        
        new_employees = new_employees.withColumnRenamed("FirstName", "EmployeeFirstName")\
                        .withColumnRenamed("LastName", "EmployeeLastName")

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        repeat_customers = new_invoices.join(new_customers, "CustomerId")\
            .groupBy("SupportRepId", "CustomerId")\
                .agg(F.count("InvoiceId").alias("InvoiceCount"))\
                    .filter(F.col("InvoiceCount") > 1)

        customer_satisfaction_df = repeat_customers.groupBy("SupportRepId") \
            .agg(F.countDistinct("CustomerID").alias("CustomerSatisfaction"))
        
        # Join Invoices with InvoiceLines to calculate Average Sales Value
        sales_data = new_invoices.join(new_invoice_lines, "InvoiceID", "inner")

        avg_sales = sales_data.groupBy("InvoiceID", "Total") \
            .agg(F.sum("Quantity").alias("QuantitySum")) \
            .withColumn("AverageSalesValue", F.col("Total") / F.col("QuantitySum"))
        
        employees_data = new_employees.join(customer_satisfaction_df, new_employees["EmployeeId"] == customer_satisfaction_df["SupportRepId"], "left")\
            .select("EmployeeId", "EmployeeLastName", "EmployeeFirstName", "CustomerSatisfaction")
        
        employees_customer_satisfaction = employees_data\
            .join(customers, employees_data["EmployeeId"] == customers["SupportRepId"], "left")\
                .select("EmployeeId", "EmployeeLastName", "EmployeeFirstName", "CustomerId", "CustomerSatisfaction")
    
        final_df = employees_customer_satisfaction.join(invoices.alias("inv"), employees_customer_satisfaction["CustomerId"] == invoices["CustomerId"], "left") \
            .select("EmployeeId", "EmployeeLastName", "EmployeeFirstName", "CustomerSatisfaction", "inv.CustomerId", "InvoiceID")\
                .join(avg_sales, "InvoiceID", "left")

        # # Apply window function to rank employees by customer satisfaction
        
        # window_spec = Window.orderBy(F.desc("RepeatCustomerCount"))
        # final_df = final_df.withColumn("rank", F.rank().over(window_spec))

        final_df.show()
        # # Add Metadata
        final_df = final_df.withColumn("created_at", F.current_timestamp())\
                        .withColumn("updated_at", F.current_timestamp())\
                        .withColumn("updated_by", F.lit("Efrat"))

        final_df.toPandas().to_sql('employee_customer_satisfication', conn, if_exists='append', index=False)
        # # Insert transformed data into a new table in SQLite using KT_DB
        # # KT_DB.insert_dataframe(conn, 'final_table', final_data_df)

        # # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  
        spark.stop()

if __name__ == "__main__":
    load()