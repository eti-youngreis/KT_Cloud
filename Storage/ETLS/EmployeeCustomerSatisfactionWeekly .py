from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# import KT_DB  # Assuming KT_DB is the library for SQLite operations
import sqlite3

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/etl_db.db')
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        employees = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Employee.csv", header=True, inferSchema=True)

        # Load other related tables
        customers = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/Invoice.csv", header=True, inferSchema=True)
        invoice_line = spark.read.csv("KT_Cloud/Storage/ETLS/csv files/InvoiceLine.csv", header=True, inferSchema=True)

        customer_invoices_df = invoices.join(customers, "CustomerId")
        employees = employees.withColumnRenamed("FirstName", "EmployeeFirstName")\
                        .withColumnRenamed("LastName", "EmployeeLastName")

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        repeat_customers = invoices.join(customers, "CustomerId")\
            .groupBy("SupportRepId", "CustomerId")\
                .agg(F.count("InvoiceId").alias("InvoiceCount"))\
                    .filter(F.col("InvoiceCount") > 1)

        customer_satisfaction_df = employees.join(repeat_customers, employees["EmployeeId"] == repeat_customers["SupportRepId"], "left")\
            .groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName")\
                    .agg(F.count("CustomerId").alias("RepeatCustomerCount"))
        
        # Join InvoiceLine with Invoice to calculate sales value per invoice          
        avg_sales_df = invoice_line.groupBy("InvoiceId")\
            .agg(F.avg(F.col("UnitPrice") * F.col("Quantity")).alias("AvgSalesValue"))
        
        # Join Invoices with the calculated Average Sales and with Employees
        avg_sales_by_employee = customer_invoices_df.join(avg_sales_df, "InvoiceId")\
            .join(employees, customer_invoices_df["SupportRepId"] == employees["EmployeeId"], "left")\
                .groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName")\
                    .agg(F.avg("AvgSalesValue").alias("AvgSalesPerInvoice"))
                    
        # Combine both Customer Satisfaction and Average Sales Value
        final_df = customer_satisfaction_df.join(avg_sales_by_employee, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"], "left")

        # Apply window function to rank employees by customer satisfaction
        
        window_spec = Window.orderBy(F.desc("RepeatCustomerCount"))
        final_df = final_df.withColumn("rank", F.rank().over(window_spec))

        # Add Metadata
        final_df = final_df.withColumn("created_at", F.current_timestamp())\
                        .withColumn("updated_at", F.current_timestamp())\
                        .withColumn("updated_by", F.lit("process:user_name"))

        
        #  Show the result
        final_df.show(200)
        final_df_pandas = final_df.toPandas()
        final_df_pandas.to_sql('employee_customer_satisfaction', conn, if_exists='replace', index=False)


        # Insert transformed data into a new table in SQLite using KT_DB
        # KT_DB.insert_dataframe(conn, 'final_table', final_data_df)

        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  
        spark.stop()

load()