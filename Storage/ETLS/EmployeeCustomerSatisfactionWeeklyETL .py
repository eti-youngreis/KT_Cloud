from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# import KT_DB  # Assuming KT_DB is the library for SQLite operations
import sqlite3

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Employee Customer Satisfaction and Sales Daily ETL") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # conn = KT_DB.connect('/path_to_sqlite.db')
    conn = sqlite3.connect('KT_Cloud/Storage/ETLS/db/employee_customer_satisfaction.db')
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        employees = spark.read.csv("D:/boto3 project/csv files/Employee.csv", header=True, inferSchema=True)
        
        # Load other related tables
        customers = spark.read.csv("D:/boto3 project/csv files/Customer.csv", header=True, inferSchema=True)
        invoices = spark.read.csv("D:/boto3 project/csv files/Invoice.csv", header=True, inferSchema=True)
        invoice_line = spark.read.csv("D:/boto3 project/csv files/InvoiceLine.csv", header=True, inferSchema=True)

        employees = employees.withColumnRenamed("FirstName", "EmployeeFirstName")\
                        .withColumnRenamed("LastName", "EmployeeLastName")

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        repeat_customers = invoices.join(customers, "CustomerId")\
            .groupBy("SupportRepId", "CustomerId")\
                .agg(F.count("InvoiceId").alias("InvoiceCount"))\
                    .filter(F.col("InvoiceCount") > 1)

        customer_satisfaction_df = repeat_customers.groupBy("SupportRepId") \
            .agg(F.countDistinct("CustomerID").alias("CustomerSatisfaction"))

        
        # Join Invoices with InvoiceLines to calculate Average Sales Value
        sales_data = invoices.join(invoice_line, "InvoiceID", "inner")

        avg_sales = sales_data.groupBy("InvoiceID", "Total") \
            .agg(F.sum("Quantity").alias("QuantitySum")) \
            .withColumn("AverageSalesValue", F.col("Total") / F.col("QuantitySum"))
        
        employees_data = employees.join(customer_satisfaction_df, employees["EmployeeId"] == customer_satisfaction_df["SupportRepId"], "left")\
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

        # # Add Metadata
        final_df = final_df.withColumn("created_at", F.current_timestamp())\
                        .withColumn("updated_at", F.current_timestamp())\
                        .withColumn("updated_by", F.lit("Efrat"))
        
        final_df_pandas = final_df.toPandas()
        final_df_pandas.to_sql('employee_customer_satisfaction', conn, if_exists='replace', index=False)

        final_df.toPandas().to_sql('employee_customer_satisfication', conn, if_exists='replace', index=False)
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