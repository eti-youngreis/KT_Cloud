from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3

# import KT_DB  # Assuming KT_DB is the library for SQLite operations
def load_employee_sales_and_customer_etl():
        # Step 1: Initialize Spark session
        spark = SparkSession.builder \
            .appName("YourAppName") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        # Step 2: Establish SQLite connection using KT_DB
        # Assuming KT_DB has a connect() method
        conn = sqlite3.connect('./Chinook.db')
        try:
            # Check if the table exists
            check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='employee_sales_and_count';"
            table_exists = conn.execute(check_table_query).fetchone()
            latest_timestamp=None
            if table_exists:
                latest_timestamp_query = "SELECT MAX(updated_at) FROM 'employee_sales_and_count'"
                latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]
            # Handle case where no data exists yet (initial load)
            if latest_timestamp is None:
                latest_timestamp = '1900-01-01'
            # EXTRACT (Loading CSVs from S3 or local storage)
            # -----------------------------------------------
            # Load the main table (e.g., customers, tracks, albums)
            print(latest_timestamp,"lllll")

            employees_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
            customers_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Customer.csv', header=True, inferSchema=True)
            invoices_df = spark.read.csv('C:/Users/shana/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
            # TRANSFORM (Apply joins, groupings, and window functions)
            # --------------------------------------------------------
            invoices_df = invoices_df.withColumn('InvoiceDate', F.to_date(F.col('InvoiceDate'), 'dd/MM/yyyy'))
            customers_df = customers_df.withColumn('created_at', F.to_date(F.col('created_at'), 'dd/MM/yyyy'))
            employees_df = employees_df.withColumn('created_at', F.to_date(F.col('created_at'), 'dd/MM/yyyy'))


            new_employees_df = employees_df.filter(employees_df["created_at"] > latest_timestamp)
            new_customers_df = customers_df.filter(customers_df["created_at"] > latest_timestamp)
            new_invoices_df = invoices_df.filter(invoices_df["InvoiceDate"] > latest_timestamp)

            custumer_in_new_invoices_df=new_invoices_df[["CustomerId"]].distinct()

            customers_connect_to_invoices=custumer_in_new_invoices_df.join(customers_df,"CustomerId","inner")

            customers_update=customers_connect_to_invoices.union(new_customers_df).distinct()
            print("customers_update:",customers_update.show())

            employees_new_custommer_df=customers_update[["SupportRepId"]].distinct()

            employees_connect_to_customer=employees_new_custommer_df.join(employees_df,employees_df['EmployeeId'] == employees_new_custommer_df['SupportRepId'],"inner").drop('SupportRepId')

            employees_update=employees_connect_to_customer.union(new_employees_df).distinct()

            employees_update = employees_update.alias("emp")
            customers_update = customers_update.alias("cust")
            employees_df = employees_df.alias("emp_df")
            customers_df = customers_df.alias("cust_df")
            invoices_df = invoices_df.alias("inv")

            employees_join_customers = employees_update.join(
                customers_df, employees_update['EmployeeId'] == customers_df['SupportRepId'], "inner"
            ).select(
                F.col("emp.EmployeeId"),
                F.col("emp.FirstName").alias("EmployeeFirstName"),
                F.col("cust_df.FirstName").alias("CustomerFirstName"),
                F.col("emp.LastName").alias("EmployeeLastName"),
                F.col("cust_df.LastName").alias("CustomerLastName"),
                F.col("cust_df.CustomerId")
            )

            customers_invoices = invoices_df.join(
                employees_join_customers, 'CustomerId', "inner")
            
            employees_sales_df = customers_invoices.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
                .agg(F.sum(customers_invoices["Total"]).alias("TotalSales"))
            
            employees_customers_df = employees_join_customers.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
                .agg(F.count("CustomerId").alias("CustomersInteractions"))
            
            final_employee_combined_df = employees_sales_df.join(employees_customers_df,
                                                                ["EmployeeId", "EmployeeFirstName",
                                                                    "EmployeeLastName"]
                                                                ).withColumn("created_at", F.current_date()) \
                .withColumn("updated_at", F.current_date()) \
                .withColumn("updated_by", F.lit(f"process:shoshana_levovitz_{F.current_date()}")).toPandas()
            # Apply window function (e.g., rank customers by spend)
            # LOAD (Save transformed data into SQLite using KT_DB)
            # ----------------------------------------------------
            # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
            # Insert transformed data into a new table in SQLite using KT_D
            employee_ids_to_delete = final_employee_combined_df['EmployeeId'].tolist()
            if employee_ids_to_delete:
                ids_placeholder = ', '.join('?' * len(employee_ids_to_delete))
                delete_query = f"DELETE FROM 'employee_sales_and_count' WHERE EmployeeId IN ({ids_placeholder})"
                conn.execute(delete_query, employee_ids_to_delete)

            final_employee_combined_df.to_sql(
                'employee_sales_and_count', conn, if_exists='append', index=False)
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
