from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def incremental_load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Incremental ETL with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method

    try:
        # Step 3: Get the latest processed timestamp from the target table
        # Assuming KT_DB has a method to execute a query and fetch the result
        latest_timestamp_query = "SELECT MAX(created_at) FROM final_table"
        latest_timestamp = KT_DB.execute_and_fetch(conn, latest_timestamp_query)

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # A very old timestamp to ensure all data is loaded initially

        # Add here latest_timestamp query for each main source of your query

        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table with only new records (based on created_at column)
        main_table = spark.read.csv("s3://path_to_bucket/main_table.csv", header=True, inferSchema=True)
        new_data_main_table = main_table.filter(main_table["created_at"] > latest_timestamp)

        # Load other related tables with new records
        related_table_1 = spark.read.csv("s3://path_to_bucket/related_table_1.csv", header=True, inferSchema=True)
        related_table_2 = spark.read.csv("s3://path_to_bucket/related_table_2.csv", header=True, inferSchema=True)

        # Optionally, apply filtering based on created_at for related tables if necessary
        new_data_related_table_1 = related_table_1.filter(related_table_1["created_at"] > latest_timestamp)
        new_data_related_table_2 = related_table_2.filter(related_table_2["created_at"] > latest_timestamp)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the main table with related_table_1
        joined_table_1 = new_data_main_table.join(new_data_related_table_1, new_data_main_table["id"] == new_data_related_table_1["main_id"], "inner")

        # Join the result with related_table_2
        joined_table_2 = joined_table_1.join(new_data_related_table_2, joined_table_1["id"] == new_data_related_table_2["main_id"], "inner")

        # Calculate measures (e.g., total spend, average values)
        aggregated_data = joined_table_2.groupBy("customer_id") \
            .agg(
                F.sum("spend").alias("total_spend"),
                F.avg("spend").alias("avg_spend")
            )

        # Apply window function (e.g., rank customers by spend)
        window_spec = Window.partitionBy().orderBy(F.desc("total_spend"))
        transformed_data = aggregated_data.withColumn("rank", F.rank().over(window_spec))

        # Calculate metadata (e.g., date of the last transaction)
        final_data = transformed_data.withColumn("last_transaction_date", F.max("transaction_date").over(window_spec))

        # LOAD (Append new transformed data to the SQLite database using KT_DB)
        # ---------------------------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_data.toPandas()

        # Insert transformed data into the SQLite database (append mode) using KT_DB
        KT_DB.insert_dataframe(conn, 'final_table', final_data_df, mode='append')  # Assuming 'append' mode is supported

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 4: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()

        from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
def incremental_load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Incremental ETL - Employee Sales and Customer Interactions") \
        .getOrCreate()
    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database
    try:
        # Step 3: Get the latest processed timestamp from the target table
        latest_timestamp_query = "SELECT MAX(created_at) FROM employee__sales_and_count_count"
        latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]
        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01'  # A very old date to ensure all data is loaded initially
        # Step 4: Load CSV files into Spark DataFrames
        employees_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Employee.csv', header=True, inferSchema=True)
        customers_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Customer.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv('C:/Users/Owner/Desktop/ETL/Invoice.csv', header=True, inferSchema=True)
        # Step 5: Filter data based on the latest_timestamp
        new_invoices_df = invoices_df.filter(F.col('InvoiceDate') > latest_timestamp)
        new_customers_df = customers_df.filter(F.col('create_at') > latest_timestamp)
        # Step 6: Select only employees associated with the updated/new customers or invoices
        updated_customers_ids = new_customers_df.select('CustomerId').distinct()#שולפת את כל הלקוחות שהוספו או עודכנו
        customers_with_new_invoices = new_invoices_df.select('CustomerId').distinct()#שולפת את כל הלקוחות עם קניות חדשות או קניות שעודכנו
        # Step 7: Combine the customer sets to get the affected employees
        relevant_customers_ids = updated_customers_ids.union(customers_with_new_invoices).distinct()#מצרפת את הלקוחות שעודכנו עם הלקוחות שהנוסף או עודכן להם קניה
        # Join Employees with Customers who they support and that have been updated
        employees_join_customers = employees_df.join(customers_df, employees_df['EmployeeId'] == customers_df['SupportRepId'], "inner") \
            .select(
                employees_df["EmployeeId"],
                employees_df["FirstName"].alias("EmployeeFirstName"),
                employees_df["LastName"].alias("EmployeeLastName"),
                customers_df["CustomerId"]
            )
        affected_employees_df = employees_join_customers.join(relevant_customers_ids, 'CustomerId', "inner")
          # Join customers with their invoices
        customers_invoices = new_invoices_df.join(affected_employees_df, 'CustomerId', "inner")
        # Aggregate sales performance per employee
        employees_sales_df = customers_invoices.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
                                .agg(F.sum(F.col("Total")).alias("TotalSales"))
        # Count customer interactions per employee
        employees_customers_df = affected_employees_df.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
                                    .agg(F.count("CustomerId").alias("CustomersInteractions"))
        # Combine sales and customer interactions data
        final_employee_df = employees_sales_df.join(employees_customers_df, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"]) \
                                      .withColumn("created_at", F.current_date()) \
                                      .withColumn("updated_at", F.current_date()) \
                                      .withColumn("updated_by", F.lit(f"process:yael_karo{F.current_date()}")).toPandas()
        # Step 12: Delete existing rows before inserting new data
        employee_ids = final_employee_df['EmployeeId'].tolist()
        placeholders = ', '.join('?' for _ in employee_ids)
        delete_query = f"DELETE FROM employee__sales_and_count_count WHERE EmployeeId IN ({placeholders})"
        conn.execute(delete_query, employee_ids)
        # Step 13: Save combined employee data to SQLite (append mode)
        final_employee_df.to_sql('employee__sales_and_count_count', conn, if_exists='append', index=False)
        # Commit the changes to the database
        # Commit the changes to the database
        conn.commit()
        # Print the latest results for verification
        print("New employee performance records:", conn.execute("SELECT * FROM 'employee__sales_and_count_count'").fetchall())
    finally:
        # Step 13: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session
if __name__ == "__main__":
    incremental_load()