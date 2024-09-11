from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_employees():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Employee Sales Performance and Customer Interactions Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('./Chinook.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        employees_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Employee.csv', header=True, inferSchema=True)
        # Load other related tables
        customers_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Customer.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Invoice.csv', header=True, inferSchema=True)
        invoiceLines_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/InvoiceLine.csv', header=True, inferSchema=True)
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
        # Join the result with related_table_2
        # joined_table_2 = joined_table_1.join(related_table_2, joined_table_1["id"] == related_table_2["main_id"], "inner")
        # Calculate measures (e.g., total spend, average values)
        employees_sales_df = customers_invoices.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
            .agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("TotalSales"))
        employees_customers_df = employees_join_customers.groupBy("EmployeeId", "EmployeeFirstName", "EmployeeLastName") \
            .agg(F.count("CustomerId").alias("CustomersInteractions"))
        final_employee_sales_df = employees_sales_df.join(employees_customers_df, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"]) \
            .withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"process:yael_karo_{F.current_date()}")).toPandas()

        final_employee_customers_df = employees_customers_df.join(employees_customers_df, ["EmployeeId", "EmployeeFirstName", "EmployeeLastName", "CustomersInteractions"]) \
            .withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"process:yael_karo_{F.current_date()}")).toPandas()

        final_employee_combined_df = employees_sales_df.join(employees_customers_df, 
            ["EmployeeId", "EmployeeFirstName", "EmployeeLastName"]
            ).withColumn("created_at", F.current_date()) \
             .withColumn("updated_at", F.current_date()) \
             .withColumn("updated_by", F.lit(f"process:yael_karo_{F.current_date()}")).toPandas()
        # Apply window function (e.g., rank customers by spend)
        # customers_invoices = invoices_df.join(invoiceLines_df, 'InvoiceId', "inner").join(customers_df, 'CustomerId', "inner").withColumn("TotalPurchase",F.sum(F.col("UnitPrice") * F.col("Quantity"))).withColumn("InvoiceMounth", F.month(invoices_df["InvoiceDate"])).groupBy("CustomerId", "InvoiceMounth") \
        # .agg(F.avg(F.col("TotalPurchase")).alias("avgCustomerInvoices"))

# חיבור הנתונים וחישוב סיכומים לפי לקוח וחודש
        # customers_invoices = invoices_df \
        #     .join(invoiceLines_df, 'InvoiceId', "inner") \
        #     .join(customers_df, 'CustomerId', "inner") \
        #     .withColumn("TotalPurchase", F.col("UnitPrice") * F.col("Quantity")) \
        #     .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
        #     .groupBy("CustomerId", "InvoiceMonth") \
        #     .agg(F.sum("TotalPurchase").alias("TotalSales")).agg(F.avg("TotalPurchase").alias("avgCustomerInvoices"))
        # customers_invoices = invoices_df \
        #     .join(invoiceLines_df, 'InvoiceId', "inner") \
        #     .join(customers_df, 'CustomerId', "inner") \
        #     .withColumn("TotalPurchase", F.col("UnitPrice") * F.col("Quantity")) \
        #     .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
        #     .groupBy("CustomerId", "InvoiceMonth") \
        #     .agg(F.avg("Total").alias("avgCustomerInvoices"))

        invoice_totals = invoiceLines_df \
            .withColumn("ItemTotal", F.col("UnitPrice") * F.col("Quantity")) \
            .groupBy("InvoiceId") \
            .agg(F.sum("ItemTotal").alias("InvoiceTotal"))

        # הצטרפות הנתונים ממספר טבלאות
        customers_invoices = invoices_df \
            .join(invoice_totals, 'InvoiceId', "inner") \
            .join(customers_df, 'CustomerId', "inner") \
            .withColumn("InvoiceMonth", F.month(invoices_df["InvoiceDate"])) \
            .groupBy("CustomerId", "InvoiceMonth") \
            .agg(F.avg("InvoiceTotal").alias("avgCustomerInvoices"))  # חישוב ממוצע הקניות ללקוח בכל חודש

        window_spec = Window.partitionBy().orderBy(F.desc("avgCustomerInvoices"))
        transformed_data = customers_invoices.withColumn(
            "rank", F.rank().over(window_spec))
        transformed_data.show(200)
        transformed_data=transformed_data.toPandas()
        # # Calculate metadata (e.g., date of the last transaction)
        # final_data = transformed_data.withColumn("last_transaction_date", F.max("transaction_date").over(window_spec))

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        # final_data_df = final_data.toPandas()

        # Insert transformed data into a new table in SQLite using KT_DB
        transformed_data.to_sql(
            'customer_invices_avg', conn, if_exists='replace', index=False)
        final_employee_combined_df.to_sql(
            'final_employee_combined', conn, if_exists='replace', index=False)
        # final_employee_customers_df.to_sql(
        #     'employee_customers', conn, if_exists='replace', index=False)
        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()
        print("final_employee_combined", conn.execute(
            "SELECT * FROM 'final_employee_combined'").fetchall())
        print("customer_invices_avg", conn.execute(
            "SELECT * FROM 'customer_invices_avg'").fetchall())

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()
        print("finally")


if __name__ == "__main__":
    load_employees()
    print("main f")
