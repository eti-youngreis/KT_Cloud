from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
# import KT_DB  # Assuming KT_DB is the library for SQLite operations


def load_customer_invoices_count_etl():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL - Average Purchase Value Over Time by Customer Type") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    # Assuming KT_DB has a connect() method
    conn = sqlite3.connect('./Chinook.db')

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        customers_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Customer.csv', header=True, inferSchema=True)
                # Load other related tables
        invoices_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Invoice.csv', header=True, inferSchema=True)
        invoiceLines_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/InvoiceLine.csv', header=True, inferSchema=True)
        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the main table with related_table_1
        # Join the result with related_table_2
        # joined_table_2 = joined_table_1.join(related_table_2, joined_table_1["id"] == related_table_2["main_id"], "inner")
        # Calculate measures (e.g., total spend, average values)

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
        transformed_data=transformed_data.toPandas()
        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        # final_data_df = final_data.toPandas()

        # Insert transformed data into a new table in SQLite using KT_DB
        transformed_data.to_sql(
            'customer_invices_avg', conn, if_exists='replace', index=False)
        # Commit the changes to the database using KT_DB's commit() function
        conn.commit()
        print("customer_invices_avg", conn.execute(
            "SELECT * FROM 'customer_invices_avg'").fetchall())

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Assuming KT_DB has a close() method
        spark.stop()


if __name__ == "__main__":
    load_customer_invoices_count_etl()
