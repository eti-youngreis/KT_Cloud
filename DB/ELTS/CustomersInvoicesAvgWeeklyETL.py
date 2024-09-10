from pyspark.sql import SparkSession
import sqlite3
def load_average_purchase_value_elt():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template without KT_DB") \
        .getOrCreate()
    # Step 2: Establish SQLite connection using sqlite3
    conn = sqlite3.connect('./Chinook.db')  # Connect to the SQLite database
    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main table (e.g., Customers, Invoices, InvoiceLines)
        customers_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Customer.csv', header=True, inferSchema=True)
                # Load other related tables
        invoices_df = spark.read.csv(
            'C:/Users/Owner/Documents/vast_data/KT_Cloud/database/Invoice.csv', header=True, inferSchema=True)
        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        customers_pd = customers_df.toPandas()
        invoices_pd = invoices_df.toPandas()
        # Load raw data into SQLite
        customers_pd.to_sql('Customers', conn, if_exists='replace', index=False)
        invoices_pd.to_sql('Invoices', conn, if_exists='replace', index=False)
        # TRANSFORM (Perform transformations with SQL queries)
        # ----------------------------------------------------
        # Join tables and calculate total purchase and average purchase per customer
        # Apply the transformations using SQLite SQL queries
        drop_query="""DROP TABLE IF EXISTS customer_invoice_avg_elt"""
        conn.execute(drop_query)
        conn.commit()
        transform_query = """
            CREATE TABLE customer_invoice_avg_elt AS 
            SELECT c.CustomerId,strftime('%m', i.InvoiceDate)  as InvoiceMonth,
                   AVG(i.Total) AS avg_spend
            FROM Customers c
            JOIN Invoices i ON c.CustomerId = i.CustomerId
            GROUP BY c.CustomerId,InvoiceMonth
        """
        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()
        print("customer_invoice_avg:", conn.execute("SELECT * FROM 'customer_invoice_avg_elt'").fetchall())
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session
if __name__ == "__main__":
    load_average_purchase_value_elt()