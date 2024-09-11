from pyspark.sql import SparkSession  # Importing SparkSession to create a connection with Spark
from pyspark.sql import functions as F  # Importing functions from Spark for aggregation and data operations
import sqlite3  # Importing sqlite3 for working with a local database

def load():
    # Creating a Spark session with an application name
    spark = SparkSession.builder.appName("CustomerPurchaseFrequencyAndTotalSpend").getOrCreate()

    try:
        # Creating a connection to the SQLite database
        conn = sqlite3.connect('./database.db')
        
        # Reading CSV files containing customer and invoice data into Spark DataFrames
        customer_df = spark.read.csv("Customer.csv", header=True, inferSchema=True)
        invoice_df = spark.read.csv("Invoice.csv", header=True, inferSchema=True)

        # Performing a join (merging tables) between customers and invoices based on CustomerId
        customer_join_invoice = customer_df.join(invoice_df, 'CustomerId', 'inner')

        # Aggregating data - calculating the total spend and number of purchases per customer
        aggregated_data = customer_join_invoice.groupBy('CustomerId', 'FirstName', 'LastName') \
        .agg(
            F.sum('Total').alias('TotalSpend'),  # Calculating the total spend
            F.count('*').alias('NumOfPurchases')  # Counting the number of purchases
        )

        # Adding new columns - creation and update timestamps, and the updater's name
        final_data = aggregated_data.withColumn('created_at', F.current_timestamp()) \
                    .withColumn('updated_at', F.current_timestamp()) \
                    .withColumn('updated_by', F.lit('Efrat'))

        # Converting the data to a Pandas DataFrame for saving to SQLite
        date_in_pandas = final_data.toPandas()

        # Saving the data into a new table in the SQLite database
        date_in_pandas.to_sql('customer_purchase_database_and_total_spend', conn, if_exists='replace', index=False)
        conn.commit()

    finally:
        # Closing the database connection and stopping the Spark session
        conn.close()
        spark.stop()


