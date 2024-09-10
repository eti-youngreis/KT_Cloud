from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3


def load():
    spark = SparkSession.builder.appName("CustomerPurchaseFrequencyAndTotalSpend").getOrCreate()

    try:
        conn = sqlite3.connect('./database.db')
        customer = spark.read.csv("Customer.csv", header=True, inferSchema=True)
        invoice = spark.read.csv("Invoice.csv", header=True, inferSchema=True)


        customer_join_invoice = customer.join(invoice,'CustomerId','inner')

        aggregated_data = customer_join_invoice.groupBy('CustomerId', 'FirstName', 'LastName') \
        .agg(
            F.sum('Total').alias('TotalSpend'),
            F.count('*').alias('NumOfPurchases')
        )

        final_data = aggregated_data.withColumn('created_at', F.current_timestamp()) \
                    .withColumn('updated_at', F.current_timestamp()) \
                    .withColumn('updated_by', F.lit('Efrat'))

        date_in_pandas = final_data.toPandas()

        date_in_pandas.to_sql('customer_purchase_database_and_total_spend', conn, if_exists='replace', index=False)
        conn.commit()

        

    finally:
        conn.close()
        spark.stop()






load()