from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = KT_DB.connect('/path_to_sqlite.db')  # Assuming KT_DB has a connect() method

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        # Load the main table (e.g., customers, tracks, albums)
        main_table = spark.read.csv("s3://path_to_bucket/main_table.csv", header=True, inferSchema=True)

        # Load other related tables
        related_table_1 = spark.read.csv("s3://path_to_bucket/related_table_1.csv", header=True, inferSchema=True)
        related_table_2 = spark.read.csv("s3://path_to_bucket/related_table_2.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        # Join the main table with related_table_1
        joined_table_1 = main_table.join(related_table_1, main_table["id"] == related_table_1["main_id"], "inner")

        # Join the result with related_table_2
        joined_table_2 = joined_table_1.join(related_table_2, joined_table_1["id"] == related_table_2["main_id"], "inner")

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

        # LOAD (Save transformed data into SQLite using KT_DB)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_data.toPandas()

        # Insert transformed data into a new table in SQLite using KT_DB
        KT_DB.insert_dataframe(conn, 'final_table', final_data_df)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()
