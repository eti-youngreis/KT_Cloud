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