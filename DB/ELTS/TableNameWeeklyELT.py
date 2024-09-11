from pyspark.sql import SparkSession
import KT_DB  # Assuming KT_DB is the library for SQLite operations

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template with KT_DB") \
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

        # LOAD (Save the raw data into SQLite using KT_DB without transformation)
        # -----------------------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        main_table_df = main_table.toPandas()
        related_table_1_df = related_table_1.toPandas()
        related_table_2_df = related_table_2.toPandas()


        KT_DB.insert_dataframe(conn, 'main_table', main_table_df)  # Insert raw main_table
        KT_DB.insert_dataframe(conn, 'related_table_1', related_table_1_df)  # Insert raw related_table_1
        KT_DB.insert_dataframe(conn, 'related_table_2', related_table_2_df)  # Insert raw related_table_2

        # TRANSFORM (Perform transformations with SQL queries using KT_DB functions)
        # -------------------------------------------------------------------------
        # Example Transformation 1: Join tables and calculate total spend and average
        transform_query_1 = """
            CREATE TABLE final_table AS
            SELECT mt.customer_id,
                   SUM(rt1.spend) AS total_spend,
                   AVG(rt1.spend) AS avg_spend,
                   MAX(rt2.transaction_date) AS last_transaction_date
            FROM main_table mt
            JOIN related_table_1 rt1 ON mt.id = rt1.main_id
            JOIN related_table_2 rt2 ON mt.id = rt2.main_id
            GROUP BY mt.customer_id
        """

        # Execute the transformation query using KT_DB's execute() method
        KT_DB.execute(conn, transform_query_1)

        # Commit the changes to the database using KT_DB's commit() function
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        KT_DB.close(conn)  # Assuming KT_DB has a close() method
        spark.stop()
