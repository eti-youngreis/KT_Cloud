from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import sqlite3


def incremental_elt():
    spark = SparkSession.builder \
        .appName("ETL Template with KT_DB") \
        .getOrCreate()
    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('./Chinook.db')  # Connect to the SQLite database

    try:
        # Get the latest processed timestamp from the target table
        latest_timestamp_query = "SELECT MAX(created_at) FROM track_play_count_and_revenue_contribution_elt"
        latest_timestamp = KT_DB.execute_and_fetch(conn, latest_timestamp_query)
        
        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = "1900-01-01 00:00:00"  # Default for initial load
        

        
        # EXTRACT & LOAD (Load CSVs into raw tables in SQLite using KT_DB)
        # --------------------------------------------------------------
        # Load the CSVs into Spark DataFrames
        track_table = spark.read.csv("./csv/Track.csv", header=True, inferSchema=True)
        invoiceLine_table = spark.read.csv("./csv/InvoiceLine.csv", header=True, inferSchema=True)
        
        # Convert Spark DataFrames to Pandas DataFrames for SQLite insertion
        # track_table_pd = track_table.toPandas()
        # invoiceLine_table_pd = invoiceLine_table.toPandas()
        
        # # Insert the full CSV data into corresponding raw tables in SQLite
        # KT_DB.insert_dataframe(conn, "raw_Track", track_table_pd, mode="append")
        # KT_DB.insert_dataframe(conn, "raw_InvoiceLine", invoiceLine_table_pd, mode="append")
        
        joined_track_invoiceLine = track_table.join(invoiceLine_table, track_table["TrackId"] == invoiceLine_table["TrackId"], "inner")

        aggregated_data = joined_track_invoiceLine.groupBy("TrackID") \
        .agg(
            F.sum("Quantity").alias("total_play_count"),
            F.sum("Quantity * UnitPrice)").alias("revenue_contribution"),
        )
        transformed_data = aggregated_data.withColumn("rank", F.rank().over(window_spec))

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


if __name__ == "__main__":
    incremental_elt()
