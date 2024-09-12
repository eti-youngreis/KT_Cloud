import sqlite3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

def incremental_load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Incremental ELT Template with SQLite") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('DB/ETLS/etl_db.db')
    cursor = conn.cursor()

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        Tracks = spark.read.csv('DB/ETLS/data/Track.csv', header=True, inferSchema=True)
        InvoiceLines = spark.read.csv('DB/ETLS/data/InvoiceLine.csv', header=True, inferSchema=True)

        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # הוספת עמודת 'created_at' לטבלאות המקור
        # Tracks = Tracks.withColumn("created_at", F.lit(current_datetime))
        # InvoiceLines = InvoiceLines.withColumn("created_at", F.lit(current_datetime))
        # Get last update time from the database to filter incremental data
        cursor.execute("SELECT MAX(updated_at) FROM Track_Play_Count_and_Revenue_Contribution")
        last_update_time = cursor.fetchone()[0]

        if last_update_time:
            last_update_time = datetime.strptime(last_update_time, '%Y-%m-%d %H:%M:%S')
            print(f"Last update time: {last_update_time}")
        else:
            print("No previous data found, performing full load.")

        # Filter only new/updated rows based on the last update time
        if last_update_time:
            Tracks = Tracks.filter(F.col("updated_at") > F.lit(last_update_time))
            InvoiceLines = InvoiceLines.filter(F.col("updated_at") > F.lit(last_update_time))

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        joined_Tracks_InvoiceLines = Tracks.join(
            InvoiceLines, Tracks["TrackId"] == InvoiceLines["TrackId"], "inner"
        ).drop(InvoiceLines["TrackId"], InvoiceLines["UnitPrice"])

        aggregated_data = joined_Tracks_InvoiceLines.groupBy("TrackId", "Name") \
            .agg(
                F.sum("Quantity").alias("total_play_count"),
                F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("revenue_contribution")
            )
        aggregated_data.select(
            "TrackId", "Name", "total_play_count", "revenue_contribution"
        ).show(truncate=False)

        # Add metadata columns
        # current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_name = "tamar_koledetzky"  # Replace with a method to fetch the current user's name if needed

        final_data_with_metadata = aggregated_data.withColumn("created_at", F.lit(current_datetime)) \
                                                  .withColumn("updated_at", F.lit(current_datetime)) \
                                                  .withColumn("updated_by", F.lit(user_name))

        # LOAD (Save transformed data into SQLite incrementally)
        # ----------------------------------------------------
        final_data_df = final_data_with_metadata.toPandas()





        # if final_data_df.empty:
        #     print("No new data to process.")
        # else:
        #     # Create table with metadata columns if it doesn't exist
        #     create_table_sql = """
        #     CREATE TABLE IF NOT EXISTS Track_Play_Count_and_Revenue_Contribution (
        #         TrackId INT,
        #         Name TEXT,
        #         total_play_count INT,
        #         revenue_contribution FLOAT,
        #         created_at TEXT,
        #         updated_at TEXT,
        #         updated_by TEXT
        #     )
        #     """
        #     cursor.execute(create_table_sql)

            # Insert transformed data into SQLite
        final_data_df.to_sql('Track_Play_Count_and_Revenue_Contribution', conn, if_exists='append', index=False)
        # Commit the changes to the database
        conn.commit()
        # Query and print the contents of the table
        cursor.execute("SELECT * FROM Track_Play_Count_and_Revenue_Contribution LIMIT 10;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load()


