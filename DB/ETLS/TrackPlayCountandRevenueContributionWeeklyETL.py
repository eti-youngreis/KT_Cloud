import sqlite3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# from pyspark.sql.window import Window
from datetime import datetime

def load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Template with SQLite") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('DB/ETLS/etl_db.db')
    cursor = conn.cursor()

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        Tracks = spark.read.csv('DB/ETLS/data/Track.csv', header=True, inferSchema=True)
        InvoiceLines = spark.read.csv('DB/ETLS/data/InvoiceLine.csv', header=True, inferSchema=True)

        # TRANSFORM (Apply joins, groupings, and window functions)
        # --------------------------------------------------------
        #  Tracks["TrackId"] == InvoiceLines["TrackId"], "inner"
        joined_Tracks_InvoiceLines = Tracks.join(InvoiceLines, Tracks["TrackId"] == InvoiceLines["TrackId"], "inner").drop(InvoiceLines["TrackId"],InvoiceLines["UnitPrice"] )

        aggregated_data = joined_Tracks_InvoiceLines.groupBy("TrackId","Name") \
            .agg(
                
                F.sum("Quantity").alias("total_play_count"),
                F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("revenue_contribution")
            )

        # window_spec = Window.partitionBy()
        # transformed_data = aggregated_data.withColumn("rank", F.rank().over(window_spec))
        # final_data = transformed_data.withColumn("last_transaction_date", F.max("transaction_date").over(window_spec))

        # Add metadata columns
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_name = "tamar_koledetzky"  # Replace this with a method to fetch the current user's name if needed

        final_data_with_metadata = aggregated_data.withColumn("created_at", F.lit(current_datetime)) \
                                             .withColumn("updated_at", F.lit(current_datetime)) \
                                             .withColumn("updated_by", F.lit(user_name))

        # LOAD (Save transformed data into SQLite)
        # ----------------------------------------------------
        # Convert Spark DataFrame to Pandas DataFrame for SQLite insertion
        final_data_df = final_data_with_metadata.toPandas()
        # Create table with metadata columns if not exists
        if final_data_df.empty:
            print("DataFrame is empty. No data to write.")
        else:
             create_table_sql = """
        CREATE TABLE IF NOT EXISTS Track_Play_Count_and_Revenue_Contribution (
            TrackId INT,
            Name TEXT,
            total_play_count INT,
            revenue_contribution FLOAT,
            created_at TEXT,
            updated_at TEXT,
            updated_by TEXT
        )
        """
        cursor.execute(create_table_sql)
            # rank INT,
        # Insert transformed data into SQLite
        final_data_df.to_sql('Track_Play_Count_and_Revenue_Contribution', conn, if_exists='replace', index=False)

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
    load()

