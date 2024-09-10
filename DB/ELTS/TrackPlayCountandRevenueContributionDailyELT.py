import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime
import pandas as pd


def get_last_processed_timestamp(cursor):
    cursor.execute("SELECT MAX(updated_at) FROM Track_Play_Count_and_Revenue_Contribution")
    result = cursor.fetchone()
    return result[0] if result[0] is not None else '1900-01-01 00:00:00'

def incrementel_load():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Track Play Count and Revenue Contribution - ELT") \
        .getOrCreate()

    # Step 2: Establish SQLite connection
    conn = sqlite3.connect('DB/ETLS/elt_db.db')
    cursor = conn.cursor()

    # Get the timestamp of the last processed data
    last_processed_timestamp = get_last_processed_timestamp(cursor)
    print(f"Last processed timestamp: {last_processed_timestamp}")

    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        Tracks = spark.read.csv('DB/ETLS/data/Track.csv', header=True, inferSchema=True)
        InvoiceLines = spark.read.csv('DB/ETLS/data/InvoiceLine.csv', header=True, inferSchema=True)

        # Filter new or updated rows based on timestamp
        Tracks = Tracks.filter(col("created_at") > last_processed_timestamp)
        InvoiceLines = InvoiceLines.filter(col("created_at") > last_processed_timestamp)

        # Save new or updated data to SQLite
        Tracks.toPandas().to_sql('Tracks', conn, if_exists='replace', index=False)
        InvoiceLines.toPandas().to_sql('InvoiceLines', conn, if_exists='replace', index=False)

        # TRANSFORM (SQL query for aggregation)
        query = """
        SELECT t.TrackId, t.Name,
               SUM(il.Quantity) AS total_play_count,
               SUM(il.Quantity * il.UnitPrice) AS revenue_contribution
        FROM InvoiceLines il
        JOIN Tracks t ON t.TrackId = il.TrackId
        GROUP BY t.TrackId, t.Name
        """
        transformed_data = pd.read_sql_query(query, conn)
        # transformed_data.select(
        #     "TrackId", "Name", "total_play_count", "revenue_contribution"
        # ).show(truncate=False)
        # Add metadata columns
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_name = "tamar_koledetzky"  # Replace with a dynamic method if needed

        transformed_data['created_at'] = current_datetime
        transformed_data['updated_at'] = current_datetime
        transformed_data['updated_by'] = user_name

        # Store the transformed data back into the SQLite database
        transformed_data.to_sql('Track_Play_Count_and_Revenue_Contribution', conn, if_exists='append', index=False)

        # Query and print the contents of the table
        cursor.execute("SELECT * FROM Track_Play_Count_and_Revenue_Contribution LIMIT 10;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

        # Load the data back into Spark DataFrame for visualization (optional)
        # transformed_df = spark.createDataFrame(transformed_data)
        # Display the result using show()
        # transformed_df.show(truncate=False)

    finally:
        # Step 3: Close SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incrementel_load()
