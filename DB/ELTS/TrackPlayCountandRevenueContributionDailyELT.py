import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime
import pandas as pd


def get_last_processed_timestamp(cursor):
    cursor.execute("SELECT MAX(updated_at) FROM Track_Play_Count_and_Revenue_Contribution")
    result = cursor.fetchone()
    return result[0] if result[0] is not None else '1900-01-01 00:00:00'

def incremental_load():
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
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        Tracks = spark.read.csv('DB/ETLS/data/Track.csv', header=True, inferSchema=True)
        InvoiceLines = spark.read.csv('DB/ETLS/data/InvoiceLine.csv', header=True, inferSchema=True)

        # Filter new or updated rows based on timestamp
        Tracks = Tracks.filter(col("updated_at") > last_processed_timestamp)
        InvoiceLines = InvoiceLines.filter(col("updated_at") > last_processed_timestamp)

        # Save new or updated data to SQLite
        Tracks.toPandas().to_sql('Tracks', conn, if_exists='replace', index=False)
        InvoiceLines.toPandas().to_sql('InvoiceLines', conn, if_exists='replace', index=False)

        # TRANSFORM (SQL query for aggregation)
        query = """
        CREATE TEMP TABLE TempDeletedRowsInfo AS
        SELECT t.TrackId, t.Name,
               SUM(il.Quantity) AS total_play_count,
               SUM(il.Quantity * il.UnitPrice) AS revenue_contribution
        FROM InvoiceLines il
        JOIN Tracks t ON t.TrackId = il.TrackId
        GROUP BY t.TrackId, t.Name
        """
        conn.execute(query)
        conn.commit()

        # DELETE existing rows from the main table based on the temp table
        query_delete = """
        DELETE FROM Track_Play_Count_and_Revenue_Contribution
        WHERE TrackId IN (SELECT TrackId FROM TempDeletedRowsInfo)
        """
        conn.execute(query_delete)
        conn.commit()

        # INSERT new data into the main table from the temp table
        query_insert = """
        INSERT INTO Track_Play_Count_and_Revenue_Contribution (TrackId, Name, total_play_count, revenue_contribution)
        SELECT TrackId, Name, total_play_count, revenue_contribution
        FROM TempDeletedRowsInfo
        """
        conn.execute(query_insert)
        conn.commit()

        # Fetch the data from the temp table to update with metadata
        transformed_data = pd.read_sql_query("SELECT * FROM TempDeletedRowsInfo", conn)

        # Add metadata columns
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_name = "tamar_koledetzky"

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

    finally:
        # Step 3: Close SQLite connection and stop Spark session
        conn.close()
        spark.stop()

if __name__ == "__main__":
    incremental_load()
