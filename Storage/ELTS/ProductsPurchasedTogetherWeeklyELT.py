from pyspark.sql import SparkSession 
import sqlite3  

# Function to perform ELT process: load data, save to SQLite, and perform transformations
def load():
    # Step 1: Initialize Spark session - This creates a Spark session for processing large datasets
    spark = SparkSession.builder \
        .appName("ELT Template with SQLite") \
        .getOrCreate() 
    
    # Step 2: Establish SQLite connection - This connects to an SQLite database file, creating it if it doesn't exist
    conn = sqlite3.connect('TrackCombinations.db')

    # Define the path to the folder containing the CSV files
    path = "D:\\בוטקאמפ\\Vast Project\\EmployeeTables\\"
    
    try:
        # EXTRACT (Loading CSVs from local storage)
        # -----------------------------------------------
        # Load the main data tables from CSV files into Spark DataFrames
        # InferSchema=True automatically infers the column data types
        invoice_line_table = spark.read.csv(f'{path}InvoiceLine.csv', header=True, inferSchema=True)
        album_table = spark.read.csv(f'{path}Album.csv', header=True, inferSchema=True)
        track_table = spark.read.csv(f'{path}Track.csv', header=True, inferSchema=True)

        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Convert Spark DataFrames to Pandas DataFrames for SQLite insertion
        invoice_line_table_df = invoice_line_table.toPandas()
        album_table_df = album_table.toPandas()
        track_table_df = track_table.toPandas()

        # Insert the DataFrames into SQLite as tables. if_exists='replace' means that the table will be overwritten if it already exists
        invoice_line_table_df.to_sql('invoice_line_table', conn, if_exists='replace', index=False)
        album_table_df.to_sql('album_table', conn, if_exists='replace', index=False)
        track_table_df.to_sql('track_table', conn, if_exists='replace', index=False)

        # TRANSFORM (Perform transformations using SQL queries in SQLite)
        # -------------------------------------------------------------------------
        # Drop the table if it exists to ensure a clean transformation process
        conn.execute("DROP TABLE IF EXISTS final_table")

        # SQL transformation: 
        # 1. Join the invoice_line, track, and album tables
        # 2. Calculate track pairs that appear in the same invoice and count their frequency
        transform_query = """
            CREATE TABLE final_table AS
            WITH invoice_details AS (
                SELECT il.InvoiceId,
                       il.TrackId,
                       t.AlbumId,
                       a.Title AS AlbumTitle,
                       t.Name AS TrackName
                FROM invoice_line_table il
                JOIN track_table t ON il.TrackId = t.TrackId
                JOIN album_table a ON t.AlbumId = a.AlbumId
            ),
            track_pairs AS (
                SELECT id1.TrackId AS TrackId1,
                       id2.TrackId AS TrackId2,
                       id1.AlbumId AS AlbumId1,
                       id2.AlbumId AS AlbumId2
                FROM invoice_details id1
                JOIN invoice_details id2 
                    ON id1.InvoiceId = id2.InvoiceId
                   AND id1.TrackId < id2.TrackId  -- Ensuring unique pairs
            )
            SELECT tp.TrackId1,
                   tp.TrackId2,
                   tp.AlbumId1,
                   tp.AlbumId2,
                   COUNT(tp.TrackId1) AS Frequency,
                   CURRENT_TIMESTAMP AS created_at,
                   CURRENT_TIMESTAMP AS updated_at,
                   'rachel_krashinski' AS updated_by
            FROM track_pairs tp
            GROUP BY tp.TrackId1, tp.TrackId2, tp.AlbumId1, tp.AlbumId2
            ORDER BY Frequency DESC;
        """

        # Execute the transformation query to create a new table in SQLite
        conn.execute(transform_query)

        # Commit the changes to the database to ensure data is saved
        conn.commit()

    finally:
        # Step 3: Clean up resources - close SQLite connection and stop the Spark session
        conn.close()  # Close SQLite connection
        spark.stop()  # Stop Spark session to release resources

# Call the function to execute the ELT process
if __name__ == "__main__":
    load()
