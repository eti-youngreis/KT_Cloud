import sqlite3  # Assuming KT_DB is the library for SQLite operations
import pandas as pd
from datetime import datetime, timedelta

def album_popularity_incremental_elt():
    # Step 1: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('DB/etl_db.db') 
    cursor = conn.cursor()

    try:
        # Step 2: Get the latest processed timestamp from the target table
        latest_timestamp_query = "SELECT MAX(updated_at) FROM AlbumPopularityAndRevenue"
        cursor.execute(latest_timestamp_query)

        # Fetch the result of the query
        latest_timestamp = cursor.fetchone()[0]


        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'  # Default for initial load
        print(latest_timestamp)
       

        # Add here latest_timestamp query for each main source of your query

        # EXTRACT & LOAD (Load CSVs into raw tables in SQLite using KT_DB)
        # --------------------------------------------------------------
        
        # Load the tables CSV as a Pandas DataFrame
        albums = pd.read_csv("DB\csv_files\Album.csv")
        tracks = pd.read_csv("DB\csv_files\Track.csv")
        invoice_lines = pd.read_csv("DB\csv_files\InvoiceLine.csv")
        
        
        # Insert the full CSV data into corresponding raw tables in SQLite
        albums.to_sql('albums', conn, if_exists='replace', index=False)
        tracks.to_sql('tracks', conn, if_exists='replace', index=False)
        invoice_lines.to_sql('invoice_lines', conn, if_exists='replace', index=False)
    

        # TRANSFORM (Perform transformations with SQL queries inside SQLite)
        # ------------------------------------------------------------------
        # Example Transformation: Join tables and calculate total spend, avg spend, etc.
        stam = "COALESCE(apr.created_at, DATETIME('now')) AS created_at"


        # Create a temporary table to store the rows to be deleted and inserted
        create_temp_table_query = f"""
        CREATE TEMP TABLE TempDeletedRowsInfo AS
        SELECT al.AlbumId,
            al.Title,
            al.ArtistId,
            SUM(tr.UnitPrice * il.Quantity) AS TotalRevenue,
            COUNT(tr.TrackId) AS TrackCount,
            apr.created_at AS created_at,
            DATETIME('now') AS updated_at,
            'process:user_name' AS updated_by
        FROM albums al
        JOIN tracks tr ON al.AlbumId = tr.AlbumId
        JOIN invoice_lines il ON il.TrackId = tr.TrackId
        LEFT JOIN AlbumPopularityAndRevenue apr ON apr.AlbumId = al.AlbumId
        WHERE al.updated_at > '{latest_timestamp}' or tr.updated_at > '{latest_timestamp}' or il.updated_at > '{latest_timestamp}'
        GROUP BY al.AlbumId;
        """

        # Delete rows from AlbumPopularityAndRevenue using the temporary table
        delete_query = """
        DELETE FROM AlbumPopularityAndRevenue
        WHERE AlbumId IN (
            SELECT AlbumId FROM TempDeletedRowsInfo
        );
        """
        
        # Insert to AlbumPopularityAndRevenue the updates from temp table
        insert_query = """
        INSERT INTO AlbumPopularityAndRevenue (AlbumId, Title, ArtistId, TotalRevenue, TrackCount, created_at, updated_at, updated_by)
        SELECT AlbumId, Title, ArtistId, TotalRevenue, TrackCount, created_at, updated_at, updated_by
        FROM TempDeletedRowsInfo;
        """






        create_temp_table_query = create_temp_table_query.strip().replace('\n', '').replace('  ', ' ')
        delete_query = delete_query.strip().replace('\n', '').replace('  ', ' ')
        insert_query = insert_query.strip().replace('\n', '').replace('  ', ' ')
        print("striped")
        cursor.execute(create_temp_table_query)
        conn.commit()
        cursor.execute(delete_query)
        conn.commit()
        cursor.execute(insert_query)
        conn.commit()
        cursor.execute("SELECT * FROM AlbumPopularityAndRevenue")
        rows = cursor.fetchall()

        # Print the data from the temporary table
        for row in rows:
            print(row)
    

    finally:
        # Step 3: Close the SQLite connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    album_popularity_incremental_elt()