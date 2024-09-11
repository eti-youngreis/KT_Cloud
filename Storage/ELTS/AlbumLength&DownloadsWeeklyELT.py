import sqlite3
import pandas as pd

def load_and_transform():
    try:
        # Step 1: Extract and Load
        print("Loading CSV files into SQLite...")
        
        # Establish SQLite connection
        conn = sqlite3.connect('D:\\b\\AlbumELT.db')
        
        # Load CSV files into SQLite tables
        pd.read_csv("D:\\csvFiles\\Album.csv").to_sql('albums', conn, if_exists='replace', index=False)
        pd.read_csv("D:\\csvFiles\\Track.csv").to_sql('tracks', conn, if_exists='replace', index=False)
        pd.read_csv("D:\\csvFiles\\InvoiceLine.csv").to_sql('invoice_lines', conn, if_exists='replace', index=False)
        
        print("Data loaded into SQLite successfully.")

        # Step 2: Transform (Apply SQL queries to transform data)
        print("Starting data transformation...")

        cursor = conn.cursor()

        # Query to get album lengths
        cursor.execute('''
            SELECT
                a.AlbumId,
                a.Title,
                a.ArtistId,
                COALESCE(SUM(t.Milliseconds), 0) AS TotalAlbumLength
            FROM albums a
            LEFT JOIN tracks t ON a.AlbumId = t.AlbumId
            GROUP BY a.AlbumId, a.Title, a.ArtistId
        ''')
        album_lengths = cursor.fetchall()

        # Query to get total downloads
        cursor.execute('''
            SELECT
                a.AlbumId,
                a.Title,
                a.ArtistId,
                COUNT(il.InvoiceLineId) AS TotalDownloads
            FROM albums a
            LEFT JOIN tracks t ON a.AlbumId = t.AlbumId
            LEFT JOIN invoice_lines il ON t.TrackId = il.TrackId
            GROUP BY a.AlbumId, a.Title, a.ArtistId
        ''')
        album_downloads = cursor.fetchall()

        # Create final summary table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS albums_summary (
                AlbumId INTEGER PRIMARY KEY,
                Title TEXT,
                ArtistId INTEGER,
                TotalAlbumLength INTEGER,
                TotalDownloads INTEGER,
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
        ''')

        # **New Step: Clear existing records to avoid duplication**
        cursor.execute('DELETE FROM albums_summary')

        # Insert new data
        for album_length in album_lengths:
            album_id = album_length[0]
            title = album_length[1]
            artist_id = album_length[2]
            total_length = album_length[3]
            total_downloads = next((d[3] for d in album_downloads if d[0] == album_id), 0)

            cursor.execute('''
                INSERT INTO albums_summary (AlbumId, Title, ArtistId, TotalAlbumLength, TotalDownloads, created_at, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'), 'process:user_name')
            ''', (album_id, title, artist_id, total_length, total_downloads))

        # Commit the transaction
        conn.commit()

        print("Transformed data loaded into albums_summary table.")

        # Verify by selecting a few rows
        cursor.execute("SELECT * FROM albums_summary ORDER BY AlbumId LIMIT 10")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error during ELT process: {e}")

    finally:
        # Close the SQLite connection
        conn.close()

# Run the load and transform function
load_and_transform()
