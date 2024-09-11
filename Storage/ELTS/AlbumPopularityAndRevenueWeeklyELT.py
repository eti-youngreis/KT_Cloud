import sqlite3
import pandas as pd
from datetime import datetime

def load_album_popularity_and_revenue_ELT():
    # Step 1: Extract - read relevant CSV files into Pandas DataFrames
    albums_df = pd.read_csv("C:/Users/user1/Downloads/KAN-134_attachments/Album.csv")
    invoice_lines_df = pd.read_csv("C:/Users/user1/Downloads/KAN-134_attachments/InvoiceLine.csv")
    tracks_df = pd.read_csv("C:/Users/user1/Downloads/KAN-134_attachments/Track.csv")

    # Step 2: Load - load raw data into SQLite tables as is
    conn = sqlite3.connect('C:/Users/user1/Desktop/0909/AlbumELT.db')
    cursor = conn.cursor()

    # Create raw data tables if they don't exist and insert the data
    cursor.execute('''CREATE TABLE IF NOT EXISTS Albums (
                      AlbumId INTEGER PRIMARY KEY,
                      Title TEXT,
                      ArtistId INTEGER)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS InvoiceLines (
                      InvoiceLineId INTEGER PRIMARY KEY,
                      InvoiceId INTEGER,
                      TrackId INTEGER,
                      UnitPrice REAL,
                      Quantity INTEGER)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Tracks (
                      TrackId INTEGER PRIMARY KEY,
                      Name TEXT,
                      AlbumId INTEGER,
                      MediaTypeId INTEGER,
                      GenreId INTEGER,
                      Composer TEXT,
                      Milliseconds INTEGER,
                      Bytes INTEGER,
                      UnitPrice REAL)''')

    # Insert data into the raw tables
    albums_df.to_sql('Albums', conn, if_exists='replace', index=False)
    invoice_lines_df.to_sql('InvoiceLines', conn, if_exists='replace', index=False)
    tracks_df.to_sql('Tracks', conn, if_exists='replace', index=False)

    # Step 3: Transform - perform transformations using SQL queries
    # Calculate total revenue and track count per album
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AlbumPopularityAndRevenue AS
        SELECT 
            a.AlbumId, 
            a.Title, 
            a.ArtistId, 
            SUM(il.UnitPrice * il.Quantity) AS TotalRevenue,
            COUNT(t.TrackId) AS TrackCount,
            DATETIME('now') AS created_at,
            DATETIME('now') AS updated_at,
            'process:user_name' AS updated_by
        FROM Albums a
        JOIN Tracks t ON a.AlbumId = t.AlbumId
        JOIN InvoiceLines il ON t.TrackId = il.TrackId
        GROUP BY a.AlbumId, a.Title, a.ArtistId
    ''')

    # Commit changes and close the connection
    conn.commit()

    # Retrieve and print the transformed data
    cursor.execute('SELECT * FROM AlbumPopularityAndRevenue')
    rows = cursor.fetchall()

    print("Transformed Data:")
    for row in rows:
        print(row)

    conn.close()

if __name__ == "__main__":
    load_album_popularity_and_revenue_ELT()
