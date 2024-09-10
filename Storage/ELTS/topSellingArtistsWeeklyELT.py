import sqlite3
import pandas as pd

def load_and_transform_data():
    # Establish SQLite connection
    conn = sqlite3.connect('D:\\בוטקמפ\\s3\\KT_Cloud\\CustomerTopSellingArtists.db')
    cursor = conn.cursor()

    try:
        # Load CSV files into SQLite tables
        csv_files = {
            'Invoice': 'D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Invoice.csv',
            'InvoiceLine': 'D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\InvoiceLine.csv',
            'Track': 'D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Track.csv',
            'Album': 'D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Album.csv',
            'Artist': 'D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Artist.csv',
            'Customer': 'D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Customer.csv'
        }

        for table_name, file_path in csv_files.items():
            df = pd.read_csv(file_path)
            df.to_sql(table_name, conn, if_exists='replace', index=False)

        # Perform transformations using SQL
        transformation_query = """
        CREATE TABLE IF NOT EXISTS top_selling_artists AS
        WITH invoice_lines_tracks AS (
            SELECT il.InvoiceId, il.TrackId, il.UnitPrice AS TrackUnitPrice, il.Quantity, t.AlbumId
            FROM InvoiceLine il
            JOIN Track t ON il.TrackId = t.TrackId
        ),
        tracks_albums AS (
            SELECT it.InvoiceId, it.TrackId, it.TrackUnitPrice, it.Quantity, a.ArtistId
            FROM invoice_lines_tracks it
            JOIN Album a ON it.AlbumId = a.AlbumId
        ),
        albums_artists AS (
            SELECT ta.InvoiceId, ta.TrackId, ta.TrackUnitPrice, ta.Quantity, ar.ArtistId, ar.Name AS ArtistName
            FROM tracks_albums ta
            JOIN Artist ar ON ta.ArtistId = ar.ArtistId
        ),
        invoices_customers AS (
            SELECT i.InvoiceId, c.CustomerId, c.City, c.State, i.InvoiceDate
            FROM Invoice i
            JOIN Customer c ON i.CustomerId = c.CustomerId
        ),
        combined_data AS (
            SELECT ic.InvoiceId, ic.City, ic.State, ic.InvoiceDate, aa.ArtistId, aa.ArtistName, aa.TrackUnitPrice, aa.Quantity
            FROM invoices_customers ic
            JOIN albums_artists aa ON ic.InvoiceId = aa.InvoiceId
        )
        SELECT
            CONCAT(combined_data.City, ', ', combined_data.State) AS Region,
            strftime('%Y', combined_data.InvoiceDate) AS Year,
            combined_data.ArtistId,
            combined_data.ArtistName,
            SUM(combined_data.TrackUnitPrice * combined_data.Quantity) AS TotalSales
        FROM combined_data
        GROUP BY Region, Year, ArtistId, ArtistName
        ORDER BY Region, Year, TotalSales DESC
        """

        cursor.execute(transformation_query)
        conn.commit()

        # Query and print the contents of the new table
        cursor.execute("SELECT * FROM top_selling_artists;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    finally:
        conn.close()

if __name__ == "__main__":
    load_and_transform_data()
