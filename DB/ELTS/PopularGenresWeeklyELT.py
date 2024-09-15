import sqlite3
from datetime import datetime
import pandas as pd


def popular_genres_by_city_full_elt():
    conn = sqlite3.connect('DB/etl_db.db')
    cursor = conn.cursor()
    try:
        
        # Check if the 'album_popularity_revenue' table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='PopularGenresByCity';
        """)
        table_exists_flag = cursor.fetchone()

        # If the table exists, drop it
        if table_exists_flag:
            print("Table 'PopularGenresByCity' exists, dropping it.")
            cursor.execute("DROP TABLE PopularGenresByCity;")
            conn.commit()
        else:
            print("PopularGenresByCity' does not exist.")
        
        
        # EXTRACT and LOAD (Loading CSVs and storing into SQLite using pandas)
        customers_df = pd.read_csv("DB\csvs\Customer.csv")
        invoices_df = pd.read_csv("DB\csvs\Invoice.csv")
        invoice_lines_df = pd.read_csv("DB\csvs\InvoiceLine.csv")
        tracks_df = pd.read_csv("DB\csvs\Track.csv")
        genres_df = pd.read_csv("DB\csvs\Genre.csv")

        # Load data into SQLite using sqlite3
        conn = sqlite3.connect('DB\etl_db.db')

        # Using pandas to_sql to load data into SQLite
        customers_df.to_sql('Customer', conn, if_exists='replace', index=False)
        invoices_df.to_sql('Invoice', conn, if_exists='replace', index=False)
        invoice_lines_df.to_sql('InvoiceLine', conn, if_exists='replace', index=False)
        tracks_df.to_sql('Track', conn, if_exists='replace', index=False)
        genres_df.to_sql('Genre', conn, if_exists='replace', index=False)
    
        query = """CREATE TABLE PopularGenresByCity AS
                    SELECT City,
                    MAX(CASE WHEN Rank = 1 THEN GenreName END) AS most_popular_genre,
                    MAX(CASE WHEN Rank = 2 THEN GenreName END) AS sec_popular_genre,
                    MAX(CASE WHEN Rank = 3 THEN GenreName END) AS third_popular_genre,
                    MAX(CASE WHEN Rank = 4 THEN GenreName END) AS fourth_popular_genre,
                    MAX(CASE WHEN Rank = 5 THEN GenreName END) AS fifth_popular_genre, 
                    created_at,updated_at,updated_by
                    FROM (
                    SELECT c.City, g.Name AS GenreName, SUM(il.Quantity) AS GenreCount,
                    RANK() OVER (PARTITION BY c.City ORDER BY SUM(il.Quantity) DESC) AS Rank,
           DATETIME('now') AS created_at,
           DATETIME('now') AS updated_at,
           'process:user_name' AS updated_by  
        FROM Customer c
        JOIN Invoice i ON c.CustomerId = i.CustomerId
        JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
        JOIN Track t ON il.TrackId = t.TrackId
        JOIN Genre g ON t.GenreId = g.GenreId
        GROUP BY c.City, g.Name
        ) 
        GROUP BY City
        """
        query = query.strip().replace('\n', '').replace('  ', ' ')
    
        cursor.execute(query)
        conn.commit()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    popular_genres_by_city_full_elt()
    