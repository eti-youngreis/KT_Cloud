import sqlite3
from datetime import datetime
import pandas as pd


def popular_genres_by_city_incremental_elt():
    conn = sqlite3.connect('DB/etl_db.db')
    cursor = conn.cursor()
    try:
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='PopularGenresByCity'")
        if cursor.fetchone() is None:
            cursor.execute('''
            CREATE TABLE PopularGenresByCity (
                City TEXT PRIMARY KEY,
                most_popular_genre TEXT,
                sec_popular_genre TEXT,
                third_popular_genre TEXT,
                fourth_popular_genre TEXT,
                fifth_popular_genre TEXT,
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
            ''')
            print("Table 'PopularGenresByCity' created.")

        # Step 4: Get the latest processed timestamp
        cursor.execute("SELECT MAX(updated_at) FROM PopularGenresByCity")
        latest_timestamp = cursor.fetchone()[0] or '1900-01-01 00:00:00'
        
        
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
    
        # Corrected SQL Query for SQLite
        create_temp_table_query = f"""CREATE TEMP TABLE TempDeletedRowsInfo AS
        SELECT temp.City,
               MAX(CASE WHEN Rank = 1 THEN GenreName END) AS most_popular_genre,
               MAX(CASE WHEN Rank = 2 THEN GenreName END) AS sec_popular_genre,
               MAX(CASE WHEN Rank = 3 THEN GenreName END) AS third_popular_genre,
               MAX(CASE WHEN Rank = 4 THEN GenreName END) AS fourth_popular_genre,
               MAX(CASE WHEN Rank = 5 THEN GenreName END) AS fifth_popular_genre,
               COALESCE(pgbc.created_at, DATETIME('now')) AS created_at, 
                DATETIME('now') as updated_at,
            'process:user_name' AS updated_by
        FROM (
            SELECT c.City, g.Name AS GenreName, SUM(il.Quantity) AS GenreCount,
                   RANK() OVER (PARTITION BY c.City ORDER BY SUM(il.Quantity) DESC) AS Rank  
            FROM Customer c
            JOIN Invoice i ON c.CustomerId = i.CustomerId
            JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
            JOIN Track t ON il.TrackId = t.TrackId
            JOIN Genre g ON t.GenreId = g.GenreId
            GROUP BY c.City, g.Name
        ) temp
         LEFT JOIN PopularGenresByCity pgbc ON pgbc.City = temp.City
         GROUP BY temp.City
            """


        
         # Delete rows from AlbumPopularityAndRevenue using the temporary table
        delete_query = """
        DELETE FROM PopularGenresByCity
        WHERE City IN (
            SELECT City FROM TempDeletedRowsInfo
        );
        """
        
        # Insert to AlbumPopularityAndRevenue the updates from temp table
        insert_query = """
        INSERT INTO PopularGenresByCity (City,
                    most_popular_genre,
                    sec_popular_genre,
                    third_popular_genre,
                    fourth_popular_genre,
                    fifth_popular_genre, 
                    created_at,updated_at,updated_by)
        SELECT City,
                    most_popular_genre,
                    sec_popular_genre,
                    third_popular_genre,
                    fourth_popular_genre,
                    fifth_popular_genre, 
                    created_at,updated_at,updated_by
        FROM TempDeletedRowsInfo;
        """
        create_temp_table_query = create_temp_table_query.strip().replace('\n', '').replace('  ', ' ')
        delete_query = delete_query.strip().replace('\n', '').replace('  ', ' ')
        insert_query = insert_query.strip().replace('\n', '').replace('  ', ' ')
        cursor.execute(create_temp_table_query)
        conn.commit()
        cursor.execute(delete_query)
        conn.commit()
        cursor.execute(insert_query)
        conn.commit()
        cursor.execute("SELECT * FROM PopularGenresByCity")
        rows = cursor.fetchall()
        print(rows)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    popular_genres_by_city_incremental_elt()
    