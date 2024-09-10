import sqlite3
import pandas as pd

def loadELT():
    try:
        # Step 1: Extract
        # Connect to the SQLite database
        conn = sqlite3.connect('D:\\b\\revenueCustomerELT.db')
        cursor = conn.cursor()
        
        # EXTRACT (Loading CSVs from local storage)
        print("Loading CSV files...")
        customers_df = pd.read_csv("D:\\csvFiles\\Customer.csv", header=0)
        invoices_df = pd.read_csv("D:\\csvFiles\\Invoice.csv", header=0)
        invoice_lines_df = pd.read_csv("D:\\csvFiles\\InvoiceLine.csv", header=0)
        tracks_df = pd.read_csv("D:\\csvFiles\\Track.csv", header=0)
        genres_df = pd.read_csv("D:\\csvFiles\\Genre.csv", header=0)

        # Step 2: Load the data into the database
        customers_df.to_sql('customers', conn, if_exists='replace', index=False)
        invoices_df.to_sql('invoices', conn, if_exists='replace', index=False)
        invoice_lines_df.to_sql('invoice_lines', conn, if_exists='replace', index=False)
        tracks_df.to_sql('tracks', conn, if_exists='replace', index=False)
        genres_df.to_sql('genres', conn, if_exists='replace', index=False)
        
        # Step 3: Transform
        print("Starting data transformation...")

        cursor = conn.cursor()

        # **Create a temporary table to aggregate revenue by customer and genre**
        cursor.execute('''
            CREATE TEMPORARY TABLE temp_revenue AS
            SELECT
                c.CustomerId,
                c.FirstName,
                c.LastName,
                g.Name AS GenreName,
                SUM(il.UnitPrice) AS TotalRevenue
            FROM customers c
            JOIN invoices i ON c.CustomerId = i.CustomerId
            JOIN invoice_lines il ON i.InvoiceId = il.InvoiceId
            JOIN tracks t ON il.TrackId = t.TrackId
            JOIN genres g ON t.GenreId = g.GenreId
            GROUP BY c.CustomerId, c.FirstName, c.LastName, g.Name
            ''')
        # # Check the content of temp_revenue
        # # cursor.execute("SELECT * FROM temp_revenue ORDER BY CustomerId LIMIT 20")
        # # temp_rows = cursor.fetchall()
        # # print("Temp Revenue Table:")
        # # for row in temp_rows:
        # #     print(row)
            
        cursor.execute('''DROP TABLE IF EXISTS customer_genre_revenue''')
        # # **Create the final table with separate columns for each genre**
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_genre_revenue AS
            SELECT
                CustomerId,
                MAX(FirstName) AS FirstName,
                MAX(LastName) AS LastName,
                SUM(CASE WHEN GenreName = 'Rock' THEN TotalRevenue ELSE 0 END) AS RockRevenue,
                SUM(CASE WHEN GenreName = 'Metal' THEN TotalRevenue ELSE 0 END) AS MetalRevenue,
                SUM(CASE WHEN GenreName = 'Latin' THEN TotalRevenue ELSE 0 END) AS LatinRevenue,
                SUM(CASE WHEN GenreName = 'Reggae' THEN TotalRevenue ELSE 0 END) AS ReggaeRevenue,
                SUM(CASE WHEN GenreName = 'Pop' THEN TotalRevenue ELSE 0 END) AS PopRevenue,
                DATETIME('now') AS created_at,
                DATETIME('now') AS updated_at,
                'system' AS updated_by
            FROM temp_revenue
            GROUP BY CustomerId
        ''')

        # Commit the transaction
        conn.commit()

        print("Transformed data loaded into customer_genre_revenue table with new columns.")

        # Verify by selecting a few rows
        cursor.execute("SELECT * FROM customer_genre_revenue ORDER BY CustomerId LIMIT 10")
        rows = cursor.fetchall()
        for row in rows:
            print(row)


    except Exception as e:
        print(f"Error during ELT process: {e}")

    finally:
        # Close the SQLite connection
        conn.close()

loadELT()
