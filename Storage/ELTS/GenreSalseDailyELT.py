from pyspark.sql import SparkSession
import sqlite3  # Assuming you're using sqlite3
import pandas as pd
from datetime import datetime
import os

BASE_URL = "C:/Users/jimmy/Desktop/תמר לימודים  יד/bootcamp/db_files"



    #     create_table_time_query = """
    #         CREATE TABLE IF NOT EXISTS tableTime AS
    #         SELECT ci.CustomerId, ci.created_at 
    #         FROM customer_invoice_avg_elt ci
    #         WHERE EXISTS (
    #             SELECT 1
    #             FROM Customers_ELT ce
    #             JOIN Invoices_ELT i ON ce.CustomerId = i.CustomerId
    #             WHERE ((i.InvoiceDate > ? OR ce.created_at > ?)
    #             AND ci.customerId = ce.customerId)
    #             group by ce.CustomerId
    #         )
    #     """


    #     print("SELECT :",conn.execute("""SELECT c.CustomerId,c.created_at,i.InvoiceDate FROM Customers_ELT c
    #             JOIN Invoices_ELT i ON c.CustomerId = i.CustomerId
    #             where i.InvoiceDate > ? or c.created_at > ?
    #             group by c.CustomerId """
    #     ,(latest_timestamp,latest_timestamp,)).fetchall())
    #     # print("SELECT :",conn.execute("""SELECT InvoiceDate FROM Invoices_ELT 
    #     #                 where InvoiceDate > ? """
    #     # ,(latest_timestamp,)).fetchall())
        
    #     conn.execute(create_table_time_query, (latest_timestamp, latest_timestamp))
    #     conn.commit()
    #     conn.execute("""DELETE FROM customer_invoice_avg_elt
    #         WHERE EXISTS (
    #             SELECT 1
    #             FROM tableTime t
    #             WHERE customer_invoice_avg_elt.CustomerId = t.CustomerId
    #         )""")

    #     conn.commit()
    #     print("customer_invoice_avg_after_del:", conn.execute("SELECT * FROM 'customer_invoice_avg_elt'").fetchall())
    #     print("tableTime:", conn.execute("SELECT * FROM 'tableTime'").fetchall())

    #     transform_query = """
    #         INSERT INTO customer_invoice_avg_elt (CustomerId,InvoiceMonth, avg_spend, created_at, updated_at, updated_by)
    #         SELECT c.CustomerId, strftime('%m', i.InvoiceDate) AS InvoiceMonth,
    #                AVG(i.Total) AS avg_spend,
    #                COALESCE(t.created_at, CURRENT_DATE) AS created_at,
    #                CURRENT_DATE AS updated_at,
    #                'process:shana_levovitz_' || CURRENT_DATE AS updated_by
    #         FROM Customers_ELT c
    #         RIGHT JOIN Invoices_ELT i ON c.CustomerId = i.CustomerId
    #         LEFT JOIN tableTime t ON c.CustomerId = t.CustomerId
    #         WHERE i.InvoiceDate > ? OR c.created_at > ?
    #         GROUP BY c.CustomerId, InvoiceMonth
    #     """
    #     conn.execute(transform_query, (latest_timestamp, latest_timestamp))

    #     # Commit the changes to the database
    #     conn.execute("""DROP TABLE IF EXISTS tableTime""")
    #     conn.commit()
    #     print("customer_invoice_avg:", conn.execute("SELECT * FROM 'customer_invoice_avg_elt'").fetchall())
    # finally:
    #     # Step 3: Close the SQLite connection and stop Spark session
    #     conn.close()  # Close the SQLite connection
    #     spark.stop()  # Stop the Spark session
        


def load():
    conn = sqlite3.connect(os.path.join(BASE_URL,"genres_table_ELT.db"))
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        genres = pd.read_csv(os.path.join(BASE_URL, "Genre.csv"))
        tracks = pd.read_csv(os.path.join(BASE_URL, "Track.csv"))
        invoice_lines = pd.read_csv(os.path.join(BASE_URL, "InvoiceLine.csv"))
        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Load raw data into SQLite
        genres.to_sql("Genres", conn, if_exists="replace", index=False)
        tracks.to_sql("Tracks", conn, if_exists="replace", index=False)
        invoice_lines.to_sql("InvoiceLines", conn, if_exists="replace", index=False)
        # TRANSFORM (Perform transformations with SQL queries using KT_DB functions)
        # -------------------------------------------------------------------------
        last_update_date = """Select max(updated_at) from genre_sales_popularity_elt"""
        latest_timestamp = conn.execute(last_update_date).fetchone()[0]

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        print("latest_timestamp:", latest_timestamp)

        transform_query = f"""
            CREATE TABLE IF NOT EXISTS genre_sales_popularity_elt AS
            SELECT
                G.GenreId,
                G.Name,
                SUM(IL.UnitPrice * IL.Quantity) AS TotalSales,
                AVG(IL.UnitPrice) AS AverageSalesPrice,
                '{datetime.now()}' AS created_at,
                '{datetime.now()}' AS updated_at,
                'Tamar Gavrielov' AS updated_by
            FROM
                Genres G
            LEFT JOIN Tracks T ON G.GenreId = T.GenreId
            LEFT JOIN InvoiceLines IL ON T.TrackId = IL.TrackId
            WHERE G.Update_at > ? OR T.Update_at > ?
            GROUP BY
                G.GenreId, G.Name;
        """
        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()
    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection


def after_load_check_answers():
    conn = sqlite3.connect(os.path.join(BASE_URL,"genres_table_ELT.db"))
    query = "SELECT * FROM genre_sales_popularity_elt"
    result = pd.read_sql(
        query, conn
    )  # Use pandas to read the SQL result into a DataFrame
    print(result.head())
    conn.close()


if __name__ == "__main__":
    load()
    after_load_check_answers()
from pyspark.sql import SparkSession
import sqlite3  # Assuming you're using sqlite3
import pandas as pd
from datetime import datetime
import os

BASE_URL = "C:/Users/jimmy/Desktop/תמר לימודים  יד/bootcamp/db_files"


def load():
    conn = sqlite3.connect(os.path.join(BASE_URL,"genres_table_ELT.db"))
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        genres = pd.read_csv(os.path.join(BASE_URL, "Genre.csv"))
        tracks = pd.read_csv(os.path.join(BASE_URL, "Track.csv"))
        invoice_lines = pd.read_csv(os.path.join(BASE_URL, "InvoiceLine.csv"))
        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Load raw data into SQLite
        genres.to_sql("Genres", conn, if_exists="replace", index=False)
        tracks.to_sql("Tracks", conn, if_exists="replace", index=False)
        invoice_lines.to_sql("InvoiceLines", conn, if_exists="replace", index=False)
        # TRANSFORM (Perform transformations with SQL queries using KT_DB functions)
        # -------------------------------------------------------------------------
        drop_query = """DROP TABLE IF EXISTS genre_sales_popularity_elt"""
        conn.execute(drop_query)
        conn.commit()
        transform_query = f"""
            CREATE TABLE genre_sales_popularity_elt AS
            SELECT
                G.GenreId,
                G.Name,
                SUM(IL.UnitPrice * IL.Quantity) AS TotalSales,
                AVG(IL.UnitPrice) AS AverageSalesPrice,
                '{datetime.now()}' AS created_at,
                '{datetime.now()}' AS updated_at,
                'Tamar Gavrielov' AS updated_by
            FROM
                Genres G
            LEFT JOIN Tracks T ON G.GenreId = T.GenreId
            LEFT JOIN InvoiceLines IL ON T.TrackId = IL.TrackId
            GROUP BY
                G.GenreId, G.Name;
        """
        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()
    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection


def after_load_check_answers():
    conn = sqlite3.connect(os.path.join(BASE_URL,"genres_table_ELT.db"))
    query = "SELECT * FROM genre_sales_popularity_elt"
    result = pd.read_sql(
        query, conn
    )  # Use pandas to read the SQL result into a DataFrame
    print(result.head())
    conn.close()


if __name__ == "__main__":
    load()
    after_load_check_answers()
