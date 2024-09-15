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
