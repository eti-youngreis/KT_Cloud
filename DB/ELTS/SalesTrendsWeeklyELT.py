
from pyspark.sql import SparkSession
import sqlite3  # Assuming you're using sqlite3
import pandas as pd
from datetime import datetime
import os

BASE_URL = "D:\\Users\\גילי\\Documents\\בוטקמפ\\csv files\\"

def load_sales_trends():
    conn = sqlite3.connect("employee_genre_sales_elt.db")
    try:
        # EXTRACT (Loading CSVs from S3 or local storage)
        # -----------------------------------------------
        employees = pd.read_csv(os.path.join(BASE_URL, "Employee.csv"))
        invoices = pd.read_csv(os.path.join(BASE_URL, "Invoice.csv"))
        invoice_lines = pd.read_csv(os.path.join(BASE_URL, "InvoiceLine.csv"))
        tracks = pd.read_csv(os.path.join(BASE_URL, "Track.csv"))
        genres = pd.read_csv(os.path.join(BASE_URL, "Genre.csv"))
        customers = pd.read_csv(os.path.join(BASE_URL, "Customer.csv"))
        # LOAD (Save the raw data into SQLite without transformation)
        # -----------------------------------------------------------------------
        # Load raw data into SQLite
        employees.to_sql("Employees", conn, if_exists="replace", index=False)
        invoices.to_sql("Invoices", conn, if_exists="replace", index=False)
        invoice_lines.to_sql("InvoiceLines", conn, if_exists="replace", index=False)
        tracks.to_sql("Tracks", conn, if_exists="replace", index=False)
        genres.to_sql("Genres", conn, if_exists="replace", index=False)
        customers.to_sql("Customers", conn, if_exists="replace", index=False)
        # TRANSFORM (Perform transformations with SQL queries using SQLite)
        # -------------------------------------------------------------------------
        drop_query = """DROP TABLE IF EXISTS employee_genre_sales_elt"""
        conn.execute(drop_query)
        conn.commit()
        transform_query = f"""
            CREATE TABLE employee_genre_sales_elt AS
            SELECT
                E.EmployeeId,
                E.FirstName || ' ' || E.LastName AS EmployeeName,
                G.Name AS Genre,
                SUM(IL.UnitPrice * IL.Quantity) AS TotalSales,
                AVG(IL.UnitPrice) AS AverageSalesPrice,
                '{datetime.now()}' AS created_at,
                '{datetime.now()}' AS updated_at,
                'Gili Bolak' AS updated_by
            FROM
                Employees E
            LEFT JOIN Customers C ON E.EmployeeId = C.SupportRepId
            LEFT JOIN Invoices I ON C.CustomerId = I.CustomerId
            LEFT JOIN InvoiceLines IL ON I.InvoiceId = IL.InvoiceId
            LEFT JOIN Tracks T ON IL.TrackId = T.TrackId
            LEFT JOIN Genres G ON T.GenreId = G.GenreId
            WHERE
                I.InvoiceDate >= date('now', '-6 months')
            GROUP BY
                E.EmployeeId, E.FirstName, E.LastName, G.Name;
        """
        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()
    finally:
        # Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection