import sqlite3
import pandas as pd

def load():
    conn = sqlite3.connect('../../../Customers_ELT.db')
    cursor = conn.cursor()
    
    try:
        # EXTRACT (Loading CSVs from local storage)
        customers = pd.read_csv("../../../Customer.csv")
        invoices = pd.read_csv("../../../Invoice.csv")
        invoice_lines = pd.read_csv("../../../InvoiceLine.csv")

        # Check if the 'customer_loyalty' table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='customer_loyalty_and_invoice_size_ELT';
        """)
        table_exists = cursor.fetchone()

        # If the table exists, get the latest processed updated_at from the customer_loyalty table
        if table_exists:
            latest_updated_at_query = "SELECT MAX(updated_at) FROM customer_loyalty_and_invoice_size_ELT"
            cursor.execute(latest_updated_at_query)
            last_updated = cursor.fetchone()[0]
        else:
            last_updated = None

        if not last_updated:
            last_updated = '1900-01-01 00:00:00'  # Default to a very old timestamp

        # Convert last_updated to a datetime object
        last_updated = pd.to_datetime(last_updated)
        print(f"Last Updated: {last_updated}")

        # Convert 'UpdatedAt' columns in CSVs to datetime, assuming format is DD/MM/YYYY
        customers['UpdatedAt'] = pd.to_datetime(customers['UpdatedAt'], dayfirst=True, errors='coerce')
        invoices['UpdatedAt'] = pd.to_datetime(invoices['UpdatedAt'], dayfirst=True, errors='coerce')
        invoice_lines['UpdatedAt'] = pd.to_datetime(invoice_lines['UpdatedAt'], dayfirst=True, errors='coerce')

        # Filter rows that have been created or updated after the last load
        customers_filtered = customers[customers['UpdatedAt'] > last_updated]
        invoices_filtered = invoices[invoices['UpdatedAt'] > last_updated]
        invoice_lines_filtered = invoice_lines[invoice_lines['UpdatedAt'] > last_updated]

        # Load filtered data into temporary tables
        customers_filtered.to_sql('Customers_temp', conn, if_exists='replace', index=False)
        invoices_filtered.to_sql('Invoices_temp', conn, if_exists='replace', index=False)
        invoice_lines_filtered.to_sql('Invoices_Line_temp', conn, if_exists='replace', index=False)

        # Save historical data for customers to be updated (loyalty_score, avg_invoice_size, created_at)
        conn.execute("""DROP TABLE IF EXISTS customers_old_data_temp""")
        conn.execute("""
            CREATE TABLE customers_old_data_temp AS
            SELECT CustomerId, loyalty_score, avg_invoice_size, created_at
            FROM customer_loyalty_and_invoice_size_ELT
            WHERE CustomerId IN (SELECT CustomerId FROM Customers_temp)
        """)
        conn.commit()

        # Remove the existing rows from the target table for customers being updated
        conn.execute("""
            DELETE FROM customer_loyalty_and_invoice_size_ELT
            WHERE CustomerId IN (SELECT CustomerId FROM Customers_temp)
        """)
        conn.commit()

        # TRANSFORM (Insert or update existing records with new data)
        transform_query = """
            INSERT INTO customer_loyalty_and_invoice_size_ELT 
            (CustomerId, FirstName, LastName, loyalty_score, avg_invoice_size, created_at, updated_at, updated_by)
            SELECT 
                C.CustomerId,
                C.FirstName,
                C.LastName,
                COALESCE(O.loyalty_score, 0) + COUNT(I.InvoiceId) AS loyalty_score,  -- Combine old and new loyalty score
                (
                    (COALESCE(O.loyalty_score, 0) * COALESCE(O.avg_invoice_size, 0)) + SUM(IL.total_spend)
                ) / NULLIF(COALESCE(O.loyalty_score, 0) + COUNT(I.InvoiceId), 0) AS avg_invoice_size,  -- Combine old and new avg invoice size
                COALESCE(O.created_at, CURRENT_TIMESTAMP) AS created_at,  -- Retain old created_at or set to current
                CURRENT_TIMESTAMP AS updated_at,
                'process:SL' AS updated_by
            FROM 
                Customers_temp C
            LEFT JOIN Invoices_temp I ON C.CustomerId = I.CustomerId
            LEFT JOIN (
                SELECT InvoiceId, SUM(UnitPrice * Quantity) AS total_spend
                FROM Invoices_Line_temp
                GROUP BY InvoiceId
            ) IL ON I.InvoiceId = IL.InvoiceId
            LEFT JOIN customers_old_data_temp O ON C.CustomerId = O.CustomerId
            GROUP BY 
                C.CustomerId, C.FirstName, C.LastName;
        """

        # Execute the transformation query
        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()

    finally:
        # Clean up temporary tables
        conn.execute("DROP TABLE IF EXISTS Customers_temp")
        conn.execute("DROP TABLE IF EXISTS Invoices_temp")
        conn.execute("DROP TABLE IF EXISTS Invoices_Line_temp")
        conn.execute("DROP TABLE IF EXISTS customers_old_data_temp")
        conn.commit()

        # Close the SQLite connection
        conn.close()


load()