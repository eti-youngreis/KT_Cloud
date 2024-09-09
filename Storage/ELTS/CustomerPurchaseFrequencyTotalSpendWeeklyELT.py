import sqlite3
import pandas as pd
from datetime import datetime

def load_elt():
    conn = sqlite3.connect('D:\\בוטקמפ\\s3\\KT_Cloud\\CustomerELT.db')
    cursor = conn.cursor()

    try:
        # EXTRACT
        customers_df = pd.read_csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Customer.csv")
        invoice_lines_df = pd.read_csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\InvoiceLine.csv")
        invoices_df = pd.read_csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Invoice.csv")

        # LOAD
        customers_df.to_sql('customers', conn, if_exists='replace', index=False)
        invoice_lines_df.to_sql('invoice_lines', conn, if_exists='replace', index=False)
        invoices_df.to_sql('invoices', conn, if_exists='replace', index=False)

        print("הנתונים נטענו לתוך הטבלאות ב-SQLite")

        cursor.execute("""
            SELECT c.CustomerId, c.FirstName, c.LastName, 
                   SUM(il.UnitPrice * il.Quantity) AS TotalSpend,
                   COUNT(DISTINCT i.InvoiceId) AS PurchaseFrequency
            FROM customers c
            JOIN invoices i ON c.CustomerId = i.CustomerId
            JOIN invoice_lines il ON i.InvoiceId = il.InvoiceId
            GROUP BY c.CustomerId, c.FirstName, c.LastName
        """)

        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["CustomerId", "FirstName", "LastName", "TotalSpend", "PurchaseFrequency"])

        # Adding timestamp columns for record creation and update
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["created_at"] = current_datetime
        df["updated_at"] = current_datetime
        df["updated_by"] = "process:user_name"

        # Write the DataFrame to the SQLite table
        df.to_sql('customer_purchase_summary', conn, if_exists='replace', index=False)

        # Commit the transaction
        conn.commit()

        cursor.execute("SELECT * FROM customer_purchase_summary;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    finally:
        # סגירת החיבור למסד הנתונים
        conn.close()

if __name__ == "__main__":
    load_elt()
