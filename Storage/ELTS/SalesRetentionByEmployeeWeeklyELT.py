import sqlite3
import pandas as pd
import os
from datetime import datetime

# Define file paths
employee_file = 'D:\\בוטקאמפ\\vastProject\\csv\\Employee.csv'
customer_file = 'D:\\בוטקאמפ\\vastProject\\csv\\Customer.csv'
invoice_file = 'D:\\בוטקאמפ\\vastProject\\csv\\Invoice.csv'
invoice_line_file = 'D:\\בוטקאמפ\\vastProject\\csv\\InvoiceLine.csv'

# Define SQLite database path
db_path = '/absolute/path/to/database.db'

# Create directory if it does not exist
if not os.path.exists(os.path.dirname(db_path)):
    os.makedirs(os.path.dirname(db_path))

# Step 1: Extract CSV files
def extract_data():
    employees_df = pd.read_csv(employee_file)
    customers_df = pd.read_csv(customer_file)
    invoices_df = pd.read_csv(invoice_file)
    invoice_lines_df = pd.read_csv(invoice_line_file)
    return employees_df, customers_df, invoices_df, invoice_lines_df

# Step 2: Load data into SQLite
def load_data_to_sqlite(employees_df, customers_df, invoices_df, invoice_lines_df):
    conn = sqlite3.connect(db_path)
    
    # Load data into SQLite
    employees_df.to_sql('Employees', conn, if_exists='replace', index=False)
    customers_df.to_sql('Customers', conn, if_exists='replace', index=False)
    invoices_df.to_sql('Invoices', conn, if_exists='replace', index=False)
    invoice_lines_df.to_sql('InvoiceLines', conn, if_exists='replace', index=False)
    
    conn.close()

# Step 3: Transform and Load data into new SQLite table
def transform_and_load():
    conn = sqlite3.connect(db_path)
    
    # Create transformed table for Average Sales per Employee and Customer Retention Rate
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS employee_metrics AS
    WITH customer_sales AS (
        SELECT
            e.EmployeeID,
            e.FirstName AS EmployeeFirstName,
            e.LastName AS EmployeeLastName,
            c.CustomerID,
            COUNT(DISTINCT i.InvoiceID) AS num_invoices,
            SUM(il.UnitPrice * il.Quantity) AS total_sales
        FROM
            Employees e
        JOIN
            Customers c ON e.EmployeeID = c.SupportRepID
        JOIN
            Invoices i ON c.CustomerID = i.CustomerID
        JOIN
            InvoiceLines il ON i.InvoiceID = il.InvoiceID
        GROUP BY
            e.EmployeeID, e.FirstName, e.LastName, c.CustomerID
    ),
    retention AS (
        SELECT
            EmployeeID,
            COUNT(DISTINCT CustomerID) AS total_customers,
            SUM(CASE WHEN num_invoices > 1 THEN 1 ELSE 0 END) AS repeat_customers_count
        FROM
            customer_sales
        GROUP BY
            EmployeeID
    )
    SELECT
        cs.EmployeeID,
        cs.EmployeeFirstName,
        cs.EmployeeLastName,
        AVG(cs.total_sales) AS average_sales_per_employee,
        COALESCE(r.repeat_customers_count * 1.0 / r.total_customers, 0) AS customer_retention_rate,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at,
        'your_user_name' AS updated_by
    FROM
        customer_sales cs
    LEFT JOIN
        retention r ON cs.EmployeeID = r.EmployeeID
    GROUP BY
        cs.EmployeeID, cs.EmployeeFirstName, cs.EmployeeLastName, r.repeat_customers_count, r.total_customers;
    '''
    
    conn.execute(create_table_query)
    conn.commit()
    conn.close()

# Main function to execute the ELT process
def load():
    # Extract data
    employees_df, customers_df, invoices_df, invoice_lines_df = extract_data()
    
    # Load data into SQLite
    load_data_to_sqlite(employees_df, customers_df, invoices_df, invoice_lines_df)
    
    # Transform and load data into new SQLite table
    transform_and_load()

# Call the function to execute the ELT process
if __name__ == "__main__":
    load()

