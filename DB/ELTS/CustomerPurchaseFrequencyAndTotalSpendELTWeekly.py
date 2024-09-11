from pyspark.sql import SparkSession  # Importing Spark library
import sqlite3  # Importing SQLite module for local database operations


def load():
    # Creating a Spark Session for performing ELT operations
    spark = SparkSession.builder.appName('Customer Purchase Frequency ELT').getOrCreate()

    # Establishing connection to the local SQLite database
    conn = sqlite3.connect('CustomerPurchaseELT.db')

    try:
        # Reading the customer and invoice CSV files, with headers and automatic schema inference
        customer_df = spark.read.csv(r"C:\\Users\\user1\\Desktop\\בוטקמפ\הפרוקיט\\elt\\csv files\\Customer.csv", header=True, inferSchema=True)
        invoice_df = spark.read.csv(r"C:\\Users\\user1\\Desktop\\בוטקמפ\הפרוקיט\\elt\\csv files\\Invoice.csv", header=True, inferSchema=True)

        # Converting the Spark DataFrame to a Pandas DataFrame for integration with SQLite
        customer = customer_df.toPandas()
        invoice = invoice_df.toPandas()

        # Saving data to SQLite using to_sql, replacing the tables if they already exist
        customer.to_sql('Customer', conn, if_exists='replace', index=False)
        invoice.to_sql('Invoice', conn, if_exists='replace', index=False)

        # Creating a new table if it does not exist, with columns to store customer purchase information
        query_create_table = """CREATE TABLE IF NOT EXISTS CustomerPurchase (
                                CustomerId INTEGER,
                                FirstName TEXT,
                                LastName TEXT,
                                TotalSpend REAL,
                                NumOfPurchases INTEGER, 
                                CreatedAt DateTime,
                                UpdatedAt DateTime,
                                UpdatedBy TEXT
                            );"""

        # Query to insert data from the customer and invoice tables into the new CustomerPurchase table
        query = """insert into CustomerPurchase (CustomerId, FirstName, LastName, TotalSpend, NumOfPurchases, CreatedAt, UpdatedAt, UpdatedBy)
                    select c.CustomerId, FirstName, LastName , sum(Total) as TotalSpend, count(*) as NumOfPurchases, 
                    DATE('now') as CreatedAt, DATE('now') as UpdatedAt, 'efrat' as UpdatedBy
                    from Customer c inner join Invoice i on c.CustomerId = i.CustomerId 
                    group by c.CustomerId, FirstName, LastName"""

        # Executing the query
        cursor = conn.cursor()
        cursor.execute(query_create_table)  # Creating the table if it doesn't exist
        cursor.execute(query)  # Inserting the data

        # Committing changes to the database
        conn.commit()

    finally:
        conn.close()  # Closing the database connection
        spark.stop()  # Stopping the Spark session


load()  # Calling the function



