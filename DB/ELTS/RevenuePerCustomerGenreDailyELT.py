from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
import pandas as pd

def incremental_load():
    current_user = "User" # when IAM is implemented, get current user for session details
    
    elt_table_name = 'revenue_per_customer_genre_elt'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    engine = create_engine('sqlite:///' + base_path + 'database.db')
    
    try:
        conn = sqlite3.connect(base_path + 'database.db')

        # Check if the 'revenue_per_customer_genre_elt' table exists
        result = False
        # Connect using SQLAlchemy
        with engine.connect() as connection:
            # Check if the table exists
            inspector = inspect(engine)
            
            if inspector.has_table(elt_table_name):
                result = True
        
        # If the table doesn't exists, create it
        if not result:
            print(f"Table '{elt_table_name}' doesn't exists, creating it.")
            conn.execute(f"select table if exists {elt_table_name}")
            conn.commit()
            conn.execute(f"""
                    create table {elt_table_name} (
                    CustomerId INTEGER,
                    GenreId INTEGER,
                    revenue_overall INTEGER,
                    created_at DATE,
                    updated_at DATE,
                    updated_by TEXT
                );""")
            conn.commit()
            
        # get the date of the last incremental load
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT MAX(updated_at) from {elt_table_name}"))
            latest_timestamp = result.fetchone()[0]

        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
            
        # extract csv files
        invoice_table = spark.read.option("header", "true").csv(base_path + "Invoice.csv")
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv")
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")
        
        invoice_line_table = invoice_line_table.withColumn("updated_at", F.to_timestamp(invoice_line_table["updated_at"], "yyyy-MM-dd"))
        invoice_line_table = invoice_line_table.filter(invoice_line_table['updated_at'] > latest_timestamp)
        
        invoice_table.toPandas().to_sql("invoice", con = conn, if_exists='replace', index=False)
        
        invoice_line_table.toPandas().to_sql("invoiceLines", con = conn, if_exists='replace', index=False)
        
        track_table.toPandas().to_sql("tracks", con = conn, if_exists='replace', index=False)
        
        conn.execute("drop table if exists temp_revenue_per_customer_genre")
        conn.execute(f"""
                    Create temporary table temp_revenue_per_customer_genre as
                    SELECT 
                        i.CustomerId, 
                        t.GenreId, 
                        SUM(il.UnitPrice * il.Quantity * (Case when il.status = 'active' then 1 else -1 end)) 
                        + (case 
                            when (select revenue_overall from {elt_table_name} where {elt_table_name}.CustomerId = i.customerId and {elt_table_name}.GenreId = t.GenreId) is not null
                            then (select revenue_overall from {elt_table_name} where {elt_table_name}.CustomerId = i.customerId and {elt_table_name}.GenreId = t.GenreId)
                            else 0
                        end) AS revenue_overall,
                        CASE 
                            WHEN (SELECT created_at FROM {elt_table_name} where {elt_table_name}.CustomerId = i.customerId and {elt_table_name}.GenreId = t.GenreId) IS NOT NULL 
                            THEN (SELECT created_at FROM {elt_table_name} where {elt_table_name}.CustomerId = i.customerId and {elt_table_name}.GenreId = t.GenreId) 
                            ELSE CURRENT_DATE 
                        END AS created_at,  
                        CURRENT_DATE AS updated_at, 
                        'WeeklyCustomerRevenuePerGenre:{current_user}' AS updated_by
                    FROM invoiceLines as il
                    JOIN invoice i ON il.InvoiceId = i.InvoiceId
                    JOIN (select distinct(TrackId), GenreId from tracks) as t on il.TrackId = t.TrackId
                    GROUP BY i.CustomerId, t.GenreId;
                     """)
        conn.commit()
        
        conn.execute(f"""
                     delete from {elt_table_name} where (CustomerId, GenreId) in (select CustomerId, GenreId from temp_revenue_per_customer_genre)
                     """)
        conn.commit()
        
        #insert new rows
        conn.execute(f"""
                     insert into {elt_table_name} (CustomerId, GenreId, revenue_overall, created_at, updated_at, updated_by)
                     select * from temp_revenue_per_customer_genre
                     """)
        conn.commit()
            
    finally:
        spark.stop()
        
if __name__ == "__main__":
    incremental_load()