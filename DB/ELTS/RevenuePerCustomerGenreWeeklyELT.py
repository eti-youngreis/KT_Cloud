from pyspark.sql import SparkSession
import sqlite3

import sqlalchemy

def load():
    
    current_user = "User" # when IAM is implemented, get current user for session details
    
    elt_table_name = 'revenue_per_customer_genre_elt'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    conn = sqlite3.connect(base_path + 'database.db')
    
    try:
        conn = sqlite3.connect(base_path + "database.db")
        
        # extract csv files
        invoice_table = spark.read.option("header", "true").csv(base_path + "Invoice.csv")
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv")
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")
        
        invoice_table.toPandas().to_sql("invoice", con = conn, if_exists='replace', index=False)
        
        invoice_line_table.toPandas().to_sql("invoiceLines", con = conn, if_exists='replace', index=False)
        
        track_table.toPandas().to_sql("tracks", con = conn, if_exists='replace', index=False)
        
        conn.execute(f"drop table if exists {elt_table_name}")
        
        conn.execute(f"""
                     create table {elt_table_name} (
                        CustomerId INTEGER,
                        GenreId INTEGER,
                        revenue_overall INTEGER,
                        created_at DATE,
                        updated_at DATE,
                        updated_by TEXT
                     );""")
        
        transformation_query = f"""
                     insert into {elt_table_name} (CustomerId, GenreId, revenue_overall, created_at, updated_at, updated_by)
                     SELECT 
                        i.CustomerId, 
                        t.GenreId, 
                        SUM(il.UnitPrice * il.Quantity) AS revenue_overall,
                        CURRENT_DATE AS created_at,  
                        CURRENT_DATE AS updated_at, 
                        'WeeklyCustomerRevenuePerGenre:{current_user}' AS updated_by
                    FROM (select * from invoiceLines where status = 'active') as il
                    JOIN invoice i ON il.InvoiceId = i.InvoiceId
                    JOIN (select distinct(TrackId), GenreId from tracks) as t on il.TrackId = t.TrackId
                    GROUP BY i.CustomerId, t.GenreId;
                """
                
        conn.execute(transformation_query)    
        conn.commit()
    finally:
        conn.close()
        spark.stop()

if __name__== "__main__":
    load()