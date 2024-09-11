from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from sqlalchemy import create_engine, Table, MetaData, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import inspect, text
import pandas as pd

def incremental_load():
    
    raise NotImplementedError()

    current_user = "User" # when IAM is implemented, get current user for session details
    
    elt_table_name = 'album_total_time_downloads_elt'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    engine = create_engine('sqlite:///' + base_path + 'database.db')
    
    try:
        # Check if the 'album_total_time_downloads_elt' table exists
        result = False
        # Connect using SQLAlchemy
        with engine.connect() as conn:
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
                    CREATE TABLE {elt_table_name} (
                    AlbumId INTEGER,
                    total_album_downloads INTEGER,
                    total_album_length INTEGER,
                    created_at DATE,
                    updated_at DATE,
                    updated_by TEXT
                );""")
            conn.commit()
        
        # get the date of the last incremental load
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(updated_at) from {elt_table_name}"))
            latest_timestamp = result.fetchone()[0]

        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        
        # extract data from csv files and filter out old data
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", header=True, 
                                                              inferSchema=True)
        track_table = track_table.withColumn("updated_at", F.to_timestamp(track_table["updated_at"], "yyyy-MM-dd"))
        
        track_table = track_table.filter(track_table["updated_at"] > latest_timestamp)
    
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                     header=True, inferSchema=True)
        invoice_line_table = invoice_line_table.withColumn("updated_at", F.to_timestamp(invoice_line_table["updated_at"], "yyyy-MM-dd"))
        
        invoice_line_table = invoice_line_table.filter(invoice_line_table["updated_at"] >  latest_timestamp)
        conn = sqlite3.connect(base_path + "database.db")

        track_table.toPandas().to_sql('tracks', con = conn, if_exists='append', index=False)
        invoice_line_table.toPandas().to_sql('invoiceLines', con = conn, if_exists='append', index=False)

        transformation_query = ""
        
        conn.execute(transformation_query)
        conn.commit()
        
    finally:
        conn.close()
        spark.stop()
        
        
if __name__ == "__main__":
    incremental_load()