from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from sqlalchemy import create_engine, Table, MetaData, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import inspect

import pandas as pd

def incremental_load():
    
    current_user = "User" # when IAM is implemented, get current user for session details
    
    elt_table_name = 'album_total_time_downloads_elt'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    conn = sqlite3.connect(base_path + 'database.db')
    
    try:
        conn = sqlite3.connect(base_path + "database.db")
        
        
        cursor = conn.cursor()

        # Check if the 'AlbumPopularityAndRevenue' table exists
        inspector = inspect(conn)

        if inspector.has_table(elt_table_name):
            # Table exists, run the query
            result = conn.execute(f"SELECT * FROM {elt_table_name}")
        else:
            # Table doesn't exist, return None or handle accordingly
            result = None
        
        print(result)
        # If the table doesn't exists, create it
        # if table_exists == None:
        #     print(f"Table '{elt_table_name}' doesn't exists, creating it.")
        #     conn.execute(f"select table if exists {elt_table_name}")
        #     conn.commit()
        #     conn.execute(f"""
        #             CREATE TABLE {elt_table_name} (
        #             AlbumId INTEGER,
        #             total_album_downloads INTEGER,
        #             total_album_length INTEGER,
        #             created_at DATE,
        #             updated_at DATE,
        #             updated_by TEXT
        #         );""")
        #     conn.commit()
        
        # get the date of the last incremental load
        latest_timestamp_query = f"SELECT MAX(updated_at) from {elt_table_name}"
        
        cursor = conn.cursor()
        cursor.execute(latest_timestamp_query)
        latest_timestamp = cursor.fetchone()[0]
        cursor.close()
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        
        # extract data from csv files and filter out old data
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", header=True, 
                                                              inferSchema=True)
        track_table = track_table.withColumn("created_at", F.to_timestamp(track_table["created_at"], "yyyy-MM-dd"))
        
        track_table = track_table.filter(track_table["created_at"] > latest_timestamp)
    
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                     header=True, inferSchema=True)
        invoice_line_table = invoice_line_table.withColumn("created_at", F.to_timestamp(invoice_line_table["created_at"], "yyyy-MM-dd"))
        
        invoice_line_table = invoice_line_table.filter(invoice_line_table["created_at"] >  latest_timestamp)
        
        # load into tables
        engine = create_engine('sqlite:///' + base_path + 'database.db')
        metadata = MetaData()
        table = Table("invoiceLines", metadata, autoload_with=engine)
        print(table)
        # with engine.connect() as connection:
            
        #     for index, row in final_data.iterrows():
                
        #         print(index)
        #         select_stmt = select(table).where(table.c.AlbumId == row['AlbumId'])
        #         result = connection.execute(select_stmt).fetchone()

        #         if result:
        #             # Record exists, update it
        #             update_stmt = (
        #                 update(table)
        #                 .where(table.c.AlbumId == row['AlbumId'])
        #                 .values(
        #                     total_album_downloads=table.c.total_album_downloads + (row['total_album_downloads'] if pd.notna(row['total_album_downloads']) else 0),
        #                     total_album_length=table.c.total_album_length + (row['total_album_length'] if pd.notna(row['total_album_length']) else 0),
        #                     updated_at=row['updated_at'],
        #                     updated_by=row['updated_by']
        #                 )
        #             )
        #             connection.execute(update_stmt)
        #         else:
        #             # Record does not exist, insert it
        #             insert_stmt = sqlite_insert(table).values(row)
        #             connection.execute(insert_stmt)
        #     connection.commit()
        
    finally:
        conn.close()
        spark.stop()
        
        
if __name__ == "__main__":
    incremental_load()