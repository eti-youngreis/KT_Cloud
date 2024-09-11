from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
from sqlalchemy import create_engine, Table, MetaData, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import inspect, text
import pandas as pd

def incremental_load():
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
        
        new_track_table = track_table.filter(track_table["updated_at"] > latest_timestamp)
    
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                     header=True, inferSchema=True)
        invoice_line_table = invoice_line_table.withColumn("updated_at", F.to_timestamp(invoice_line_table["updated_at"], "yyyy-MM-dd"))
        
        invoice_line_table = invoice_line_table.filter(invoice_line_table["updated_at"] >  latest_timestamp)
        conn = sqlite3.connect(base_path + "database.db")
        new_track_table.toPandas().to_sql('new_tracks', con = conn, if_exists = 'replace', index = False)
        track_table.toPandas().to_sql('tracks', con = conn, if_exists='replace', index=False)
        invoice_line_table.toPandas().to_sql('invoiceLines', con = conn, if_exists='replace', index=False)

        # save data about album lengths in temporary table (include total_downloads with base data from target table)
        conn.execute("""
                     drop table if exists temp_album_length
                     """)
        conn.execute(f"""
                     create temporary table temp_album_length as 
                        select tracks.AlbumId,
                        sum(CASE WHEN tracks.status = 'active' THEN Milliseconds ELSE -Milliseconds END)
                        + (CASE
                            WHEN (SELECT total_album_length FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId) IS NOT NULL
                            THEN (SELECT total_album_length FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId)
                            ELSE 0
                        end)
                        as total_album_length,
                        (select total_album_downloads from {elt_table_name} where {elt_table_name}.AlbumId = tracks.AlbumId) as total_album_downloads,
                        CASE 
                            WHEN (SELECT created_at FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId) IS NOT NULL 
                            THEN (SELECT created_at FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId) 
                            ELSE CURRENT_DATE 
                        END AS created_at
                        from new_tracks tracks group by AlbumId
                     """)
        
        conn.commit()
        # save data about total downloads for album, include column with basic album length from target table
        conn.execute("drop table if exists temp_album_downloads")
        conn.execute(f"""
                    create temporary table temp_album_downloads as
                        select AlbumId,
                        (select total_album_length from {elt_table_name} where {elt_table_name}.AlbumId = AlbumId) as total_album_length,
                        SUM(CASE when il.status = 'active' then quantity else -quantity end) 
                        + (case
                            WHEN (SELECT total_album_downloads FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId) IS NOT NULL
                            THEN (SELECT total_album_downloads FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId)
                            ELSE 0 
                        end) 
                        as total_album_downloads,
                        CASE 
                            WHEN (SELECT created_at FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId) IS NOT NULL 
                            THEN (SELECT created_at FROM {elt_table_name} WHERE {elt_table_name}.AlbumId = tracks.AlbumId) 
                            ELSE CURRENT_DATE 
                        END AS created_at
                        from invoiceLines il left join (select distinct(TrackId), AlbumId from tracks) tracks
                        on il.TrackId = tracks.TrackId
                        group by AlbumId
                    """)
        conn.commit()
        # COALESCE the data in the temporary tables, the first param is the default in case they both exist
        conn.execute("drop table if exists temp_downloads_time_total")
        conn.execute(f"""
                    create temporary table temp_downloads_time_total as
                    SELECT 
                        COALESCE(temp_album_downloads.AlbumId, temp_album_length.AlbumId) AS AlbumId,
                        COALESCE(temp_album_downloads.total_album_downloads, temp_album_length.total_album_downloads) as total_album_downloads,
                        COALESCE(temp_album_length.total_album_length, temp_album_downloads.total_album_length) as total_album_length,
                        COALESCE(temp_album_downloads.created_at, temp_album_length.created_at) as created_at,         
                        CURRENT_DATE as updated_at,
                        'DailyAlbumTotalsELT:{current_user}' as updated_by             
                    FROM 
                        temp_album_downloads
                    FULL JOIN 
                        temp_album_length
                    ON 
                        temp_album_downloads.AlbumId = temp_album_length.AlbumId;
                    """)
        conn.commit()
        
        # delete old rows
        conn.execute(f"""
                     delete from {elt_table_name} where AlbumId in (select AlbumId from temp_downloads_time_total)
                     """)
        conn.commit()
        
        #insert new rows
        conn.execute(f"""
                     insert into {elt_table_name} (AlbumId, total_album_downloads, total_album_length, created_at, updated_at, updated_by)
                     select * from temp_downloads_time_total
                     """)
        conn.commit()
    finally:
        conn.close()
        spark.stop()
        