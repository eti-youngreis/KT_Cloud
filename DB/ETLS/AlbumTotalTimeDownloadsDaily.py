from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from sqlalchemy import create_engine, Table, MetaData, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import pandas as pd

def incremental_load():
    # configure basic variables
    current_user = "User" # when IAM is implemented, get current user for session details
    
    etl_table_name = 'album_total_time_downloads'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ETL Template with SQLite").getOrCreate()

    try:
        # connect to database
        conn = sqlite3.connect(base_path + "database.db")
        
        # get the date of the last incremental load
        latest_timestamp_query = f"SELECT MAX(updated_at) from {etl_table_name}"
        
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
        
        # total time per album
        total_time_per_album = track_table.groupBy("AlbumId").agg(
            F.sum(
                F.when(F.col("status") == "active", F.col("Milliseconds"))
                .otherwise(-F.col("Milliseconds"))
            ).alias("total_album_length")
        ).drop(track_table['status'])

        track_table = track_table.filter(F.col("status") == F.lit('active'))
        track_table = track_table.drop(track_table['status'])
        
        # total downloads per album
        joined_table = track_table.join(
            invoice_line_table,
            track_table["TrackId"] == invoice_line_table["TrackId"],
            "left"
        ).drop(invoice_line_table["TrackId"])

        # Compute total_downloads_per_album with status-based conditional logic
        total_downloads_per_album = joined_table.groupBy("AlbumId").agg(
            F.sum(
                F.when(F.col("status") == "active", F.col("Quantity"))
                .otherwise(-F.col("Quantity"))
            ).alias("total_album_downloads")
        )
        
        final_data = total_downloads_per_album.join(total_time_per_album, total_time_per_album["AlbumId"] ==\
            total_downloads_per_album["AlbumId"]).drop(total_time_per_album["AlbumId"])\
                .withColumn("created_at", F.current_date()) \
                .withColumn("updated_at", F.current_date()) \
                .withColumn("updated_by", F.lit(f"DailyAlbumTotalsDailyETL:{current_user}"))
        
        final_data = final_data.toPandas()
        
        engine = create_engine('sqlite:///' + base_path + 'database.db')
        metadata = MetaData()
        table = Table(etl_table_name, metadata, autoload_with=engine)
        
        with engine.connect() as connection:
            for index, row in final_data.iterrows():
                row = row.to_dict()
                
                print(row['total_album_downloads'])
                select_stmt = select(table).where(table.c.AlbumId == row['AlbumId'])
                result = connection.execute(select_stmt).fetchone()
                
                
                if result:
                    # Record exists, update it
                    update_stmt = (
                        update(table)
                        .where(table.c.AlbumId == row['AlbumId'])
                        .values(
                            total_album_downloads=table.c.total_album_downloads + (row['total_album_downloads'] if pd.notna(row['total_album_downloads']) else 0),
                            total_album_length=table.c.total_album_length + (row['total_album_length'] if pd.notna(row['total_album_length']) else 0),
                            updated_at=row['updated_at'],
                            updated_by=row['updated_by']
                        )
                    )
                    connection.execute(update_stmt)
                else:
                    # Record does not exist, insert it
                    insert_stmt = sqlite_insert(table).values(row)
                    connection.execute(insert_stmt)
            connection.commit()
        
    finally:
        conn.close()
        spark.stop()


