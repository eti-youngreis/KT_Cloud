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
        
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                     header=True, inferSchema=True)
        # filter data according to the last time it was updated
        track_table = track_table.withColumn("updated_at", F.to_timestamp(track_table["updated_at"], "yyyy-MM-dd"))
        
        track_table = track_table.filter(track_table["updated_at"] > latest_timestamp)
    
        invoice_line_table = invoice_line_table.withColumn("updated_at", F.to_timestamp(invoice_line_table["updated_at"], "yyyy-MM-dd"))
        
        invoice_line_table = invoice_line_table.filter(invoice_line_table["updated_at"] >  latest_timestamp)
        invoice_line_table = invoice_line_table.filter(invoice_line_table["updated_at"] >  latest_timestamp)
        
        # total time per album
        total_time_per_album = track_table.groupBy("AlbumId").agg(
            F.sum(
                F.when(F.col("status") == "active", F.col("Milliseconds"))
                .otherwise(-F.col("Milliseconds"))
            ).alias("total_album_length")
        ).drop(track_table['status'])

        # read entire track table regardless of date - for total album downloads calculation
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", header=True, 
                                                              inferSchema=True).select("TrackId", "AlbumId")
        
        # keep one line for each track Id, in case of update
        track_table = track_table.distinct()
        
        # total downloads per album
        joined_table = invoice_line_table.join(
            track_table,
            track_table["TrackId"] == invoice_line_table["TrackId"],
            how="left"
        ).drop(track_table["TrackId"]).select("AlbumId", "TrackId", "Quantity", "UnitPrice", "status")

        # Compute total_downloads_per_album with status-based conditional logic
        total_downloads_per_album = joined_table.groupBy("AlbumId").agg(
            F.sum(
                F.when(F.col("status") == "active", F.col("Quantity"))
                .otherwise(-F.col("Quantity"))
            ).alias("total_album_downloads")
        ).select("AlbumId", "total_album_downloads")
        
        # add metadata
        final_data = total_downloads_per_album.join(total_time_per_album, total_time_per_album["AlbumId"] ==\
            total_downloads_per_album["AlbumId"], how = "outer")\
                .withColumn("created_at", F.current_date()) \
                .withColumn("updated_at", F.current_date()) \
                .withColumn("updated_by", F.lit(f"DailyAlbumTotalsDailyETL:{current_user}"))\
                .withColumn( "NewAlbumId", F.coalesce(total_time_per_album["AlbumId"], total_downloads_per_album["AlbumId"]))\
                .drop(total_time_per_album["AlbumId"])
        
        # select required columns and rename NewAlbumId to AlbumId
        final_data = final_data.select('NewAlbumId', 'total_album_downloads', 'total_album_length', 'created_at', 'updated_at', 'updated_by')
        final_data = final_data.withColumn(
            'AlbumId', F.col('NewAlbumId')
        ).drop('NewAlbumId')
        
        # convert dataframe to pandas
        final_data = final_data.toPandas()
        
        # extract old data from target table
        existing_data = pd.read_sql(f'SELECT * FROM {etl_table_name}', conn)

        # Merge new data with existing data
        
        # keep original creation date if exists, otherwise new
        final_data['created_at'] = final_data.apply(
            lambda row: existing_data.loc[existing_data['AlbumId'] == row['AlbumId'],
                                          'created_at'].values[0] if row['AlbumId'] in existing_data['AlbumId'].values else\
                                              row['created_at'],
            axis = 1
        )
        
        # add new download total to existing total if exists, otherwise apply new total
        final_data['total_album_downloads'] = final_data.apply(
            lambda row: existing_data.loc[existing_data['AlbumId'] == row['AlbumId'], 'total_album_downloads'].values[0] + (row['total_album_downloads'] if not pd.isna(row['total_album_downloads']) else 0.0) if\
                row['AlbumId'] in existing_data['AlbumId'].values else\
                (row['total_album_downloads'] if not pd.isna(row['total_album_downloads']) else 0.0),
            axis = 1
        )
       
        # add new album length to old album length if exists, otherwise apply new
        final_data['total_album_length'] = final_data.apply(
            lambda row: existing_data.loc[existing_data['AlbumId'] == row['AlbumId'], 'total_album_length'].values[0] + (row['total_album_length'] if not pd.isna(row['total_album_length']) else 0.0) if\
                row['AlbumId'] in existing_data['AlbumId'].values else\
                (row['total_album_length'] if not pd.isna(row['total_album_length']) else 0.0),
            axis = 1 
        )
        
        # list ids of albums that have new data
        ids_to_delete = final_data['AlbumId'].tolist()

        # delete the old rows of these ids
        delete_query = f"""
        DELETE FROM {etl_table_name}
        WHERE AlbumId IN ({', '.join(['?' for _ in ids_to_delete])})
        """
        conn.execute(delete_query, ids_to_delete)
        conn.commit()
        
        # insert the new rows
        final_data.to_sql(etl_table_name, con = conn, if_exists='append', index=False)
        
    finally:
        conn.close()
        spark.stop()
