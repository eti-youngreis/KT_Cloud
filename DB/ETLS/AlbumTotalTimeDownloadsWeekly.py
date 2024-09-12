from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
from numpy import int64

# import KT_DB
import sqlite3


def load():
    # configure basic variables
    current_user = "User" # when IAM is implemented, get current user for session details
    
    etl_table_name = 'album_total_time_downloads'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ETL Template with SQLite").getOrCreate()

    try:
        # connect to database
        conn = sqlite3.connect(base_path + "database.db")
        
        # extract data from csv files
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", 
                                                              header=True, inferSchema=True)

        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                     header=True, inferSchema=True)
        # only active invoice lines should be consider when calculating overall downloads per album
        invoice_line_table = invoice_line_table.filter(F.col('status') == F.lit('active'))
        
        # total time per album, only consider active track records
        active_track_table = track_table.filter(track_table['status'] == F.lit('active'))
        
        total_time_per_album = active_track_table.groupBy("AlbumId").agg(
            F.sum("Milliseconds").alias("total_album_length")
        )
        
        # consider each track only once regardless of updates
        track_table = track_table.select("TrackId", "AlbumId").distinct()
        # total downloads per album
        total_downloads_per_album = track_table.join(
            invoice_line_table,
            track_table["TrackId"] == invoice_line_table["TrackId"],
            "left",
        ).drop(invoice_line_table["TrackId"]).groupBy("AlbumId").agg(
            F.sum("Quantity").alias("total_album_downloads"),
        )
    
        # join derived conclusions to final data and add metadata
        final_data = total_time_per_album.join(total_downloads_per_album, total_time_per_album["AlbumId"] ==\
            total_downloads_per_album["AlbumId"]).drop(total_time_per_album["AlbumId"])\
            .withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"WeeklyAlbumTotalsETL:{current_user}"))
    
        # load data to database
        final_data = final_data.toPandas()
        
        final_data['AlbumId'] = final_data['AlbumId'].astype(int)
    
        final_data.to_sql(name=etl_table_name, con = conn, if_exists='replace', index=False)   
        
    finally:
        conn.close()
        spark.stop()