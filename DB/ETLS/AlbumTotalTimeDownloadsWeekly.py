from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

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
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")

        invoice_line_table = spark.read.option("header", "true").csv(
            base_path + "InvoiceLine.csv"
        )
        
        # total time per album
        total_time_per_album = track_table.groupBy("AlbumId").agg(
            F.sum("Milliseconds").alias("total_album_length")
        )

        # total downloads per album
        joined_track_invoice_line_table = track_table.join(
            invoice_line_table,
            track_table["TrackId"] == invoice_line_table["TrackId"],
            "left",
        ).drop(invoice_line_table["TrackId"])
        
        total_downloads_per_album = joined_track_invoice_line_table.groupBy("AlbumId").agg(
            F.sum("Quantity").alias("total_album_downloads"),
        )

    
        # join derived conclusions to final data
        final_data = total_time_per_album.join(total_downloads_per_album, total_time_per_album["AlbumId"] ==\
            total_downloads_per_album["AlbumId"]).drop(total_downloads_per_album["AlbumId"])
        
        
        # transformed data including metadata 
        final_data = final_data.withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"DailyAlbumETL:{current_user}"))
    
        # load data to database
        final_data = final_data.toPandas()
        
        
        final_data['AlbumId'] = final_data['AlbumId'].astype(int)
    
        final_data.to_sql(name=etl_table_name, con = conn, if_exists='replace', index=False)   
        
    finally:
        conn.close()
        spark.stop()

