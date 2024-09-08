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
        album_table = spark.read.option("header", "true").csv(base_path + "Album.csv")

        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")

        invoice_line_table = spark.read.option("header", "true").csv(
            base_path + "InvoiceLine.csv"
        )

        # join tables in order to transform data
        joined_album_track_table = album_table.join(
            track_table, album_table["AlbumId"] == track_table["AlbumId"], "inner"
        ).drop(track_table["AlbumId"])
        joined_album_track_invoice_line_table = joined_album_track_table.join(
            invoice_line_table,
            joined_album_track_table["TrackId"] == invoice_line_table["TrackId"],
            "inner",
        ).drop(invoice_line_table["TrackId"])

        # aggregate data to derive conclusions
        aggregated_data = joined_album_track_invoice_line_table.groupBy("AlbumId").agg(
            F.sum("Milliseconds").alias("total_album_length"),
            F.sum("Quantity").alias("total_album_downloads"),
        )
        
        # transformed data including metadata 
        final_data = aggregated_data.withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"DailyAlbumETL:{current_user}"))
    
        # load data to database
        final_data = final_data.toPandas()
            
        final_data.to_sql(name=etl_table_name, con = conn, if_exists='replace', index=False)   
        
    finally:
        conn.close()
        spark.stop()

