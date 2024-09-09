from pyspark.sql import SparkSession
import sqlite3

def load():
    
    current_user = "User" # when IAM is implemented, get current user for session details
    
    etl_table_name = 'album_total_time_downloads'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    conn = sqlite3.connect(base_path + 'database.db')
    
    try:
        conn = sqlite3.connect(base_path + "database.db")
        
        # extract data from csv files
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")

        invoice_line_table = spark.read.option("header", "true").csv(
            base_path + "InvoiceLine.csv"
        )
        
        track_table.toPandas().to_sql('tracks', conn, if_exists='replace', index=False)
        invoice_line_table.toPandas().to_sql('invoices', conn, if_exists='replace', index = False)
        
        conn.execute("""
                     """)
        
    finally:
        conn.close()
        spark.stop()