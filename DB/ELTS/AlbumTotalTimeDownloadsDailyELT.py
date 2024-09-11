from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3

def incremental_load():
    
    current_user = "User" # when IAM is implemented, get current user for session details
    
    elt_table_name = 'album_total_time_downloads_elt'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ELT Template with SQLite").getOrCreate()
    
    conn = sqlite3.connect(base_path + 'database.db')
    
    try:
        conn = sqlite3.connect(base_path + "database.db")
        
        
        
    finally:
        conn.close()
        spark.stop()