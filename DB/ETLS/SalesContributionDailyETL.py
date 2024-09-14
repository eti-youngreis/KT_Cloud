import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import sqlite3

def load_sales_distribution_daily_increment():
    # שלב 1: אתחול סשן Spark
    spark = SparkSession.builder \
        .appName("Daily Incremental Sales Distribution by Media Type") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    # שלב 2: חיבור ל-SQLite
    conn = sqlite3.connect('./Chinook.db')
    
    try:
        # בדוק אם הטבלה קיימת
        check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='SalesDistributionByMediaType';"
        table_exists = conn.execute(check_table_query).fetchone()
        latest_timestamp = None
        if table_exists:
            latest_timestamp_query = "SELECT MAX(updated_at) FROM SalesDistributionByMediaType"
            latest_timestamp = conn.execute(latest_timestamp_query).fetchone()[0]
        
        # טיפול במקרה שבו אין נתונים קיימים (טעינה ראשונית)
        if latest_timestamp is None:
            latest_timestamp = '1900-01-01 00:00:00'
        
        # EXTRACT (טעינת קבצי CSV ממאגר או אחסון מקומי)
        invoice_df = spark.read.csv('C:/Users/leabe/Documents/data/Invoice.csv', header=True, inferSchema=True)
        invoice_line_df = spark.read.csv('C:/Users/leabe/Documents/data/InvoiceLine.csv', header=True, inferSchema=True)
        track_df = spark.read.csv('C:/Users/leabe/Documents/data/Track.csv', header=True, inferSchema=True)
        media_type_df = spark.read.csv('C:/Users/leabe/Documents/data/MediaType.csv', header=True, inferSchema=True)
        
        # המרת עמודות תאריך
        invoice_df = invoice_df.withColumn('InvoiceDate', F.to_date(F.col('InvoiceDate'), 'dd/MM/yyyy'))
        
        # TRANSFORM (ביצוע חיבורים, קבוצות)
        joined_df = invoice_df.join(invoice_line_df, invoice_df.InvoiceId == invoice_line_df.InvoiceId) \
            .join(track_df, invoice_line_df.TrackId == track_df.TrackId) \
            .join(media_type_df, track_df.MediaTypeId == media_type_df.MediaTypeId) \
            .filter(F.col('InvoiceDate') > latest_timestamp)
        
        joined_df = joined_df.withColumn('Hour', F.hour(F.col('InvoiceDate')))
        
        sales_distribution = joined_df.groupBy('MediaType', 'Hour') \
            .agg(F.count('InvoiceLineId').alias('Frequency'), F.sum('Total').alias('TotalSales'))
        
        # הוספת מטה-נתונים
        current_user = os.getenv('USER')  # הצב את שם המשתמש האמיתי שלך
        sales_distribution = sales_distribution.withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"process:{current_user}"))

        # LOAD (שמירת נתונים מעובדים ל-SQLite באמצעות SQLite3)
        final_data_df = sales_distribution.toPandas()
        
        if table_exists:
            ids_placeholder = ', '.join('?' * len(final_data_df['MediaType']))
            delete_query = f"DELETE FROM SalesDistributionByMediaType WHERE MediaType IN ({ids_placeholder})"
            conn.execute(delete_query, final_data_df['MediaType'].tolist())
        
        final_data_df.to_sql('SalesDistributionByMediaType', conn, if_exists='append', index=False)
        conn.commit()
    
    finally:
        # סיום חיבור SQLite וסשן Spark
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load_sales_distribution_daily_increment()
