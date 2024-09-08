from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

import sqlite3

def load():
    current_user = "User" # when IAM is implemented, get current user for session details
    
    etl_table_name = 'revenue_per_customer_genre'
    
    base_path = "../etl_files/"
    spark = SparkSession.builder.appName("ETL Template with SQLite").getOrCreate()

    
    try:
        conn = sqlite3.connect(base_path + "database.db")
        customer_table = spark.read.option("header", "true").csv(base_path + "Customer.csv")
        invoice_table = spark.read.option("header", "true").csv(base_path + "Invoice.csv")
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv")
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv")
        genre_table = spark.read.option("header", "true").csv(base_path + "Genre.csv")
        
        customer_invoices_table = customer_table.join(invoice_table, customer_table['CustomerId'] == \
            invoice_table['CustomerId']).drop(invoice_table['CustomerId'])
            
        customer_invoice_line = customer_invoices_table.join(invoice_line_table, customer_invoices_table[ \
            'InvoiceId'] == invoice_line_table['InvoiceId']).drop(invoice_line_table['InvoiceId'])
        
        customer_track_table = customer_invoice_line.join(track_table, customer_invoice_line['TrackId'] == track_table['TrackId']).drop(customer_invoice_line['TrackId']).drop(customer_invoice_line['UnitPrice'])
        
        customer_genre_table = customer_track_table.join(genre_table, customer_track_table['GenreId'] == genre_table['GenreId']).drop(customer_track_table['GenreId'])

        
        customer_genre_table = customer_genre_table.withColumn(
            'invoice_line_total', 
            F.col('UnitPrice') * F.col('Quantity')
        )    
        
        aggregated_data = customer_genre_table.groupBy("CustomerId", "GenreId").agg(
            F.sum("invoice_line_total").alias("revenue_overall")
        )
        
        final_data = aggregated_data.withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"WeeklyCustomerRevenuePerGenre:{current_user}"))
    
        final_data = final_data.toPandas()
        
        final_data.to_sql(name=etl_table_name, con = conn, if_exists='replace', index=False)   

    finally:
        pass

        