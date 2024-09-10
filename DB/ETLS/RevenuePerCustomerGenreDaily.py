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
    
    etl_table_name = 'revenue_per_customer_genre'
    
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
        
        # extract data from csv files
        invoice_table = spark.read.option("header", "true").csv(base_path + "Invoice.csv", 
                                                                header=True, inferSchema=True)
      
        invoice_table = invoice_table.drop(invoice_table['status'])
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                header=True, inferSchema=True)
        
        invoice_line_table = invoice_line_table.withColumn("created_at", F.to_timestamp(invoice_line_table["created_at"], "yyyy-MM-dd"))
        
        invoice_line_table = invoice_line_table.filter(invoice_line_table["created_at"] > latest_timestamp)
        
        
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", 
                                                                header=True, inferSchema=True)
        track_table = track_table.drop(track_table['status'])
        
        invoices = invoice_table.join(invoice_line_table, invoice_table[ \
            'InvoiceId'] == invoice_line_table['InvoiceId']).drop(invoice_line_table['InvoiceId'])
                
        invoices_tracks = invoices.join(track_table, invoices['TrackId'] == \
            track_table['TrackId']).drop(invoices['TrackId']).drop(track_table['UnitPrice'])\
                
                
        invoices_tracks = invoices_tracks\
        .withColumn(
            'invoice_line_total', 
            F.when(F.col('status') == 'active', F.col('UnitPrice') * F.col('Quantity'))  # Add if active
            .otherwise(-1 * F.col('UnitPrice') * F.col('Quantity'))  # Subtract if not active
        )
        
        
        final_data = invoices_tracks.groupBy("CustomerId", "GenreId").agg(
            F.sum("invoice_line_total").alias("revenue_overall")
        ).withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"DailyCustomerRevenuePerGenre:{current_user}"))
        
        final_data = final_data.toPandas()
        
        print(final_data)
        
        engine = create_engine('sqlite:///' + base_path + 'database.db')
        metadata = MetaData()
        table = Table(etl_table_name, metadata, autoload_with=engine)
        
        with engine.connect() as connection:
            for index, row in final_data.iterrows():
                row = row.to_dict()
                select_stmt = select(table).where(table.c.GenreId == row['GenreId']).where(table.c.CustomerId == row['CustomerId'])
                result = connection.execute(select_stmt).fetchone()
                
                
                if result:
                    # Record exists, update it
                    update_stmt = (
                        update(table)
                        .where(table.c.GenreId == row['GenreId']).where(table.c.CustomerId == row['CustomerId'])
                        .values(
                            revenue_overall=table.c.revenue_overall + (row['revenue_overall'] if pd.notna(row['revenue_overall']) else 0),
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
        
        
if __name__ == "__main__":
    incremental_load()