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
        
        invoice_line_table = invoice_line_table.withColumn("updated_at", F.to_timestamp(invoice_line_table["updated_at"], "yyyy-MM-dd"))
        
        # only invoice lines created within the range we are dealing with
        invoice_line_table = invoice_line_table.filter(invoice_line_table["updated_at"] > latest_timestamp)
        
        
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", 
                                                                header=True, inferSchema=True)
        track_table = track_table.drop(track_table['status'])
        
        # assign customer Id to each of his invoice lines
        invoices = invoice_table.join(invoice_line_table, invoice_table[ \
            'InvoiceId'] == invoice_line_table['InvoiceId']).drop(invoice_line_table['InvoiceId'])
        
        # assign track data to each invoice line      
        invoices_tracks = invoices.join(track_table, invoices['TrackId'] == \
            track_table['TrackId']).drop(invoices['TrackId']).drop(track_table['UnitPrice'])\
                
        # calculate the total for each invoice line       
        invoices_tracks = invoices_tracks\
        .withColumn(
            'invoice_line_total', 
            F.when(F.col('status') == 'active', F.col('UnitPrice') * F.col('Quantity'))  # Add if active
            .otherwise(-1 * F.col('UnitPrice') * F.col('Quantity'))  # Subtract if not active
        )
        
        # group by the total revenue per customer per genere
        final_data = invoices_tracks.groupBy("CustomerId", "GenreId").agg(
            F.sum("invoice_line_total").alias("revenue_overall")
        ).withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"DailyCustomerRevenuePerGenre:{current_user}"))
        
        final_data = final_data.toPandas()
        
        # extract old data from target table
        existing_data = pd.read_sql(f'SELECT * FROM {etl_table_name}', conn)

        # Merge new data with existing data
        # keep original creation date if exists, otherwise new
        final_data['created_at'] = final_data.apply(
            lambda row: existing_data.loc[(existing_data['CustomerId'] == row['CustomerId']) & (existing_data['GenreId'] == row['GenreId']),
                                          'created_at'].values[0] if\
                                              (row['CustomerId'] in existing_data['CustomerId'].values) and\
                                                  (row['GenreId'] in existing_data['GenreId'].values) else\
                                              row['created_at'],
            axis = 1
        )
        
        # add new revenue calculation to old revenue calculation if exists, otherwise apply new
        final_data['revenue_overall'] = final_data.apply(
            lambda row: existing_data.loc[(existing_data['CustomerId'] == row['CustomerId']) & (existing_data['GenreId'] == row['GenreId']),
                                          'revenue_overall'].values[0] + (row['revenue_overall'] if not pd.isna(row['revenue_overall']) else 0.0) if\
                (row['CustomerId'] in existing_data['CustomerId'].values) and (row['GenreId'] in existing_data['GenreId'].values) else\
                (row['revenue_overall'] if not pd.isna(row['revenue_overall']) else 0.0),
            axis = 1
        )
        
        print(final_data)
        # list ids of customer genre pairs that have new data
        ids_to_delete = final_data[['CustomerId', 'GenreId']].values.tolist()

        # Generate the query with tuples
        placeholders = ', '.join(['(?, ?)' for _ in ids_to_delete])
        
        # delete the old rows of these ids
        delete_query = f"""
        DELETE FROM {etl_table_name}
        WHERE (CustomerId, GenreId) IN ({placeholders})
        """
        flattened_ids = [item for sublist in ids_to_delete for item in sublist]

        conn.execute(delete_query, flattened_ids)
        conn.commit()
        
        # insert the new rows
        final_data.to_sql(etl_table_name, con = conn, if_exists='append', index=False)
    
    finally:
        conn.close()
        spark.stop()
        
        