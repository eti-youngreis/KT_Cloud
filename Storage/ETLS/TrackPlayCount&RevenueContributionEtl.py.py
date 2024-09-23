from pyspark.sql import SparkSession
import sqlite3
import traceback
user_name = "Shani_K"  # שם המשתמש שלך

def load_track_play_count_and_revenue_contribution_elt(spark):
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("ELT Template with KT_DB") \
        .getOrCreate()

    # Step 2: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database
    try:
        # EXTRACT (שליפה של קבצי CSV)
        track_table = spark.read.csv("./csv/Track.csv", header=True, inferSchema=True)
        invoiceLine_table = spark.read.csv("./csv/InvoiceLine.csv", header=True, inferSchema=True)
    

    # Convert Spark DataFrames to Pandas DataFrames before loading to SQLite
        track_table_pd = track_table.toPandas()
        invoiceLine_table_pd = invoiceLine_table.toPandas()


        # customers_pd['created_at'] = pd.to_datetime(customers_pd['created_at'], format='%d/%m/%Y %H:%M')
        # invoices_pd['InvoiceDate'] = pd.to_datetime(invoices_pd['InvoiceDate'], format='%d/%m/%Y %H:%M')

        track_table_pd.to_sql('Track', conn, if_exists='replace', index=False)
        invoiceLine_table_pd.to_sql('InvoicesLine', conn, if_exists='replace', index=False)
        
        # TRANSFORM (טרנספורמציות של הנתונים עם PySpark)
        # track_table.createOrReplaceTempView("Track")
        # invoiceLine_table.createOrReplaceTempView("InvoiceLine")
        
        drop_query="""DROP TABLE IF EXISTS track_play_count_and_revenue_contribution_elt"""
        conn.execute(drop_query)
        conn.commit()
        
        transform_query = """
        SELECT 
            t.TrackID,
            t.Name,
            t.AlbumID,
            SUM(inv.Quantity) AS total_play_count,
            SUM(inv.Quantity * inv.UnitPrice) AS revenue_contribution,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at,
            'process:{user_name}' AS updated_by
        FROM Track t
        JOIN InvoiceLine inv ON t.TrackID = inv.TrackID
        GROUP BY t.TrackID
        """

        conn.execute(transform_query)
        # Commit the changes to the database
        conn.commit()
        print("track_play_count_and_revenue_contribution_elt:", conn.execute("SELECT * FROM track_play_count_and_revenue_contribution_elt").fetchall())
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        # Step 3: Close the SQLite connection and stop Spark session
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session
if __name__ == "__main__":
    load_track_play_count_and_revenue_contribution_elt()



    # revenue_contribution_df = spark.sql(transform_query)
    # return revenue_contribution_df

