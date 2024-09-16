from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import os
from pyspark.sql.window import Window

# Set environment variables for Spark and Java
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark-3.5.2-bin-hadoop3\spark-3.5.2-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\רוט\AppData\Local\Programs\Python\Python312\python.exe"
os.environ["PATH"] += f";{os.environ['JAVA_HOME']}\\bin"


def load_Track_Popularity():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder.appName(
        "Track_Popularity Weekly ETL").getOrCreate()

    # Path to the SQLite database
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\Track_Popularity_db.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # EXTRACT (Load CSV files into Spark DataFrames)
        tracks_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Track.csv', header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\InvoiceLine.csv', header=True, inferSchema=True)
        invoices_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Invoice.csv', header=True, inferSchema=True)

        # Assign aliases to DataFrames to avoid ambiguity
        tracks_df_alias = tracks_df.alias("t")
        invoice_lines_df_alias = invoice_lines_df.alias("il")
        invoices_df_alias = invoices_df.alias("i")

        # JOIN the DataFrames
        full_data_df = invoices_df_alias.join(invoice_lines_df_alias, "InvoiceId", "inner") \
            .join(tracks_df_alias, "TrackId", "inner")

        # Calculate total copies sold for each track
        popularity_df = full_data_df.groupBy("t.TrackId") \
            .agg(F.sum("il.Quantity").alias("CopiesSold")) \
            .orderBy(F.desc("CopiesSold"))

        # Define the window for ranking popularity
        window_spec = Window.orderBy(F.desc("CopiesSold"))

        # Add a rank column for popularity
        popularity_with_rank_df = popularity_df.withColumn(
            "PopularityRank", F.rank().over(window_spec))


        final_data = tracks_df_alias.join(
                    popularity_with_rank_df,
                    tracks_df_alias["TrackId"] == popularity_with_rank_df["TrackId"],
                    "left"
                ).select(
                    tracks_df_alias["*"], 
                    popularity_with_rank_df["PopularityRank"],  
                    F.current_timestamp().alias("created_at"),  
                    F.current_timestamp().alias("updated_at"),  
                    F.lit("sarit_roth").alias("updated_by") 
                )


        final_data.show()

        # LOAD (Save transformed data into SQLite database)
        final_data_df = final_data.toPandas()

        try:
            # Drop the table if it already exists
            cursor.execute('DROP TABLE IF EXISTS track_popularity')
            # Create table in SQLite if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS track_popularity (
                    TrackId INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    AlbumId INTEGER,
                    MediaTypeId INTEGER,
                    GenreId INTEGER,
                    Composer TEXT,
                    Milliseconds INTEGER,
                    Bytes INTEGER,
                    UnitPrice DECIMAL,
                    CopiesSold INTEGER,
                    PopularityRank INTEGER,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    updated_by TEXT
                )
            ''')

            # Insert transformed data into track_popularity table
            final_data_df.to_sql('track_popularity', conn,
                                 if_exists='append', index=False)

            # Commit the changes to the database
            conn.commit()

        except Exception as e:
            print(f"An error occurred during database operations: {e}")

    finally:
        # Step 3: Clean up resources
        cursor.close()  # Close the cursor
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session


# Execute the function
# load_Track_Popularity()
