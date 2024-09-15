from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
import os

# Set environment variables for Spark and Java
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark-3.5.2-bin-hadoop3\spark-3.5.2-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\רוט\AppData\Local\Programs\Python\Python312\python.exe"
os.environ["PATH"] += f";{os.environ['JAVA_HOME']}\\bin"

def process_in_batches(df, batch_size=1000):
    total = df.count()
    batches = (total // batch_size) + (1 if total % batch_size != 0 else 0)
    
    all_track_ids = []
    for i in range(batches):
        batch = df.select("TrackId").limit(batch_size).offset(i * batch_size).collect()
        batch_ids = [row.TrackId for row in batch]
        all_track_ids.extend(batch_ids)
        # Process batch here if needed
    
    return all_track_ids

def load_Track_Length_and_Download_Frequency():
    # Step 1: Initialize Spark session
    spark = SparkSession.builder \
        .appName("Track_Length_and_Download_Frequency ETL") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.default.parallelism", "100") \
        .getOrCreate()

    # Path to the SQLite database
    db_path = r'D:\Users\רוט\Desktop\temp\KT_Cloud\DB\ETLS\track_db.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        check_table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='trackLengthandDownloadFrequencyDailyETL';"
        table_exists = conn.execute(check_table_query).fetchone()
        latest_timestamp = None
        if table_exists:
            latest_timestamp_query = "SELECT MAX(updated_at) FROM trackLengthandDownloadFrequencyDailyETL"
            latest_timestamp = conn.execute(
                latest_timestamp_query).fetchone()[0]

        # Handle case where no data exists yet (initial load)
        latest_timestamp = latest_timestamp if latest_timestamp is not None else "2000-01-01"

        # EXTRACT (Load CSV files into Spark DataFrames)
        tracks_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Track_with_updated_at.csv', header=True, inferSchema=True)
        albums_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\Album_with_updated_at.csv', header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv(
            'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\InvoiceLine_with_updated_at.csv', header=True, inferSchema=True)

        # Convert date columns to proper format
        tracks_df = tracks_df.withColumn(
            'updated_at', F.to_timestamp(F.col('updated_at'), 'dd/MM/yyyy'))
        invoice_lines_df = invoice_lines_df.withColumn(
            'updated_at', F.to_timestamp(F.col('updated_at'), 'dd/MM/yyyy'))
        albums_df = albums_df.withColumn(
            'updated_at', F.to_timestamp(F.col('updated_at'), 'dd/MM/yyyy'))

        joined_df = tracks_df.join(
            albums_df, tracks_df.AlbumId == albums_df.AlbumId, 'inner')

        print(albums_df.columns)
        # Step 3: Define a filter for the conditions

        # Step 4: Filter the joined DataFrame based on the conditions
        albums_to_include = tracks_df.filter(tracks_df.updated_at > latest_timestamp).select(
            tracks_df.AlbumId).distinct().rdd.flatMap(lambda x: x).collect()

        filter_condition = (
            (tracks_df.updated_at > latest_timestamp) |
            (albums_df.updated_at > latest_timestamp) |
            (albums_df.AlbumId.isin(albums_to_include))
        )

        filtered_tracks_df = joined_df.filter(filter_condition)

        # Step 5: Select the desired columns (if necessary)
        tracks_albums_df = filtered_tracks_df.select(
            tracks_df.TrackId,
            tracks_df.Name.alias('TrackName'),
            tracks_df.MediaTypeId,
            tracks_df.GenreId,
            tracks_df.Composer,
            tracks_df.Milliseconds,
            tracks_df.Bytes,
            tracks_df.UnitPrice,
            albums_df.AlbumId,
            # albums_df.Title.alias('AlbumTitle'),
            # tracks_df.updated_at.alias('track_updated_at'),
            # albums_df.updated_at.alias('album_updated_at'),
            # F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            F.lit("sarit_roth").alias("updated_by")
        )
        # TRANSFORM (Join tables and calculate metrics)
        window_album = Window.partitionBy("AlbumId")

        tracks_albums_df = tracks_albums_df \
            .withColumn("AverageTrackLength", F.avg("Milliseconds").over(window_album))

        # Join tracks and invoice lines DataFrames, then calculate download frequency
        # track_downloads_df = tracks_df.join(invoice_lines_df, "TrackId") \
        #     .drop(invoice_lines_df.TrackId)

        # window_track = Window.partitionBy("TrackId")

        # # Calculate Download Quantity using Window
        # track_downloads_df = track_downloads_df \
        # .withColumn("DownloadQuantity", F.sum("Quantity").over(window_track)) \

        # track_downloads_df = track_downloads_df \
        #     .withColumn("DownloadFrequency", F.count("InvoiceLineId").over(window_track))

        # Combine final data and add metadata columns
        # final_data = tracks_df \
        #     .join(tracks_albums_df, "AlbumId", "left") \
        #     .join(track_downloads_df, "TrackId", "left")
        final_data = tracks_albums_df
        try:
            # Create table in SQLite if it doesn't exist
            cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trackLengthandDownloadFrequencyDailyETL (
                        TrackID INTEGER PRIMARY KEY,
                        TrackName TEXT NOT NULL,
                        AlbumID INTEGER,
                        MediaTypeId INTEGER,
                        GenreID INTEGER,
                        Composer TEXT,
                        Milliseconds INTEGER,
                        Bytes INTEGER,
                        UnitPrice REAL,
                        AverageTrackLength REAL,

                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        updated_by TEXT
                    )
                ''')
            conn.commit()

        except Exception as e:
            print(f"An error occurred during database operations: {e}")
        # Step 1: Extract TrackId from final_data and convert to a list
        # במקום השורה הקיימת, נשתמש בגישה הבאה:
        track_ids = process_in_batches(final_data)

        # Step 2: Prepare SQL query with WHERE IN to fetch only existing records
        # Generate placeholders for SQL query
        placeholders = ','.join('?' for _ in track_ids)
        query = f'''
            SELECT TrackId, created_at
            FROM trackLengthandDownloadFrequencyDailyETL
            WHERE TrackId IN ({placeholders})
        '''

        # Step 3: Execute query and fetch results from SQLite
        cursor.execute(query, track_ids)
        existing_records = cursor.fetchall()
        if existing_records:
            # Step 4: Convert the results into a DataFrame
            existing_df = spark.createDataFrame(
                existing_records, schema=["TrackId", "original_created_at"])

            joined_df = final_data.join(existing_df.select(
                "TrackId", "created_at"), on="TrackId", how="left")

            # Step 2: בצע עדכון של עמודת created_at ב-final_data לפי existing_df
            final_data_updated = joined_df.withColumn(
                "created_at",
                F.coalesce(F.col("existing_df.created_at"),
                           F.col(F.current_timestamp()))
            )
        else:
            print("No existing records found.")
            # אם אין רשומות קיימות, פשוט נוסיף עמודת created_at עם הזמן הנוכחי
            final_data_updated = final_data.withColumn(
                "created_at", F.current_timestamp()
            )
            

        final_data_updated = final_data_updated.toPandas()
        try:
            final_data_updated.to_sql('trackLengthandDownloadFrequencyDailyETL', conn, index=False, if_exists='append')
            conn.commit()
        except Exception as e:
            print(f"An error occurred during database operations: {e}")

    finally:
        # Step 3: Clean up resources
        cursor.close()  # Close the cursor
        conn.close()  # Close the SQLite connection
        spark.stop()  # Stop the Spark session


# Execute the function
# load_Track_Length_and_Download_Frequency()



# import pandas as pd
# from datetime import datetime

# def add_updated_at_column(file_path):
#     # שלב 1: קריאה של קובץ ה-CSV
#     df = pd.read_csv(file_path)

#     # שלב 2: הוספת עמודת updated_at
#     current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     df['updated_at'] = current_time

#     # שלב 3: שמירה של הקובץ עם העמודה החדשה
#     new_file_path = file_path.replace('.csv', '_with_updated_at.csv')
#     df.to_csv(new_file_path, index=False)

#     print(f"File saved with 'updated_at' column: {new_file_path}")

# # קריאה לפונקציה עם הקובץ שלך
# file_path = r'D:\\Users\\רוט\\Desktop\\temp\\KT_Cloud\\DB\\ETLS\\InvoiceLine.csv'
# add_updated_at_column(file_path)
