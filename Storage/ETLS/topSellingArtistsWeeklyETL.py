from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime



def load_top_selling_artists():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Top Selling Artists") \
        .getOrCreate()

    # Establish SQLite connection using sqlite3
    conn = sqlite3.connect('D:\\בוטקמפ\\s3\\KT_Cloud\\CustomerTopSellingArtists.db')
    cursor = conn.cursor()

    try:
        # EXTRACT (Loading CSVs from local storage)
        invoices_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Invoice.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\InvoiceLine.csv", header=True, inferSchema=True)
        tracks_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Track.csv", header=True, inferSchema=True)
        albums_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Album.csv", header=True, inferSchema=True)
        artists_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Artist.csv", header=True, inferSchema=True)
        customers_df = spark.read.csv("D:\\בוטקמפ\\s3\\KT_Cloud\\csv_files\\Customer.csv", header=True, inferSchema=True)

        # TRANSFORM (Apply joins, aggregations, and calculations)
        # Join invoice lines with tracks
        invoice_lines_tracks_df = invoice_lines_df.alias("il").join(
            tracks_df.alias("t"),
            on=F.col("il.TrackId") == F.col("t.TrackId"),
            how="inner"
        ).select(
            F.col("il.InvoiceId"),
            F.col("il.TrackId"),
            F.col("il.UnitPrice").alias("TrackUnitPrice"),
            F.col("il.Quantity"),
            F.col("t.AlbumId")
        )

        # Join with albums
        tracks_albums_df = invoice_lines_tracks_df.alias("it").join(
            albums_df.alias("a"),
            on=F.col("it.AlbumId") == F.col("a.AlbumId"),
            how="inner"
        ).select(
            F.col("it.InvoiceId"),
            F.col("it.TrackId"),
            F.col("it.TrackUnitPrice"),
            F.col("it.Quantity"),
            F.col("a.ArtistId")
        )

        # Join with artists
        albums_artists_df = tracks_albums_df.alias("ta").join(
            artists_df.alias("ar"),
            on=F.col("ta.ArtistId") == F.col("ar.ArtistId"),
            how="inner"
        ).select(
            F.col("ta.InvoiceId"),
            F.col("ar.ArtistId"),
            F.col("ar.Name").alias("ArtistName"),
            F.col("ta.TrackUnitPrice"),
            F.col("ta.Quantity")
        )

        # Join with customers to include region info
        invoices_customers_df = invoices_df.alias("i").join(
            customers_df.alias("c"),
            on=F.col("i.CustomerId") == F.col("c.CustomerId"),
            how="inner"
        ).select(
            F.col("i.InvoiceId"),
            F.col("c.CustomerId"),
            F.col("c.City"),
            F.col("c.State"),
            F.col("i.InvoiceDate")
        )

        # Combine the dataframes
        combined_df = invoices_customers_df.alias("ic").join(
            albums_artists_df.alias("aa"),
            on=F.col("ic.InvoiceId") == F.col("aa.InvoiceId"),
            how="inner"
        )

        # Extract year from InvoiceDate and add Region (combined City and State)
        combined_df = combined_df.withColumn("Region", F.concat(F.col("City"), F.lit(", "), F.col("State"))) \
            .withColumn("Year", F.year("InvoiceDate"))

        # Aggregation to calculate total sales per artist by region and year
        sales_df = combined_df.groupBy("Region", "Year", "ArtistId", "ArtistName") \
            .agg(
                F.sum(F.col("TrackUnitPrice") * F.col("Quantity")).alias("TotalSales")
            )

        # Ranking artists by sales within each region and year
        window_spec = Window.partitionBy("Region", "Year").orderBy(F.desc("TotalSales"))
        ranked_df = sales_df.withColumn("Rank", F.row_number().over(window_spec))

        # Filter top 3 artists per region and year
        top_artists_df = ranked_df.filter(F.col("Rank") <= 3)

        # Adding timestamp columns for record creation and update
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_df = top_artists_df.withColumn("created_at", F.lit(current_datetime)) \
            .withColumn("updated_at", F.lit(current_datetime)) \
            .withColumn("updated_by", F.lit("process:user_name"))

        # LOAD (Save transformed data into SQLite using sqlite3)
        final_data_df = final_df.toPandas()
        if final_data_df.empty:
            print("DataFrame is empty. No data to write.")
        else:
            # Create the table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_selling_artists (
            Region TEXT,
            Year INTEGER,
            ArtistId INTEGER,
            ArtistName TEXT,
            TotalSales REAL,
            Rank INTEGER,
            created_at TEXT,
            updated_at TEXT,
            updated_by TEXT
            )
            """)
            # Write the DataFrame to the SQLite table
            final_data_df.to_sql('top_selling_artists', conn, if_exists='replace', index=False)
            conn.commit()

            # Query and print the contents of the table
            cursor.execute("SELECT * FROM top_selling_artists;")
            rows = cursor.fetchall()
            for row in rows:
                print(row)

    finally:
        conn.close()
        spark.stop()

if __name__ == "__main__":
    load_top_selling_artists()
