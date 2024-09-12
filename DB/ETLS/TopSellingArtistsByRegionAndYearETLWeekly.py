from pyspark.sql import SparkSession  # Importing SparkSession to create a connection with Spark
from pyspark.sql import functions as F  # Importing functions from Spark for aggregation and data operations
import sqlite3  # Importing sqlite3 for working with a local database
from pyspark.sql.window import Window  # Importing Window to use a Window Function


def load():
    # Creating a Spark session using SparkSession
    spark = SparkSession.builder.appName('TopSellingArtistsByRegionAndYear').getOrCreate()

    try:
        # Connecting to the SQLite database
        conn = sqlite3.connect('top_selling_artists.db')

        # Loading CSV files into DataFrames
        invoice_df = spark.read.csv('Invoice.csv', header=True, inferSchema=True)
        invoice_line_df = spark.read.csv('InvoiceLine.csv', header=True, inferSchema=True)
        track_df = spark.read.csv('Track.csv', header=True, inferSchema=True)
        album_df = spark.read.csv('Album.csv', header=True, inferSchema=True)
        artist_df = spark.read.csv('Artist.csv', header=True, inferSchema=True)

        # Joining the invoice table with the invoice line table
        invoice_and_invoice_line = invoice_df.join(invoice_line_df,'InvoiceId', 'inner').select(
            F.year(F.col('InvoiceDate')).alias('year'),  # Extracting the year from the date and renaming the column
            (F.col('UnitPrice') * F.col('Quantity')).alias('TotalSum'),  # Calculating total amount per item
            invoice_df['BillingCountry'].alias('region'),  # Defining the region based on the billing country
            invoice_line_df['TrackId'],  # Keeping the track identifier (TrackId)
        )

        # Joining the invoice and invoice line with the track table
        invoice_line_and_track = invoice_and_invoice_line.join(track_df, 'TrackId', 'inner').select(
            invoice_and_invoice_line['Year'],  # Keeping the year from the joined table
            invoice_and_invoice_line['Region'],  # Keeping the region from the joined table
            invoice_and_invoice_line['TotalSum'],  # Keeping the total amount from the joined table
            track_df['AlbumId'],  # Keeping the album identifier (AlbumId)
        )

        # Joining the track table with the album table
        track_and_album = invoice_line_and_track.join(album_df, 'AlbumId', 'inner').select(
            invoice_line_and_track['Year'],  # Keeping the year from the joined table
            invoice_line_and_track['Region'],  # Keeping the region from the joined table
            invoice_line_and_track['TotalSum'],  # Keeping the total amount from the joined table
            album_df['ArtistId']  # Keeping the artist identifier (ArtistId)
        )

        # Joining the album table with the artist table
        artist_sales_df = track_and_album.join(artist_df, 'ArtistId', 'inner').select(
            track_and_album['Year'],  # Keeping the year from the joined table
            track_and_album['Region'],  # Keeping the region from the joined table
            track_and_album['TotalSum'],  # Keeping the total amount from the joined table
            track_and_album['ArtistId'],  # Keeping the artist identifier from the joined table
            artist_df['name'].alias('ArtistName')
        )

        # Aggregating the data by artist, year, and region, calculating the total sales amount
        aggregated_data = artist_sales_df.groupBy('ArtistId','Year','Region', 'ArtistName').agg(F.sum('TotalSum').alias('Sum'))

        # Defining a window by year and region, ordering by total sales amount
        window_spec = Window.partitionBy('Year','Region').orderBy(F.desc('Sum'))
        ranked_df = aggregated_data.withColumn("rank", F.row_number().over(window_spec))  # Creating a rank column

        # Filtering the top 3 artists per region and year
        top_selling_artists = ranked_df.filter(F.col('rank') <= 3)

        # Adding new columns for metadata: creation date, update date, and updater name
        final_data = top_selling_artists.withColumn('created_at', F.current_timestamp()) \
                    .withColumn('updated_at', F.current_timestamp()) \
                    .withColumn('updated_by', F.lit('Efrat')) \
                    .select('Region','Year','ArtistName','created_at','updated_at','updated_by')

        # Converting the data to a Pandas DataFrame for saving into SQLite
        date_in_pandas = final_data.toPandas()

        # Saving the data into a new table in the SQLite database
        date_in_pandas.to_sql('top_selling_artists', conn, if_exists='replace', index=False)
        conn.commit()  # Performing a commit to save changes to the database

    finally:
        # Closing the database connection and stopping the Spark session
        conn.close()
        spark.stop()
