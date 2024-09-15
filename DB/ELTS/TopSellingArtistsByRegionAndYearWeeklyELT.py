from pyspark.sql import SparkSession
import sqlite3


def load():
    spark = SparkSession.builder.appName('Top-Selling Artists ELT').getOrCreate()

    conn =sqlite3.connect('top_selling_artists.db')

    invoice_df = spark.read.csv('Invoice.csv',header=True, inferSchema=True)
    invoice_line_df = spark.read.csv('InvoiceLine.csv', header=True, inferSchema=True)
    track_df = spark.read.csv('Track.csv', header=True,inferSchema=True)
    album_df = spark.read.csv('Album.csv', header=True,inferSchema=True)
    artist_df = spark.read.csv('Artist.csv', header=True,inferSchema=True)


    invoice = invoice_df.toPandas()
    invoice_line = invoice_line_df.toPandas()
    track = track_df.toPandas()
    album = album_df.toPandas()
    artist = artist_df.toPandas()


    invoice.to_sql('Invoice', conn, if_exists='replace', index=False)
    invoice_line.to_sql('InvoiceLine', conn, if_exists='replace', index=False)
    track.to_sql('Track', conn, if_exists='replace', index=False)
    album.to_sql('Album', conn, if_exists='replace', index=False)
    artist.to_sql('Artist', conn, if_exists='replace', index=False)


    q = """create table if not exists TopSellingArtistsELT (
            Region,
            Year,
            TotalSum
                )"""





