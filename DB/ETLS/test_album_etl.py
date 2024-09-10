import AlbumTotalTimeDownloadsWeekly
import sqlite3
import pandas as pd
import pytest
import AlbumTotalTimeDownloadsDaily

etl_table_name = 'album_total_time_downloads'
base_path = "../etl_files/"


def get_sqlite_data(conn):
    # Fetch data from SQLite table
    return pd.read_sql(f'select * from {etl_table_name}', con=conn)

def get_track_table():
    # Load Track data
    return pd.read_csv(base_path + "Track.csv")

def get_invoice_line_table():
    # Load InvoiceLine data
    return pd.read_csv(base_path + "InvoiceLine.csv")

def calculate_total_track_time(track_table):
    # Calculate total album length
    active_tracks = track_table[track_table['status'] != 'deleted']

    # Group by 'AlbumId' and aggregate the 'Milliseconds' column
    return active_tracks.groupby('AlbumId').agg(
        total_album_length_pandas=pd.NamedAgg(column='Milliseconds', aggfunc='sum')
    ).reset_index()

def calculate_total_downloads(track_table, invoice_line_table):
    # Calculate total downloads
    active_invoice_lines = invoice_line_table[invoice_line_table['status'] != 'deleted']

    # Merge track table with active invoice lines on 'TrackId' and group by 'AlbumId'
    return pd.merge(track_table, active_invoice_lines, on='TrackId', how='left').groupby('AlbumId').agg(
        total_downloads_pandas=pd.NamedAgg(column='Quantity', aggfunc='sum')
    ).reset_index()

def merge_dataframes(total_track_time_per_album, total_downloads_per_album, etl_result):
    # Merge the two DataFrames and compare with ETL result
    pandas_data = pd.merge(total_track_time_per_album, total_downloads_per_album, on='AlbumId', how='outer')
    return pd.merge(pandas_data, etl_result)

@pytest.fixture
def sqlite_connection():
    # Set up a fixture for SQLite connection
    conn = sqlite3.connect(base_path + "database.db")
    yield conn
    conn.close()

@pytest.fixture
def comparison_data(sqlite_connection):
    
    # Fetch data from SQLite
    etl_result = get_sqlite_data(sqlite_connection)
    
    # Load CSV tables
    track_table = get_track_table()
    invoice_line_table = get_invoice_line_table()

    # Calculate data using pandas
    total_track_time_per_album = calculate_total_track_time(track_table)
    total_downloads_per_album = calculate_total_downloads(track_table, invoice_line_table)

    # Merge pandas data and compare with ETL result
    return merge_dataframes(total_track_time_per_album, total_downloads_per_album, etl_result)


@pytest.mark.parametrize("row", range(1, 347))  # Assuming max 100 rows, change based on actual size
def test_each_row(row, comparison_data):
    # For each row, compare pandas and ETL results
    data_row = comparison_data.iloc[row]

    album_length_pandas = data_row['total_album_length_pandas'] if not pd.isna(data_row['total_album_length_pandas']) else 0.0
    album_length_etl = data_row['total_album_length'] if not pd.isna(data_row['total_album_length']) else 0.0

    downloads_pandas = data_row['total_downloads_pandas'] if not pd.isna(data_row['total_downloads_pandas']) else 0.0
    downloads_etl = data_row['total_album_downloads'] if not pd.isna(data_row['total_album_downloads']) else 0.0

    assert compare_album_length(album_length_pandas, album_length_etl), f"Mismatch in album length for AlbumId {data_row['AlbumId']} pandas:{album_length_pandas} != etl:{album_length_etl}"
    assert compare_total_downloads(downloads_pandas, downloads_etl), f"Mismatch in downloads for AlbumId {data_row['AlbumId']} pandas:{downloads_pandas} != etl:{downloads_etl}"
    
def compare_album_length(album_length_pandas, album_length_etl):
    return album_length_pandas == album_length_etl

def compare_total_downloads(downloads_pandas, downloads_etl):
    return downloads_pandas == downloads_etl
