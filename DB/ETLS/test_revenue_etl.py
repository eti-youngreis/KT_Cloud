import sqlite3
import pandas as pd
import RevenuePerCustomerGenreWeekly
from numpy import int64
import pytest

etl_table_name = 'revenue_per_customer_genre'
base_path = "../etl_files/"

def get_sqlite_data(conn):
    # Fetch ETL results from the database
    return pd.read_sql(f'select * from {etl_table_name}', con=conn)

def load_invoice_data():
    return pd.read_csv(base_path + "Invoice.csv")[['InvoiceId', 'CustomerId']]

def load_invoice_line_data():
    invoice_lines = pd.read_csv(base_path + "InvoiceLine.csv")[['InvoiceId', 'TrackId', 'Quantity', 'UnitPrice','status']]
    return invoice_lines[invoice_lines['status'] == 'active']

def load_track_data():
    tracks = pd.read_csv(base_path + "Track.csv")[['TrackId', 'GenreId', 'status']]
    return tracks[tracks['status'] == 'active']

def calculate_revenue_per_customer_and_genre(invoice_data, invoice_line_data, track_data):
    invoices_with_details = pd.merge(invoice_data, invoice_line_data, how = "left")
    invoices_with_details_and_tracks = pd.merge(invoices_with_details, track_data, how="left")
    
    invoices_with_details_and_tracks['total_for_track'] = invoices_with_details_and_tracks['Quantity'] * \
        invoices_with_details_and_tracks['UnitPrice']

    # Group by CustomerId and GenreId to get revenue
    return invoices_with_details_and_tracks.groupby(['CustomerId', 'GenreId']).agg(
        revenue=pd.NamedAgg(column='total_for_track', aggfunc='sum')
    ).reset_index()

def prepare_comparison_df(etl_result, revenue_per_customer_and_genre):
    # Ensure both CustomerId and GenreId are of the same type
    etl_result['CustomerId'] = etl_result['CustomerId'].astype(int)
    etl_result['GenreId'] = etl_result['GenreId'].astype(int)
    revenue_per_customer_and_genre['CustomerId'] = revenue_per_customer_and_genre['CustomerId'].astype(int)
    revenue_per_customer_and_genre['GenreId'] = revenue_per_customer_and_genre['GenreId'].astype(int)

    # Merge the ETL and pandas-generated data
    return pd.merge(etl_result[['CustomerId', 'GenreId', 'revenue_overall']], revenue_per_customer_and_genre, on=['CustomerId', 'GenreId'], how='left')

@pytest.fixture
def sqlite_connection():
    # Fixture to create and close SQLite connection
    conn = sqlite3.connect(base_path + "database.db")
    yield conn
    conn.close()

@pytest.fixture
def comparison_data(sqlite_connection):
    
    # Fetch ETL result from SQLite database
    etl_result = get_sqlite_data(sqlite_connection)
    
    # Load invoice, invoice line, and track data
    invoice_data = load_invoice_data()
    invoice_line_data = load_invoice_line_data()
    track_data = load_track_data()
    
    # Calculate revenue per customer and genre using pandas
    revenue_per_customer_and_genre = calculate_revenue_per_customer_and_genre(invoice_data, invoice_line_data, track_data)

    # Prepare DataFrame for comparison
    return prepare_comparison_df(etl_result, revenue_per_customer_and_genre)

@pytest.mark.parametrize("index", range(1, 440))
def test_revenue_per_customer_genre(index, comparison_data):
    # For each row, compare pandas and ETL results
    row = comparison_data.iloc[index]

    # Handle NaN in comparison by assuming mismatches with NaN will be ignored
    revenue_overall = row['revenue_overall'] if pd.notna(row['revenue_overall']) else 0.0
    revenue_pandas = row['revenue'] if pd.notna(row['revenue']) else 0.0

    assert int64(revenue_overall) == int64(revenue_pandas), f"Mismatch found for CustomerId {row['CustomerId']}, GenreId {row['GenreId']} pandas:{revenue_pandas} != etl:{revenue_overall}"
