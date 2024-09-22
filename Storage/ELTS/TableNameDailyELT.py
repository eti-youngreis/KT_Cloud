

def incremental_elt():
    # Step 1: Establish SQLite connection using KT_DB
    conn = sqlite3.connect('./Chinook.db')  # Connect to SQLite database

    try:
        # Step 2: Get the latest processed timestamp from the target table
        latest_timestamp_query = "SELECT MAX(created_at) FROM final_table"
        latest_timestamp = KT_DB.execute_and_fetch(conn, latest_timestamp_query)

        # Handle case where no data exists yet (initial load)
        if latest_timestamp is None:
            latest_timestamp = "1900-01-01 00:00:00"  # Default for initial load

        # Add here latest_timestamp query for each main source of your query

        # EXTRACT & LOAD (Load CSVs into raw tables in SQLite using KT_DB)
        # --------------------------------------------------------------

        # Load the main table CSV as a Pandas DataFrame
        main_table_df = pd.read_csv("path_to_main_table.csv")

        # Load related table CSVs as Pandas DataFrames
        related_table_1_df = pd.read_csv("path_to_related_table_1.csv")
        related_table_2_df = pd.read_csv("path_to_related_table_2.csv")

        # Insert the full CSV data into corresponding raw tables in SQLite
        KT_DB.insert_dataframe(conn, "raw_main_table", main_table_df, mode="append")
        KT_DB.insert_dataframe(
            conn, "raw_related_table_1", related_table_1_df, mode="append"
        )
        KT_DB.insert_dataframe(
            conn, "raw_related_table_2", related_table_2_df, mode="append"
        )

        # Commit the changes after loading raw data
        KT_DB.commit(conn)

        # TRANSFORM (Perform transformations with SQL queries inside SQLite)
        # ------------------------------------------------------------------
        # Example Transformation: Join tables and calculate total spend, avg spend, etc.
        transform_query = """
            INSERT INTO final_table (customer_id, total_spend, avg_spend, last_transaction_date)
            SELECT mt.customer_id,
                   SUM(rt1.spend) AS total_spend,
                   AVG(rt1.spend) AS avg_spend,
                   MAX(rt2.transaction_date) AS last_transaction_date
            FROM raw_main_table mt
            JOIN raw_related_table_1 rt1 ON mt.id = rt1.main_id
            JOIN raw_related_table_2 rt2 ON mt.id = rt2.main_id
            WHERE mt.created_at > ?
            GROUP BY mt.customer_id
        """

        # Execute the transformation query using KT_DB, passing latest_timestamp as a parameter
        KT_DB.execute(conn, transform_query, (latest_timestamp,))

        # Commit the changes after the transformation
        KT_DB.commit(conn)

    finally:
        # Step 3: Close the SQLite connection
        KT_DB.close(conn)
