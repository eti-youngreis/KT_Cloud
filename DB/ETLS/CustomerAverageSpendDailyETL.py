
import sqlite3
from datetime import datetime

from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as spark_func

from DB.ETLS.ColumnNames import ColumnNames
from DB.ETLS.utils import add_columns_to_dataframe, get_latest_timestamp, load_csv_files

USER_NAME = 'USER'
PROCCESS = 'CustomerAverageSpendDailyETL'


class CSV_PATHS:
    CUSTOMER_CSV = 'Customer.csv'
    INVOICE_CSV = 'Invoice.csv'


def incremental_load():

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Customer Average Spend Daily ETL") \
        .getOrCreate()

    # Establish SQLite connection
    conn: sqlite3.Connection = sqlite3.connect('chinook.db')

    base_path = 'DB/csvs/'

    target_table_name = 'CustomerAverageSpend'

    try:
        # Get the latest timestamp from the target table
        latest_timestamp = get_latest_timestamp(target_table_name, conn)

        # Load CSV files
        customers, invoices = load_csv_files(spark, [
            base_path + CSV_PATHS.CUSTOMER_CSV,
            base_path + CSV_PATHS.INVOICE_CSV
        ])
        # Add UpdatedAt to customers
        customers = add_columns_to_dataframe(
            customers, {ColumnNames.UPDATED_AT: datetime.now()})

        # Add UpdatedAt to invoices
        invoices = add_columns_to_dataframe(
            invoices, {ColumnNames.UPDATED_AT: datetime.now()})

        # Filter invoices to include only new invoices
        filtered_invoices: DataFrame = invoices.filter(
            spark_func.col(ColumnNames.UPDATED_AT) > latest_timestamp)

        # Get identifiers of customers with new invoices
        customers_with_new_invoices_ids = filtered_invoices.select(
            ColumnNames.CUSTOMER_ID).distinct()

        # Filter customers to include only new customers or old customers with new invoices
        filtered_customers: DataFrame = customers.filter(
            (spark_func.col(ColumnNames.UPDATED_AT) > latest_timestamp) |
            (spark_func.col(ColumnNames.CUSTOMER_ID).isin(customers_with_new_invoices_ids.rdd.flatMap(lambda x: x).collect())))

        # Join customers and invoices
        customers_invoices: DataFrame = filtered_customers.join(
            other=filtered_invoices, on=ColumnNames.CUSTOMER_ID, how='left')

        # Calculate for each customer the total amount spent and the average spend per invoice
        customers_analytics: DataFrame = customers_invoices.groupBy(ColumnNames.CUSTOMER_ID) \
            .agg(
                spark_func.sum(ColumnNames.TOTAL).alias(
                    f'{ColumnNames.CUSTOMER_LIFE_TIME_VALUE}'),
                spark_func.avg(ColumnNames.TOTAL).alias(
                    f'{ColumnNames.AVERAGE_SPEND_PER_INVOICE}')
        )

        now = datetime.now()

        # Add metadata columns
        final_customers_analytics = add_columns_to_dataframe(
            customers_analytics,
            {
                ColumnNames.UPDATED_AT: now,
                ColumnNames.UPDATED_BY: f'{PROCCESS}:{USER_NAME}',
                ColumnNames.CREATED_AT: now
            }
        )

        # Get the CreatedAt values for the customers who are going to be updated
        try:
            select_created_at_query = f"""SELECT {ColumnNames.CUSTOMER_ID}, {ColumnNames.CREATED_AT} FROM {
                target_table_name} WHERE {ColumnNames.CUSTOMER_ID} IN ({", ".join(["?"] * len(final_customers_analytics.columns))})"""

            created_at_df = conn.execute(select_created_at_query, final_customers_analytics.select(
                ColumnNames.CUSTOMER_ID)).fetchall()

        except Exception as e:
            raise

        # Convert to Pandas DataFrame
        final_customers_analytics: DataFrame = customers_analytics.toPandas()

        # Update the CreatedAt values for the customers who are going to be updated to the original CreatedAt values
        created_at_dict = dict(zip(created_at_df[0], created_at_df[1]))

        final_customers_analytics.loc[final_customers_analytics[ColumnNames.CUSTOMER_ID].isin(created_at_dict.keys()),
                                      ColumnNames.CREATED_AT] = final_customers_analytics[ColumnNames.CUSTOMER_ID].map(created_at_dict)

        # Delete the rows that are going to be updated
        delete_query = f"""DELETE FROM '{target_table_name}' WHERE '{
            ColumnNames.CUSTOMER_ID}' IN ('{", ".join(["?"] * len(final_customers_analytics.columns))}')"""

        conn.execute(delete_query, final_customers_analytics.select(
            ColumnNames.CUSTOMER_ID).values)

        # Insert the new and updated rows
        insert_final_customers_analytics_query = f"""INSERT INTO '{target_table_name}' ('{", ".join(
            final_customers_analytics.columns)}') VALUES ('{", ".join(["?"] * len(final_customers_analytics.columns))}')"""

        conn.execute(insert_final_customers_analytics_query,
                     final_customers_analytics.values())

        # Commit the changes
        conn.commit()

    finally:
        # Close the connection and stop the SparkSession
        conn.close()
        spark.stop()

incremental_load()
# conn: sqlite3.Connection = sqlite3.connect('chinook.db')
# target_table_name = 'CustomerAverageSpend'
# cursor = conn.cursor()
# cursor.execute(f"PRAGMA table_info({target_table_name})")
# columns = [column[1] for column in cursor.fetchall()]

# if ColumnNames.CREATED_AT not in columns:
#     print(f"Warning: {ColumnNames.CREATED_AT} column not found in {target_table_name}")
#     # Handle the missing column scenario

# print('Column Names: ', columns)