from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sqlite3
from datetime import datetime


def initialize_spark_session(app_name: str) -> SparkSession:
    """
    Initializes and returns a Spark session.
    
    Args:
    app_name (str): Name of the Spark application.
    
    Returns:
    SparkSession: The initialized Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def connect_to_sqlite(db_path: str) -> sqlite3.Connection:
    """
    Establishes a connection to the SQLite database.
    
    Args:
    db_path (str): Path to the SQLite database file.
    
    Returns:
    sqlite3.Connection: The SQLite connection object.
    """
    return sqlite3.connect(db_path)


def load_data(spark: SparkSession, path_to_tables: str) -> tuple:
    """
    Loads CSV data into Spark DataFrames.
    
    Args:
    spark (SparkSession): The active Spark session.
    path_to_tables (str): The path to the directory containing the CSV files.
    
    Returns:
    tuple: DataFrames for employees, invoices, and customers.
    """
    employee_table = spark.read.csv(f'{path_to_tables}\\Employee.csv', header=True, inferSchema=True)
    invoice_table = spark.read.csv(f'{path_to_tables}\\Invoice.csv', header=True, inferSchema=True)
    customer_table = spark.read.csv(f'{path_to_tables}\\Customer.csv', header=True, inferSchema=True)
    return employee_table, invoice_table, customer_table


def transform_data(employee_table: DataFrame, customer_table: DataFrame, invoice_table: DataFrame) -> DataFrame:
    """
    Transforms the data by joining tables and applying window functions.
    
    Args:
    employee_table (DataFrame): Employee data.
    customer_table (DataFrame): Customer data.
    invoice_table (DataFrame): Invoice data.
    
    Returns:
    DataFrame: Transformed data with calculated metrics.
    """
    current_time = datetime.now()
    
    # Define aliases for the tables
    employee_alias = employee_table.alias('e')
    customer_alias = customer_table.alias('c')
    invoice_alias = invoice_table.alias('i')

    # Join the tables
    joined_employee_customer = employee_alias \
        .join(customer_alias, F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
        .join(invoice_alias, F.col('i.CustomerId') == F.col('c.CustomerId'))

    # Define the window spec by partitioning over 'EmployeeId'
    employee_window = Window.partitionBy('e.EmployeeId')

    # Calculate metrics using window functions
    transformed_data = joined_employee_customer \
        .withColumn('successful_sales', F.sum(F.when(F.col('i.Total') > 0, 1).otherwise(0)).over(employee_window)) \
        .withColumn('total_invoices', F.count('i.InvoiceId').over(employee_window)) \
        .withColumn('invoice_count', F.count('i.InvoiceId').over(Window.partitionBy('e.EmployeeId', 'c.CustomerId'))) \
        .withColumn('average_invoice_count', F.avg('invoice_count').over(employee_window)) \
        .withColumn('conversion_rate', F.col('successful_sales') / F.col('total_invoices')) \
        .withColumn('created_at', F.lit(current_time)) \
        .withColumn('updated_at', F.lit(current_time)) \
        .withColumn('updated_by', F.lit('Shani_Strauss')) \
        .dropDuplicates(['e.EmployeeId'])  # Drop duplicates to maintain one row per EmployeeId

    # Select the relevant columns
    final_data_df = transformed_data.select(
        'e.EmployeeId', 
        'e.FirstName', 
        'e.LastName', 
        'successful_sales', 
        'total_invoices', 
        'average_invoice_count', 
        'conversion_rate', 
        'created_at', 
        'updated_at', 
        'updated_by'
    )

    return final_data_df


def load_to_sqlite(df: DataFrame, conn: sqlite3.Connection, table_name: str):
    """
    Loads the transformed data into the SQLite database.
    
    Args:
    df (DataFrame): The transformed Spark DataFrame.
    conn (sqlite3.Connection): SQLite connection object.
    table_name (str): The name of the table to load data into.
    """
    # Convert Spark DataFrame to Pandas DataFrame and write to SQLite table
    df.toPandas().to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()


def cleanup(spark: SparkSession, conn: sqlite3.Connection):
    """
    Cleans up resources by closing the SQLite connection and stopping the Spark session.
    
    Args:
    spark (SparkSession): The active Spark session.
    conn (sqlite3.Connection): SQLite connection object.
    """
    conn.close()
    spark.stop()


def load():
    # Initialize Spark session
    spark = initialize_spark_session('ETL Template with KT_DB')
    
    # Establish SQLite connection
    conn = connect_to_sqlite('employee.db')
    path_to_tables = 'C:\\Users\\USER\\Desktop\\3.9.2024\\Emploee_Tables\\'

    try:
        # Load data
        employee_table, invoice_table, customer_table = load_data(spark, path_to_tables)

        # Transform data
        final_data_df = transform_data(employee_table, customer_table, invoice_table)

        # Load data into SQLite database
        load_to_sqlite(final_data_df, conn, 'employee_data')

    finally:
        # Clean up resources
        cleanup(spark, conn)


# Execute the ETL process
if __name__ == "__main__":
    load()
