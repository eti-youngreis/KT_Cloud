from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import sqlite3
import pandas as pd

def transform_data(employee_table: DataFrame, customer_table: DataFrame, invoice_table: DataFrame, conn) -> DataFrame:
    current_time = datetime.now()
    last_updated_at = get_last_updated_at(conn)

    # Alias tables
    employee_alias = employee_table.alias('e')
    customer_alias = customer_table.alias('c')
    invoice_alias = invoice_table.alias('i')

    # Filter updated customers and invoices
    customer_new = customer_alias.filter((F.col('c.updated_at') > last_updated_at) & (F.col('c.status') == 'active'))
    invoice_new = invoice_alias.filter((F.col('i.updated_at') > last_updated_at) & (F.col('i.status') == 'active'))

    # Find customers related to updated invoices
    customers_related_to_updated_invoices = invoice_new \
        .join(customer_alias, F.col('i.CustomerId') == F.col('c.CustomerId'), 'inner') \
        .select(F.col('c.CustomerId').alias('related_customer_id'), F.col('c.SupportRepId'))

    # Find employees related to updated customers
    employees_related_to_updated_customers = employee_alias \
        .join(customer_new, F.col('c.SupportRepId') == F.col('e.EmployeeId'), 'inner') \
        .select(F.col('e.EmployeeId').alias('related_employee_id'))

    # Find employees related to customers who have updated invoices
    employees_related_to_updated_invoices = employee_alias \
        .join(customers_related_to_updated_invoices, F.col('e.EmployeeId') == F.col('c.SupportRepId'), 'inner') \
        .select(F.col('e.EmployeeId').alias('related_employee_id'))

    # Combine all employees who need updates
    employees_to_update = employees_related_to_updated_customers \
        .union(employees_related_to_updated_invoices) \
        .distinct()

    # Define the window specification for aggregation
    employee_window = Window.partitionBy('related_employee_id')

    # Joining employees with customers and invoices
    joined_data = employees_to_update.alias('etu') \
        .join(employee_alias.alias('e2'), F.col('etu.related_employee_id') == F.col('e2.EmployeeId'), 'inner') \
        .join(customer_alias.alias('c2'), F.col('e2.EmployeeId') == F.col('c2.SupportRepId'), 'inner') \
        .join(invoice_alias.alias('i2'), F.col('c2.CustomerId') == F.col('i2.CustomerId'), 'inner')

    # Calculate metrics
    transformed_data = joined_data \
        .withColumn('successful_sales', F.sum(F.when(F.col('i2.Total') > 0, 1).otherwise(0)).over(employee_window)) \
        .withColumn('total_invoices', F.count('i2.InvoiceId').over(employee_window)) \
        .withColumn('conversion_rate', F.col('successful_sales') / F.col('total_invoices')) \
        .withColumn('created_at', F.lit(current_time)) \
        .withColumn('updated_at', F.lit(current_time)) \
        .withColumn('updated_by', F.lit('Shani_Strauss'))

    return transformed_data

def initialize_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def connect_to_sqlite(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)

def load_data(spark: SparkSession, path_to_tables: str) -> tuple:
    employee_table = spark.read.csv(f'{path_to_tables}/Employee.csv', header=True, inferSchema=True)
    invoice_table = spark.read.csv(f'{path_to_tables}/Invoice.csv', header=True, inferSchema=True)
    customer_table = spark.read.csv(f'{path_to_tables}/Customer.csv', header=True, inferSchema=True)
    return employee_table, invoice_table, customer_table

def add_columns(df: DataFrame, status_value: str = 'active') -> DataFrame:
    return df \
        .withColumn('updated_at', F.current_timestamp()) \
        .withColumn('status', F.lit(status_value))

def load_data_with_columns(spark: SparkSession, path_to_tables: str) -> tuple:
    employee_table = spark.read.csv(f'{path_to_tables}/Employee.csv', header=True, inferSchema=True)
    invoice_table = spark.read.csv(f'{path_to_tables}/Invoice.csv', header=True, inferSchema=True)
    customer_table = spark.read.csv(f'{path_to_tables}/Customer.csv', header=True, inferSchema=True)

    customer_table = add_columns(customer_table)
    invoice_table = add_columns(invoice_table)

    return employee_table, invoice_table, customer_table

def get_last_updated_at(conn: sqlite3.Connection) -> datetime:
    query = "SELECT MAX(updated_at) FROM employee_data"
    last_updated_at = pd.read_sql(query, conn).iloc[0, 0]
    return last_updated_at if last_updated_at is not pd.NA else datetime(1970, 1, 1)


def update_employee_data(transformed_data: DataFrame, db_path: str, table_name: str):
    conn = connect_to_sqlite(db_path)
    
    try:
        # Convert PySpark DataFrame to Pandas DataFrame
        transformed_data_pd = transformed_data.toPandas()

        # Load existing data from SQLite into a pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        existing_data = pd.read_sql(query, conn)

        # Check if 'created_at' exists in the existing data
        if 'created_at' not in existing_data.columns:
            # Create 'created_at' column in the existing data if it doesn't exist
            existing_data['created_at'] = pd.NaT  # Assign None or NaT to 'created_at'
            
        # Join new and existing data on EmployeeId
        updated_data = pd.merge(transformed_data_pd, existing_data[['EmployeeId', 'created_at']], 
                                on='EmployeeId', how='left')
        

        # Fill in created_at for new rows where it doesn't exist
#         updated_data['created_at'] = updated_data['created_at'].fillna(pd.Timestamp.now())

        # Collect employee IDs to update
        employee_ids_to_update = transformed_data_pd['EmployeeId'].tolist()
        
        # Delete old records in the table for these EmployeeIds
        query = f"DELETE FROM {table_name} WHERE EmployeeId IN ({','.join(map(str, employee_ids_to_update))})"
        conn.execute(query)
        conn.commit()

        # Insert updated data back into SQLite
        updated_data.to_sql(table_name, conn, if_exists='append', index=False)
    finally:
        conn.close()



def load_to_sqlite(df: DataFrame, conn: sqlite3.Connection, table_name: str):
    df.toPandas().to_sql(table_name, conn, if_exists='append', index=False)
    conn.commit()

def create_employee_data_table(db_path: str):
    conn = connect_to_sqlite(db_path)
    try:
        query = """
    CREATE TABLE IF NOT EXISTS employee_data (
        EmployeeId INTEGER PRIMARY KEY,
        successful_sales INTEGER,
        total_invoices INTEGER,
        conversion_rate REAL,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        updated_by TEXT
);

        """
        conn.execute(query)
        conn.commit()
    finally:
        conn.close()

def cleanup(spark: SparkSession, conn: sqlite3.Connection):
    if conn is not None:
        conn.close()
    spark.stop()

def incremental_load():
    spark = initialize_spark_session('ETL Template with KT_DB')
    db_path = 'C:/Users/USER/Desktop/3.9.2024/Emploee_Tables/employee.db'
    path_to_tables = 'C:/Users/USER/Desktop/3.9.2024/Emploee_Tables/'

    try:
        # Create the table if it doesn't exist
        create_employee_data_table(db_path)
        
        employee_table, invoice_table, customer_table = load_data_with_columns(spark, path_to_tables)
        final_data_df = transform_data(employee_table, customer_table, invoice_table, connect_to_sqlite(db_path))
        update_employee_data(final_data_df, db_path, 'employee_data')
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cleanup(spark, None)

if __name__ == "__main__":
    incremental_load()
