from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import KT_DB  

def load():
    spark = SparkSession.builder \
        .appName("Average Sales and Customer Retention") \
        .getOrCreate()

    conn = KT_DB.connect('/EmployeeSalesRetention.db')

    path = "C:\\Users\\The user\\Desktop\\csv" 
    try:
        # קריאת הקבצים
        Employees_table = spark.read.csv(f"{path}\\Employee.csv", header=True, inferSchema=True)
        Invoices_table = spark.read.csv(f"{path}\\Invoice.csv", header=True, inferSchema=True)
        Customers_table = spark.read.csv(f"{path}\\Customer.csv", header=True, inferSchema=True)
        InvoiceLines_table = spark.read.csv(f"{path}\\InvoiceLine.csv", header=True, inferSchema=True)
        
        # ביצוע JOINים לטבלאות
        joined_table_1 = Invoices_table.join(Employees_table, Invoices_table["EmployeeId"] == Employees_table["EmployeeId"], "inner")
        joined_table_2 = joined_table_1.join(InvoiceLines_table, Invoices_table["InvoiceId"] == InvoiceLines_table["InvoiceId"], "inner")
        joined_table_3 = joined_table_2.join(Customers_table, Invoices_table["CustomerId"] == Customers_table["CustomerId"], "inner")

        # חישוב סכום המכירות הממוצע לכל עובד
        employee_sales = joined_table_3.groupBy("EmployeeId") \
            .agg(
                F.avg(F.col("UnitPrice") * F.col("Quantity")).alias("avg_sales")
            )
        
        # חישוב שיעור השימור של לקוחות
        # למטרת השימור, נחשב את מספר הלקוחות החוזרים לכל עובד
        customer_window = Window.partitionBy("EmployeeId", "CustomerId").orderBy("InvoiceDate")
        retention_data = joined_table_3.withColumn("is_repeated", F.when(F.col("InvoiceDate") > F.first("InvoiceDate").over(customer_window), 1).otherwise(0))
        retention_summary = retention_data.groupBy("EmployeeId") \
            .agg(
                F.sum("is_repeated").alias("total_repeated_customers"),
                F.countDistinct("CustomerId").alias("total_customers")
            ) \
            .withColumn("retention_rate", F.col("total_repeated_customers") / F.col("total_customers"))

        # שמירה למסד הנתונים
        final_data_df = employee_sales.join(retention_summary, on="EmployeeId", how="inner").toPandas()
        KT_DB.insert_dataframe(conn, 'employee_sales_retention', final_data_df)
        KT_DB.commit(conn)

    finally:
        KT_DB.close(conn)
        spark.stop()

if __name__ == "__main__":
    load()
