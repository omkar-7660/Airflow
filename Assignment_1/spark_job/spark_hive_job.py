from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit, regexp_replace, to_date, unix_timestamp

def main():
    # Initialize Spark session
    spark= SparkSession.builder \
        .appName("GCS to Hive") \
        .config("spark.sql.warehouse.dir", "gs://airflow_project_omtech/Assignment_1/hive_data/") \
        .enableHiveSuppoer() \
        .getOrCreate()
    

    # Read the CSV file from GCS
    bucket=f'airflow_project_omtech'
    file_path=f'gs://{bucket}/Assignment_1/source_prod/employee.csv'
    employee = spark.read.csv(file_path, header=True, inferSchema=True)

    # Data cleaning and transformation
    filtered_employee = employee.filter(employee.salary >=60000) # Adjust the salary threshold as needed

    #HQL to create the hive table 
    spark.sql(f"CREATE DATABASE IF NOT EXISTS airflow")

    spark.sql(f"""CREATE TABLE IF NOT EXISTS airflow.filtered_employee (
        emp_id INT,
        emp_name STRING,
        dept_id INT,
        salary INT
        )
        STORED AS PARQUET
        """)
    
    # Write the DataFrame to Hive table
    try:
        filtered_employee.write.mode("append").format('hive').saveAsTable("airflow.filtered_employee")
        print("Data written to Hive table successfully.")

    except Exception as e:
        print(f"Error writing data to Hive table: {e}")

    spark.stop()


if __name__ == "__main__":
    main()

