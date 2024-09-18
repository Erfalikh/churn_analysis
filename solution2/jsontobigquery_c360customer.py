from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os

# Initialize the Spark session
spark = SparkSession.builder.appName("Ingest and Join to BigQuery").getOrCreate()

# Path to input files for each table in Google Cloud Storage
users_file_path = "gs://etl-proj-ash/raw/users/*"
orders_file_path = "gs://etl-proj-ash/raw/orders/*"
events_file_path = "gs://etl-proj-ash/raw/events/*"

# Temporary GCS bucket for BigQuery
temp_gcs_bucket = "gs://etl-proj-ash/raw/sparkcode/tempdir"

# Function to determine the file format based on file extension
def get_file_format(file_path):
    file_extension = os.path.splitext(file_path)[1].lower()
    if file_extension == ".csv":
        return "csv"
    elif file_extension == ".json":
        return "json"
    else:
        raise ValueError("Unsupported file format. Only CSV and JSON are supported.")

# Function to read files based on file format
def read_files(input_file_path):
    file_list = spark.sparkContext.wholeTextFiles(input_file_path).keys().collect()
    if not file_list:
        raise FileNotFoundError(f"No files found at {input_file_path}")
    
    file_format = get_file_format(file_list[0])
    
    if file_format == "csv":
        return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_file_path)
    else:
        return spark.read.format("json").load(input_file_path)

# Read and process Users data
users_df = read_files(users_file_path)
users_df = users_df.withColumnRenamed("id", "user_id")
users_df = users_df.withColumn("churn", col("churn").cast("boolean"))
users_df = users_df.withColumn("creation_date", to_timestamp(col("creation_date"), "yyyy-MM-dd HH:mm:ss"))    
users_df = users_df.withColumn("age_group", col("age_group").cast("float"))
users_df = users_df.withColumn("gender", col("gender").cast("float"))
users_df = users_df.withColumn("last_activity_date", to_timestamp(col("last_activity_date"), "yyyy-MM-dd HH:mm:ss")) 
users_df.write.format("bigquery").option("table", "etl-proj-433402.etldataprocall.users").option("temporaryGcsBucket", temp_gcs_bucket).save()

# Read and process Orders 
orders_df = read_files(orders_file_path)
orders_df = orders_df.withColumnRenamed("id", "order_id")
orders_df = orders_df.withColumn("amount", col("amount").cast("float"))
orders_df = orders_df.withColumn("item_count", col("item_count").cast("float"))
orders_df = orders_df.withColumn("transaction_date", to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss"))
orders_df.write.format("bigquery").option("table", "etl-proj-433402.etldataprocall.orders").option("temporaryGcsBucket", temp_gcs_bucket).save()

# Read and process Events data
events_df = read_files(events_file_path)
events_df = events_df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
events_df.write.format("bigquery").option("table", "etl-proj-433402.etldataprocall.events").option("temporaryGcsBucket", temp_gcs_bucket).save()

# Perform joins to create the c360_customer master table
c360_customer_df = users_df.join(orders_df, users_df["user_id"] == orders_df["user_id"], "left").join(events_df, users_df["user_id"] == events_df["user_id"], "left").drop(orders_df["user_id"]).drop(events_df["user_id"])

# Check the final schema
c360_customer_df.printSchema()

# Write the final joined data to BigQuery as the c360_customer table
c360_customer_df.write.format("bigquery").option("table", "etl-proj-433402.etldataprocall.c360_customer").option("temporaryGcsBucket", temp_gcs_bucket).save()

# Stop the Spark session
spark.stop()
