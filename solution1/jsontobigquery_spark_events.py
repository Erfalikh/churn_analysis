from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp  
import os

# Initialize the Spark session
spark = SparkSession.builder.appName("Process CSV or JSON to BigQuery").getOrCreate()

# Path to the input files (either CSV or JSON) in Google Cloud Storage
input_file_path = "gs://etl-proj-ash/raw/events/*"  # Use a wildcard to handle multiple files

# Function to determine the file format based on file extension
def get_file_format(file_path):
    file_extension = os.path.splitext(file_path)[1].lower()
    if file_extension == ".csv":
        return "csv"
    elif file_extension == ".json":
        return "json"
    else:
        raise ValueError("Unsupported file format. Only CSV and JSON are supported.")

# Use Spark to list files in the GCS path (make sure it lists only one type of file at a time)
file_list = spark.sparkContext.wholeTextFiles(input_file_path).keys().collect()

# Check if there are any files found
if not file_list:
    raise FileNotFoundError("No files found at the specified path.")

# Determine the format based on the first file's extension
file_format = get_file_format(file_list[0])

# Read the files based on the detected format
if file_format == "csv":
    # Read CSV files
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_file_path)
    print("Processing CSV files.")
elif file_format == "json":
    # Read JSON files
    df = spark.read.format("json").load(input_file_path)
    print("Processing JSON files.")

df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

# Print the schema and show the first few rows (optional)
df.printSchema()
df.show()

# Path to the temporary Google Cloud Storage bucket for BigQuery
temp_gcs_bucket = "gs://etl-proj-ash/raw/sparkcode/tempdir"

# Write the DataFrame to BigQuery
df.write \
    .format("bigquery") \
    .option("table", "etl-proj-433402.dataprocetl.events") \
    .option("temporaryGcsBucket", temp_gcs_bucket) \
    .save()

# Stop the Spark session
spark.stop()
