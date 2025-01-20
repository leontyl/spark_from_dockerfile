import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define schema
# Define schema
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),  # We'll parse date as timestamp
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True)
])

# Initialize SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Retrieve environment variables for input and output paths
input_base_path = os.environ.get("SPARK_MY_INPUT_DATA")
output_base_path = os.environ.get("SPARK_MY_OUTPUT_DATA")

# Validate environment variables
if not input_base_path:
    raise EnvironmentError("Environment variable SPARK_MY_INPUT_DATA is not set")
if not output_base_path:
    raise EnvironmentError("Environment variable SPARK_MY_OUTPUT_DATA is not set")

# Define paths using the environment variables
csv_input_path = os.path.join(input_base_path, "csv/retail")
parquet_output_path = os.path.join(output_base_path, "parquet/retail")
checkpoint_path = os.path.join(output_base_path, "checkpoint/retail")

# Read CSV files as a streaming DataFrame
csv_stream_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .option("inferSchema", "true") \
    .csv(csv_input_path)

# Write the data to Parquet format
query = csv_stream_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(parquet_output_path)

# Graceful termination with error handling
try:
    query.awaitTermination()
except Exception as e:
    print(f"Streaming query failed: {e}")
    query.stop()
    spark.stop()
    raise
