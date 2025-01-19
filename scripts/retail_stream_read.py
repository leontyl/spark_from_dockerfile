from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to Parquet Stream") \
    .getOrCreate()

# Define the schema for the CSV data
schema = StructType([
    StructField("event_time", TimestampType(), True),  # Use TimestampType for date and time
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_session", StringType(), True)
])

# Define the input directory for incoming CSV files and the output directory for Parquet files
input_directory = "/home/spark/data/input/csv/retail"
parquet_location = "/home/spark/data/output/parquet/retail"

# Create a streaming DataFrame that reads new CSV files
streaming_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(input_directory)

# Write the streaming DataFrame to Parquet, but do not write directly to the table
# Instead, we will just store the files in the specified output directory
query = streaming_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/home/spark/data/checkpoints") \
    .option("path", parquet_location) \
    .start()

# Wait for the streaming job to terminate
query.awaitTermination()
