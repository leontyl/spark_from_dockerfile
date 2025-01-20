import os
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet Example") \
    .getOrCreate()

# Retrieve the output path from the environment variable
output_base_path = os.getenv("SPARK_MY_OUTPUT_DATA")
parquet_path = f"{output_base_path}/parquet/retail"

# Read the Parquet files into a DataFrame
parquet_df = spark.read.format("parquet").load(parquet_path)

# Show the first 20 rows of the DataFrame
parquet_df.show()

# Optionally, print the schema
parquet_df.printSchema()
