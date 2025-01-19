from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Creating table tb_retail") \
    .enableHiveSupport() \
    .getOrCreate()

# Use a database
spark.sql("CREATE DATABASE IF NOT EXISTS my_database")
spark.sql("USE my_database")

# Define schema explicitly
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True),
])

# Create an empty DataFrame with the schema
empty_df = spark.createDataFrame([], schema)

# Define directory for Parquet files
parquet_location = "/home/spark/data/output/parquet/retail"

# Write the empty DataFrame to the location to initialize the Parquet file
empty_df.write.mode("overwrite").parquet(parquet_location)

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS tb_retail")

# Create the table with the Parquet location
spark.sql(f"""
    CREATE TABLE tb_retail
    USING PARQUET
    LOCATION '{parquet_location}'
""")

# Verify table creation
df = spark.sql("SELECT * FROM tb_retail")
df.show()
