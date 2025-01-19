from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Step 1: Initialize SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("CSV to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Step 2: Define paths
csv_input_path = "path/to/your/csv/files"  # Input folder containing CSV files
delta_output_path = "path/to/delta/output"  # Output folder for Delta Lake table
checkpoint_path = "path/to/checkpoint"  # Checkpoint folder for streaming

# Step 3: Read CSV files as a streaming DataFrame
csv_stream_df = spark.readStream \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_input_path)

# Step 4: Write the data to Delta format
query = csv_stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(delta_output_path)

# Step 5: Wait for the stream to finish (for production, this would run indefinitely)
query.awaitTermination()

# Step 6: Query Delta Table (Optional, for testing)
# Uncomment below if you want to read and inspect the Delta table after the script runs
# delta_table = DeltaTable.forPath(spark, delta_output_path)
# delta_table.toDF().show()

# Stop the Spark session (Optional)
# spark.stop()
