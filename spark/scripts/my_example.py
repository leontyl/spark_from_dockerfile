from pyspark.sql import SparkSession

spark = SparkSession.builder .appName("MyApp").master("spark://spark-master:7077").getOrCreate()

data = [("Alice", 34), ("Bob", 36), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()
