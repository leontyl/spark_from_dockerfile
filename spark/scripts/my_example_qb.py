import pyspark

# Already configured session

spark = pyspark.sql.SparkSession.builder.appName("MyApp").getOrCreate()

# Session with Configuration
pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.qbeast.sql.QbeastSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "io.qbeast.catalog.QbeastCatalog").getOrCreate()


#data = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id: int, age:string")
#data.write.mode("overwrite").option("columnsToIndex", "id,age").saveAsTable("qbeast_table")