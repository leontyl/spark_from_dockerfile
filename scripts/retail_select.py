from pyspark.sql import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Select Data from tb_retail") \
    .enableHiveSupport() \
    .getOrCreate()

# Switch to the same database
spark.sql("USE my_database")

# Log current catalog
#current_catalog = spark.catalog.currentCatalog()
#logger.info(f"Current catalog: {current_catalog}")

# List all databases in spark_catalog
#databases = spark.catalog.listDatabases()
#for db in databases:
#    logger.info(f"Database found: {db.name}")

# List all tables in the current catalog
#tables = spark.catalog.listTables()

#spark.catalog.refreshTable("tb_retail")

# Log the list of table names
#for table in tables:
 #   logger.info(f"Table found: {table.name}")  

df = spark.sql("SELECT * FROM tb_retail ORDER BY event_time DESC")

# Show the results
df.show()
