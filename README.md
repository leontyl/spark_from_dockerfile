
*Example*

**Build image**
```
 docker build . -t leonty/spark:latest
```
**Run container**
<br>
```
docker run --name spark -p 4040:4040 --mount type=bind,source=C:/dev/Repos/spark_from_dockerfile/scripts,dst=/home/spark/scripts --mount type=bind,source=C:/dev/Repos/spark_from_dockerfile/data,dst=/home/spark/data -it leonty/spark:latest  /bin/bash

wget --no-cache --timeout=3000 https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/all/online-retail-dataset.csv -P $SPARK_MY_INPUT_DATA/csv/retail

```  

**Scala**
```
$SPARK_HOME/bin/spark-shell \
  --packages io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog \
  --conf spark.jars.ivy=/tmp/.ivy2

  val ecommerce = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("src/test/resources/ecommerce100K_2019_Oct.csv")
  ```

**Submit job**
  ...

  $SPARK_HOME/bin/spark-submit --master local $SPARK_MY_SCRIPTS/retail_select.py

  $SPARK_HOME/bin/spark-submit --master local $SPARK_MY_SCRIPTS/retail_select_from_parquet.py
  ...
