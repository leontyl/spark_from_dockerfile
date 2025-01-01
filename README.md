
*Example*

**Run container**
```docker run -p 4040:4040 -it leonty/spark /bin/bash```

**Scala**
```$SPARK_HOME/bin/spark-shell \
  --packages io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog \
  --conf spark.jars.ivy=/tmp/.ivy2```
