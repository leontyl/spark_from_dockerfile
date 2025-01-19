FROM spark:3.5.4-scala2.12-java11-python3-ubuntu

USER root

# Install required tools
RUN set -ex; \
    apt-get update && \
    apt-get install -y git wget && \
    rm -rf /var/lib/apt/lists/*

# Download Qbeast and Delta Lake JAR files
RUN wget https://repo1.maven.org/maven2/io/qbeast/qbeast-spark_2.12/0.7.0/qbeast-spark_2.12-0.7.0.jar -P $SPARK_HOME/jars/
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar -P $SPARK_HOME/jars/

RUN mkdir -p /home/spark/.ivy2/cache && \
    chmod -R 777 /home/spark/.ivy2;

RUN mkdir -p /home/spark/scripts;    
RUN mkdir -p /home/spark/data/checkpoints;
RUN mkdir -p /home/spark/data/input/json;
RUN mkdir -p /home/spark/data/input/csv/cart_events;
RUN mkdir -p /home/spark/data/input/csv/retail;
RUN mkdir -p /home/spark/data/output/parquet/cart_events;    
RUN mkdir -p /home/spark/data/output/parquet/retail;    

ENV SPARK_MY_INPUT_DATA=/home/spark/data/input
ENV SPARK_MY_SCRIPTS=/home/spark/scripts

#RUN wget --no-cache --timeout=3000 https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/all/online-retail-dataset.csv -P $SPARK_MY_INPUT_DATA/csv/retail

RUN chmod -R 777 /home/spark

# Clone Qbeast Spark repository (if needed)
# RUN git clone https://github.com/Qbeast-io/qbeast-spark.git

# Install PySpark
RUN pip install pyspark

# Install DeltaSpark
RUN pip install delta-spark

# Change permissions
RUN chmod -R 777 /opt/spark

# Exposing Ports
EXPOSE 4040

# Switch back to spark user
USER spark

CMD ["/bin/bash"]