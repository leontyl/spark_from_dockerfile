FROM bitnami/spark:3.5.2
USER root
RUN install_packages curl
USER 1001
RUN curl https://repo.maven.apache.org/maven2/io/qbeast/qbeast-spark_2.12/0.7.0/qbeast-spark_2.12-0.7.0.jar --output /opt/bitnami/spark/jars/qbeast-spark_2.12-0.7.0.jar

# Set permissions for Spark directories
USER root
RUN chmod -R 777 /opt/bitnami/spark
USER 1001
