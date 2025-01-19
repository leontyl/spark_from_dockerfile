  $SPARK_HOME/bin/spark-submit --master local $SPARK_MY_SCRIPTS/cart_events_create.py
  $SPARK_HOME/bin/spark-submit --master local $SPARK_MY_SCRIPTS/cart_events_stream_read.py