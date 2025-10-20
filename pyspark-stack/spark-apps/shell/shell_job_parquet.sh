#!/bin/bash

  export PATH=$PATH:/opt/hadoop/bin

  #echo "  Running Spark job..."  
  
  spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  --conf HADOOP_USER_NAME=hdfs \
  --driver-memory 512M \
  --executor-memory 512M \
  --conf spark.executor.memoryOverhead=128 \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /opt/spark-apps/script/job_parquet.py
