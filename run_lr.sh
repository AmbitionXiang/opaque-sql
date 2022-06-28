#!/bin/bash

spark-submit --class edu.berkeley.cs.rise.opaque.benchmark.LogisticRegression --executor-memory 64g --driver-memory 64g --master local[1] \
    --conf "spark.driver.maxResultSize=0" --conf "spark.files.maxPartitionBytes=32g" --conf "spark.reducer.maxSizeInFlight=32g" --conf "spark.python.worker.memory=64g" --conf "spark.kryoserializer.buffer.max=2047m" --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"  \
    ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar