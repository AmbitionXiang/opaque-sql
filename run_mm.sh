#!/bin/bash

spark-submit --class edu.berkeley.cs.rise.opaque.benchmark.MM --executor-memory 128g --driver-memory 128g --master local[1] \
    --conf "spark.driver.maxResultSize=0" --conf "spark.executor.instances=1" --conf "spark.sql.shuffle.partitions=20000" \
    ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar
