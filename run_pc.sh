#!/bin/bash

spark-submit --class edu.berkeley.cs.rise.opaque.benchmark.PC --executor-memory 32g --driver-memory 32g --master local[1] \
    --conf "spark.driver.maxResultSize=0" --conf "spark.kryoserializer.buffer.max=2047m" --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"  \
    ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar
