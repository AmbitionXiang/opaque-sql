/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque.benchmark

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.{SecurityLevel, Encrypted}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TC {
  val spark = SparkSession
    .builder()
    .appName("BenchmarkTC")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism

  def run(spark: SparkSession, securityLevel: SecurityLevel, numPartitions: Int)
    : Int = {
    import spark.implicits._
    val inputSchema = StructType(Seq(
      StructField("src", IntegerType, false),
      StructField("dst", IntegerType, false)))
    println(s"$numPartitions")
    var data =
      Utils.ensureCached(
        securityLevel.applyTo(
          spark.read
            .schema(inputSchema)
            .option("delimiter", " ")
            .csv(s"/opt/data/tc_opaque_fb/test_file_0")
            .repartition(numPartitions)))
    Utils.force(data)

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "tc",
      "system" -> securityLevel.name) {
      val edges =
        data
          .distinct()
          .select($"dst".as("src1"), $"src".as("dst1"))
      var oldCount = 0L
      var nextCount = data.count()
      var iter = 0
      while (nextCount != oldCount && iter < 5) {
        iter += 1
        oldCount = nextCount
        data = data.union(
          data.join(edges, $"src" === $"src1")
            .select($"dst1".as("src"), $"dst"))
          .distinct()
        Utils.ensureCached(data)
        nextCount = data.count()
      }
      println(s"count=${nextCount}")
    }
    0
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    run(this.spark, Encrypted, this.numPartitions)
  }
}