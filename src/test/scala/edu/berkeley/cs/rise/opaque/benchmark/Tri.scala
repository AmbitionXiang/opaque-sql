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

object Tri {
  val spark = SparkSession
    .builder()
    .appName("BenchmarkTri")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism

  def run(spark: SparkSession, securityLevel: SecurityLevel, numPartitions: Int)
    : Int = {
    import spark.implicits._
    val inputSchema = StructType(Seq(
      StructField("src", IntegerType, false),
      StructField("dst", IntegerType, false)))
    println(s"$numPartitions")
    // var data =
    //   Utils.ensureCached(
    //     securityLevel.applyTo(
    //       spark.read
    //         .schema(inputSchema)
    //         .option("delimiter", " ")
    //         .csv(s"/opt/data/tri_opaque_soc-Slashdot0811/test_file_0")
    //         .repartition(numPartitions)))
    var data =
      Utils.ensureCached(
        securityLevel.applyTo(
          spark.read
            .schema(inputSchema)
            .option("delimiter", " ")
            .csv(s"/opt/data/small_data/tc_opaque_7.txt")
            .repartition(numPartitions)))
    Utils.force(data)

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "tri",
      "system" -> securityLevel.name) {
      val graph =
        data
          .filter($"src" =!= $"dst")
          .distinct()
      Utils.ensureCached(graph)

      val graph1 = graph.select($"src".as("src1"), $"dst".as("dst1"))
      val tmp = graph.select($"src".as("keya0"), $"dst".as("keya1"), lit(1.0f).as("cons"))

      val com = graph.join(graph1, $"src" === $"src1")
      val newcom = com.select($"dst".as("keyb0"), $"dst1".as("keyb1"), $"src", $"dst", $"dst1")

      val c = newcom.join(tmp, $"keya0" === $"keyb0" && $"keya1" === $"keyb1")
        .count()

      println(s"count=${c}")
    }
    0
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    run(this.spark, Encrypted, this.numPartitions)
  }
}