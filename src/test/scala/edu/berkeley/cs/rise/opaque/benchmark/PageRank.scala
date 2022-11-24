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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object PageRank {
  val spark = SparkSession
    .builder()
    .appName("BenchmarkPR")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism

  def run(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      numPartitions: Int
  ): Int = {
    import spark.implicits._
    val inputSchema = StructType(
      Seq(
        StructField("src", IntegerType, false),
        StructField("dst", IntegerType, false)
      )
    )
    // val data = 
    //   Utils.ensureCached(
    //     securityLevel.applyTo(
    //       spark.read
    //         .schema(inputSchema)
    //         .option("delimiter", " ")
    //         .csv(s"/opt/data/pr_opaque_cit-Patents/test_file_0")
    //     )
    //   )

    val data = 
      Utils.ensureCached(
        securityLevel.applyTo(
          spark.read
            .schema(inputSchema)
            .option("delimiter", " ")
            .csv(s"/opt/data/small_data/pr_opaque.txt")
        )
      )

    Utils.time("load edges") { Utils.force(data) }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "pagerank",
      "system" -> securityLevel.name,
    ) {
      val links = 
        Utils.ensureCached(
          data
            .distinct()
            .select($"src", $"dst")
        )

      var ranks = links
        .groupBy($"src")
        .agg(
          count(lit(1)).as("size")
        )
        .select($"src".as("id"), $"size", lit(1.0).as("weight"))

      val result =
        links
          .join(ranks, $"id" === $"src")
          .select($"dst", ($"weight" / $"size").as("weightedRank"))
          .groupBy("dst")
          .agg(sum("weightedRank").as("totalIncomingRank"))
          .select($"dst", (lit(0.15) + lit(0.85) * $"totalIncomingRank").as("rank"))
      Utils.force(result)

      result.collect.toSet
    }
    0
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    run(this.spark, Encrypted, this.numPartitions)
  }
}
