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

import java.util.Random
import scala.io.Source

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.{SecurityLevel, Encrypted}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MM {
  val spark = SparkSession
    .builder()
    .appName("BenchmarkMM")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism

  def run(
      spark: SparkSession, securityLevel: SecurityLevel, numPartitions: Int)
    : Int = {
    import spark.implicits._
    val inputSchema = StructType(Seq(
      StructField("r", IntegerType, false),
      StructField("c", IntegerType, false),
      StructField("v", DoubleType, false)))
    val d0 =
      Utils.ensureCached(
        securityLevel.applyTo(
          spark.read
            .schema(inputSchema)
            .option("delimiter", " ")
            .csv(s"/opt/data/mm_opaque_a_2000_20/test_file_0")
            .repartition(numPartitions)))
    Utils.force(d0)

    val d1 =
      Utils.ensureCached(
        securityLevel.applyTo(
          spark.read
            .schema(inputSchema)
            .option("delimiter", " ")
            .csv(s"/opt/data/mm_opaque_b_20_2000/test_file_0")
            .repartition(numPartitions)))
    Utils.force(d1)

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "matrix",
      "system" -> securityLevel.name) {

      val d0t = d0.select($"c".as("key"), $"r".as("a1"), $"v".as("a2"))
      val d1t = d1.select($"r".as("key"), $"c".as("b1"), $"v".as("b2"))
      val mc = d0t.join(d1t, "key")
        .withColumn("v", $"a2"*$"b2")
        .select($"a1", $"b1", $"v")
        .groupBy("a1", "b1").agg(sum("v"))

      val count = mc.count()
      println(s"count = ${count}")
    }
    0
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    run(this.spark, Encrypted, this.numPartitions)
  }
}