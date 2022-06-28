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
import scala.math.sqrt

import breeze.linalg.DenseVector
import breeze.linalg.squaredDistance
import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.expressions.ClosestPoint.closestPoint
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum
import edu.berkeley.cs.rise.opaque.{SecurityLevel, Encrypted}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PC {
  val spark = SparkSession
    .builder()
    .appName("BenchmarkPC")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism

  def data(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      path: String,
      numPartitions: Int)
    : DataFrame = {

    val data = Source.fromFile(s"/opt/data/"+path)
        .getLines()
        .zipWithIndex
        .map{ case(x, count) => Row(count, x.toDouble) }
        .toArray

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("r", DoubleType)))

    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data, numPartitions),
        schema))
  }

  def run(
      spark: SparkSession, securityLevel: SecurityLevel, numPartitions: Int)
    : Int = {
    import spark.implicits._

    val d0 = Utils.ensureCached(data(spark, securityLevel, "pe_opaque_a_108/test_file_0", numPartitions))
      .select($"id", $"r".as("ra"))
    val d1 = Utils.ensureCached(data(spark, securityLevel, "pe_opaque_b_108/test_file_0", numPartitions))
      .select($"id", $"r".as("rb"))
    Utils.time("read pearson data a") { Utils.force(d0) }
    Utils.time("read pearson data b") { Utils.force(d1) }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "pearson",
      "system" -> securityLevel.name) {

      val mx = d0.agg(sum($"ra")).first().getDouble(0)/d0.count()
      val my = d1.agg(sum($"rb")).first().getDouble(0)/d1.count()

      println(s"mx=${mx}, my=${my}")
      val zipped = d0.join(d1, "id")
        .select($"ra", $"rb")

      val up = zipped.withColumn("up", ($"ra"-lit(mx))*($"rb"-lit(my)))
        .select($"up")
      val lowx = zipped.withColumn("lowx", ($"ra"-lit(mx))*($"ra"-lit(mx)))
        .select($"lowx")
      val lowy = zipped.withColumn("lowy", ($"rb"-lit(my))*($"rb"-lit(my)))
        .select($"lowy")
      val r1 = up.agg(sum($"up")).first().getDouble(0)
      val r2 = lowx.agg(sum($"lowx")).first().getDouble(0)
      val r3 = lowy.agg(sum($"lowy")).first().getDouble(0)
      println(s"r = ${r1/(sqrt(r2)*sqrt(r3))}")
    }
    0
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    run(this.spark, Encrypted, this.numPartitions)
  }

}