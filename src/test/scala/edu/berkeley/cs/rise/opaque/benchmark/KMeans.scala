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

object KMeans {
  val spark = SparkSession
    .builder()
    .appName("BenchmarkKM")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism

  def data(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      numPartitions: Int
  ): DataFrame = {
    // val data = Source.fromFile(s"/opt/data/km_opaque_41065/test_file_0")
    //   .getLines()
    //   .map(x => Row(x.split(" ").map(_.trim.toDouble)))
    //   .toArray
    val data = Source.fromFile(s"/opt/data/small_data/km_opaque_50000_5.txt")
      .getLines()
      .map(x => Row(x.split(" ").map(_.trim.toDouble)))
      .toArray
    val schema = StructType(Seq(StructField("p", DataTypes.createArrayType(DoubleType))))

    securityLevel.applyTo(
      spark.createDataFrame(spark.sparkContext.makeRDD(data, numPartitions), schema)
    )
  }

  def train(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      numPartitions: Int,
      K: Int,
      convergeDist: Double
  ): Array[Array[Double]] = {
    import spark.implicits._
    val vectorsum = new VectorSum

    val points = Utils.ensureCached(data(spark, securityLevel, numPartitions))
    Utils.time("Read k-means data") { Utils.force(points) }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "k-means",
      "system" -> securityLevel.name
    ) {

      // Sample k random points.
      // TODO: Assumes points are already permuted randomly.
      var centroids = points.take(K).map(_.getSeq[Double](0).toArray)
      var tempDist = 1.0

      var iter = 0
      while (tempDist > convergeDist && iter < 5) {
        iter += 1
        val newCentroids = points
          .select(
            closestPoint($"p", lit(centroids)).as("oldCentroid"),
            $"p".as("centroidPartialSum"),
            lit(1).as("centroidPartialCount")
          )
          .groupBy($"oldCentroid")
          .agg(
            vectorsum($"centroidPartialSum").as("centroidSum"),
            sum($"centroidPartialCount").as("centroidCount")
          )
          .select(
            $"oldCentroid",
            vectormultiply($"centroidSum", (lit(1.0) / $"centroidCount")).as("newCentroid")
          )
          .collect

        tempDist = 0.0
        for (row <- newCentroids) {
          tempDist += squaredDistance(
            new DenseVector(row.getSeq[Double](0).toArray),
            new DenseVector(row.getSeq[Double](1).toArray)
          )
        }

        centroids = newCentroids.map(_.getSeq[Double](1).toArray)
      }

      centroids
    }
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    train(this.spark, Encrypted, this.numPartitions, 10, 0.3)
  }
}
