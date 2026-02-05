/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.TestUtils.findOperator

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuHashJoin}

class BroadcastHashJoinSuite extends SparkQueryCompareTestSuite {

  test("broadcast hint isn't propagated after a join") {
    val conf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark)
      val df2 = nonZeroLongsDf(spark)

      val df3 = df1.join(broadcast(df2), Seq("longs"), "inner").drop(df2("longs"))
      val df4 = longsDf(spark)
      val df5 = df4.join(df3, Seq("longs"), "inner")

      // execute the plan so that the final adaptive plan is available when AQE is on
      df5.collect()
      val plan = df5.queryExecution.executedPlan

      val bhjCount = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
      assert(bhjCount.size === 1)

      val shjCount = PlanUtils.findOperators(plan, _.isInstanceOf[GpuShuffledSymmetricHashJoinExec])
      assert(shjCount.size === 1)
    }, conf)
  }

  test("broadcast hint in SQL") {
    withGpuSparkSession(spark => {
      longsDf(spark).createOrReplaceTempView("t")
      longsDf(spark).createOrReplaceTempView("u")

      for (name <- Seq("BROADCAST", "BROADCASTJOIN", "MAPJOIN")) {
        val plan1 = spark.sql(s"SELECT /*+ $name(t) */ * FROM t JOIN u ON t.longs = u.longs")
        val plan2 = spark.sql(s"SELECT /*+ $name(u) */ * FROM t JOIN u ON t.longs = u.longs")

        // execute the plan so that the final adaptive plan is available when AQE is on
        plan1.collect()
        val finalPlan1 = findOperator(plan1.queryExecution.executedPlan,
          _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(finalPlan1.get.asInstanceOf[GpuHashJoin].buildSide == GpuBuildLeft)

        // execute the plan so that the final adaptive plan is available when AQE is on
        plan2.collect()
        val finalPlan2 = findOperator(plan2.queryExecution.executedPlan,
          _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(finalPlan2.get.asInstanceOf[GpuHashJoin].buildSide == GpuBuildRight)
      }
    })
  }

  // Test for SPARK-51738 followup: GpuHashJoin should use DataType.equalsStructurally
  // instead of sameType to allow struct types with different field names but same structure.
  // This test verifies the equalsStructurally behavior that enables the SPARK-51738 feature.
  test("equalsStructurally allows struct types with different field names") {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.rapids.execution.TrampolineUtil

    // Create two struct types with same structure but different field names
    val structA = StructType(Seq(StructField("a", IntegerType, nullable = false)))
    val structB = StructType(Seq(StructField("b", IntegerType, nullable = false)))

    // TrampolineUtil.sameType returns false because field names differ
    assert(!TrampolineUtil.sameType(structA, structB),
      "sameType should return false for structs with different field names")

    // equalsStructurally returns true because structure is the same
    assert(DataType.equalsStructurally(structA, structB, ignoreNullability = true),
      "equalsStructurally should return true for structurally equivalent types")

    // Also test with nullability differences
    val structANullable = StructType(Seq(StructField("a", IntegerType, nullable = true)))
    assert(DataType.equalsStructurally(structA, structANullable, ignoreNullability = true),
      "equalsStructurally should ignore nullability differences when ignoreNullability=true")
  }
}
