/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.execution.columnar.PartitionBatchPruningSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuInMemoryTableScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsPartitionBatchPruningSuite
    extends PartitionBatchPruningSuite with RapidsSQLTestsBaseTrait {

  override def checkBatchPruning(
      query: String,
      _expectedReadPartitions: Int,
      _expectedReadBatches: Int)(
      expectedQueryResult: => Seq[Any]): Unit = {
    testRapids(query) {
      val df = sql(query)
      val queryExecution = df.queryExecution

      assertResult(expectedQueryResult.toArray, s"Wrong query result: $queryExecution") {
        df.collect().map(_(0)).toArray
      }

      val gpuCacheScans = queryExecution.executedPlan.collect {
        case scan: GpuInMemoryTableScanExec => scan
      }
      assert(gpuCacheScans.nonEmpty,
        s"Expected GpuInMemoryTableScanExec in plan:\n${queryExecution.executedPlan}")
    }
  }

  testRapids("disable IN_MEMORY_PARTITION_PRUNING") {
    withSQLConf(SQLConf.IN_MEMORY_PARTITION_PRUNING.key -> "false") {
      val df = sql("SELECT key FROM pruningData WHERE key = 1")
      assertResult(Array(1)) {
        df.collect().map(_(0)).toArray
      }

      val gpuCacheScans = df.queryExecution.executedPlan.collect {
        case scan: GpuInMemoryTableScanExec => scan
      }
      assert(gpuCacheScans.nonEmpty,
        s"Expected GpuInMemoryTableScanExec in plan:\n${df.queryExecution.executedPlan}")
    }
  }
}
