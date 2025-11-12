/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.datasources.parquet.ParquetCommitterSuite

class RapidsParquetCommitterSuite extends ParquetCommitterSuite {
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Enable RAPIDS plugin and all configurations from RapidsSQLTestsBaseTrait
    spark.conf.set("spark.rapids.sql.enabled", "true")
    spark.conf.set("spark.plugins", "com.nvidia.spark.SQLPlugin")
    spark.conf.set("spark.sql.queryExecutionListeners",
      "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
    spark.conf.set("spark.sql.cache.serializer", "com.nvidia.spark.ParquetCachedBatchSerializer")
    spark.conf.set("spark.rapids.sql.explain", "ALL")
    spark.conf.set("spark.rapids.sql.csv.read.decimal.enabled", "true")
    spark.conf.set("spark.rapids.sql.format.avro.enabled", "true")
    spark.conf.set("spark.rapids.sql.format.avro.read.enabled", "true")
    spark.conf.set("spark.rapids.sql.format.hive.text.write.enabled", "true")
    spark.conf.set("spark.rapids.sql.format.json.enabled", "true")
    spark.conf.set("spark.rapids.sql.format.json.read.enabled", "true")
    spark.conf.set("spark.rapids.sql.incompatibleDateFormats.enabled", "true")
    spark.conf.set("spark.rapids.sql.python.gpu.enabled", "true")
    spark.conf.set("spark.rapids.sql.rowBasedUDF.enabled", "true")
    spark.conf.set("spark.rapids.sql.window.collectList.enabled", "true")
    spark.conf.set("spark.rapids.sql.window.collectSet.enabled", "true")
    spark.conf.set("spark.rapids.sql.window.range.byte.enabled", "true")
    spark.conf.set("spark.rapids.sql.window.range.short.enabled", "true")
    spark.conf.set("spark.rapids.sql.expression.Ascii", "true")
    spark.conf.set("spark.rapids.sql.expression.Conv", "true")
    spark.conf.set("spark.rapids.sql.expression.GetJsonObject", "true")
    spark.conf.set("spark.rapids.sql.expression.JsonToStructs", "true")
    spark.conf.set("spark.rapids.sql.expression.StructsToJson", "true")
    spark.conf.set("spark.rapids.sql.exec.CollectLimitExec", "true")
    spark.conf.set("spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec", "true")
    spark.conf.set("spark.rapids.sql.exec.WindowInPandasExec", "true")
    spark.conf.set("spark.rapids.sql.hasExtendedYearValues", "false")
  }
}

