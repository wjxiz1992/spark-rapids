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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import org.apache.spark.api.python.PythonEvalType

/**
 * Shim for ArrowEvalPythonExec supported eval types.
 * For Spark 4.1+, all Arrow UDF types including new scalar Arrow UDFs are supported.
 */
object ArrowEvalPythonExecShims {
  /**
   * Returns the supported Python eval types for ArrowEvalPythonExec.
   * These correspond to the types that Spark's ArrowEvalPythonExec accepts.
   */
  def supportedEvalTypes: Array[Int] = Array(
    PythonEvalType.SQL_ARROW_BATCHED_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
  )
}
