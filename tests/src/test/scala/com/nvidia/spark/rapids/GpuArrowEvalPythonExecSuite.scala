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

package com.nvidia.spark.rapids

import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.GpuArrowEvalPythonExec
import org.apache.spark.sql.rapids.execution.python.shims.ArrowEvalPythonExecShims

/**
 * Unit tests for GpuArrowEvalPythonExec evalType validation (SPARK-53243).
 *
 * This test suite verifies that:
 * 1. ArrowEvalPythonExecShims returns valid supported eval types
 * 2. GpuArrowEvalPythonExec rejects invalid eval types with IllegalArgumentException
 * 3. GpuArrowEvalPythonExec accepts valid eval types (e.g., SQL_SCALAR_PANDAS_UDF)
 *
 * Note: PythonEvalType is private[spark], so we use numeric constants directly.
 * These values are defined in org.apache.spark.api.python.PythonEvalType:
 *   SQL_SCALAR_PANDAS_UDF = 200
 *   SQL_SCALAR_PANDAS_ITER_UDF = 204
 */
class GpuArrowEvalPythonExecSuite extends AnyFunSuite {

  // PythonEvalType constants (private[spark], so we define them here)
  private val SQL_SCALAR_PANDAS_UDF = 200
  private val SQL_SCALAR_PANDAS_ITER_UDF = 204

  test("ArrowEvalPythonExecShims.supportedEvalTypes should not be empty") {
    val supportedTypes = ArrowEvalPythonExecShims.supportedEvalTypes
    assert(supportedTypes.nonEmpty,
      "supportedEvalTypes should contain at least one eval type")
  }

  test("ArrowEvalPythonExecShims.supportedEvalTypes should contain SQL_SCALAR_PANDAS_UDF") {
    // SQL_SCALAR_PANDAS_UDF (200) is supported in all Spark versions
    val supportedTypes = ArrowEvalPythonExecShims.supportedEvalTypes
    assert(supportedTypes.contains(SQL_SCALAR_PANDAS_UDF),
      s"supportedEvalTypes should contain SQL_SCALAR_PANDAS_UDF (200), " +
        s"but got: ${supportedTypes.mkString(", ")}")
  }

  test("GpuArrowEvalPythonExec should reject invalid evalType with IllegalArgumentException") {
    val mockChild = mock(classOf[SparkPlan])
    when(mockChild.output).thenReturn(Seq.empty)

    // Use an invalid evalType that is not in any supported list
    val invalidEvalType = 999

    val exception = intercept[IllegalArgumentException] {
      GpuArrowEvalPythonExec(
        udfs = Seq.empty,
        resultAttrs = Seq.empty,
        child = mockChild,
        evalType = invalidEvalType
      )
    }

    assert(exception.getMessage.contains("Unexpected eval type"),
      s"Exception message should mention 'Unexpected eval type', " +
        s"but got: ${exception.getMessage}")
    assert(exception.getMessage.contains(invalidEvalType.toString),
      s"Exception message should contain the invalid evalType value ($invalidEvalType), " +
        s"but got: ${exception.getMessage}")
  }

  test("GpuArrowEvalPythonExec should reject negative evalType") {
    val mockChild = mock(classOf[SparkPlan])
    when(mockChild.output).thenReturn(Seq.empty)

    val negativeEvalType = -1

    assertThrows[IllegalArgumentException] {
      GpuArrowEvalPythonExec(
        udfs = Seq.empty,
        resultAttrs = Seq.empty,
        child = mockChild,
        evalType = negativeEvalType
      )
    }
  }

  test("GpuArrowEvalPythonExec should accept valid evalType SQL_SCALAR_PANDAS_UDF") {
    val mockChild = mock(classOf[SparkPlan])
    when(mockChild.output).thenReturn(Seq.empty)

    // SQL_SCALAR_PANDAS_UDF (200) is supported in all Spark versions
    val validEvalType = SQL_SCALAR_PANDAS_UDF

    // Should not throw - this verifies the validation accepts valid types
    val exec = GpuArrowEvalPythonExec(
      udfs = Seq.empty,
      resultAttrs = Seq.empty,
      child = mockChild,
      evalType = validEvalType
    )

    assert(exec.evalType == validEvalType,
      s"evalType should be $validEvalType, but got ${exec.evalType}")
  }

  test("GpuArrowEvalPythonExec should accept valid evalType SQL_SCALAR_PANDAS_ITER_UDF") {
    val mockChild = mock(classOf[SparkPlan])
    when(mockChild.output).thenReturn(Seq.empty)

    // SQL_SCALAR_PANDAS_ITER_UDF (204) is supported in all Spark versions
    val validEvalType = SQL_SCALAR_PANDAS_ITER_UDF

    // Should not throw
    val exec = GpuArrowEvalPythonExec(
      udfs = Seq.empty,
      resultAttrs = Seq.empty,
      child = mockChild,
      evalType = validEvalType
    )

    assert(exec.evalType == validEvalType,
      s"evalType should be $validEvalType, but got ${exec.evalType}")
  }
}
