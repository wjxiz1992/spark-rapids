/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object GetJsonObjectShim {
  private lazy val runtimeQuotedQuestionMarkSupport: Option[Boolean] = {
    val expected = "QUESTION"
    val json = Literal.create(UTF8String.fromString(s"""{"?":"$expected"}"""), StringType)
    val path = Literal.create(UTF8String.fromString("$['?']"), StringType)
    GetJsonObjectRuntimeSemantics.classifyQuotedQuestionMarkResult(
      GetJsonObject(json, path).eval(null), expected)
  }

  /**
   * Detect whether this Spark runtime includes SPARK-46761 semantics. Some vendors backported the
   * fix without changing the upstream Spark version, so a version check is not sufficient.
   */
  def quotedQuestionMarkSupport: Option[Boolean] = runtimeQuotedQuestionMarkSupport
}
