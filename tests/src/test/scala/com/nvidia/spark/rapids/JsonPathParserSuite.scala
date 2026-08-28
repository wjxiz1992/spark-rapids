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

import com.nvidia.spark.rapids.PathInstruction.{Key, Named}
import com.nvidia.spark.rapids.shims.{GetJsonObjectRuntimeSemantics, GetJsonObjectShim}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class JsonPathParserSuite extends AnyFunSuite {
  private val questionMarkPath = List(Key, Named("?"))

  test("quoted question marks follow the selected parser dialect") {
    val fixedCases = Seq(
      "$['?']" -> List(Key, Named("?")),
      "$['a?b']" -> List(Key, Named("a?b")),
      "$.outer['?']" -> List(Key, Named("outer"), Key, Named("?")))

    fixedCases.foreach { case (path, expected) =>
      assert(JsonPathParser.parse(path, allowQuestionMarkInQuotedName = true) === Some(expected))
      assert(JsonPathParser.parse(path, allowQuestionMarkInQuotedName = false).isEmpty)
    }
  }

  test("unquoted and malformed paths are independent of the selected parser dialect") {
    Seq(true, false).foreach { allowQuestionMark =>
      assert(JsonPathParser.parse("$.?", allowQuestionMark) === Some(questionMarkPath))
      assert(JsonPathParser.parse("$['ordinary']", allowQuestionMark) ===
        Some(List(Key, Named("ordinary"))))
      assert(JsonPathParser.parse("$['']", allowQuestionMark).isEmpty)
      assert(JsonPathParser.parse("$['unterminated]", allowQuestionMark).isEmpty)
    }
  }

  test("quoted question mark probe result is classified fail closed") {
    assert(GetJsonObjectRuntimeSemantics.classifyQuotedQuestionMarkResult(
      UTF8String.fromString("QUESTION")) === Some(true))
    assert(GetJsonObjectRuntimeSemantics.classifyQuotedQuestionMarkResult(null) === Some(false))
    assert(GetJsonObjectRuntimeSemantics.classifyQuotedQuestionMarkResult(
      UTF8String.fromString("unexpected")).isEmpty)
    assert(GetJsonObjectRuntimeSemantics.classifyQuotedQuestionMarkResult(
      throw new RuntimeException("probe failed")).isEmpty)
  }

  test("unknown runtime semantics require CPU fallback") {
    assert(GpuGetJsonObjectMeta.unsupportedReason(None) ===
      Some(GpuGetJsonObjectMeta.UNKNOWN_QUESTION_MARK_SUPPORT_REASON))
    assert(GpuGetJsonObjectMeta.unsupportedReason(Some(true)).isEmpty)
    assert(GpuGetJsonObjectMeta.unsupportedReason(Some(false)).isEmpty)
  }

  test("literal path parsing handles null without changing the selected dialect") {
    assert(GpuGetJsonObjectMeta.parseLiteralPath(
      null, allowQuestionMarkInQuotedName = true).isEmpty)
    assert(GpuGetJsonObjectMeta.parseLiteralPath(
      null, allowQuestionMarkInQuotedName = false).isEmpty)
  }

  test("shim capability matches the active Spark CPU expression") {
    val json = Literal.create(UTF8String.fromString("""{"?":"QUESTION"}"""), StringType)
    val path = Literal.create(UTF8String.fromString("$['?']"), StringType)
    val cpuResult = GetJsonObject(json, path).eval(null)
    val expected = GetJsonObjectRuntimeSemantics.classifyQuotedQuestionMarkResult(cpuResult)

    assert(expected.isDefined)
    assert(GetJsonObjectShim.quotedQuestionMarkSupport === expected)
  }
}
