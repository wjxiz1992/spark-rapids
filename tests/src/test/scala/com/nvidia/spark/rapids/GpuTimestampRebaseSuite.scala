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

import java.time.ZoneId

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CudfTestHelper.assertColumnsAreEqual
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.util.RebaseDateTime

class GpuTimestampRebaseSuite extends AnyFunSuite {
  private val LosAngeles = "America/Los_Angeles"

  private val losAngelesInputs: Array[java.lang.Long] = Array(
    null,
    -30578137077000001L,
    -30578137077000000L,
    -30578137076999999L,
    -30578137076876544L,
    -12219264000000001L,
    -12219264000000000L,
    -12219263999999999L,
    -2717640000000001L,
    -2717640000000000L,
    -2717639999999999L,
    -2208988800000000L)

  private val losAngelesExpected: Array[java.lang.Long] = Array(
    null,
    -30578655899000001L,
    -30578655899000000L,
    -30578655898999999L,
    -30578655898876544L,
    -12220128422000001L,
    -12219264422000000L,
    -12219264421999999L,
    -2717640422000001L,
    -2717640000000000L,
    -2717639999999999L,
    -2208988800000000L)

  private def assertRebase(
      timeZoneId: String,
      input: Array[java.lang.Long],
      expected: Array[java.lang.Long]): Unit = {
    withResource(GpuTimestampRebaseUtils.createJulianToGregorianMicrosContext(timeZoneId)) {
      context =>
      withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(input: _*)) { inputColumn =>
        withResource(context.rebase(inputColumn)) { actual =>
          withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(expected: _*)) {
            expectedColumn =>
            assertColumnsAreEqual(expectedColumn, actual)
          }
        }
      }
    }
  }

  test("timezone-specific timestamp rebase matches Spark at every critical boundary") {
    losAngelesInputs.zip(losAngelesExpected).foreach { case (input, expected) =>
      if (input != null) {
        assert(RebaseDateTime.rebaseJulianToGregorianMicros(LosAngeles, input) === expected)
      }
    }
    assertRebase(LosAngeles, losAngelesInputs, losAngelesExpected)
  }

  test("UTC timestamp rebase remains distinct from Los Angeles historical rules") {
    val input = Array[java.lang.Long](-30578137076876544L)
    val expected = Array[java.lang.Long](-30578655476876544L)
    assert(RebaseDateTime.rebaseJulianToGregorianMicros("UTC", input.head) === expected.head)
    assertRebase("UTC", input, expected)
  }

  test("fixed-offset and short timezone IDs match Spark") {
    val fixedOffset = "GMT+05:30"
    val fixedOffsetInputs: Array[java.lang.Long] = Array(
      -30578137076876544L,
      -12219312600000001L,
      -12219312600000000L,
      -12219312599999999L)
    val fixedOffsetExpected: Array[java.lang.Long] = Array(
      -30578655476876544L,
      -12220176600000001L,
      -12219312600000000L,
      -12219312599999999L)
    fixedOffsetInputs.zip(fixedOffsetExpected).foreach { case (input, expected) =>
      assert(RebaseDateTime.rebaseJulianToGregorianMicros(fixedOffset, input) === expected)
    }
    assertRebase(fixedOffset, fixedOffsetInputs, fixedOffsetExpected)

    assert(RebaseDateTime.rebaseJulianToGregorianMicros(
      "PST", losAngelesInputs(4)) === losAngelesExpected(4))
    assertRebase("PST", Array(losAngelesInputs(4)), Array(losAngelesExpected(4)))
  }

  test("valid JDK timezone missing from Spark's bundled map uses the exact fallback") {
    val timeZoneId = "Europe/Kyiv"
    assume(ZoneId.getAvailableZoneIds.contains(timeZoneId),
      s"$timeZoneId is not available in this JDK timezone database")
    val input: Array[java.lang.Long] = Array(
      -30578137076876544L,
      -12219264000000001L,
      -12219264000000000L,
      0L)
    val expected: Array[java.lang.Long] = Array(
      -30578655600876544L,
      -12219264124000001L,
      -12219264124000000L,
      0L)
    input.zip(expected).foreach { case (value, rebased) =>
      assert(RebaseDateTime.rebaseJulianToGregorianMicros(timeZoneId, value) === rebased)
    }
    assertRebase(timeZoneId, input, expected)
    assertRebase(timeZoneId, Array[java.lang.Long](null, 0L),
      Array[java.lang.Long](null, 0L))
  }

  test("timestamp rebase preserves empty and sliced columns") {
    assertRebase(LosAngeles, Array.empty[java.lang.Long], Array.empty[java.lang.Long])

    withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(
        0L, losAngelesInputs(4), losAngelesInputs(5), 0L)) { input =>
      withResource(input.subVector(1, 3)) { slicedInput =>
        withResource(GpuTimestampRebaseUtils.createJulianToGregorianMicrosContext(LosAngeles)) {
          context =>
          withResource(context.rebase(slicedInput)) { actual =>
            withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(
                losAngelesExpected(4), losAngelesExpected(5))) { expected =>
              assertColumnsAreEqual(expected, actual)
            }
          }
        }
      }
    }
  }

  test("timestamp rebase uses Spark's exact fallback before the first map boundary") {
    val input: Array[java.lang.Long] = Array(
      -62135740800000001L,
      -100000000000000000L)
    val expected: Array[java.lang.Long] = Array(
      -62135568422000001L,
      -99999050022000000L)
    input.zip(expected).foreach { case (value, rebased) =>
      assert(RebaseDateTime.rebaseJulianToGregorianMicros(LosAngeles, value) === rebased)
    }
    assertRebase(LosAngeles, input, expected)
  }
}
