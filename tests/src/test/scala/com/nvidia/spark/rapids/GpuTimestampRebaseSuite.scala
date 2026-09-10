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
import java.util.TimeZone

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector, ColumnView}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CudfTestHelper.assertColumnsAreEqual
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.rapids.RebaseDateTimeBridge

class GpuTimestampRebaseSuite extends AnyFunSuite {
  private val timeZones = Seq(
    "UTC", "America/Los_Angeles", "Asia/Shanghai", "PST", "EST", "GMT+05:30", "GMT-03:30")

  private def assertRebase(
      timeZone: String,
      values: Array[java.lang.Long],
      rebase: ColumnView => ColumnVector): Unit = {
    // Use Spark's public implementation as the oracle, including its Calendar slow path.
    val expected = values.map { value =>
      if (value == null) null else java.lang.Long.valueOf(
        RebaseDateTime.rebaseJulianToGregorianMicros(timeZone, value.longValue()))
    }
    withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(values: _*)) { input =>
      withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(expected: _*)) { cpu =>
        withResource(rebase(input)) { gpu =>
          assertColumnsAreEqual(cpu, gpu)
        }
      }
    }
  }

  for (timeZone <- timeZones) {
    test(s"timestamp rebase matches Spark at every map boundary in $timeZone") {
      val info = RebaseDateTimeBridge.getJulianToGregorianMicros(timeZone).get
      // Include every calendar and historical timezone transition, not only October 1582.
      // Keep this batch free of nulls and BCE values so it exercises the GPU lookup path.
      val values = info.switches.flatMap(switch => Seq(switch - 1L, switch, switch + 1L))
        .filter(_ >= info.switches.head).distinct.map(java.lang.Long.valueOf)
      withResource(new GpuTimestampRebaseUtils.LazyJulianToGregorianMicrosContext(timeZone)) {
        context => assertRebase(timeZone, values, context.rebase)
      }
    }

    test(s"timestamp rebase preserves nulls and modern values in a legacy batch in $timeZone") {
      val values = Array[java.lang.Long](null, -30578137076876544L, 0L,
        RebaseDateTime.lastSwitchJulianTs - 1L, RebaseDateTime.lastSwitchJulianTs, null)
      withResource(new GpuTimestampRebaseUtils.LazyJulianToGregorianMicrosContext(timeZone)) {
        context => assertRebase(timeZone, values, context.rebase)
      }
    }

    test(s"timestamp rebase matches Spark before the first map boundary in $timeZone") {
      val first = RebaseDateTimeBridge.getJulianToGregorianMicros(timeZone).get.switches.head
      val values = Array[java.lang.Long](first - 1L, first, first + 1L,
        -100000000000000000L, 0L, null)
      withResource(new GpuTimestampRebaseUtils.LazyJulianToGregorianMicrosContext(timeZone)) {
        context => assertRebase(timeZone, values, context.rebase)
      }
    }
  }

  test("timestamp rebase preserves empty, all-null and modern columns") {
    for (values <- Seq(Array.empty[java.lang.Long], Array[java.lang.Long](null, null),
        Array[java.lang.Long](0L, RebaseDateTime.lastSwitchJulianTs, null))) {
      withResource(new GpuTimestampRebaseUtils.LazyJulianToGregorianMicrosContext("UTC")) {
        context => assertRebase("UTC", values, context.rebase)
      }
    }
  }

  test("timestamp rebase supports sliced column views") {
    val values = Array[java.lang.Long](-30578137076876544L, null, 0L)
    val timeZone = "America/Los_Angeles"
    withResource(GpuTimestampRebaseUtils.createJulianToGregorianMicrosContext(timeZone)) {
      context =>
        assertRebase(timeZone, values, _ => {
          withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(
              (Array[java.lang.Long](0L) ++ values ++ Array[java.lang.Long](0L)): _*)) {
            padded =>
              withResource(padded.subVector(1, values.length + 1)) { slice =>
                context.rebase(slice)
              }
          }
        })
    }
  }

  test("a missing rebase map uses Spark's host implementation") {
    val values = Array[java.lang.Long](null, -30578137076876544L,
      -100000000000000000L, 0L)
    // Exercise the missing-map branch even if a future Spark tzdb covers all JDK IDs.
    withResource(new GpuTimestampRebaseUtils.JulianToGregorianMicrosContext(
        "America/Los_Angeles", None, None)) { context =>
      assertRebase("America/Los_Angeles", values, context.rebase)
    }
    // Also exercise factory selection for actual JDK IDs absent from the runtime Spark map.
    ZoneId.getAvailableZoneIds.asScala.toSeq.sorted
      .find(RebaseDateTimeBridge.getJulianToGregorianMicros(_).isEmpty).foreach { timeZone =>
        withResource(GpuTimestampRebaseUtils.createJulianToGregorianMicrosContext(timeZone)) {
          context => assertRebase(timeZone, values, context.rebase)
        }
      }
  }

  test("a normalized fixed-offset ID retains the JVM timezone in the BCE fallback") {
    val timeZone = "EST"
    val readerZone = TimeZone.getTimeZone(timeZone).toZoneId.getId
    // One microsecond before Julian March 1, 101 BCE at midnight in EST. Resolving
    // the normalized "-05:00" through TimeZone.getTimeZone(String) silently uses GMT.
    val values = Array[java.lang.Long](-65317950000000001L, -100000000000000000L,
      -30578137076876544L, 0L, null)
    withResource(new GpuTimestampRebaseUtils.LazyJulianToGregorianMicrosContext(readerZone)) {
      context => assertRebase(timeZone, values, context.rebase)
    }
  }
}
