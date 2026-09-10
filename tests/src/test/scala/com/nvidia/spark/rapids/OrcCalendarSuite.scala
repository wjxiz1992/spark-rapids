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

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.sql.Timestamp
import java.time.{LocalDate, ZoneId}
import java.util.TimeZone

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsReaderType.RapidsReaderType
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{
  DateColumnVector, ListColumnVector, LongColumnVector, StructColumnVector, TimestampColumnVector}
import org.apache.orc.{OrcFile, TypeDescription}
import org.apache.orc.impl.RecordReaderImpl

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

class OrcCalendarSuite extends SparkQueryCompareTestSuite {

  private val legacyDateResource = "test-data/before_1582_date_v2_4.snappy.orc"
  private val dateValue = LocalDate.of(1200, 1, 1).toEpochDay
  private val modernDateValue = LocalDate.of(2000, 1, 1).toEpochDay

  private def calendarConf(
      readerType: RapidsReaderType,
      useChunkedReader: Boolean,
      v1SourceList: String): SparkConf = {
    new SparkConf()
      .set(SQLConf.USE_V1_SOURCE_LIST.key, v1SourceList)
      .set(RapidsConf.ORC_READER_TYPE.key, readerType.toString)
      .set(RapidsConf.CHUNKED_READER.key, useChunkedReader.toString)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, Integer.MAX_VALUE.toString)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, (1L << 30).toString)
      .set("spark.sql.files.maxPartitionBytes", (1L << 30).toString)
  }

  private def readLegacyDateResource(spark: SparkSession) = {
    val resource = Option(Thread.currentThread().getContextClassLoader
      .getResource(legacyDateResource)).getOrElse {
      throw new IllegalStateException(s"Missing Spark test resource: $legacyDateResource")
    }
    val file = File.createTempFile("spark-24-date", ".orc")
    file.deleteOnExit()
    val input = resource.openStream()
    try {
      Files.copy(input, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      input.close()
    }
    spark.read.orc(file.getCanonicalPath)
  }

  private def setDate(vector: DateColumnVector): Unit = {
    vector.setUsingProlepticCalendar(true)
    vector.vector(0) = dateValue
  }

  private def writeCalendarFile(
      spark: SparkSession,
      base: File,
      id: Int,
      writerUsedProlepticGregorian: Boolean): Unit = {
    val schema = TypeDescription.createStruct()
      .addField("id", TypeDescription.createInt())
      .addField("top_date", TypeDescription.createDate())
      .addField("modern_date", TypeDescription.createDate())
      .addField("nested", TypeDescription.createStruct()
        .addField("nested_date", TypeDescription.createDate()))
      .addField("dates", TypeDescription.createList(TypeDescription.createDate()))
    val options = OrcFile.writerOptions(spark.sparkContext.hadoopConfiguration)
      .setSchema(schema)
      .setProlepticGregorian(writerUsedProlepticGregorian)
    val path = new Path(base.getCanonicalPath, s"calendar-$id.orc")
    val writer = OrcFile.createWriter(path, options)
    try {
      val batch = schema.createRowBatch()
      batch.cols(0).asInstanceOf[LongColumnVector].vector(0) = id
      setDate(batch.cols(1).asInstanceOf[DateColumnVector])
      val modernDate = batch.cols(2).asInstanceOf[DateColumnVector]
      modernDate.setUsingProlepticCalendar(true)
      modernDate.vector(0) = modernDateValue

      val nested = batch.cols(3).asInstanceOf[StructColumnVector]
      setDate(nested.fields(0).asInstanceOf[DateColumnVector])

      val dates = batch.cols(4).asInstanceOf[ListColumnVector]
      dates.offsets(0) = 0
      dates.lengths(0) = 1
      dates.childCount = 1
      setDate(dates.child.asInstanceOf[DateColumnVector])

      batch.size = 1
      writer.addRowBatch(batch)
    } finally {
      writer.close()
    }

    withResourceIfAllowed(OrcFile.createReader(path,
      OrcFile.readerOptions(spark.sparkContext.hadoopConfiguration))) { reader =>
      assert(reader.writerUsedProlepticGregorian() === writerUsedProlepticGregorian,
        s"unexpected calendar metadata in $path")
    }
  }

  private def writeMixedCalendarFiles(spark: SparkSession, base: File): Unit = {
    assert(base.mkdirs())
    writeCalendarFile(spark, base, id = 0, writerUsedProlepticGregorian = false)
    writeCalendarFile(spark, base, id = 1, writerUsedProlepticGregorian = true)
  }

  private def writeTimestampCalendarFile(
      spark: SparkSession,
      base: File,
      proleptic: Boolean,
      includeBce: Boolean,
      includeProlepticCutover: Boolean): Int = {
    val schema = TypeDescription.fromString(
      "struct<id:int,ts:timestamp,modern_ts:timestamp,null_ts:timestamp," +
        "nested:struct<value:timestamp>,timestamps:array<timestamp>>")
    val path = new Path(base.getCanonicalPath, s"timestamps-$proleptic.orc")
    val writer = OrcFile.createWriter(path,
      OrcFile.writerOptions(spark.sparkContext.hadoopConfiguration)
        .setSchema(schema).setProlepticGregorian(proleptic))
    val cutover = Timestamp.valueOf("1582-10-15 00:00:00").getTime * 1000L
    val bceValues = if (includeBce) Seq[java.lang.Long](-100000000000000000L) else Seq.empty
    // The legacy reader fix covers the cutover. Proleptic cross-timezone cutover reads
    // still have a separate #131 discrepancy, preserved in the ignored regression below.
    val cutoverValues = if (!proleptic || includeProlepticCutover) {
      Seq[java.lang.Long](cutover - 1L, cutover, cutover + 1L)
    } else {
      Seq.empty
    }
    val values = bceValues ++ Seq[java.lang.Long](null,
      Timestamp.valueOf("1001-01-01 01:02:03.123456").getTime * 1000L + 456L) ++
      cutoverValues ++ Seq[java.lang.Long](0L, 946684800123456L)

    def setTimestamp(vector: TimestampColumnVector, row: Int, value: java.lang.Long): Unit = {
      // Supply hybrid-calendar input and let ORC convert it when writing a proleptic file.
      vector.setUsingProlepticCalendar(false)
      if (value == null) {
        vector.noNulls = false
        vector.isNull(row) = true
      } else {
        vector.time(row) = Math.floorDiv(value.longValue(), 1000L)
        vector.nanos(row) = Math.floorMod(value.longValue(), 1000000L).toInt * 1000
      }
    }

    try {
      val batch = schema.createRowBatch()
      val nested = batch.cols(4).asInstanceOf[StructColumnVector]
      val timestamps = batch.cols(5).asInstanceOf[ListColumnVector]
      values.zipWithIndex.foreach { case (value, row) =>
        batch.cols(0).asInstanceOf[LongColumnVector].vector(row) =
          row * 2 + (if (proleptic) 1 else 0)
        setTimestamp(batch.cols(1).asInstanceOf[TimestampColumnVector], row, value)
        setTimestamp(batch.cols(2).asInstanceOf[TimestampColumnVector], row, 946684800123456L)
        setTimestamp(batch.cols(3).asInstanceOf[TimestampColumnVector], row, null)
        setTimestamp(nested.fields(0).asInstanceOf[TimestampColumnVector], row, value)
        nested.noNulls = false
        nested.isNull(row) = row == values.size - 1
        timestamps.offsets(row) = row * 2L
        timestamps.lengths(row) = 2L
        setTimestamp(timestamps.child.asInstanceOf[TimestampColumnVector], row * 2, value)
        setTimestamp(timestamps.child.asInstanceOf[TimestampColumnVector], row * 2 + 1, null)
      }
      timestamps.childCount = values.size * 2
      batch.size = values.size
      writer.addRowBatch(batch)
    } finally {
      writer.close()
    }
    withResourceIfAllowed(OrcFile.createReader(path,
      OrcFile.readerOptions(spark.sparkContext.hadoopConfiguration))) { reader =>
      assert(reader.writerUsedProlepticGregorian() === proleptic)
      val rows = reader.rows().asInstanceOf[RecordReaderImpl]
      try {
        assert(reader.getStripes.size() > 0)
        reader.getStripes.asScala.foreach { stripe =>
          assert(rows.readStripeFooter(stripe).getWriterTimezone === TimeZone.getDefault.getID)
        }
      } finally {
        rows.close()
      }
    }
    values.size
  }

  private val timestampZonePairs = Seq(
    ("UTC", "America/Los_Angeles", false),
    ("America/Los_Angeles", "UTC", false),
    ("PST", "Asia/Shanghai", false),
    ("Asia/Shanghai", "PST", false),
    ("EST", "America/Los_Angeles", false),
    ("UTC", "EST", false),
    ("GMT+05:30", "UTC", false),
    ("UTC", "GMT-03:30", false),
    ("UTC", "EST", true))

  // Cover every reader family and both scan APIs, CPU reader modes and chunking modes.
  // The original Spark 2.4 fixture additionally covers the full V1/vectorized/chunked product.
  private val timestampReaderModes = Seq(
    (RapidsReaderType.PERFILE, false, "orc", false),
    (RapidsReaderType.PERFILE, true, "", true),
    (RapidsReaderType.COALESCING, false, "", true),
    (RapidsReaderType.MULTITHREADED, true, "orc", false))

  private def readTimestampMicros(
      spark: SparkSession,
      base: File,
      gpuScan: Option[String]): Array[Row] = {
    val frame = spark.read.orc(base.getCanonicalPath)
    gpuScan.foreach(ExecutionPlanCaptureCallback.assertContains(frame, _))
    def micros(row: SpecializedGetters, ordinal: Int): java.lang.Long = {
      if (row.isNullAt(ordinal)) null else java.lang.Long.valueOf(row.getLong(ordinal))
    }
    // Read Catalyst's microseconds directly, before java.sql.Timestamp materialization.
    // Preserve parent/child null masks as well as every array element.
    frame.queryExecution.executedPlan.executeCollect().map { row =>
      val nested = if (row.isNullAt(4)) null else Row(micros(row.getStruct(4, 1), 0))
      val array = row.getArray(5)
      Row(row.getInt(0), micros(row, 1), micros(row, 2), micros(row, 3), nested,
        (0 until array.numElements()).map(micros(array, _)))
    }
  }

  private def checkTimestampCalendars(
      writerZone: String,
      readerZone: String,
      includeBce: Boolean,
      readerType: RapidsReaderType,
      chunked: Boolean,
      v1SourceList: String,
      vectorized: Boolean,
      includeProlepticCutover: Boolean = false): Unit = {
    val originalTimeZone = TimeZone.getDefault
    val scanClass = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan"
    val conf = calendarConf(readerType, chunked, v1SourceList)
      .set("spark.sql.orc.impl", "native")
      .set("spark.sql.orc.enableVectorizedReader", vectorized.toString)
    try {
      withTempPath { base =>
        val rowCount = withCpuSparkSession(spark => {
          TimeZone.setDefault(TimeZone.getTimeZone(writerZone))
          assert(base.mkdirs())
          writeTimestampCalendarFile(spark, base, proleptic = false,
            includeBce = includeBce, includeProlepticCutover = includeProlepticCutover) +
            writeTimestampCalendarFile(spark, base, proleptic = true,
              includeBce = includeBce, includeProlepticCutover = includeProlepticCutover)
        }, conf)
        val sessionZones = Seq(readerZone, if (readerZone == "UTC") "Asia/Shanghai" else "UTC")
        val results = sessionZones.map { sessionZone =>
          withClue(s"JVM=$readerZone, session=$sessionZone: ") {
            def read(spark: SparkSession, gpuScan: Option[String]): Array[Row] = {
              // Set this inside the session callback: session initialization resets the JVM TZ.
              TimeZone.setDefault(TimeZone.getTimeZone(readerZone))
              spark.conf.set("spark.sql.session.timeZone", sessionZone)
              readTimestampMicros(spark, base, gpuScan)
            }
            val cpu = withCpuSparkSession(read(_, None), conf)
            val gpu = withGpuSparkSession(read(_, Some(scanClass)), conf)
            assert(cpu.length === rowCount)
            compareResults(sort = true, floatEpsilon = 0.0, fromCpu = cpu, fromGpu = gpu)
            gpu
          }
        }
        // Changing only the SQL session zone must not change the stored timestamp micros.
        compareResults(sort = true, floatEpsilon = 0.0,
          fromCpu = results.head, fromGpu = results.last)
      }
    } finally {
      TimeZone.setDefault(originalTimeZone)
    }
  }

  for {
    (writerZone, readerZone, includeBce) <- timestampZonePairs
    (readerType, chunked, v1SourceList, vectorized) <- timestampReaderModes
  } {
    test(s"read mixed ORC timestamp calendars from $writerZone in $readerZone with " +
        s"$readerType, chunked=$chunked, source=($v1SourceList), vectorized=$vectorized, " +
        s"BCE=$includeBce") {
      checkTimestampCalendars(writerZone, readerZone, includeBce,
        readerType, chunked, v1SourceList, vectorized)
    }
  }

  // KNOWN_ISSUE: https://github.com/NVIDIA/cudf-spark/issues/131 (P2).
  // Recover when proleptic ORC cutover conversion matches CPU across writer/reader zones.
  // This also fails with main's timezone converter; the legacy rows match after rebasing.
  ignore("proleptic ORC cutover from America/Los_Angeles to UTC (#131)") {
    checkTimestampCalendars("America/Los_Angeles", "UTC", includeBce = false,
      readerType = RapidsReaderType.PERFILE, chunked = false,
      v1SourceList = "orc", vectorized = false,
      includeProlepticCutover = true)
  }

  test("proleptic nested ORC date rebase reuses the unchanged struct column") {
    withGpuSparkSession { _ =>
      withResource(ColumnVector.daysFromInts(0)) { dateColumn =>
        withResource(ColumnVector.makeStruct(dateColumn)) { structColumn =>
          withResource(GpuOrcTimezoneUtils.rebaseOrcDateTime(
            new Table(structColumn), ZoneId.systemDefault(),
            writerUsedProlepticGregorian = true)) { result =>
            assert(result.getColumn(0) eq structColumn)
          }
        }
      }
    }
  }

  for {
    v1SourceList <- Seq("orc", "")
    useChunkedReader <- Seq(false, true)
  } {
    testSparkResultsAreEqual(
      s"read Spark 2.4 legacy ORC date, source list is ($v1SourceList), " +
        s"chunked=$useChunkedReader",
      readLegacyDateResource,
      conf = calendarConf(RapidsReaderType.PERFILE, useChunkedReader, v1SourceList),
      repart = 0,
      skipCanonicalizationCheck = true,
      existClasses = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan") {
      frame => frame
    }
  }

  for {
    readerType <- Seq(RapidsReaderType.COALESCING, RapidsReaderType.MULTITHREADED)
    useChunkedReader <- Seq(false, true)
  } {
    val v1SourceList = if (useChunkedReader) "" else "orc"
    testSparkReadResultsAreEqual(
      s"read mixed legacy and proleptic ORC dates with $readerType, " +
        s"source list is ($v1SourceList), chunked=$useChunkedReader",
      file => spark => {
        val frame = spark.read.orc(file.getCanonicalPath)
        assert(frame.queryExecution.executedPlan.execute().getNumPartitions === 1,
          "the legacy and proleptic ORC files must be assigned to one reader")
        frame
      },
      writeMixedCalendarFiles,
      conf = calendarConf(readerType, useChunkedReader, v1SourceList),
      repart = 0,
      skipCanonicalizationCheck = true,
      existClasses = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan") {
      frame => frame.selectExpr(
        "id",
        "top_date",
        "modern_date",
        "nested.nested_date AS nested_date",
        "dates[0] AS list_date").orderBy("id")
    }
  }

}
