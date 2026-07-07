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

package com.nvidia.spark.rapids.parquet

import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

class ParquetSchemaUtilsSuite extends AnyFunSuite {
  private val missingChildrenSchema = new StructType().add("_1",
    new StructType().add("_101", IntegerType).add("_102", LongType))

  private def assertClippedSchema(parquetSchema: String, expectedSchema: String): Unit = {
    val actual = ParquetSchemaUtils.clipParquetSchema(
      MessageTypeParser.parseMessageType(parquetSchema),
      missingChildrenSchema,
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)
    assertResult(MessageTypeParser.parseMessageType(expectedSchema))(actual)
  }

  test("missing struct retains the cheaper physical path") {
    assertClippedSchema(
      parquetSchema =
        """message root {
          |  optional group _1 {
          |    optional group _1 (MAP) {
          |      repeated group key_value {
          |        required boolean key;
          |        optional group value {
          |          optional binary _1;
          |          optional int32 _2;
          |        }
          |      }
          |    }
          |    optional group _2 (LIST) {
          |      repeated group list {
          |        optional int32 element;
          |      }
          |    }
          |  }
          |}
          |""".stripMargin,
      expectedSchema =
        """message spark_schema {
          |  optional group _1 {
          |    optional group _2 (LIST) {
          |      repeated group list {
          |        optional int32 element;
          |      }
          |    }
          |  }
          |}
          |""".stripMargin)
  }

  test("missing struct minimizes maximum repetition level") {
    assertClippedSchema(
      parquetSchema =
        """message root {
          |  optional group _1 {
          |    optional group _1 (MAP) {
          |      repeated group key_value {
          |        optional group key (LIST) {
          |          repeated group list {
          |            optional int32 element;
          |          }
          |        }
          |        optional group value (LIST) {
          |          repeated group list {
          |            optional group element (LIST) {
          |              repeated group list {
          |                optional group element (LIST) {
          |                  repeated group list {
          |                    optional int32 element;
          |                  }
          |                }
          |              }
          |            }
          |          }
          |        }
          |      }
          |    }
          |    optional group _2 (LIST) {
          |      repeated group list {
          |        optional group element (LIST) {
          |          repeated group list {
          |            optional int32 element;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
          |""".stripMargin,
      expectedSchema =
        """message spark_schema {
          |  optional group _1 {
          |    optional group _2 (LIST) {
          |      repeated group list {
          |        optional group element (LIST) {
          |          repeated group list {
          |            optional int32 element;
          |          }
          |        }
          |      }
          |    }
          |  }
          |}
          |""".stripMargin)
  }

  test("missing struct preserves legacy MAP_KEY_VALUE children") {
    assertClippedSchema(
      parquetSchema =
        """message root {
          |  optional group _1 {
          |    repeated group entries (MAP_KEY_VALUE) {
          |      required int32 key;
          |      optional int64 value;
          |    }
          |  }
          |}
          |""".stripMargin,
      expectedSchema =
        """message spark_schema {
          |  optional group _1 {
          |    repeated group entries (MAP_KEY_VALUE) {
          |      required int32 key;
          |      optional int64 value;
          |    }
          |  }
          |}
          |""".stripMargin)
  }

  test("missing struct reports an empty physical group") {
    val error = intercept[IllegalArgumentException] {
      assertClippedSchema(
        parquetSchema =
          """message root {
            |  optional group _1 {
            |    optional group empty {
            |    }
            |  }
            |}
            |""".stripMargin,
        expectedSchema = "message spark_schema {}")
    }
    assert(error.getMessage.contains("findCheapestGroupField called on empty group"))
  }
}
