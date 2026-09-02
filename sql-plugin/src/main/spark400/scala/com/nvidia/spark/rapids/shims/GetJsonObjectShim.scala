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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.spark.SparkConf

object GetJsonObjectShim {
  private val FIXED_NAMED_PART_REGEXP = "[^\\']+"

  private[rapids] def partRegexpInNamed(conf: SparkConf): String = FIXED_NAMED_PART_REGEXP

  /**
   * Spark 4 includes SPARK-46761, which accepts question marks in quoted path names.
   */
  def partRegexpInNamed: String = FIXED_NAMED_PART_REGEXP
}
