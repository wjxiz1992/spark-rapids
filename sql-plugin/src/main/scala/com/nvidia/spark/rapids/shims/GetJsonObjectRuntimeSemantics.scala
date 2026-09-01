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

package com.nvidia.spark.rapids.shims

import scala.util.Try

import org.apache.spark.unsafe.types.UTF8String

private[rapids] object GetJsonObjectRuntimeSemantics {
  private val ExpectedQuestionMarkValue = "QUESTION"

  /**
   * Classify the result of evaluating the quoted-question-mark probe with the active CPU
   * Catalyst implementation.
   *
   * @param result lazily evaluated probe result; evaluation failures are treated as unknown
   * @return `Some(true)` for the SPARK-46761 result, `Some(false)` for the legacy null result,
   *         or `None` for an unexpected value or evaluation failure
   */
  def classifyQuotedQuestionMarkResult(result: => Any): Option[Boolean] = {
    Try(result).toOption match {
      case Some(null) => Some(false)
      case Some(value: UTF8String) if value.toString == ExpectedQuestionMarkValue => Some(true)
      case _ => None
    }
  }
}
