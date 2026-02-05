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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
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
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Shim trait for contextIndependentFoldable support.
 * 
 * In Spark 4.1+, Expression has a contextIndependentFoldable method that indicates
 * whether an expression can be folded without relying on external context
 * (timezone, session configs, catalogs).
 * 
 * For Spark versions before 4.1, this trait is empty as the method doesn't exist.
 */
trait GpuContextIndependentFoldableShim extends Expression

/**
 * Marker trait for expressions that are always context-independent foldable.
 * Examples: Literal, constants
 * 
 * For Spark versions before 4.1, this trait is empty.
 */
trait GpuAlwaysContextIndependentFoldable extends GpuContextIndependentFoldableShim

/**
 * Marker trait for expressions whose contextIndependentFoldable depends on children.
 * The expression is context-independent foldable if all children are.
 * 
 * For Spark versions before 4.1, this trait is empty.
 */
trait GpuChildDependentContextIndependentFoldable extends GpuContextIndependentFoldableShim
