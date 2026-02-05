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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Shim trait for contextIndependentFoldable support in Spark 4.1+.
 * 
 * Expression.contextIndependentFoldable indicates whether an expression can be folded
 * without relying on external context (timezone, session configs, catalogs).
 * If true, the expression can be safely evaluated during DDL operations.
 */
trait GpuContextIndependentFoldableShim extends Expression

/**
 * Trait for expressions that are always context-independent foldable.
 * Examples: Literal, constants
 * 
 * In Spark 4.1+, this overrides contextIndependentFoldable to return true.
 */
trait GpuAlwaysContextIndependentFoldable extends GpuContextIndependentFoldableShim {
  override def contextIndependentFoldable: Boolean = true
}

/**
 * Trait for expressions whose contextIndependentFoldable depends on children.
 * The expression is context-independent foldable if all children are.
 * 
 * In Spark 4.1+, this overrides contextIndependentFoldable to delegate to children.
 */
trait GpuChildDependentContextIndependentFoldable extends GpuContextIndependentFoldableShim {
  override def contextIndependentFoldable: Boolean = 
    children.forall(_.contextIndependentFoldable)
}
