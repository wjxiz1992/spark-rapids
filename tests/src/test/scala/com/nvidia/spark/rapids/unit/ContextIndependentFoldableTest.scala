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

package com.nvidia.spark.rapids.unit

import com.nvidia.spark.rapids.{GpuCoalesce, GpuLiteral, GpuUnitTests}

import org.apache.spark.sql.rapids.{GpuCreateArray, GpuCreateMap, GpuCreateNamedStruct}
import org.apache.spark.sql.types._

/**
 * Tests for contextIndependentFoldable behavior on GPU expressions.
 * Following SPARK-52575, GPU expressions should implement contextIndependentFoldable
 * to indicate whether they can be folded without relying on external context.
 */
class ContextIndependentFoldableTest extends GpuUnitTests {

  test("GpuLiteral should be foldable") {
    val intLiteral = GpuLiteral(42, IntegerType)
    assert(intLiteral.foldable, "GpuLiteral should be foldable")
    
    val stringLiteral = GpuLiteral("test", StringType)
    assert(stringLiteral.foldable, "GpuLiteral should be foldable")
    
    val nullLiteral = GpuLiteral(null, IntegerType)
    assert(nullLiteral.foldable, "GpuLiteral with null should be foldable")
  }

  test("GpuCoalesce should be foldable when all children are foldable") {
    val lit1 = GpuLiteral(1, IntegerType)
    val lit2 = GpuLiteral(2, IntegerType)
    
    val coalesce = GpuCoalesce(Seq(lit1, lit2))
    assert(coalesce.foldable, "GpuCoalesce with all literal children should be foldable")
  }

  test("GpuCreateArray should be foldable when all children are foldable") {
    val lit1 = GpuLiteral(1, IntegerType)
    val lit2 = GpuLiteral(2, IntegerType)
    
    val array = GpuCreateArray(Seq(lit1, lit2), useStringTypeWhenEmpty = false)
    assert(array.foldable, "GpuCreateArray with all literal children should be foldable")
  }

  test("GpuCreateMap should be foldable when all children are foldable") {
    val key1 = GpuLiteral("a", StringType)
    val val1 = GpuLiteral(1, IntegerType)
    val key2 = GpuLiteral("b", StringType)
    val val2 = GpuLiteral(2, IntegerType)
    
    val map = GpuCreateMap(Seq(key1, val1, key2, val2), useStringTypeWhenEmpty = false)
    assert(map.foldable, "GpuCreateMap with all literal children should be foldable")
  }

  test("GpuCreateNamedStruct should be foldable when all value expressions are foldable") {
    val name1 = GpuLiteral("field1", StringType)
    val val1 = GpuLiteral(1, IntegerType)
    val name2 = GpuLiteral("field2", StringType)
    val val2 = GpuLiteral("test", StringType)
    
    val struct = GpuCreateNamedStruct(Seq(name1, val1, name2, val2))
    assert(struct.foldable, "GpuCreateNamedStruct with all literal values should be foldable")
  }
}
