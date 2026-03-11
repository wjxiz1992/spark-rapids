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

import com.nvidia.spark.rapids.{GpuBoundReference, GpuLiteral, GpuUnitTests}

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.{GpuCreateNamedStruct, GpuGetStructField}
import org.apache.spark.sql.types._

/**
 * Tests for metadata propagation in GpuGetStructField and GpuCreateNamedStruct.
 * Following SPARK-51624, metadata from GetStructField should be preserved
 * when creating new structs via CreateNamedStruct.
 */
class GetStructFieldMetadataTest extends GpuUnitTests {

  private val testMetadata = new MetadataBuilder()
    .putString("comment", "test field")
    .putLong("maxLength", 100)
    .build()

  private val structWithMetadata = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true, testMetadata),
    StructField("value", DoubleType, nullable = true)
  ))

  test("GpuGetStructField should return field metadata") {
    val structRef = GpuBoundReference(0, structWithMetadata, nullable = false)(ExprId(0), "struct")
    
    // Get field with metadata (ordinal 1 = "name" field)
    val getStructField = GpuGetStructField(structRef, 1, Some("name"))
    
    assert(getStructField.metadata == testMetadata,
      "GpuGetStructField should return the field's metadata")
    assert(getStructField.metadata.getString("comment") == "test field")
    assert(getStructField.metadata.getLong("maxLength") == 100)
  }

  test("GpuGetStructField should return empty metadata for field without metadata") {
    val structRef = GpuBoundReference(0, structWithMetadata, nullable = false)(ExprId(0), "struct")
    
    // Get field without metadata (ordinal 0 = "id" field)
    val getStructField = GpuGetStructField(structRef, 0, Some("id"))
    
    assert(getStructField.metadata == Metadata.empty,
      "GpuGetStructField should return empty metadata for fields without metadata")
  }

  test("GpuCreateNamedStruct should propagate metadata from GpuGetStructField") {
    val structRef = GpuBoundReference(0, structWithMetadata, nullable = false)(ExprId(0), "struct")
    
    // Get field with metadata
    val getStructField = GpuGetStructField(structRef, 1, Some("name"))
    
    // Create new struct using the extracted field
    val newStruct = GpuCreateNamedStruct(Seq(
      GpuLiteral("extracted_name"), getStructField
    ))
    
    // Verify the resulting struct has the metadata propagated
    val resultType = newStruct.dataType
    assert(resultType.fields.length == 1)
    assert(resultType.fields(0).name == "extracted_name")
    assert(resultType.fields(0).metadata == testMetadata,
      "GpuCreateNamedStruct should propagate metadata from GpuGetStructField")
    assert(resultType.fields(0).metadata.getString("comment") == "test field")
  }

  test("GpuCreateNamedStruct should handle mixed expressions correctly") {
    val structRef = GpuBoundReference(0, structWithMetadata, nullable = false)(ExprId(0), "struct")
    
    // Mix of: GpuGetStructField with metadata, literal, GpuGetStructField without metadata
    val getWithMeta = GpuGetStructField(structRef, 1, Some("name"))
    val getWithoutMeta = GpuGetStructField(structRef, 0, Some("id"))
    
    val newStruct = GpuCreateNamedStruct(Seq(
      GpuLiteral("field_with_meta"), getWithMeta,
      GpuLiteral("literal_field"), GpuLiteral("constant"),
      GpuLiteral("field_without_meta"), getWithoutMeta
    ))
    
    val resultType = newStruct.dataType
    assert(resultType.fields.length == 3)
    
    // First field should have metadata from GetStructField
    assert(resultType.fields(0).name == "field_with_meta")
    assert(resultType.fields(0).metadata == testMetadata)
    
    // Second field (from literal) should have empty metadata
    assert(resultType.fields(1).name == "literal_field")
    assert(resultType.fields(1).metadata == Metadata.empty)
    
    // Third field should have empty metadata (field had no metadata)
    assert(resultType.fields(2).name == "field_without_meta")
    assert(resultType.fields(2).metadata == Metadata.empty)
  }
}
