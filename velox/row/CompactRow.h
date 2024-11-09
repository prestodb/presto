/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#pragma once

#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::row {

class CompactRow {
 public:
  explicit CompactRow(const RowVectorPtr& vector);

  /// Returns the serialized sizes of the rows at specified indexes.
  ///
  /// TODO: optimizes using columnar serialization size calculation.
  void serializedRowSizes(
      const folly::Range<const vector_size_t*>& rows,
      vector_size_t** sizes) const;

  /// Returns row size if all fields are fixed width. Return std::nullopt if
  /// there are variable-width fields.
  static std::optional<int32_t> fixedRowSize(const RowTypePtr& rowType);

  /// Returns serialized size of the row at specified index. Use only if
  /// 'fixedRowSize' returned std::nullopt.
  int32_t rowSize(vector_size_t index) const;

  /// Serializes row at specified index into 'buffer'.
  /// 'buffer' must have sufficient capacity and set to all zeros.
  int32_t serialize(vector_size_t index, char* buffer) const;

  /// Serializes rows in the range [offset, offset + size) into 'buffer' at
  /// given 'bufferOffsets'. 'buffer' must have sufficient capacity and set to
  /// all zeros for null-bits handling. 'bufferOffsets' must be pre-filled with
  /// the write offsets for each row and must be accessible for 'size' elements.
  /// The caller must ensure that the space between each offset in
  /// 'bufferOffsets' is no less than the 'fixedRowSize' or 'rowSize'.
  void serialize(
      vector_size_t offset,
      vector_size_t size,
      const size_t* bufferOffsets,
      char* buffer) const;

  /// Deserializes multiple rows into a RowVector of specified type. The type
  /// must match the contents of the serialized rows.
  static RowVectorPtr deserialize(
      const std::vector<std::string_view>& data,
      const RowTypePtr& rowType,
      memory::MemoryPool* pool);

 private:
  explicit CompactRow(const VectorPtr& vector);

  void initialize(const TypePtr& type);

  bool isNullAt(vector_size_t) const;

  /// Fixed-width types only. Returns number of bytes used by single value.
  int32_t valueBytes() const {
    return valueBytes_;
  }

  /// Writes fixed-width value at specified index into 'buffer'. Value must not
  /// be null.
  void serializeFixedWidth(vector_size_t index, char* buffer) const;

  /// Writes range of fixed-width values between 'offset' and 'offset + size'
  /// into 'buffer'. Values can be null.
  void serializeFixedWidth(
      vector_size_t offset,
      vector_size_t size,
      char* buffer) const;

  /// Returns serialized size of variable-width row.
  int32_t variableWidthRowSize(vector_size_t index) const;

  /// Writes variable-width value at specified index into 'buffer'. Value must
  /// not be null. Returns number of bytes written to 'buffer'.
  int32_t serializeVariableWidth(vector_size_t index, char* buffer) const;

 private:
  /// Returns serialized size of array row.
  int32_t arrayRowSize(vector_size_t index) const;

  /// Serializes array value to buffer. Value must not be null. Returns number
  /// of bytes written to 'buffer'.
  int32_t serializeArray(vector_size_t index, char* buffer) const;

  /// Returns serialized size of map row.
  int32_t mapRowSize(vector_size_t index) const;

  /// Serializes map value to buffer. Value must not be null. Returns number of
  /// bytes written to 'buffer'.
  int32_t serializeMap(vector_size_t index, char* buffer) const;

  /// Returns serialized size of a range of values.
  int32_t arrayRowSize(
      const CompactRow& elements,
      vector_size_t offset,
      vector_size_t size,
      bool fixedWidth) const;

  /// Serializes a range of values into buffer. Returns number of bytes written
  /// to 'buffer'.
  int32_t serializeAsArray(
      const CompactRow& elements,
      vector_size_t offset,
      vector_size_t size,
      bool fixedWidth,
      char* buffer) const;
  ;

  /// Returns serialized size of struct value.
  int32_t rowRowSize(vector_size_t index) const;

  /// Serializes struct value to buffer. Value must not be null.
  int32_t serializeRow(vector_size_t index, char* buffer) const;

  /// Serializes struct values in range [offset, offset + size) to buffer.
  /// Value must not be null.
  void serializeRow(
      vector_size_t offset,
      vector_size_t size,
      char* buffer,
      const size_t* bufferOffsets) const;

  const TypeKind typeKind_;
  DecodedVector decoded_;

  /// True if values of 'typeKind_' have fixed width.
  bool fixedWidthTypeKind_{false};

  /// ARRAY, MAP and ROW types only.
  std::vector<CompactRow> children_;
  std::vector<bool> childIsFixedWidth_;

  /// True if this is a flat fixed-width vector whose consecutive values can be
  /// copied into serialized buffer in bulk.
  bool supportsBulkCopy_{false};

  // ROW type only. Number of bytes used by null flags.
  size_t rowNullBytes_;

  // Fixed-width types only. Number of bytes used for a single value.
  size_t valueBytes_;
};
} // namespace facebook::velox::row
