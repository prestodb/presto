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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/ComplexViewTypes.h"
#include "velox/expression/ComplexWriterTypes.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::aggregate {

// Represents a list of values, including nulls, for an array/map/distinct value
// set in aggregation. Bit-packed null flags are stored separately from the
// non-null values.
class ValueList {
 public:
  void appendValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator);

  template <typename T>
  void appendValue(
      const exec::OptionalAccessor<Generic<T>>& value,
      HashStringAllocator* allocator) {
    if (!value.has_value()) {
      appendNull(allocator);
    } else {
      VELOX_DCHECK(!value->isNull());
      appendNonNull(*value->base(), value->decodedIndex(), allocator);
    }
  }

  void appendRange(
      const VectorPtr& vector,
      vector_size_t offset,
      vector_size_t size,
      HashStringAllocator* allocator);

  int32_t size() const {
    return size_;
  }

  // Called after 'finalize()' to get access to 'data' allocation.
  HashStringAllocator::Header* dataBegin() {
    return dataBegin_;
  }

  // Called after 'finalize()' to get access to 'nulls' allocation.
  HashStringAllocator::Header* nullsBegin() {
    return nullsBegin_;
  }

  uint64_t lastNulls() const {
    return lastNulls_;
  }

  void free(HashStringAllocator* allocator) {
    if (size_) {
      allocator->free(nullsBegin_);
      allocator->free(dataBegin_);
    }
  }

 private:
  // An array_agg or related begins with an allocation of 5 words and
  // 4 bytes for header. This is compact for small arrays (up to 5
  // bigints) and efficient if needs to be extended (stores 4 bigints
  // and a next pointer. This could be adaptive, with smaller initial
  // sizes for lots of small arrays.
  static constexpr int kInitialSize = 44;

  void appendNull(HashStringAllocator* allocator);

  void appendNonNull(
      const BaseVector& vector,
      vector_size_t index,
      HashStringAllocator* allocator);

  void prepareAppend(HashStringAllocator* allocator);

  // Writes lastNulls_ word to the 'nulls' block.
  void writeLastNulls(HashStringAllocator* allocator);

  // 'Nulls' allocation (potentially multi-part).
  HashStringAllocator::Header* nullsBegin_{nullptr};
  HashStringAllocator::Position nullsCurrent_{nullptr, nullptr};

  // 'Data' allocation (potentially multi-part)
  HashStringAllocator::Header* dataBegin_{nullptr};
  HashStringAllocator::Position dataCurrent_{nullptr, nullptr};

  // Number of values added, including nulls.
  uint32_t size_{0};

  // Bytes added. Used to control allocation of reserve for future appends.
  int32_t bytes_{0};

  // Last nulls word. 'size_ % 64' is the null bit for the next element.
  uint64_t lastNulls_{0};
};

// Extracts values from the ValueList into provided vector.
class ValueListReader {
 public:
  explicit ValueListReader(ValueList& values);

  bool next(BaseVector& output, vector_size_t outputIndex);

 private:
  const vector_size_t size_;
  const vector_size_t lastNullsStart_;
  const uint64_t lastNulls_;
  ByteInputStream dataStream_;
  ByteInputStream nullsStream_;
  uint64_t nulls_;
  vector_size_t pos_{0};
};

// Write ValueList accumulators to Array-typed intermediate or final result
// vectors.
// TODO: This API only works if it is the only logic writing to `writer`.
template <typename T>
void copyValueListToArrayWriter(
    facebook::velox::exec::ArrayWriter<T>& writer,
    ValueList& elements) {
  writer.resetLength();
  auto size = elements.size();
  if (size == 0) {
    return;
  }
  writer.reserve(size);

  ValueListReader reader(elements);
  for (vector_size_t i = 0; i < size; ++i) {
    reader.next(*writer.elementsVector(), writer.valuesOffset() + i);
  }
  writer.resize(size);
}

} // namespace facebook::velox::aggregate
