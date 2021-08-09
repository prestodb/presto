/*
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

#include "velox/exec/Aggregate.h"
#include "velox/exec/HashStringAllocator.h"
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
      exec::HashStringAllocator* allocator);

  void appendRange(
      const VectorPtr& vector,
      vector_size_t offset,
      vector_size_t size,
      exec::HashStringAllocator* allocator);

  int32_t size() const {
    return size_;
  }

  // Called after all data has been appended.
  void finalize(exec::HashStringAllocator* allocator) {
    if (size_ % 64 != 0) {
      writeLastNulls(allocator);
    }
  }

  // Called after 'finalize()' to get access to 'data' allocation.
  exec::HashStringAllocator::Header* dataBegin() {
    return dataBegin_;
  }

  // Called after 'finalize()' to get access to 'nulls' allocation.
  exec::HashStringAllocator::Header* nullsBegin() {
    return nullsBegin_;
  }

  void free(exec::HashStringAllocator* allocator) {
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

  void appendNull(exec::HashStringAllocator* allocator);

  void appendNonNull(
      const BaseVector& vector,
      vector_size_t index,
      exec::HashStringAllocator* allocator);

  void prepareAppend(exec::HashStringAllocator* allocator);

  // Writes lastNulls_ word to the 'nulls' block.
  void writeLastNulls(exec::HashStringAllocator* allocator);

  // 'Nulls' allocation (potentially multi-part).
  exec::HashStringAllocator::Header* nullsBegin_{nullptr};
  exec::HashStringAllocator::Position nullsCurrent_{nullptr, nullptr};

  // 'Data' allocation (potentially multi-part)
  exec::HashStringAllocator::Header* dataBegin_{nullptr};
  exec::HashStringAllocator::Position dataCurrent_{nullptr, nullptr};

  // Total bytes written.
  uint64_t totalBytes_{0};

  // Number of values added, including nulls.
  uint32_t size_{0};

  // Last nulls word. 'size_ % 64' is the null bit for the next element.
  uint64_t lastNulls_{0};
};

// Extracts values from the ValueList into provided vector.
class ValueListReader {
 public:
  explicit ValueListReader(ValueList& values);

  bool next(BaseVector& output, vector_size_t outputIndex);

 private:
  ValueList& values_;
  ByteStream dataStream_;
  ByteStream nullsStream_;
  uint64_t nulls_;
  vector_size_t pos_{0};
};

} // namespace facebook::velox::aggregate
