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

#include "velox/common/base/IOUtils.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::aggregate::prestosql {

/// A list of non-null values of complex type stored in possibly non-contiguous
/// memory allocated via HashStringAllocator. Provides index-based access to
/// individual elements. Allows removing elements from the end of the list.
///
/// Designed to be used together with F14FastSet or F14FastMap.
///
/// Used in aggregations that deduplicate complex values, e.g. set_agg and
/// set_union.
class AddressableNonNullValueList {
 public:
  struct Entry {
    HashStringAllocator::Position offset;
    size_t size;
    uint64_t hash;
  };

  struct Hash {
    size_t operator()(const Entry& key) const {
      return key.hash;
    }
  };

  struct EqualTo {
    const TypePtr& type;

    bool operator()(const Entry& left, const Entry& right) const {
      return AddressableNonNullValueList::equalTo(left, right, type);
    }
  };

  /// Append a non-null value to the end of the list. Returns 'index' that can
  /// be used to access the value later.
  Entry append(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator);

  /// Append a non-null serialized value to the end of the list.
  /// Returns position that can be used to access the value later.
  HashStringAllocator::Position appendSerialized(
      const StringView& value,
      HashStringAllocator* allocator);

  /// Removes last element. 'position' must be a value returned from the latest
  /// call to 'append'.
  void removeLast(const Entry& entry) {
    currentPosition_ = entry.offset;
    --size_;
  }

  /// Returns number of elements in the list.
  int32_t size() const {
    return size_;
  }

  /// Returns true if elements at 'left' and 'right' are equal.
  static bool
  equalTo(const Entry& left, const Entry& right, const TypePtr& type);

  /// Copies the specified element to 'result[index]'.
  static void
  read(const Entry& position, BaseVector& result, vector_size_t index);

  /// Copies to 'dest' entry.size bytes at position.
  static void readSerialized(const Entry& position, char* dest);

  void free(HashStringAllocator& allocator) {
    if (size_ > 0) {
      allocator.free(firstHeader_);
    }
  }

 private:
  ByteOutputStream initStream(HashStringAllocator* allocator);

  // Memory allocation (potentially multi-part).
  HashStringAllocator::Header* firstHeader_{nullptr};
  HashStringAllocator::Position currentPosition_{nullptr, nullptr};

  // Number of values added.
  uint32_t size_{0};
};

} // namespace facebook::velox::aggregate::prestosql
