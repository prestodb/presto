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
#include "velox/common/hyperloglog/DenseHll.h"
#include "velox/common/memory/HashStringAllocator.h"

namespace facebook::velox::common::hll {
/// HyperLogLog implementation using sparse storage layout.
/// It uses 26-bit buckets and provides high accuracy for low cardinalities.
/// Memory usage: 4 bytes for each observed bucket.
class SparseHll {
 public:
  explicit SparseHll(HashStringAllocator* allocator)
      : entries_{StlAllocator<uint32_t>(allocator)} {}

  SparseHll(const char* serialized, HashStringAllocator* allocator);

  void setSoftMemoryLimit(uint32_t softMemoryLimit) {
    softNumEntriesLimit_ = softMemoryLimit / 4;
  }

  /// Returns true if soft memory limit has been reached. False, otherwise.
  bool insertHash(uint64_t hash);

  int64_t cardinality() const;

  /// Returns cardinality estimate from the specified serialized digest.
  static int64_t cardinality(const char* serialized);

  /// Serializes internal state using Presto SparseV2 format.
  void serialize(int8_t indexBitLength, char* output) const;

  static std::string serializeEmpty(int8_t indexBitLength);

  /// Returns true if 'input' has Presto SparseV2 format.
  static bool canDeserialize(const char* input);

  /// Returns the size of the serialized state without serialising.
  int32_t serializedSize() const;

  /// Merges the state of another instance into this one.
  void mergeWith(const SparseHll& other);

  /// Merges the state of another instance (in serialized form) into this one.
  void mergeWith(const char* serialized);

  /// Merges state into provided instance of DenseHll.
  void toDense(DenseHll& denseHll) const;

  /// Returns current memory usage.
  int32_t inMemorySize() const;

  // Clear accumulated state and release memory. Used to free up memory after
  // converting to dense layout.
  void reset() {
    entries_.clear();
    entries_.shrink_to_fit();
  }

  // For testing: sanity checks internal state.
  void verify() const;

 private:
  void mergeWith(size_t otherSize, const uint32_t* otherEntries);

  /// A list of observed buckets. Each entry is a 32 bit integer encoding 26-bit
  /// bucket and 6-bit value (number of zeros in the input hash after the bucket
  /// + 1).
  std::vector<uint32_t, StlAllocator<uint32_t>> entries_;

  /// Number of entries that can be stored before reaching soft memory limit.
  uint32_t softNumEntriesLimit_{0};
};
} // namespace facebook::velox::common::hll
