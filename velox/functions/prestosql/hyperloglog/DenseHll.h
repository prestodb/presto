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

namespace facebook::velox::aggregate::hll {
class SparseHll;

/// HyperLogLog implementation using dense storage layout.
/// The number of bits to use as bucket (indexBitLength) is specified by the
/// user and must be in [4, 16] range. More bits provides higher accuracy, but
/// require more memory.
///
/// Memory usage: 2 ^ (indexBitLength - 1) bytes. 2KB for indexBitLength of 12
/// which provides max standard error of 0.023.
class DenseHll {
 public:
  DenseHll(int8_t indexBitLength, HashStringAllocator* allocator);

  DenseHll(const char* serialized, HashStringAllocator* allocator);

  /// Creates an uninitialized instance that doesn't allcate any significant
  /// memory. The caller must call initialize before using the HLL.
  explicit DenseHll(HashStringAllocator* allocator)
      : deltas_{StlAllocator<int8_t>(allocator)},
        overflowBuckets_{StlAllocator<uint16_t>(allocator)},
        overflowValues_{StlAllocator<int8_t>(allocator)} {}

  /// Allocates memory that can fit 2 ^ indexBitLength buckets.
  void initialize(int8_t indexBitLength);

  int8_t indexBitLength() const {
    return indexBitLength_;
  }

  void insertHash(uint64_t hash);

  /// Inserts pre-computed {bucket, value} pair. These value must be compatible
  /// with computeIndex and computeValue methods called with the indexBitLength
  /// value of this HLL. Used by SparseHll.toDense().
  void insert(int32_t index, int8_t value);

  int64_t cardinality() const;

  static int64_t cardinality(const char* serialized);

  /// Serializes internal state using Presto DenseV2 format.
  void serialize(char* output);

  /// Returns true if 'input' contains Presto DenseV2 format indicator.
  static bool canDeserialize(const char* input);

  /// Returns true if 'input' contains Presto DenseV2 format indicator and the
  /// rest of the data matches HLL format:
  /// 1 byte for version
  /// 1 byte for index bit length, index bit length must be in [4,16]
  /// 1 byte for baseline value
  /// 2^(n-1) bytes for buckets, values in buckets must be in [0,63]
  /// 2 bytes for # overflow buckets
  /// 3 * #overflow buckets bytes for overflow buckets/values
  /// More information here:
  /// https://engineering.fb.com/2018/12/13/data-infrastructure/hyperloglog/
  static bool canDeserialize(const char* input, int size);

  static int8_t deserializeIndexBitLength(const char* input);

  /// Returns the size of the serialized state without serialising.
  int32_t serializedSize() const;

  /// Merges the state of another instance into this one.
  /// The other HLL Must have the same value of indexBitLength.
  void mergeWith(const DenseHll& other);

  void mergeWith(const char* serialized);

  /// Returns an estimate of memory usage for DenseHll instance with the
  /// specified number of bits per bucket.
  static int32_t estimateInMemorySize(int8_t indexBitLength);

 private:
  int8_t getDelta(int32_t index) const;

  void setDelta(int32_t index, int8_t value);

  int8_t getOverflow(int32_t index) const;

  int findOverflowEntry(int32_t index) const;

  void adjustBaselineIfNeeded();

  void sortOverflows();

  int8_t updateOverflow(int32_t index, int overflowEntry, int8_t delta);

  void addOverflow(int32_t index, int8_t overflow);

  void removeOverflow(int overflowEntry);

  void mergeWith(
      int8_t otherBaseline,
      const int8_t* otherDeltas,
      int16_t otherOverflows,
      const uint16_t* otherOverflowBuckets,
      const int8_t* otherOverflowValues);

  /// Number of first bits of the hash to calculate buckets from.
  int8_t indexBitLength_;

  /// The baseline value for the deltas.
  int8_t baseline_{0};

  /// Number of zero deltas.
  int32_t baselineCount_;

  /// Per-bucket values represented as deltas from the baseline_. Each entry
  /// stores 2 values, 4 bits each. The maximum value that can be stored is 15.
  /// Larger values are stored in a separate overflow list.
  std::vector<int8_t, StlAllocator<int8_t>> deltas_;

  /// Number of overflowing values, e.g. values where delta from baseline is
  /// greater than 15.
  int16_t overflows_{0};

  /// List of buckets with overflowing values.
  std::vector<uint16_t, StlAllocator<uint16_t>> overflowBuckets_;

  /// Overflowing values stored as deltas from the deltas: value - 15 -
  /// baseline.
  std::vector<int8_t, StlAllocator<int8_t>> overflowValues_;
};
} // namespace facebook::velox::aggregate::hll
