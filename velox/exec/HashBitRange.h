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

#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

// Describes a bit range inside a 64 bit hash number for use in
// partitioning data.
class HashBitRange {
 public:
  HashBitRange(uint8_t begin, uint8_t end)
      : begin_(begin), end_(end), fieldMask_(bits::lowMask(end - begin)) {
    VELOX_CHECK_LE(begin_, end_);
    VELOX_CHECK_LE(end_, 64);
  }
  HashBitRange() : HashBitRange(0, 0) {}

  int32_t partition(uint64_t hash, int32_t numPartitions) const {
    int32_t number = (hash >> begin_) & fieldMask_;
    return number < numPartitions ? number : -1;
  }

  int32_t partition(uint64_t hash) const {
    return (hash >> begin_) & fieldMask_;
  }

  uint8_t begin() const {
    return begin_;
  }

  uint8_t end() const {
    return end_;
  }

  uint8_t numBits() const {
    return end_ - begin_;
  }

  int32_t numPartitions() const {
    return 1 << numBits();
  }

  inline bool operator==(const HashBitRange& other) const {
    return std::tie(begin_, end_) == std::tie(other.begin_, other.end_);
  }

  inline bool operator!=(const HashBitRange& other) const {
    return !(*this == other);
  }

 private:
  // Low bit number of hash number bit range.
  uint8_t begin_;

  // Bit number of first bit above the hash number bit range.
  uint8_t end_;

  uint64_t fieldMask_;
};
} // namespace facebook::velox::exec
