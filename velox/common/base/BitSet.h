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

#include <cstdint>
#include <vector>

#include "velox/common/base/BitUtil.h"

namespace facebook::velox {
/// Dynamic size dense bit set that keeps track of maximum set bit.
class BitSet {
 public:
  /// Constructs a bitSet. 'min' is the lowest possible member of the set.
  /// Values below this are not present and inserting these is a no-op. 'min' is
  /// used when using this as an IN predicate filter.
  explicit BitSet(int64_t min) : min_(min) {}

  void insert(int64_t index) {
    int64_t bit = index - min_;
    if (bit < 0) {
      return;
    }
    if (bit < lastSetBit_) {
      bits::setBit(bits_.data(), bit, true);
      return;
    }
    lastSetBit_ = bit;
    if (lastSetBit_ >= bits_.size() * 64) {
      bits_.resize(std::max<int64_t>(bits_.size() * 2, bits::nwords(bit + 1)));
    }
    bits::setBit(bits_.data(), bit, true);
  }

  bool contains(uint32_t index) const {
    uint64_t bit = index - min_;
    if (bit >= bits_.size() * 64) {
      // If index was < min_, bit will have wrapped around and will be >
      // size * 64.
      return false;
    }
    return bits::isBitSet(bits_.data(), bit);
  }

  /// Returns the largest element of the set or 'min_ - 1' if empty.
  int64_t max() const {
    return lastSetBit_ + min_;
  }

  const uint64_t* bits() const {
    return bits_.data();
  }

 private:
  const int64_t min_;
  std::vector<uint64_t> bits_;
  int64_t lastSetBit_ = -1;
};

} // namespace facebook::velox
