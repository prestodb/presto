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

#include <vector>
#include "velox/common/base/BitUtil.h"
#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

/// Set of OperandId . Uses the id() as an index into a bitmap.
class OperandSet {
 public:
  /// True if id of 'object' is in 'this'.
  bool contains(int32_t id) const {
    return id < bits_.size() * 64 && velox::bits::isBitSet(bits_.data(), id);
  }

  bool operator==(const OperandSet& other) const;

  /// Returns hash code depending on set bits. The allocated size does not
  /// affect the hash, only set bits. Works with == to allow use as a key of
  /// hash table.
  size_t hash() const {
    size_t result = 0;
    for (auto word : bits_) {
      if (word) {
        result ^= word;
      }
    }
    return result * 121 ^ (result >> 9);
  }

  // True if no members.
  bool empty() const {
    for (auto word : bits_) {
      if (word) {
        return false;
      }
    }
    return true;
  }

  /// Inserts id of 'object'.
  void add(OperandId id) {
    ensureSize(id);
    velox::bits::setBit(bits_.data(), id);
  }

  /// Returns the number of ids below 'id'.
  int32_t ordinal(int32_t id) {
    return bits::countBits(bits_.data(), 0, id);
  }

  /// Returns true if 'this' is a subset of 'super'.
  bool isSubset(const OperandSet& super) const;

  /// Erases id of 'object'.
  void erase(OperandId id) {
    if (id < bits_.size() * 64) {
      velox::bits::clearBit(bits_.data(), id);
    }
  }

  void except(const OperandSet& other) {
    velox::bits::forEachSetBit(
        other.bits_.data(), 0, other.bits_.size() * 64, [&](auto id) {
          if (id < bits_.size() * 64) {
            velox::bits::clearBit(bits_.data(), id);
          }
        });
  }

  /// Adds all ids in 'other'.
  void unionSet(const OperandSet& other);

  /// Erases all ids not in 'other'.
  void intersect(const OperandSet& other);

  /// Applies 'func' to each object in 'this'.
  template <typename Func>
  void forEach(Func func) const {
    velox::bits::forEachSetBit(
        bits_.data(), 0, bits_.size() * 64, [&](auto i) { func(i); });
  }

  size_t size() const {
    return bits::countBits(
        bits_.data(), 0, sizeof(bits_[0]) * 8 * bits_.size());
  }

  std::string toString() const;

 private:
  void ensureSize(int32_t id) {
    ensureWords(velox::bits::nwords(id + 1));
  }

  void ensureWords(int32_t size) {
    if (bits_.size() < size) {
      bits_.resize(size);
    }
  }

  // A one bit corresponds to the id of each member.
  std::vector<uint64_t> bits_;
};

template <typename V>
inline bool isZero(const V& bits, size_t begin, size_t end) {
  for (size_t i = begin; i < end; ++i) {
    if (bits[i]) {
      return false;
    }
  }
  return true;
}

inline bool OperandSet::operator==(const OperandSet& other) const {
  // The sets are equal if they have the same bits set. Trailing words of zeros
  // do not count.
  auto l1 = bits_.size();
  auto l2 = other.bits_.size();
  for (unsigned i = 0; i < l1 && i < l2; ++i) {
    if (bits_[i] != other.bits_[i]) {
      return false;
    }
  }
  if (l1 < l2) {
    return isZero(other.bits_, l1, l2);
  }
  if (l2 < l1) {
    return isZero(bits_, l2, l1);
  }
  return true;
}

inline bool OperandSet::isSubset(const OperandSet& super) const {
  auto l1 = bits_.size();
  auto l2 = super.bits_.size();
  for (unsigned i = 0; i < l1 && i < l2; ++i) {
    if (bits_[i] & ~super.bits_[i]) {
      return false;
    }
  }
  if (l2 < l1) {
    return isZero(bits_, l2, l1);
  }
  return true;
}

struct OperandSetHasher {
  size_t operator()(const OperandSet& set) const {
    return set.hash();
  }
};

struct OperandSetComparer {
  bool operator()(const OperandSet& left, const OperandSet& right) const {
    return left == right;
  }
};
} // namespace facebook::velox::wave
