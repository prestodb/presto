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

#include <folly/container/F14Set.h>

#include <velox/type/Filter.h>
#include "velox/common/base/RawVector.h"
#include "velox/exec/Operator.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

// Represents a unique scalar or string value and its mapping to a
// small integer range for use as part of a normalized key or array
// index.
class UniqueValue {
 public:
  explicit UniqueValue(int64_t value) {
    size_ = sizeof(int64_t);
    data_ = value;
  }

  explicit UniqueValue(const char* value, uint32_t size) {
    size_ = size;
    data_ = 0;
    if (size <= sizeof(data_)) {
      memcpy(&data_, value, size);
    } else {
      data_ = reinterpret_cast<int64_t>(value);
    }
  }

  explicit UniqueValue(Date value) {
    // The number of valid bytes of Date stored in data_ is
    // (int64_t)value.days().
    size_ = sizeof(int64_t);
    data_ = value.days();
  }

  explicit UniqueValue(IntervalDayTime value) {
    // The number of valid bytes of IntervalDayTime stored in data_ is
    // (int64_t)value.milliseconds().
    size_ = sizeof(int64_t);
    data_ = value.milliseconds();
  }

  uint32_t size() const {
    return size_;
  }

  uint32_t id() const {
    return id_;
  }

  void setId(uint32_t id) {
    id_ = id;
  }

  int64_t data() const {
    return data_;
  }

  void setData(int64_t data) {
    data_ = data;
  }

 private:
  uint64_t data_;
  uint32_t size_;
  uint32_t id_;
};

struct UniqueValueHasher {
  size_t operator()(const UniqueValue& value) const {
    auto size = value.size();
    if (size <= sizeof(int64_t)) {
      return simd::crc32U64(0, value.data());
    }

    uint32_t hash = 0;
    auto data = reinterpret_cast<const uint64_t*>(value.data());

    size_t wordIndex = 0;
    auto numFullWords = size / 8;
    for (; wordIndex < numFullWords; ++wordIndex) {
      hash = simd::crc32U64(hash, *(data + wordIndex));
    }

    auto numBytesRemaining = size - wordIndex * 8;
    if (numBytesRemaining > 0) {
      auto lastWord = bits::loadPartialWord(
          reinterpret_cast<const uint8_t*>(data + wordIndex),
          numBytesRemaining);
      hash = simd::crc32U64(hash, lastWord);
    }

    return hash;
  }
};

struct UniqueValueComparer {
  bool operator()(const UniqueValue& left, const UniqueValue& right) const {
    auto size = left.size();
    if (size != right.size()) {
      return false;
    }
    if (size <= sizeof(int64_t)) {
      return left.data() == right.data();
    }
    return memcmp(
               reinterpret_cast<const char*>(left.data()),
               reinterpret_cast<const char*>(right.data()),
               size) == 0;
  }
};

class VectorHasher {
 public:
  static constexpr uint64_t kUnmappable = ~0UL;
  // Largest range that can be a part of a normalized key. 59 bits,
  // corresponds to 7 byte strings represented as numbers (56 bits of
  // data and 3 of length).
  static constexpr int64_t kMaxRange = ~0UL >> 5;
  static constexpr uint64_t kRangeTooLarge = ~0UL;
  // Stop counting distinct values after this many and revert to regular hash.
  static constexpr int32_t kMaxDistinct = 100'000;

  // Indicates reserving kMaxDistinct possible values when supplied as
  // reservePct to enableValueIds().
  static constexpr int32_t kNoLimit = -1;

  VectorHasher(TypePtr type, column_index_t channel)
      : channel_(channel), type_(std::move(type)), typeKind_(type_->kind()) {
    if (typeKind_ == TypeKind::BOOLEAN) {
      // We do not need samples to know the cardinality or limits of a bool
      // vector.
      hasRange_ = true;
      min_ = 0;
      max_ = 1;
    }
  }

  static std::unique_ptr<VectorHasher> create(
      TypePtr type,
      column_index_t channel) {
    return std::make_unique<VectorHasher>(std::move(type), channel);
  }

  column_index_t channel() const {
    return channel_;
  }

  const TypePtr& type() const {
    return type_;
  }

  TypeKind typeKind() const {
    return typeKind_;
  }

  static constexpr uint64_t kNullHash = BaseVector::kNullHash;

  // Decodes the 'vector' in preparation for calling hash() or
  // computeValueIds(). The decoded vector can be accessed via decodedVector()
  // getter.
  void decode(const BaseVector& vector, const SelectivityVector& rows) {
    decoded_.decode(vector, rows);
  }

  DecodedVector& decodedVector() {
    return decoded_;
  }

  // Computes a hash for 'rows' in the vector previously decoded via decode()
  // call and stores it in 'result'. If 'mix' is true, mixes the hash with
  // existing value in 'result'.
  void
  hash(const SelectivityVector& rows, bool mix, raw_vector<uint64_t>& result);

  // Computes a hash for 'rows' using precomputedHash_ (just like from a const
  // vector) and stores it in 'result'.
  // If 'mix' is true, mixes the hash with existing value in 'result'.
  void hashPrecomputed(
      const SelectivityVector& rows,
      bool mix,
      raw_vector<uint64_t>& result) const;

  // Precompute hash of a given single value (vector has just one row) into
  // precomputedHash_. Used for constant partition keys.
  void precompute(const BaseVector& value);

  // Computes a normalized key for 'rows' in the vector previously decoded via
  // decode() call and stores this in 'result'. If this is not the first hasher
  // with normalized keys, updates the partially computed normalized key in
  // 'result'. Returns true if all the values could be mapped to the
  // normalized key range. If some values could not be mapped
  // the statistics are updated to reflect the new values. This
  // behavior corresponds to group by, where we must rehash if all the
  // new keys could not be represented.
  bool computeValueIds(
      const SelectivityVector& rows,
      raw_vector<uint64_t>& result);

  // Same as computeValueIds, but takes input stored row-wise.
  bool computeValueIdsForRows(
      char** groups,
      int32_t numGroups,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      raw_vector<uint64_t>& result);

  struct ScratchMemory {
    DecodedVector decoded;
    raw_vector<uint64_t> hashes;
  };

  // Updates the value id in 'result' for 'rows' in 'values'. If some value does
  // not have an id, result is not modified at the position and the position is
  // removed from 'rows'. This behavior corresponds to hash join probe, where we
  // have a miss if any of the keys has a value that is not represented.
  //
  // This method can be called concurrently from multiple threads. To allow for
  // that the caller must provide 'scratchMemory'. 'noNulls' means that the
  // positions in 'rows' are not checked for null values.
  void lookupValueIds(
      const BaseVector& values,
      SelectivityVector& rows,
      ScratchMemory& scratchMemory,
      raw_vector<uint64_t>& result,
      bool noNulls = true) const;

  // Returns true if either range or distinct values have not overflowed.
  bool mayUseValueIds() const {
    return hasRange_ || !distinctOverflow_;
  }

  // Returns an instance of the filter corresponding to a set of unique values.
  // Returns null if distinctOverflow_ is true.
  std::unique_ptr<common::Filter> getFilter(bool nullAllowed) const;

  void resetStats() {
    uniqueValues_.clear();
    uniqueValuesStorage_.clear();
  }

  // Sets 'this' to range mode and adds 'reservePct' values to the
  // range, half below and half above, staying within bounds of the
  // data type. In this mode, hashed values become offsets from the
  // lower end of the padded range times 'multiplier'. Returns
  // 'multiplier' times the number of distinct values 'this' can
  // produce. Does not accept kNoLimit for 'reservePct'.
  uint64_t enableValueRange(uint64_t multiplier, int32_t reservePct);

  // Sets this to 'value ids' mode, where each distinct value has an
  // integer id times 'multiplier'. Leaves 'reservePct' % values at
  // the end of the distinct ids range. Returns 'multiplier' times the
  // number of distinct values reserved. 'reservePct' = kNoLimit means
  // that we reserve kMaxDistinct distinct values.
  uint64_t enableValueIds(uint64_t multiplier, int32_t reservePct);

  // Returns the number of distinct values in range and distinct-values modes.
  // kRangeTooLarge means that the mode is not applicable. If 'reservePct' is
  // non-zero, pads the range with 'reservePct' % extra values. For 'asRange'
  // half is added below and half above the range, however not exceeding limits
  // of the data type. For 'asDistinct' the values are added to the end of the
  // range of ids.
  void
  cardinality(int32_t reservePct, uint64_t& asRange, uint64_t& asDistincts);

  void analyze(
      char** groups,
      int32_t numGroups,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask);

  bool isRange() const {
    return isRange_;
  }

  static bool typeKindSupportsValueIds(TypeKind kind) {
    switch (kind) {
      case TypeKind::BOOLEAN:
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
      case TypeKind::DATE:
      case TypeKind::INTERVAL_DAY_TIME:
        return true;
      default:
        return false;
    }
  }

  // Merges the value ids information of 'other' into 'this'. Ranges
  // and distinct values are unioned.
  void merge(const VectorHasher& other);

  // true if no values have been added.
  bool empty() const {
    return !hasRange_ && uniqueValues_.empty();
  }

  std::string toString() const;

 private:
  static constexpr uint32_t kStringASRangeMaxSize = 7;
  static constexpr uint32_t kStringBufferUnitSize = 1024;
  static constexpr uint64_t kMaxDistinctStringsBytes = 1 << 20;

  // Maps a binary string of up to 7 bytes to int64_t. Each size maps
  // to a different numeric range, so leading zeros are considered.
  static inline int64_t stringAsNumber(const char* data, int32_t size) {
    int64_t word =
        bits::loadPartialWord(reinterpret_cast<const uint8_t*>(data), size);
    return size == 0 ? word : word + (1L << (size * 8));
  }

  template <typename T>
  inline int64_t toInt64(T value) const {
    return value;
  }

  // Sets the data statistics from 'other'. Does not set the mapping mode.
  void copyStatsFrom(const VectorHasher& other);

  template <TypeKind Kind>
  bool makeValueIds(const SelectivityVector& rows, uint64_t* result);

  template <typename T>
  bool makeValueIdsFlatNoNulls(const SelectivityVector& rows, uint64_t* result);

  template <typename T>
  bool makeValueIdsFlatWithNulls(
      const SelectivityVector& rows,
      uint64_t* result);

  template <typename T, bool mayHaveNulls>
  bool makeValueIdsDecoded(const SelectivityVector& rows, uint64_t* result);

  template <TypeKind Kind>
  bool makeValueIdsForRows(
      char** groups,
      int32_t numGroups,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      uint64_t* result) {
    using T = typename TypeTraits<Kind>::NativeType;
    for (int32_t i = 0; i < numGroups; ++i) {
      if (isNullAt(groups[i], nullByte, nullMask)) {
        if (multiplier_ == 1) {
          result[i] = 0;
        }
      } else {
        auto id = valueId<T>(valueAt<T>(groups[i], offset));
        if (id == kUnmappable) {
          return false;
        }
        result[i] =
            multiplier_ == 1 ? toInt64(id) : result[i] + multiplier_ * id;
      }
    }
    return true;
  }

  template <TypeKind Kind>
  void lookupValueIdsTyped(
      const DecodedVector& decoded,
      SelectivityVector& rows,
      raw_vector<uint64_t>& hashes,
      uint64_t* result,
      bool noNulls) const;

  // Fast path for range mapping of int64/int32 keys.
  template <typename T>
  void lookupIdsRangeSimd(
      const DecodedVector& decoded,
      SelectivityVector& rows,
      uint64_t* result) const;

  template <TypeKind Kind>
  void analyzeTyped(
      char** groups,
      int32_t numGroups,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask) {
    using T = typename TypeTraits<Kind>::NativeType;
    for (auto i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      if (isNullAt(group, nullByte, nullMask)) {
        continue;
      }
      analyzeValue(valueAt<T>(group, offset));
    }
  }

  template <typename T>
  void analyzeValue(T value) {
    auto normalized = toInt64(value);
    if (!rangeOverflow_) {
      updateRange(normalized);
    }
    if (!distinctOverflow_) {
      UniqueValue unique(normalized);
      unique.setId(uniqueValues_.size() + 1);
      if (uniqueValues_.insert(unique).second) {
        if (uniqueValues_.size() > kMaxDistinct) {
          distinctOverflow_ = true;
        }
      }
    }
  }

  template <typename T>
  bool tryMapToRangeSimd(
      const T* values,
      const SelectivityVector& rows,
      uint64_t* result);

  template <typename T>
  bool tryMapToRange(
      const T* values,
      const SelectivityVector& rows,
      uint64_t* result) {
    VELOX_DCHECK(isRange_);
    if (!isRange_) {
      return false;
    }

    if constexpr (
        std::is_same_v<T, std::int64_t> || std::is_same_v<T, std::int32_t> ||
        std::is_same_v<T, std::int16_t>) {
      if (rows.isAllSelected() && multiplier_ == 1) {
        return tryMapToRangeSimd(values, rows, result);
      }
    }

    bool inRange = true;
    rows.template testSelected([&](vector_size_t row) {
      auto int64Value = toInt64(values[row]);
      if (int64Value > max_ || int64Value < min_) {
        inRange = false;
        return false;
      }
      auto hash = int64Value - min_ + 1;
      result[row] = multiplier_ == 1 ? hash : result[row] + multiplier_ * hash;
      return true;
    });

    return inRange;
  }

  template <typename T>
  uint64_t valueId(T value) {
    auto int64Value = toInt64(value);
    if (isRange_) {
      if (int64Value > max_ || int64Value < min_) {
        return kUnmappable;
      }
      return int64Value - min_ + 1;
    }

    UniqueValue unique(value);
    unique.setId(uniqueValues_.size() + 1);
    auto pair = uniqueValues_.insert(unique);
    if (!pair.second) {
      return pair.first->id();
    }
    updateRange(int64Value);
    if (uniqueValues_.size() >= rangeSize_) {
      return kUnmappable;
    }
    return unique.id();
  }

  template <typename T>
  uint64_t lookupValueId(T value) const {
    auto int64Value = toInt64(value);
    if (isRange_) {
      if (int64Value > max_ || int64Value < min_) {
        return kUnmappable;
      }
      return int64Value - min_ + 1;
    }
    UniqueValue unique(value);
    auto iter = uniqueValues_.find(unique);
    if (iter != uniqueValues_.end()) {
      return iter->id();
    }
    return kUnmappable;
  }

  void updateRange(int64_t value) {
    if (hasRange_) {
      if (value < min_) {
        min_ = value;
      } else if (value > max_) {
        max_ = value;
      }
    } else {
      hasRange_ = true;
      min_ = max_ = value;
    }
  }

  void copyStringToLocal(const UniqueValue* unique);

  static inline bool
  isNullAt(const char* group, int32_t nullByte, uint8_t nullMask) {
    return (group[nullByte] & nullMask) != 0;
  }

  template <typename T>
  static inline T valueAt(const char* group, int32_t offset) {
    return *reinterpret_cast<const T*>(group + offset);
  }

  template <TypeKind Kind>
  void hashValues(const SelectivityVector& rows, bool mix, uint64_t* result);

  const column_index_t channel_;
  const TypePtr type_;
  const TypeKind typeKind_;

  DecodedVector decoded_;
  raw_vector<uint64_t> cachedHashes_;

  // Single precomputed hash for constant partition keys.
  uint64_t precomputedHash_{0};

  // Members for fast map to int domain for array/normalized key.
  // Maximum integer mapping. If distinct count exceeds this,
  // array/normalized key mapping fails.
  uint64_t rangeSize_ = 0;

  // Multiply int mapping by this before adding it to array index/normalized
  // key.
  uint64_t multiplier_ = 1;

  // True if the mapping is simply value - min_.
  bool isRange_ = false;

  // True if 'min_' and 'max_' are initialized.
  bool hasRange_ = false;

  // True when range or distinct mapping is not possible or practical.
  bool rangeOverflow_ = false;
  bool distinctOverflow_ = false;

  // Bounds of the range if 'isRange_' is true.
  int64_t min_ = 1;
  int64_t max_ = 0;
  // Table for mapping distinct values to small ints.
  folly::F14FastSet<UniqueValue, UniqueValueHasher, UniqueValueComparer>
      uniqueValues_;

  // Memory for unique string values.
  std::vector<std::string> uniqueValuesStorage_;
  uint64_t distinctStringsBytes_ = 0;
};

template <>
inline int64_t VectorHasher::toInt64(Date value) const {
  return value.days();
}

template <>
inline int64_t VectorHasher::toInt64(IntervalDayTime value) const {
  return value.milliseconds();
}

template <>
bool VectorHasher::makeValueIdsForRows<TypeKind::VARCHAR>(
    char** groups,
    int32_t numGroups,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* result);

template <>
void VectorHasher::analyzeValue(StringView value);

template <>
inline bool VectorHasher::tryMapToRange(
    const StringView* /*values*/,
    const SelectivityVector& /*rows*/,
    uint64_t* /*result*/) {
  return false;
}

template <>
inline uint64_t VectorHasher::valueId(StringView value) {
  auto size = value.size();
  auto data = value.data();
  if (isRange_) {
    if (size > kStringASRangeMaxSize) {
      return kUnmappable;
    }
    int64_t number = stringAsNumber(data, size);
    if (number < min_ || number > max_) {
      return kUnmappable;
    }
    return number - min_ + 1;
  }

  UniqueValue unique(data, size);
  unique.setId(uniqueValues_.size() + 1);
  auto pair = uniqueValues_.insert(unique);
  if (!pair.second) {
    return pair.first->id();
  }
  copyStringToLocal(&*pair.first);
  if (!rangeOverflow_) {
    if (size > kStringASRangeMaxSize) {
      rangeOverflow_ = true;
    } else {
      updateRange(stringAsNumber(data, size));
    }
  }
  if (uniqueValues_.size() >= rangeSize_ || distinctOverflow_) {
    return kUnmappable;
  }
  return unique.id();
}

template <>
inline uint64_t VectorHasher::lookupValueId(StringView value) const {
  auto size = value.size();
  auto data = value.data();
  if (isRange_) {
    if (size > kStringASRangeMaxSize) {
      return kUnmappable;
    }
    int64_t number = stringAsNumber(data, size);
    if (number < min_ || number > max_) {
      return kUnmappable;
    }
    return number - min_ + 1;
  }
  UniqueValue unique(data, size);
  auto iter = uniqueValues_.find(unique);
  if (iter != uniqueValues_.end()) {
    return iter->id();
  }
  return kUnmappable;
}

template <>
inline uint64_t VectorHasher::valueId(bool value) {
  return value ? 2 : 1;
}

template <>
inline bool VectorHasher::tryMapToRange(
    const bool* values,
    const SelectivityVector& rows,
    uint64_t* result) {
  rows.template applyToSelected([&](vector_size_t row) {
    auto hash = valueId(values[row]);
    result[row] = multiplier_ == 1 ? hash : result[row] + multiplier_ * hash;
  });
  return true;
}

template <>
bool VectorHasher::tryMapToRange(
    const StringView* /*values*/,
    const SelectivityVector& /*rows*/,
    uint64_t* /*result*/);

template <>
bool VectorHasher::makeValueIdsFlatNoNulls<bool>(
    const SelectivityVector& rows,
    uint64_t* result);

template <>
bool VectorHasher::makeValueIdsFlatWithNulls<bool>(
    const SelectivityVector& rows,
    uint64_t* result);

template <>
bool VectorHasher::makeValueIdsDecoded<bool, true>(
    const SelectivityVector& rows,
    uint64_t* result);

template <>
bool VectorHasher::makeValueIdsDecoded<bool, false>(
    const SelectivityVector& rows,
    uint64_t* result);

} // namespace facebook::velox::exec

#include "velox/exec/VectorHasher-inl.h"
