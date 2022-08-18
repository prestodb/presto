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

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Range.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::aggregate {

class AggregationHook : public ValueHook {
 public:
  // Constants for identifying hooks for specialized template instantiations.

  static constexpr Kind kSumFloatToDouble = 1;
  static constexpr Kind kSumDoubleToDouble = 2;
  static constexpr Kind kSumIntegerToBigint = 3;
  static constexpr Kind kSumBigintToBigint = 4;
  static constexpr Kind kBigintMax = 5;
  static constexpr Kind kBigintMin = 6;
  static constexpr Kind kFloatMax = 7;
  static constexpr Kind kFloatMin = 8;
  static constexpr Kind kDoubleMax = 9;
  static constexpr Kind kDoubleMin = 10;

  // Make null behavior known at compile time. This is useful when
  // templating a column decoding loop with a hook.
  static constexpr bool kSkipNulls = true;

  AggregationHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls)
      : offset_(offset),
        nullByte_(nullByte),
        nullMask_(nullMask),
        clearNullMask_(~nullMask_),
        groups_(groups),
        numNulls_(numNulls) {}

  bool acceptsNulls() const override final {
    return false;
  }

  // Fallback implementation of fast path. Prefer defining special
  // cases for all subclasses.
  void addValues(
      const vector_size_t* rows,
      const void* values,
      vector_size_t size,
      uint8_t valueWidth) override {
    auto valuesAsChar = reinterpret_cast<const char*>(values);
    for (auto i = 0; i < size; ++i) {
      addValue(rows[i], valuesAsChar + valueWidth * i);
    }
  }

 protected:
  inline char* findGroup(vector_size_t row) {
    return groups_[row];
  }

  inline bool clearNull(char* group) {
    if (*numNulls_) {
      uint8_t mask = group[nullByte_];
      if (mask & nullMask_) {
        group[nullByte_] = mask & clearNullMask_;
        --*numNulls_;
        return true;
      }
    }
    return false;
  }

  int32_t currentRow_ = 0;
  const int32_t offset_;
  const int32_t nullByte_;
  const uint8_t nullMask_;
  const uint8_t clearNullMask_;
  char* const* const groups_;
  uint64_t* numNulls_;
};

namespace {
template <typename TOutput, typename TInput>
inline void updateSingleValue(TOutput& result, TInput value) {
  if constexpr (
      std::is_same_v<TOutput, double> || std::is_same_v<TOutput, float>) {
    result += value;
  } else {
    result = checkedPlus<TOutput>(result, value);
  }
}
} // namespace

template <typename TValue, typename TAggregate>
class SumHook final : public AggregationHook {
 public:
  SumHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls)
      : AggregationHook(offset, nullByte, nullMask, groups, numNulls) {}

  Kind kind() const override {
    if (std::is_same_v<TAggregate, double>) {
      if (std::is_same_v<TValue, double>) {
        return kSumDoubleToDouble;
      }
      if (std::is_same_v<TValue, float>) {
        return kSumFloatToDouble;
      }
    } else if (std::is_same_v<TAggregate, int64_t>) {
      if (std::is_same_v<TValue, int32_t>) {
        return kSumIntegerToBigint;
      }
      if (std::is_same_v<TValue, int64_t>) {
        return kSumBigintToBigint;
      }
    }
    return kGeneric;
  }

  void addValue(vector_size_t row, const void* value) override {
    auto group = findGroup(row);
    clearNull(group);
    updateSingleValue(
        *reinterpret_cast<TAggregate*>(group + offset_),
        *reinterpret_cast<const TValue*>(value));
  }
};

template <typename TValue, typename TAggregate, typename UpdateSingleValue>
class SimpleCallableHook final : public AggregationHook {
 public:
  SimpleCallableHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls,
      UpdateSingleValue updateSingleValue)
      : AggregationHook(offset, nullByte, nullMask, groups, numNulls),
        updateSingleValue_(updateSingleValue) {}

  Kind kind() const override {
    return kGeneric;
  }

  void addValue(vector_size_t row, const void* value) override {
    auto group = findGroup(row);
    clearNull(group);
    updateSingleValue_(
        *reinterpret_cast<TAggregate*>(group + offset_),
        *reinterpret_cast<const TValue*>(value));
  }

 private:
  UpdateSingleValue updateSingleValue_;
};

template <typename T, bool isMin>
class MinMaxHook final : public AggregationHook {
 public:
  MinMaxHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls)
      : AggregationHook(offset, nullByte, nullMask, groups, numNulls) {}

  Kind kind() const override {
    if (isMin) {
      if (std::is_same_v<T, int64_t>) {
        return kBigintMin;
      }
      if (std::is_same_v<T, float>) {
        return kFloatMin;
      }
      if (std::is_same_v<T, double>) {
        return kDoubleMin;
      }
    } else {
      if (std::is_same_v<T, int64_t>) {
        return kBigintMax;
      }
      if (std::is_same_v<T, float>) {
        return kFloatMax;
      }
      if (std::is_same_v<T, double>) {
        return kDoubleMax;
      }
    }
    return kGeneric;
  }

  void addValue(vector_size_t row, const void* value) override {
    auto group = findGroup(row);
    if (clearNull(group) ||
        (*reinterpret_cast<T*>(group + offset_) >
         *reinterpret_cast<const T*>(value)) == isMin) {
      *reinterpret_cast<T*>(group + offset_) =
          *reinterpret_cast<const T*>(value);
    }
  }
};

} // namespace facebook::velox::aggregate
