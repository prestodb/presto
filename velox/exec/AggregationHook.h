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

#include "folly/CPortability.h"

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Range.h"
#include "velox/type/FloatingPointUtil.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::aggregate {

class AggregationHook : public ValueHook {
 public:
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

template <typename TAggregate, bool Overflow = false>
class SumHook final : public AggregationHook {
 public:
  SumHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls)
      : AggregationHook(offset, nullByte, nullMask, groups, numNulls) {}

  Kind kind() const final {
    if (std::is_same_v<TAggregate, double>) {
      return kDoubleSum;
    } else if (std::is_same_v<TAggregate, int64_t>) {
      return Overflow ? kBigintSumOverflow : kBigintSum;
    }
    return kGeneric;
  }

  void addValue(vector_size_t row, int64_t value) final {
    if constexpr (std::is_integral_v<TAggregate>) {
      addValueImpl(row, value);
    } else {
      VELOX_UNREACHABLE();
    }
  }

  void addValue(vector_size_t row, float value) final {
    if constexpr (std::is_floating_point_v<TAggregate>) {
      addValueImpl(row, value);
    } else {
      VELOX_UNREACHABLE();
    }
  }

  void addValue(vector_size_t row, double value) final {
    if constexpr (std::is_floating_point_v<TAggregate>) {
      addValueImpl(row, value);
    } else {
      VELOX_UNREACHABLE();
    }
  }

  // Spark's sum function sets Overflow to true and intentionally let the result
  // value be automatically wrapped around when integer overflow happens. Hence,
  // disable undefined behavior sanitizer to not fail on signed integer
  // overflow.  The disablement of the sanitizer only affects SumHook that is
  // used for pushdown of sum aggregation functions. It doesn't affect the
  // Presto's sum function that sets Overflow to false because overflow is
  // handled explicitly in checkedPlus.
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
  FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("signed-integer-overflow")
#endif
  static void add(TAggregate& result, TAggregate value) {
    if constexpr (
        (std::is_same_v<TAggregate, int64_t> && Overflow) ||
        std::is_same_v<TAggregate, double> ||
        std::is_same_v<TAggregate, float>) {
      result += value;
    } else {
      result = checkedPlus<TAggregate>(result, value);
    }
  }

 private:
  template <typename T>
  void addValueImpl(vector_size_t row, T value) {
    auto group = findGroup(row);
    clearNull(group);
    add(*reinterpret_cast<TAggregate*>(group + offset_), value);
  }
};

template <typename TAggregate, typename UpdateSingleValue>
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

  Kind kind() const final {
    return kGeneric;
  }

  void addValue(vector_size_t row, int64_t value) final {
    addValueImpl(row, value);
  }

  void addValue(vector_size_t row, float value) final {
    addValueImpl(row, value);
  }

  void addValue(vector_size_t row, double value) final {
    addValueImpl(row, value);
  }

 private:
  template <typename T>
  void addValueImpl(vector_size_t row, T value) {
    auto group = findGroup(row);
    clearNull(group);
    updateSingleValue_(*reinterpret_cast<TAggregate*>(group + offset_), value);
  }

  UpdateSingleValue updateSingleValue_;
};

template <typename TAggregate, bool isMin>
class MinMaxHook final : public AggregationHook {
 public:
  MinMaxHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls)
      : AggregationHook(offset, nullByte, nullMask, groups, numNulls) {}

  Kind kind() const final {
    if (std::is_same_v<TAggregate, int64_t>) {
      return isMin ? kBigintMin : kBigintMax;
    }
    if (std::is_same_v<TAggregate, float> ||
        std::is_same_v<TAggregate, double>) {
      return isMin ? kFloatingPointMin : kFloatingPointMax;
    }
    return kGeneric;
  }

  void addValue(vector_size_t row, int64_t value) final {
    if constexpr (std::is_integral_v<TAggregate>) {
      addValueImpl(row, value);
    } else {
      VELOX_UNREACHABLE();
    }
  }

  void addValue(vector_size_t row, float value) final {
    if constexpr (std::is_floating_point_v<TAggregate>) {
      addValueImpl(row, value);
    } else {
      VELOX_UNREACHABLE();
    }
  }

  void addValue(vector_size_t row, double value) final {
    if constexpr (std::is_floating_point_v<TAggregate>) {
      addValueImpl(row, value);
    } else {
      VELOX_UNREACHABLE();
    }
  }

 private:
  template <typename T>
  void addValueImpl(vector_size_t row, T value) {
    auto group = findGroup(row);
    auto* currPtr = reinterpret_cast<TAggregate*>(group + offset_);
    if constexpr (std::is_floating_point_v<TAggregate>) {
      static const auto isGreater =
          util::floating_point::NaNAwareGreaterThan<TAggregate>{};
      if (clearNull(group) || isGreater(*currPtr, value) == isMin) {
        *currPtr = value;
      }
    } else {
      if (clearNull(group) || (*currPtr > value) == isMin) {
        *currPtr = value;
      }
    }
  }
};

} // namespace facebook::velox::aggregate
