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

#include "velox/common/base/CompareFlags.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::functions {

namespace {
template <typename T>
struct TimestampWithTimezoneComparisonSupport {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  /// Timestamp with time zone values contain the milliseconds
  /// already adjusted to UTC/GMT.
  /// That means they are already normalized on UTC. We can directly compare
  /// the UTC seconds and don't need to convert anything.
  FOLLY_ALWAYS_INLINE
  int64_t toTimestampMillis(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto inputTimeStamp = unpackTimestampUtc(timestampWithTimezone);
    return inputTimeStamp.toMillis();
  }
};

} // namespace

#define VELOX_GEN_BINARY_EXPR(Name, Expr, TResult)                 \
  template <typename T>                                            \
  struct Name : public TimestampWithTimezoneComparisonSupport<T> { \
    VELOX_DEFINE_FUNCTION_TYPES(T);                                \
    template <typename TInput>                                     \
    FOLLY_ALWAYS_INLINE void                                       \
    call(TResult& result, const TInput& lhs, const TInput& rhs) {  \
      result = (Expr);                                             \
    }                                                              \
  };

#define VELOX_GEN_BINARY_EXPR_TIMESTAMP_WITH_TIME_ZONE(Name, tsExpr, TResult) \
  template <typename T>                                                       \
  struct Name##TimestampWithTimezone                                          \
      : public TimestampWithTimezoneComparisonSupport<T> {                    \
    VELOX_DEFINE_FUNCTION_TYPES(T);                                           \
    FOLLY_ALWAYS_INLINE void call(                                            \
        bool& result,                                                         \
        const arg_type<TimestampWithTimezone>& lhs,                           \
        const arg_type<TimestampWithTimezone>& rhs) {                         \
      result = (tsExpr);                                                      \
    }                                                                         \
  };

VELOX_GEN_BINARY_EXPR(LtFunction, lhs < rhs, bool);
VELOX_GEN_BINARY_EXPR(GtFunction, lhs > rhs, bool);
VELOX_GEN_BINARY_EXPR(LteFunction, lhs <= rhs, bool);
VELOX_GEN_BINARY_EXPR(GteFunction, lhs >= rhs, bool);

VELOX_GEN_BINARY_EXPR_TIMESTAMP_WITH_TIME_ZONE(
    LtFunction,
    this->toTimestampMillis(lhs) < this->toTimestampMillis(rhs),
    bool);
VELOX_GEN_BINARY_EXPR_TIMESTAMP_WITH_TIME_ZONE(
    GtFunction,
    this->toTimestampMillis(lhs) > this->toTimestampMillis(rhs),
    bool);
VELOX_GEN_BINARY_EXPR_TIMESTAMP_WITH_TIME_ZONE(
    LteFunction,
    this->toTimestampMillis(lhs) <= this->toTimestampMillis(rhs),
    bool);
VELOX_GEN_BINARY_EXPR_TIMESTAMP_WITH_TIME_ZONE(
    GteFunction,
    this->toTimestampMillis(lhs) >= this->toTimestampMillis(rhs),
    bool);

#undef VELOX_GEN_BINARY_EXPR
#undef VELOX_GEN_BINARY_EXPR_TIMESTAMP_WITH_TIME_ZONE

template <typename T>
struct DistinctFromFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(bool& result, const TInput& lhs, const TInput& rhs) {
    result = (lhs != rhs); // Return true if distinct.
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  callNullable(bool& result, const TInput* lhs, const TInput* rhs) {
    if (!lhs and !rhs) { // Both nulls -> not distinct -> false.
      result = false;
    } else if (!lhs or !rhs) { // Only one is null -> distinct -> true.
      result = true;
    } else { // Both not nulls - use usual comparison.
      call(result, *lhs, *rhs);
    }
  }
};

template <typename T>
struct EqFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Used for primitive inputs.
  template <typename TInput>
  void call(bool& out, const TInput& lhs, const TInput& rhs) {
    out = (lhs == rhs);
  }

  // For arbitrary nested complex types. Can return null.
  bool call(
      bool& out,
      const arg_type<Generic<T1>>& lhs,
      const arg_type<Generic<T1>>& rhs) {
    static constexpr CompareFlags kFlags = CompareFlags::equality(
        CompareFlags::NullHandlingMode::kNullAsIndeterminate);

    auto result = lhs.compare(rhs, kFlags);
    if (!result.has_value()) {
      return false;
    }
    out = (result.value() == 0);
    return true;
  }
};

template <typename T>
struct EqFunctionTimestampWithTimezone
    : public TimestampWithTimezoneComparisonSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      bool& result,
      const arg_type<TimestampWithTimezone>& lhs,
      const arg_type<TimestampWithTimezone>& rhs) {
    result = this->toTimestampMillis(lhs) == this->toTimestampMillis(rhs);
  }
};

template <typename T>
struct NeqFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Used for primitive inputs.
  template <typename TInput>
  void call(bool& out, const TInput& lhs, const TInput& rhs) {
    out = (lhs != rhs);
  }

  // For arbitrary nested complex types. Can return null.
  bool call(
      bool& out,
      const arg_type<Generic<T1>>& lhs,
      const arg_type<Generic<T1>>& rhs) {
    if (EqFunction<T>().call(out, lhs, rhs)) {
      out = !out;
      return true;
    } else {
      return false;
    }
  }
};

template <typename T>
struct NeqFunctionTimestampWithTimezone
    : public TimestampWithTimezoneComparisonSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      bool& result,
      const arg_type<TimestampWithTimezone>& lhs,
      const arg_type<TimestampWithTimezone>& rhs) {
    result = this->toTimestampMillis(lhs) != this->toTimestampMillis(rhs);
  }
};

template <typename TExec>
struct BetweenFunction {
  template <typename T>
  FOLLY_ALWAYS_INLINE void
  call(bool& result, const T& value, const T& low, const T& high) {
    result = value >= low && value <= high;
  }
};

} // namespace facebook::velox::functions
