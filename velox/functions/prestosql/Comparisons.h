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

namespace facebook::velox::functions {
#define VELOX_GEN_BINARY_EXPR(Name, Expr, TResult)                \
  template <typename T>                                           \
  struct Name {                                                   \
    VELOX_DEFINE_FUNCTION_TYPES(T);                               \
    template <typename TInput>                                    \
    FOLLY_ALWAYS_INLINE void                                      \
    call(TResult& result, const TInput& lhs, const TInput& rhs) { \
      result = (Expr);                                            \
    }                                                             \
  };

VELOX_GEN_BINARY_EXPR(NeqFunction, lhs != rhs, bool);
VELOX_GEN_BINARY_EXPR(LtFunction, lhs < rhs, bool);
VELOX_GEN_BINARY_EXPR(GtFunction, lhs > rhs, bool);
VELOX_GEN_BINARY_EXPR(LteFunction, lhs <= rhs, bool);
VELOX_GEN_BINARY_EXPR(GteFunction, lhs >= rhs, bool);

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

#undef VELOX_GEN_BINARY_EXPR

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
    static constexpr CompareFlags kFlags = {
        false, false, /*euqalsOnly*/ true, true /*stopAtNull*/};
    auto result = lhs.compare(rhs, kFlags);
    if (!result.has_value()) {
      return false;
    }
    out = (result.value() == 0);
    return true;
  }
};

template <typename T>
struct BetweenFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const TInput& value,
      const TInput& low,
      const TInput& high) {
    result = value >= low && value <= high;
  }
};

} // namespace facebook::velox::functions
