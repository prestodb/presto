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

#include "velox/functions/Macros.h"

namespace facebook {
namespace velox {
namespace functions {

#define VELOX_GEN_BINARY_EXPR(Name, Expr, TResult) \
  template <typename T>                            \
  VELOX_UDF_BEGIN(Name)                            \
  FOLLY_ALWAYS_INLINE bool call(                   \
      out_type<TResult>& result,                   \
      const arg_type<T>& lhs,                      \
      const arg_type<T>& rhs) {                    \
    result = (Expr);                               \
    return true;                                   \
  }                                                \
  VELOX_UDF_END();

VELOX_GEN_BINARY_EXPR(eq, lhs == rhs, bool);
VELOX_GEN_BINARY_EXPR(neq, lhs != rhs, bool);
VELOX_GEN_BINARY_EXPR(lt, lhs < rhs, bool);
VELOX_GEN_BINARY_EXPR(gt, lhs > rhs, bool);
VELOX_GEN_BINARY_EXPR(lte, lhs <= rhs, bool);
VELOX_GEN_BINARY_EXPR(gte, lhs >= rhs, bool);

#undef VELOX_GEN_BINARY_EXPR

template <typename T>
VELOX_UDF_BEGIN(between)
FOLLY_ALWAYS_INLINE
    bool call(bool& result, const T& value, const T& low, const T& high) {
  result = value >= low && value <= high;
  return true;
}
VELOX_UDF_END();

} // namespace functions
} // namespace velox
} // namespace facebook
