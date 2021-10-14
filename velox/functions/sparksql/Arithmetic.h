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

#include <cmath>
#include <limits>
#include <system_error>
#include <type_traits>

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
VELOX_UDF_BEGIN(pmod)
FOLLY_ALWAYS_INLINE bool call(T& result, const T a, const T n) {
  if (UNLIKELY(n == 0)) {
    return false;
  }
  T r = a % n;
  result = (r > 0) ? r : (r + n) % n;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(remainder)
FOLLY_ALWAYS_INLINE bool call(T& result, const T a, const T n) {
  if (UNLIKELY(n == 0)) {
    return false;
  }
  result = a % n;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(unaryminus)
FOLLY_ALWAYS_INLINE bool call(T& result, const T a) {
  if constexpr (std::is_integral_v<T>) {
    // Avoid undefined integer overflow.
    result = a == std::numeric_limits<T>::min() ? a : -a;
  } else {
    result = -a;
  }
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(divide)
FOLLY_ALWAYS_INLINE bool
call(double& result, const double num, const double denom) {
  if (UNLIKELY(denom == 0)) {
    return false;
  }
  result = num / denom;
  return true;
}
VELOX_UDF_END();

/*
  In Spark both ceil and floor must return Long type
  sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala
*/
template <typename T>
int64_t safeDoubleToInt64(const T& /*value*/) {
  throw std::runtime_error("Invalid input for floor/ceil");
}

template <>
inline int64_t safeDoubleToInt64(const double& arg) {
  if (std::isnan(arg)) {
    return 0;
  }
  if (arg > std::numeric_limits<int64_t>::max()) {
    return std::numeric_limits<int64_t>::max();
  }
  if (arg < std::numeric_limits<int64_t>::min()) {
    return std::numeric_limits<int64_t>::min();
  }
  return arg;
}

template <>
inline int64_t safeDoubleToInt64(const int64_t& arg) {
  return arg;
}

template <typename T>
VELOX_UDF_BEGIN(ceil)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, const T value) {
  if constexpr (std::is_integral_v<T>) {
    result = value;
  } else {
    result = safeDoubleToInt64(std::ceil(value));
  }
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(floor)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, const T value) {
  if constexpr (std::is_integral_v<T>) {
    result = value;
  } else {
    result = safeDoubleToInt64(std::floor(value));
  }
  return true;
}
VELOX_UDF_END();
} // namespace facebook::velox::functions::sparksql
