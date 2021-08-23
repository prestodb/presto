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
#include <cstdlib>
#include <functional>

#include "folly/CPortability.h"
#include "velox/functions/Macros.h"
#include "velox/functions/common/ArithmeticImpl.h"

namespace facebook {
namespace velox {
namespace functions {

template <typename T>
VELOX_UDF_BEGIN(plus)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const T& b) {
  result = plus(a, b);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(minus)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const T& b) {
  result = minus(a, b);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(multiply)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const T& b) {
  result = multiply(a, b);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(divide)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const T& b)
// depend on compiler have correct behaviour for divide by zero
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("float-divide-by-zero")))
#endif
#endif
{
  result = a / b;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(ceil)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a) {
  result = ceil(a);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(floor)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a) {
  result = floor(a);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(abs)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a) {
  result = abs(a);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(negate)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a) {
  result = negate(a);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(round)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const int32_t b = 0) {
  result = round(a, b);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(power)
FOLLY_ALWAYS_INLINE bool call(double& result, const T& a, const T& b) {
  result = std::pow(a, b);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(exp)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::exp(a);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(min)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const T& b) {
  result = std::min(a, b);
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(clamp)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& v, const T& lo, const T& hi) {
  result = std::clamp(v, lo, hi);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(ln)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::log(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(sqrt)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::sqrt(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(cbrt)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::cbrt(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(width_bucket)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    double operand,
    double bound1,
    double bound2,
    int64_t bucketCount) {
  // TODO: These checks are costing ~13% performance penalty, it would be nice
  //       to optimize for the common cases where bounds and bucket count are
  //       constant to skip these checks
  // Benchmark reference: WidthBucketBenchmark.cpp
  // Benchmark Result:
  // https://github.com/facebookexternal/velox/pull/1225#discussion_r665621405
  VELOX_USER_CHECK_GT(bucketCount, 0, "bucketCount must be greater than 0");
  VELOX_USER_CHECK(!std::isnan(operand), "operand must not be NaN");
  VELOX_USER_CHECK(std::isfinite(bound1), "first bound must be finite");
  VELOX_USER_CHECK(std::isfinite(bound2), "second bound must be finite");
  VELOX_USER_CHECK_NE(bound1, bound2, "bounds cannot equal each other");

  double lower = std::min(bound1, bound2);
  double upper = std::max(bound1, bound2);

  if (operand < lower) {
    result = 0;
  } else if (operand > upper) {
    VELOX_USER_CHECK_NE(
        bucketCount,
        std::numeric_limits<int64_t>::max(),
        "Bucket for value {} is out of range",
        operand);
    result = bucketCount + 1;
  } else {
    result =
        (int64_t)((double)bucketCount * (operand - lower) / (upper - lower) + 1);
  }

  if (bound1 > bound2) {
    result = bucketCount - result + 1;
  }
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_and)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a, T b) {
  result = a & b;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_not)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a) {
  result = ~a;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_or)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a, T b) {
  result = a | b;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_xor)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a, T b) {
  result = a ^ b;
  return true;
}
VELOX_UDF_END();

} // namespace functions
} // namespace velox
} // namespace facebook
