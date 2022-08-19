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

#include <algorithm>
#include <cmath>
#include <type_traits>
#include "folly/CPortability.h"

namespace facebook::velox::functions {

/// Round function
/// when AlwaysRoundNegDec is false, presto semantics is followed which does not
/// round negative decimals for integrals and round it otherwise
template <typename TNum, typename TDecimals, bool alwaysRoundNegDec = false>
FOLLY_ALWAYS_INLINE TNum
round(const TNum& number, const TDecimals& decimals = 0) {
  static_assert(!std::is_same_v<TNum, bool> && "round not supported for bool");

  if constexpr (std::is_integral_v<TNum>) {
    if constexpr (alwaysRoundNegDec) {
      if (decimals >= 0)
        return number;
    } else {
      return number;
    }
  }
  if (!std::isfinite(number)) {
    return number;
  }

  double factor = std::pow(10, decimals);
  if (number < 0) {
    return (std::round(number * factor * -1) / factor) * -1;
  }
  return std::round(number * factor) / factor;
}

// This is used by Velox for floating points plus.
template <typename T>
T plus(const T a, const T b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
{
  return a + b;
}

// This is used by Velox for floating points minus.
template <typename T>
T minus(const T a, const T b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
{
  return a - b;
}

// This is used by Velox for floating points multiply.
template <typename T>
T multiply(const T a, const T b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
{
  return a * b;
}

// This is used by Velox for floating points divide.
template <typename T>
T divide(const T& a, const T& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("float-divide-by-zero")))
#endif
#endif
{
  T result = a / b;
  return result;
}

// This is used by Velox for floating points modulus.
template <typename T>
T modulus(const T a, const T b) {
  if (b == 0) {
    // Match Presto semantics
    return std::numeric_limits<T>::quiet_NaN();
  }
  return std::fmod(a, b);
}

template <typename T>
T negate(const T& arg) {
  T results = std::negate<std::remove_cv_t<T>>()(arg);
  return results;
}

template <typename T>
T abs(const T& arg) {
  T results = std::abs(arg);
  return results;
}

template <typename T>
T floor(const T& arg) {
  T results = std::floor(arg);
  return results;
}

template <typename T>
T ceil(const T& arg) {
  T results = std::ceil(arg);
  return results;
}

} // namespace facebook::velox::functions
