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
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::functions {

/// Round function
/// When AlwaysRoundNegDec is false, presto semantics is followed which does not
/// round negative decimals for integrals and round it otherwise
/// Note that is is likely techinically impossible for this function to return
/// expected results in all cases as the loss of precision plagues it on both
/// paths: factor multiplication for large numbers and addition of truncated
/// number to the rounded fraction for small numbers.
/// We are trying to minimize the loss of precision by using the best path for
/// the number, but the journey is likely not over yet.
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

  // If we just need to get rid of all the decimals.
  if (decimals == 0) {
    return std::round(number);
  }

  // For negative 'decimals', we aren't going to lose any precision - we divide
  // first (multiply by factor which is < 1.0).
  if (decimals < 0) {
    const double factor = std::pow(10, decimals);
    return std::round(number * factor) / factor;
  }

  // Get the fraction part and return number 'as is' if fraction part is 0.
  const TNum trancated = std::trunc(number);
  const TNum fraction = number - trancated;
  if (fraction == 0.0)
    return number;

  const double factor = std::pow(10, decimals);

  // Smaller numbers are less affected by precision loss being multiplied by the
  // factor, but more affected by precision loss by adding truncated number to
  // the rounded fraction in the end. Because of that, we use factor
  // multiplication path on the smaller numbers.
  // The threshold is a somewhat arbitrary/empirical number taking up 44 bits in
  // the integer form.
  if constexpr (!std::is_integral_v<TNum>) {
    if (fabs(number) < 17592186044415) {
      return std::round(number * factor) / factor;
    }
  }

  // We implement the algorithm below for positive 'decimals' nd the large
  // numbers because on the large numbers we would have precision loss when
  // multiplying number by the factor, which would lose or gain some [power of
  // 2] whole units.

  const TNum roundedFractions = std::round(fraction * factor) / factor;
  return trancated + roundedFractions;
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

FOLLY_ALWAYS_INLINE double truncate(double number, int32_t decimals) {
  const bool decNegative = (decimals < 0);
  const auto log10Size = DoubleUtil::kPowersOfTen.size(); // 309
  if (decNegative && decimals <= -log10Size) {
    return 0.0;
  }

  const uint64_t absDec = std::abs(decimals);
  const double tmp = (absDec < log10Size) ? DoubleUtil::kPowersOfTen[absDec]
                                          : std::pow(10.0, (double)absDec);

  const double valueMulTmp = number * tmp;
  if (!decNegative && !std::isfinite(valueMulTmp)) {
    return number;
  }

  const double valueDivTmp = number / tmp;
  if (number >= 0.0) {
    return decimals < 0 ? std::floor(valueDivTmp) * tmp
                        : std::floor(valueMulTmp) / tmp;
  } else {
    return decimals < 0 ? std::ceil(valueDivTmp) * tmp
                        : std::ceil(valueMulTmp) / tmp;
  }
}

// helper function for calculating upper and lower limit of wilson interval
template <bool isUpper>
FOLLY_ALWAYS_INLINE double
wilsonInterval(int64_t successes, int64_t trials, double z) {
  VELOX_USER_CHECK_GE(successes, 0, "number of successes must not be negative");
  VELOX_USER_CHECK_GT(trials, 0, "number of trials must be positive");
  VELOX_USER_CHECK_LE(
      successes,
      trials,
      "number of successes must not be larger than number of trials");
  VELOX_USER_CHECK_GE(z, 0, "z-score must not be negative");

  double s{static_cast<double>(successes)};
  double n{static_cast<double>(trials)};
  double p{s / n};

  // Wilson interval limits are solutions of a quadratic equation.
  // Let the equation be {ax^2 + bx + c = 0}.
  // r will store the value (-b + sqrt(b*b - 4*a*c)).
  double a, c, r;

  // Compute the equations differently depending on whether z is large or small.
  // This helps to avoid computations like (INFINITY/INFINITY),
  // yielding accurate results in the limit as z approaches infinity.
  if (z < 1) {
    a = n + z * z;
    c = s * p;
    r = 2 * s + z * z + z * std::sqrt(z * z + 4 * s * (1 - p));
  } else {
    a = n / (z * z) + 1;
    c = s * p / (z * z);
    r = 2 * s / (z * z) + 1 + std::sqrt(1 + 4 * s * (1 - p) / (z * z));
  }

  // Since (s, n, z >= 0), r >= 0 is guaranteed, but r == 0 needs to be handled.
  if constexpr (isUpper) {
    return r / (2 * a);
  } else {
    return (r > 0) ? (2 * c) / r : 0;
  }
}

} // namespace facebook::velox::functions
