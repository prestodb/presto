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

#include <cerrno>
#include <charconv>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <limits>
#include <system_error>
#include <type_traits>

#include "folly/CPortability.h"
#include "velox/common/base/Doubles.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"

namespace facebook::velox::functions {

inline constexpr int kMinRadix = 2;
inline constexpr int kMaxRadix = 36;
inline constexpr long kLongMax = std::numeric_limits<int64_t>::max();
inline constexpr long kLongMin = std::numeric_limits<int64_t>::min();

inline constexpr char digits[36] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
    'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

namespace {

template <typename T>
struct PlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = plus(a, b);
  }
};

template <typename T>
struct MinusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = minus(a, b);
  }
};

template <typename T>
struct MultiplyFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = multiply(a, b);
  }
};

// Multiply function for IntervalDayTime * Double and Double * IntervalDayTime.
template <typename T>
struct IntervalMultiplyFunction {
  FOLLY_ALWAYS_INLINE double sanitizeInput(double d) {
    if UNLIKELY (std::isnan(d)) {
      return 0;
    }
    return d;
  }

  template <
      typename T1,
      typename T2,
      typename = std::enable_if_t<
          (std::is_same_v<T1, int64_t> && std::is_same_v<T2, double>) ||
          (std::is_same_v<T1, double> && std::is_same_v<T2, int64_t>)>>
  FOLLY_ALWAYS_INLINE void call(int64_t& result, T1 a, T2 b) {
    double resultDouble;
    if constexpr (std::is_same_v<T1, double>) {
      resultDouble = sanitizeInput(a) * b;
    } else {
      resultDouble = sanitizeInput(b) * a;
    }

    if LIKELY (
        std::isfinite(resultDouble) && resultDouble >= kLongMin &&
        resultDouble <= kMaxDoubleBelowInt64Max) {
      result = int64_t(resultDouble);
    } else {
      result = resultDouble > 0 ? kLongMax : kLongMin;
    }
  }
};

template <typename T>
struct DivideFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b)
// depend on compiler have correct behaviour for divide by zero
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("float-divide-by-zero")))
#endif
#endif
  {
    result = a / b;
  }
};

template <typename T>
struct IntervalDivideFunction {
  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t a, double b)
// Depend on compiler have correct behaviour for divide by zero
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("float-divide-by-zero")))
      __attribute__((__no_sanitize__("float-cast-overflow")))
#endif
#endif
  {
    if UNLIKELY (a == 0 || std::isnan(b) || !std::isfinite(b)) {
      result = 0;
      return;
    }
    double resultDouble = a / b;
    if LIKELY (
        std::isfinite(resultDouble) && resultDouble >= kLongMin &&
        resultDouble <= kMaxDoubleBelowInt64Max) {
      result = int64_t(resultDouble);
    } else {
      result = resultDouble > 0 ? kLongMax : kLongMin;
    }
  }
};

template <typename T>
struct ModulusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = modulus(a, b);
  }
};

template <typename T>
struct CeilFunction {
  template <typename TOutput, typename TInput = TOutput>
  FOLLY_ALWAYS_INLINE void call(TOutput& result, const TInput& a) {
    if constexpr (std::is_integral_v<TInput>) {
      result = a;
    } else {
      result = ceil(a);
    }
  }
};

template <typename T>
struct FloorFunction {
  template <typename TOutput, typename TInput = TOutput>
  FOLLY_ALWAYS_INLINE void call(TOutput& result, const TInput& a) {
    if constexpr (std::is_integral_v<TInput>) {
      result = a;
    } else {
      result = floor(a);
    }
  }
};

template <typename TExec>
struct AbsFunction {
  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
    result = abs(a);
  }
};

template <typename TExec>
struct DecimalAbsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<ShortDecimal<P1, S1>>& result,
      const arg_type<ShortDecimal<P1, S1>>& a) {
    result = (a < 0) ? -a : a;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<LongDecimal<P1, S1>>& result,
      const arg_type<LongDecimal<P1, S1>>& a) {
    result = (a < 0) ? -a : a;
  }
};

template <typename T>
struct NegateFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a) {
    result = negate(a);
  }
};

template <typename T>
struct RoundFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const int32_t b = 0) {
    result = round(a, b);
  }
};

template <typename T>
struct PowerFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(double& result, const TInput& a, const TInput& b) {
    result = std::pow(a, b);
  }
};

template <typename T>
struct ExpFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::exp(a);
  }
};

template <typename T>
struct MinFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = std::min(a, b);
  }
};

template <typename T>
struct ClampFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& v, const TInput& lo, const TInput& hi) {
    // std::clamp emits less efficient ASM
    const TInput& a = v < lo ? lo : v;
    result = a > hi ? hi : a;
  }
};

template <typename T>
struct LnFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::log(a);
  }
};

template <typename T>
struct Log2Function {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::log2(a);
  }
};

template <typename T>
struct Log10Function {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::log10(a);
  }
};

template <typename T>
struct CosFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::cos(a);
  }
};

template <typename T>
struct CoshFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::cosh(a);
  }
};

template <typename T>
struct AcosFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::acos(a);
  }
};

template <typename T>
struct SinFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::sin(a);
  }
};

template <typename T>
struct AsinFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::asin(a);
  }
};

template <typename T>
struct TanFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::tan(a);
  }
};

template <typename T>
struct TanhFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::tanh(a);
  }
};

template <typename T>
struct AtanFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::atan(a);
  }
};

template <typename T>
struct Atan2Function {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput y, TInput x) {
    result = std::atan2(y, x);
  }
};

template <typename T>
struct SqrtFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::sqrt(a);
  }
};

template <typename T>
struct CbrtFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::cbrt(a);
  }
};

template <typename T>
struct WidthBucketFunction {
  FOLLY_ALWAYS_INLINE void call(
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
  }
};

template <typename T>
struct RadiansFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = a * (M_PI / 180);
  }
};

template <typename T>
struct DegreesFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = a * (180 / M_PI);
  }
};

template <typename T>
struct SignFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a) {
    if constexpr (std::is_floating_point<TInput>::value) {
      if (std::isnan(a)) {
        result = std::numeric_limits<TInput>::quiet_NaN();
      } else {
        result = (a == 0.0) ? 0.0 : (a > 0.0) ? 1.0 : -1.0;
      }
    } else {
      result = (a == 0) ? 0 : (a > 0) ? 1 : -1;
    }
  }
};

template <typename T>
struct InfinityFunction {
  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = std::numeric_limits<double>::infinity();
  }
};

template <typename T>
struct IsFiniteFunction {
  FOLLY_ALWAYS_INLINE void call(bool& result, double a) {
    result = !std::isinf(a);
  }
};

template <typename T>
struct IsInfiniteFunction {
  FOLLY_ALWAYS_INLINE void call(bool& result, double a) {
    result = std::isinf(a);
  }
};

template <typename T>
struct IsNanFunction {
  FOLLY_ALWAYS_INLINE void call(bool& result, double a) {
    result = std::isnan(a);
  }
};

template <typename T>
struct NanFunction {
  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = std::numeric_limits<double>::quiet_NaN();
  }
};

FOLLY_ALWAYS_INLINE void checkRadix(int64_t radix) {
  VELOX_USER_CHECK_GE(
      radix,
      kMinRadix,
      "Radix must be between {} and {}.",
      kMinRadix,
      kMaxRadix);
  VELOX_USER_CHECK_LE(
      radix,
      kMaxRadix,
      "Radix must be between {} and {}.",
      kMinRadix,
      kMaxRadix);
}

template <typename T>
struct FromBaseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(int64_t& result, const arg_type<Varchar>& input, int64_t radix) {
    checkRadix(radix);

    auto begin = input.begin();
    if (input.size() > 0 && (*input.begin()) == '+')
      begin = input.begin() + 1;
    auto status = std::from_chars(begin, input.end(), result, radix);

    VELOX_USER_CHECK(
        (status.ec != std::errc::invalid_argument && status.ptr == input.end()),
        "Not a valid base-{} number: {}.",
        radix,
        input.getString());

    VELOX_USER_CHECK_NE(
        status.ec,
        std::errc::result_out_of_range,
        "{} is out of range.",
        input.getString());
  }
};

template <typename T>
struct ToBaseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename B>
  void
  applyToBase(out_type<Varchar>& result, const B& value, const int64_t& radix) {
    if (value == 0) {
      result.resize(1);
      result.data()[0] = '0';
    } else {
      B runningValue = value < 0 ? -1 * value : value;
      B remainder;
      char* resultPtr;
      int128_t resultSize =
          (int128_t)std::floor(std::log(runningValue) / std::log(radix)) + 1;
      if (value < 0) {
        resultSize += 1;
        result.resize(resultSize);
        resultPtr = result.data();
        resultPtr[0] = '-';
      } else {
        result.resize(resultSize);
        resultPtr = result.data();
      }
      int64_t index = resultSize;
      while (runningValue > 0) {
        remainder = runningValue % radix;
        resultPtr[--index] = digits[remainder];
        runningValue /= radix;
      }
    }
  }

  void call(
      out_type<Varchar>& result,
      const int64_t& inputValue,
      const int64_t& radix) {
    checkRadix(radix);
    if (inputValue == std::numeric_limits<int64_t>::min()) {
      // Special case for min inputValue to avoid overflow.
      applyToBase<int128_t>(result, inputValue, radix);
    } else {
      applyToBase<int64_t>(result, inputValue, radix);
    }
  }
};

template <typename T>
struct PiFunction {
  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = M_PI;
  }
};

template <typename T>
struct EulerConstantFunction {
  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = M_E;
  }
};

template <typename T>
struct TruncateFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = std::trunc(a);
  }

  FOLLY_ALWAYS_INLINE void call(double& result, double a, int32_t n) {
    result = truncate(a, n);
  }
};

template <typename T>
struct WilsonIntervalUpperFunction {
  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const int64_t& successes,
      const int64_t& trials,
      const double& z) {
    result = wilsonInterval<true /*isUpper*/>(successes, trials, z);
  }
};

template <typename T>
struct WilsonIntervalLowerFunction {
  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const int64_t& successes,
      const int64_t& trials,
      const double& z) {
    result = wilsonInterval<false /*isUpper*/>(successes, trials, z);
  }
};

template <typename T>
struct CosineSimilarityFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  double normalizeMap(const null_free_arg_type<Map<Varchar, double>>& map) {
    double norm = 0.0;
    for (const auto& [key, value] : map) {
      norm += (value * value);
    }
    return std::sqrt(norm);
  }

  double mapDotProduct(
      const null_free_arg_type<Map<Varchar, double>>& leftMap,
      const null_free_arg_type<Map<Varchar, double>>& rightMap) {
    double result = 0.0;
    for (const auto& [key, value] : leftMap) {
      auto it = rightMap.find(key);
      if (it != rightMap.end()) {
        result += value * it->second;
      }
    }
    return result;
  }

  void callNullFree(
      out_type<double>& result,
      const null_free_arg_type<Map<Varchar, double>>& leftMap,
      const null_free_arg_type<Map<Varchar, double>>& rightMap) {
    if (leftMap.empty() || rightMap.empty()) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double normLeftMap = normalizeMap(leftMap);
    if (normLeftMap == 0.0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double normRightMap = normalizeMap(rightMap);
    if (normRightMap == 0.0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double dotProduct = mapDotProduct(leftMap, rightMap);
    result = dotProduct / (normLeftMap * normRightMap);
  }
};

} // namespace
} // namespace facebook::velox::functions
