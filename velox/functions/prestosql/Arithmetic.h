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
#include <cstdlib>
#include <functional>
#include <system_error>

#include "folly/CPortability.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"

namespace facebook::velox::functions {

inline constexpr int kMinRadix = 2;
inline constexpr int kMaxRadix = 36;

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

template <typename T>
struct AbsFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a) {
    result = abs(a);
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

  FOLLY_ALWAYS_INLINE void
  call(out_type<Varchar>& result, const int64_t& value, const int64_t& radix) {
    checkRadix(radix);

    if (value == 0) {
      result.resize(1);
      result.data()[0] = '0';
    } else {
      int64_t runningValue = std::abs(value);
      int64_t remainder;
      char* resultPtr;
      int64_t resultSize =
          (int64_t)std::floor(std::log(runningValue) / std::log(radix)) + 1;
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
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::trunc(a);
  }
};

} // namespace
} // namespace facebook::velox::functions
