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
#include "velox/functions/prestosql/ArithmeticImpl.h"

namespace facebook::velox::functions {

template <typename T>
struct PlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = plus(a, b);
    return true;
  }
};

template <typename T>
struct MinusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = minus(a, b);
    return true;
  }
};

template <typename T>
struct MultiplyFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = multiply(a, b);
    return true;
  }
};

template <typename T>
struct DivideFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b)
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
};

template <typename T>
struct ModulusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = modulus(a, b);
    return true;
  }
};

template <typename T>
struct CeilFunction {
  template <typename TOutput, typename TInput = TOutput>
  FOLLY_ALWAYS_INLINE bool call(TOutput& result, const TInput& a) {
    if constexpr (std::is_integral<TInput>::value) {
      result = a;
    } else {
      result = ceil(a);
    }
    return true;
  }
};

template <typename T>
struct FloorFunction {
  template <typename TOutput, typename TInput = TOutput>
  FOLLY_ALWAYS_INLINE bool call(TOutput& result, const TInput& a) {
    if constexpr (std::is_integral<TInput>::value) {
      result = a;
    } else {
      result = floor(a);
    }
    return true;
  }
};

template <typename T>
struct AbsFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TInput& result, const TInput& a) {
    result = abs(a);
    return true;
  }
};

template <typename T>
struct NegateFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TInput& result, const TInput& a) {
    result = negate(a);
    return true;
  }
};

template <typename T>
struct RoundFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const int32_t b = 0) {
    result = round(a, b);
    return true;
  }
};

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
struct MinFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = std::min(a, b);
    return true;
  }
};

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

VELOX_UDF_BEGIN(log2)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::log2(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(log10)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::log10(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(cos)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::cos(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(cosh)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::cosh(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(acos)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::acos(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(sin)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::sin(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(asin)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::asin(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(tan)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::tan(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(tanh)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::tanh(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(atan)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = std::atan(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(atan2)
FOLLY_ALWAYS_INLINE bool call(double& result, double y, double x) {
  result = std::atan2(y, x);
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

VELOX_UDF_BEGIN(radians)
FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
  result = a * (M_PI / 180);
  return true;
}
VELOX_UDF_END();

template <typename T>
struct SignFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TInput& result, const TInput& a) {
    if constexpr (std::is_floating_point<TInput>::value) {
      if (std::isnan(a)) {
        result = std::numeric_limits<TInput>::quiet_NaN();
      } else {
        result = (a == 0.0) ? 0.0 : (a > 0.0) ? 1.0 : -1.0;
      }
    } else {
      result = (a == 0) ? 0 : (a > 0) ? 1 : -1;
    }
    return true;
  }
};

VELOX_UDF_BEGIN(infinity)
FOLLY_ALWAYS_INLINE bool call(double& result) {
  result = std::numeric_limits<double>::infinity();
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(is_finite)
FOLLY_ALWAYS_INLINE bool call(bool& result, double a) {
  result = !std::isinf(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(is_infinite)
FOLLY_ALWAYS_INLINE bool call(bool& result, double a) {
  result = std::isinf(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(is_nan)
FOLLY_ALWAYS_INLINE bool call(bool& result, double a) {
  result = std::isnan(a);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(nan)
FOLLY_ALWAYS_INLINE bool call(double& result) {
  result = std::numeric_limits<double>::quiet_NaN();
  return true;
}
VELOX_UDF_END();

} // namespace facebook::velox::functions
