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

#include <bitset>
#include <cmath>
#include <limits>
#include <system_error>
#include <type_traits>

#include "velox/functions/Macros.h"
#include "velox/functions/lib/ToHex.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct RemainderFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput a, const TInput n) {
    if (UNLIKELY(n == 0)) {
      return false;
    }
    // std::numeric_limits<int64_t>::min() % -1 could crash the program since
    // abs(std::numeric_limits<int64_t>::min()) can not be represented in
    // int64_t.
    if (UNLIKELY(n == 1 || n == -1)) {
      result = 0;
    } else {
      result = a % n;
    }
    return true;
  }
};

template <typename T>
struct PModIntFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TInput& result, const TInput a, const TInput n)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    TInput r;
    bool notNull = RemainderFunction<T>().call(r, a, n);
    if (!notNull) {
      return false;
    }

    result = (r > 0) ? r : (r + n) % n;
    return true;
  }
};

template <typename T>
struct PModFloatFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput a, const TInput n) {
    if (UNLIKELY(n == (TInput)0)) {
      return false;
    }
    TInput r = fmod(a, n);
    result = (r > 0) ? r : fmod(r + n, n);
    return true;
  }
};

template <typename T>
struct UnaryMinusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TInput& result, const TInput a) {
    if constexpr (std::is_integral_v<TInput>) {
      // Avoid undefined integer overflow.
      result = a == std::numeric_limits<TInput>::min() ? a : -a;
    } else {
      result = -a;
    }
    return true;
  }
};

template <typename T>
struct DivideFunction {
  FOLLY_ALWAYS_INLINE bool
  call(double& result, const double num, const double denom) {
    if (UNLIKELY(denom == 0)) {
      return false;
    }
    result = num / denom;
    return true;
  }
};

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
  static const int64_t kMax = std::numeric_limits<int64_t>::max();
  static const int64_t kMin = std::numeric_limits<int64_t>::min();
  // On some compilers if we cast 'kMax' to a double, we can get a number larger
  // than 'kMax'. This will allow 'arg' values > 'kMax'. The workaround
  // here is to use uint64_t to represent ('kMax' + 1), which can be represented
  // exactly as double. We then check if the difference with 'arg' <= 1.
  if ((static_cast<uint64_t>(kMax) + 1) - arg <= 1) {
    return kMax;
  }
  if (arg < kMin) {
    return kMin;
  }
  return arg;
}

template <>
inline int64_t safeDoubleToInt64(const int64_t& arg) {
  return arg;
}

template <typename T>
struct CeilFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const TInput value) {
    if constexpr (std::is_integral_v<TInput>) {
      result = value;
    } else {
      result = safeDoubleToInt64(std::ceil(value));
    }
    return true;
  }
};

template <typename T>
struct FloorFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const TInput value) {
    if constexpr (std::is_integral_v<TInput>) {
      result = value;
    } else {
      result = safeDoubleToInt64(std::floor(value));
    }
    return true;
  }
};

template <typename T>
struct AcoshFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::acosh(a);
  }
};

template <typename T>
struct AsinhFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::asinh(a);
  }
};

template <typename T>
struct AtanhFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::atanh(a);
  }
};

template <typename T>
struct SecFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = 1 / std::cos(a);
  }
};

template <typename T>
struct CscFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = 1 / std::sin(a);
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
struct ToBinaryStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varchar>& result, const arg_type<int64_t>& input) {
    auto str = std::bitset<64>(input).to_string();
    str.erase(0, std::min(str.find_first_not_of('0'), str.size() - 1));
    result = str;
  }
};

template <typename T>
struct SinhFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = std::sinh(a);
  }
};

template <typename T>
struct HypotFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a, double b) {
    result = std::hypot(a, b);
  }
};

template <typename T>
struct Log2Function {
  FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
    if (a <= 0.0) {
      return false;
    }
    result = std::log2(a);
    return true;
  }
};

template <typename T>
struct Log1pFunction {
  FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
    if (a <= -1) {
      return false;
    }
    result = std::log1p(a);
    return true;
  }
};

template <typename T>
struct CotFunction {
  FOLLY_ALWAYS_INLINE void call(double& result, double a) {
    result = 1 / std::tan(a);
  }
};

template <typename T>
struct Atan2Function {
  FOLLY_ALWAYS_INLINE void call(double& result, double y, double x) {
    // Spark (as of Spark 3.5)'s atan2 SQL function is internally calculated by
    // Math.atan2(y + 0.0, x + 0.0). We do the same here for compatibility.
    //
    // The sign (+/-) for 0.0 matters because it could make atan2 output
    // different results. For example:

    // * std::atan2(0.0, 0.0) = 0
    // * std::atan2(0.0, -0.0) = 3.1415926535897931
    // * std::atan2(-0.0, -0.0) = -3.1415926535897931
    // * std::atan2(-0.0, 0.0) = 0

    // By doing x + 0.0 or y + 0.0, we make sure all the -0s have been
    // replaced by 0s before sending to atan2 function. So the function
    // will always return atan2(0.0, 0.0) = 0 for atan2(+0.0/-0.0, +0.0/-0.0).
    result = std::atan2(y + 0.0, x + 0.0);
  }
};

template <typename T>
struct Log10Function {
  FOLLY_ALWAYS_INLINE bool call(double& result, double a) {
    if (a <= 0.0) {
      return false;
    }
    result = std::log10(a);
    return true;
  }
};

template <typename T>
struct IsNanFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(bool& result, TInput a) {
    result = std::isnan(a);
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(bool& result, const TInput* a) {
    if (a) {
      call(result, *a);
    } else {
      result = false;
    }
  }
};

template <typename T>
struct ToHexVarbinaryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    ToHexUtil::toHex(input, result);
  }
};

template <typename T>
struct ToHexVarcharFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    ToHexUtil::toHex(input, result);
  }
};

template <typename T>
struct ToHexBigintFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& input) {
    ToHexUtil::toHex(input, result);
  }
};
} // namespace facebook::velox::functions::sparksql
