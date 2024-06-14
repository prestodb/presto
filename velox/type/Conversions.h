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

#include <folly/Conv.h>
#include <folly/Expected.h>
#include <cctype>
#include <string>
#include <type_traits>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Status.h"
#include "velox/type/CppToType.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"

DECLARE_bool(experimental_enable_legacy_cast);

namespace facebook::velox::util {

struct DefaultCastPolicy {
  static constexpr bool truncate = false;
  static constexpr bool legacyCast = false;
};

struct TruncateCastPolicy {
  static constexpr bool truncate = true;
  static constexpr bool legacyCast = false;
};

struct LegacyCastPolicy {
  static constexpr bool truncate = false;
  static constexpr bool legacyCast = true;
};

struct TruncateLegacyCastPolicy {
  static constexpr bool truncate = true;
  static constexpr bool legacyCast = true;
};

template <TypeKind KIND, typename = void, typename TPolicy = DefaultCastPolicy>
struct Converter {
  using TTo = typename TypeTraits<KIND>::NativeType;

  template <typename TFrom>
  static Expected<TTo> tryCast(TFrom) {
    static const std::string kErrorMessage = fmt::format(
        "Conversion to {} is not supported", TypeTraits<KIND>::name);

    return folly::makeUnexpected(Status::UserError(kErrorMessage));
  }
};

namespace detail {

template <typename T, typename F>
Expected<T> callFollyTo(const F& v) {
  const auto result = folly::tryTo<T>(v);
  if (result.hasError()) {
    if (threadSkipErrorDetails()) {
      return folly::makeUnexpected(Status::UserError());
    }
    return folly::makeUnexpected(Status::UserError(
        "{}", folly::makeConversionError(result.error(), "").what()));
  }

  return result.value();
}

} // namespace detail

/// To BOOLEAN converter.
template <typename TPolicy>
struct Converter<TypeKind::BOOLEAN, void, TPolicy> {
  using T = bool;

  template <typename TFrom>
  static Expected<T> tryCast(const TFrom& v) {
    if constexpr (TPolicy::truncate) {
      return folly::makeUnexpected(
          Status::UserError("Conversion to BOOLEAN is not supported"));
    }

    return detail::callFollyTo<T>(v);
  }

  static Expected<T> tryCast(folly::StringPiece v) {
    return detail::callFollyTo<T>(v);
  }

  static Expected<T> tryCast(const StringView& v) {
    return detail::callFollyTo<T>(folly::StringPiece(v));
  }

  static Expected<T> tryCast(const std::string& v) {
    return detail::callFollyTo<T>(v);
  }

  static Expected<T> tryCast(const bool& v) {
    return detail::callFollyTo<T>(v);
  }

  static Expected<T> tryCast(const float& v) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return false;
      }
      return v != 0;
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const double& v) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return false;
      }
      return v != 0;
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int8_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int16_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int32_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int64_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const Timestamp&) {
    return folly::makeUnexpected(Status::UserError(
        "Conversion of Timestamp to Boolean is not supported"));
  }
};

/// To TINYINT, SMALLINT, INTEGER, BIGINT, and HUGEINT converter.
template <TypeKind KIND, typename TPolicy>
struct Converter<
    KIND,
    std::enable_if_t<
        KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
            KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT ||
            KIND == TypeKind::HUGEINT,
        void>,
    TPolicy> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static Expected<T> tryCast(const From&) {
    static const std::string kErrorMessage = fmt::format(
        "Conversion to {} is not supported", TypeTraits<KIND>::name);

    return folly::makeUnexpected(Status::UserError(kErrorMessage));
  }

  static Expected<T> convertStringToInt(const folly::StringPiece v) {
    // Handling integer target cases
    T result = 0;
    int index = 0;
    int len = v.size();
    if (len == 0) {
      return folly::makeUnexpected(Status::UserError(
          "Cannot cast an empty string to an integral value."));
    }

    // Setting negative flag
    bool negative = false;
    // Setting decimalPoint flag
    bool decimalPoint = false;
    if (v[0] == '-' || v[0] == '+') {
      if (len == 1) {
        return folly::makeUnexpected(Status::UserError(
            "Cannot cast an '{}' string to an integral value.", v[0]));
      }
      negative = v[0] == '-';
      index = 1;
    }
    if (negative) {
      for (; index < len; index++) {
        // Truncate the decimal
        if (!decimalPoint && v[index] == '.') {
          decimalPoint = true;
          if (++index == len) {
            break;
          }
        }
        if (!std::isdigit(v[index])) {
          return folly::makeUnexpected(
              Status::UserError("Encountered a non-digit character"));
        }
        if (!decimalPoint) {
          result = checkedMultiply<T>(result, 10, CppToType<T>::name);
          result = checkedMinus<T>(result, v[index] - '0', CppToType<T>::name);
        }
      }
    } else {
      for (; index < len; index++) {
        // Truncate the decimal
        if (!decimalPoint && v[index] == '.') {
          decimalPoint = true;
          if (++index == len) {
            break;
          }
        }
        if (!std::isdigit(v[index])) {
          return folly::makeUnexpected(
              Status::UserError("Encountered a non-digit character"));
        }
        if (!decimalPoint) {
          result = checkedMultiply<T>(result, 10, CppToType<T>::name);
          result = checkedPlus<T>(result, v[index] - '0', CppToType<T>::name);
        }
      }
    }
    // Final result
    return result;
  }

  static Expected<T> tryCast(folly::StringPiece v) {
    if constexpr (TPolicy::truncate) {
      return convertStringToInt(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const StringView& v) {
    if constexpr (TPolicy::truncate) {
      return convertStringToInt(folly::StringPiece(v));
    } else {
      return detail::callFollyTo<T>(folly::StringPiece(v));
    }
  }

  static Expected<T> tryCast(const std::string& v) {
    if constexpr (TPolicy::truncate) {
      return convertStringToInt(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const bool& v) {
    return folly::to<T>(v);
  }

  struct LimitType {
    static constexpr bool kByteOrSmallInt =
        std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t>;

    static int64_t minLimit() {
      if (kByteOrSmallInt) {
        return std::numeric_limits<int32_t>::min();
      }
      return std::numeric_limits<T>::min();
    }

    static int64_t maxLimit() {
      if (kByteOrSmallInt) {
        return std::numeric_limits<int32_t>::max();
      }
      return std::numeric_limits<T>::max();
    }

    static T min() {
      if (kByteOrSmallInt) {
        return 0;
      }
      return std::numeric_limits<T>::min();
    }

    static T max() {
      if (kByteOrSmallInt) {
        return -1;
      }
      return std::numeric_limits<T>::max();
    }

    template <typename FP>
    static Expected<T> tryCast(const FP& v) {
      if (kByteOrSmallInt) {
        return T(int32_t(v));
      }
      return T(v);
    }
  };

  static Expected<T> tryCast(const float& v) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return 0;
      }

      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
      } else if (v > LimitType::maxLimit()) {
        return LimitType::max();
      } else if (v < LimitType::minLimit()) {
        return LimitType::min();
      }

      return LimitType::tryCast(v);
    } else {
      if (std::isnan(v)) {
        return folly::makeUnexpected(
            Status::UserError("Cannot cast NaN to an integral value."));
      }
      return detail::callFollyTo<T>(std::round(v));
    }
  }

  static Expected<T> tryCast(const double& v) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return 0;
      }

      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
      } else if (v > LimitType::maxLimit()) {
        return LimitType::max();
      } else if (v < LimitType::minLimit()) {
        return LimitType::min();
      }

      return LimitType::tryCast(v);
    } else {
      if (std::isnan(v)) {
        return folly::makeUnexpected(
            Status::UserError("Cannot cast NaN to an integral value."));
      }
      return detail::callFollyTo<T>(std::round(v));
    }
  }

  static Expected<T> tryCast(const int8_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int16_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int32_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }

  static Expected<T> tryCast(const int64_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return detail::callFollyTo<T>(v);
    }
  }
};

/// To REAL and DOUBLE converter.
template <TypeKind KIND, typename TPolicy>
struct Converter<
    KIND,
    std::enable_if_t<KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE, void>,
    TPolicy> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static Expected<T> tryCast(const From& v) {
    return detail::callFollyTo<T>(v);
  }

  static Expected<T> tryCast(folly::StringPiece v) {
    return tryCast<folly::StringPiece>(v);
  }

  static Expected<T> tryCast(const StringView& v) {
    return tryCast<folly::StringPiece>(folly::StringPiece(v));
  }

  static Expected<T> tryCast(const std::string& v) {
    return tryCast<std::string>(v);
  }

  static Expected<T> tryCast(const bool& v) {
    return tryCast<bool>(v);
  }

  static Expected<T> tryCast(const float& v) {
    return tryCast<float>(v);
  }

  static Expected<T> tryCast(const double& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return tryCast<double>(v);
    }
  }

  static Expected<T> tryCast(const int8_t& v) {
    return tryCast<int8_t>(v);
  }

  static Expected<T> tryCast(const int16_t& v) {
    return tryCast<int16_t>(v);
  }

  // Convert integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static Expected<T> tryCast(const int32_t& v) {
    return static_cast<T>(v);
  }

  // Convert large integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static Expected<T> tryCast(const int64_t& v) {
    return static_cast<T>(v);
  }

  // Convert large integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static Expected<T> tryCast(const int128_t& v) {
    return static_cast<T>(v);
  }

  static Expected<T> tryCast(const Timestamp&) {
    return folly::makeUnexpected(Status::UserError(
        "Conversion of Timestamp to Real or Double is not supported"));
  }
};

/// To VARBINARY converter.
template <typename TPolicy>
struct Converter<TypeKind::VARBINARY, void, TPolicy> {
  // Same semantics of TypeKind::VARCHAR converter.
  template <typename T>
  static Expected<std::string> tryCast(const T& val) {
    return Converter<TypeKind::VARCHAR, void, TPolicy>::tryCast(val);
  }
};

/// To VARCHAR converter.
template <typename TPolicy>
struct Converter<TypeKind::VARCHAR, void, TPolicy> {
  template <typename T>
  static Expected<std::string> tryCast(const T& val) {
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
      if constexpr (TPolicy::legacyCast) {
        auto str = folly::to<std::string>(val);
        normalizeStandardNotation(str);
        return str;
      }

      // Implementation below is close to String.of(double) of Java. For
      // example, for some rare cases the result differs in precision by
      // the least significant bit.
      if (FOLLY_UNLIKELY(std::isinf(val) || std::isnan(val))) {
        return folly::to<std::string>(val);
      }
      if ((val > -10'000'000 && val <= -0.001) ||
          (val >= 0.001 && val < 10'000'000) || val == 0.0) {
        auto str = fmt::format("{}", val);
        normalizeStandardNotation(str);
        return str;
      }
      // Precision of float is at most 8 significant decimal digits. Precision
      // of double is at most 17 significant decimal digits.
      auto str =
          fmt::format(std::is_same_v<T, float> ? "{:.7E}" : "{:.16E}", val);
      normalizeScientificNotation(str);
      return str;
    }

    return folly::to<std::string>(val);
  }

  static Expected<std::string> tryCast(const Timestamp& val) {
    TimestampToStringOptions options;
    options.precision = TimestampToStringOptions::Precision::kMilliseconds;
    if constexpr (!TPolicy::legacyCast) {
      options.zeroPaddingYear = true;
      options.dateTimeSeparator = ' ';
    }
    return val.toString(options);
  }

  static Expected<std::string> tryCast(const bool& val) {
    return val ? "true" : "false";
  }

  /// Normalize the given floating-point standard notation string in place, by
  /// appending '.0' if it has only the integer part but no fractional part. For
  /// example, for the given string '12345', replace it with '12345.0'.
  static void normalizeStandardNotation(std::string& str) {
    if (!FLAGS_experimental_enable_legacy_cast &&
        str.find(".") == std::string::npos && isdigit(str[str.length() - 1])) {
      str += ".0";
    }
  }

  /// Normalize the given floating-point scientific notation string in place, by
  /// removing the trailing 0s of the coefficient as well as the leading '+' and
  /// 0s of the exponent. For example, for the given string '3.0000000E+005',
  /// replace it with '3.0E5'. For '-1.2340000E-010', replace it with
  /// '-1.234E-10'.
  static void normalizeScientificNotation(std::string& str) {
    size_t idxE = str.find('E');
    VELOX_DCHECK_NE(
        idxE,
        std::string::npos,
        "Expect a character 'E' in scientific notation.");

    int endCoef = idxE - 1;
    while (endCoef >= 0 && str[endCoef] == '0') {
      --endCoef;
    }
    VELOX_DCHECK_GT(endCoef, 0, "Coefficient should not be all zeros.");

    int pos = endCoef + 1;
    if (str[endCoef] == '.') {
      pos++;
    }
    str[pos++] = 'E';

    int startExp = idxE + 1;
    if (str[startExp] == '-') {
      str[pos++] = '-';
      startExp++;
    }
    while (startExp < str.length() &&
           (str[startExp] == '0' || str[startExp] == '+')) {
      startExp++;
    }
    VELOX_DCHECK_LT(
        startExp, str.length(), "Exponent should not be all zeros.");
    str.replace(pos, str.length() - startExp, str, startExp);
    pos += str.length() - startExp;

    str.resize(pos);
  }
};

/// To TIMESTAMP converter.
template <typename TPolicy>
struct Converter<TypeKind::TIMESTAMP, void, TPolicy> {
  template <typename From>
  static Expected<Timestamp> tryCast(const From& /* v */) {
    return folly::makeUnexpected(
        Status::UserError("Conversion to Timestamp is not supported"));
  }

  static Expected<Timestamp> tryCast(folly::StringPiece v) {
    return fromTimestampString(
        v.data(), v.size(), TimestampParseMode::kPrestoCast);
  }

  static Expected<Timestamp> tryCast(const StringView& v) {
    return fromTimestampString(
        v.data(), v.size(), TimestampParseMode::kPrestoCast);
  }

  static Expected<Timestamp> tryCast(const std::string& v) {
    return fromTimestampString(
        v.data(), v.size(), TimestampParseMode::kPrestoCast);
  }
};

} // namespace facebook::velox::util
