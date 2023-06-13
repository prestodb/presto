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
#include <cctype>
#include <string>
#include <type_traits>
#include "velox/common/base/Exceptions.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"

namespace facebook::velox::util {

template <TypeKind KIND, typename = void, bool TRUNCATE = false>
struct Converter {
  template <typename T>
  static typename TypeTraits<KIND>::NativeType cast(T) {
    VELOX_UNSUPPORTED(
        "Conversion to {} is not supported", TypeTraits<KIND>::name);
  }
};

template <>
struct Converter<TypeKind::BOOLEAN> {
  using T = bool;

  template <typename From>
  static T cast(const From& v) {
    return folly::to<T>(v);
  }

  static T cast(folly::StringPiece v) {
    return folly::to<T>(v);
  }

  static T cast(const StringView& v) {
    return folly::to<T>(folly::StringPiece(v));
  }

  static T cast(const std::string& v) {
    return folly::to<T>(v);
  }

  static T cast(const Date&) {
    VELOX_UNSUPPORTED("Conversion of Date to Boolean is not supported");
  }

  static T cast(const Timestamp&) {
    VELOX_UNSUPPORTED("Conversion of Timestamp to Boolean is not supported");
  }
};

template <TypeKind KIND, bool TRUNCATE>
struct Converter<
    KIND,
    std::enable_if_t<
        KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
            KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
            KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT,
        void>,
    TRUNCATE> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static T cast(const From&) {
    VELOX_UNSUPPORTED(
        "Conversion to {} is not supported", TypeTraits<KIND>::name);
  }

  static T convertStringToInt(const folly::StringPiece v) {
    // Handling boolean target case fist because it is in this scope
    if constexpr (std::is_same_v<T, bool>) {
      return folly::to<T>(v);
    } else {
      // Handling integer target cases
      T result = 0;
      int index = 0;
      int len = v.size();
      if (len == 0) {
        VELOX_USER_FAIL("Cannot cast an empty string to an integral value.");
      }

      // Setting negative flag
      bool negative = false;
      if (v[0] == '-') {
        if (len == 1) {
          VELOX_USER_FAIL("Cannot cast an '-' string to an integral value.");
        }
        negative = true;
        index = 1;
      }
      if (negative) {
        for (; index < len; index++) {
          if (!std::isdigit(v[index])) {
            VELOX_USER_FAIL("Encountered a non-digit character");
          }
          result = result * 10 - (v[index] - '0');
          // Overflow check
          if (result > 0) {
            VELOX_USER_FAIL("Value is too large for type");
          }
        }
      } else {
        for (; index < len; index++) {
          if (!std::isdigit(v[index])) {
            VELOX_USER_FAIL("Encountered a non-digit character");
          }
          result = result * 10 + (v[index] - '0');
          // Overflow check
          if (result < 0) {
            VELOX_USER_FAIL("Value is too large for type");
          }
        }
      }
      // Final result
      return result;
    }
  }

  static T cast(folly::StringPiece v) {
    try {
      if constexpr (TRUNCATE) {
        return convertStringToInt(v);
      } else {
        return folly::to<T>(v);
      }
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const StringView& v) {
    try {
      if constexpr (TRUNCATE) {
        return convertStringToInt(folly::StringPiece(v));
      } else {
        return folly::to<T>(folly::StringPiece(v));
      }
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const std::string& v) {
    try {
      if constexpr (TRUNCATE) {
        return convertStringToInt(v);
      } else {
        return folly::to<T>(v);
      }
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const bool& v) {
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
    static T cast(const FP& v) {
      if (kByteOrSmallInt) {
        return T(int32_t(v));
      }
      return T(v);
    }
  };

  static T cast(const float& v) {
    if constexpr (TRUNCATE) {
      if (std::isnan(v)) {
        return 0;
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
      } else if (v > LimitType::maxLimit()) {
        return LimitType::max();
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::min();
      } else if (v < LimitType::minLimit()) {
        return LimitType::min();
      }
      return LimitType::cast(v);
    } else {
      if (std::isnan(v)) {
        VELOX_USER_FAIL("Cannot cast NaN to an integral value.");
      }
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const double& v) {
    if constexpr (TRUNCATE) {
      if (std::isnan(v)) {
        return 0;
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
      } else if (v > LimitType::maxLimit()) {
        return LimitType::max();
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::min();
      } else if (v < LimitType::minLimit()) {
        return LimitType::min();
      }
      return LimitType::cast(v);
    } else {
      if (std::isnan(v)) {
        VELOX_USER_FAIL("Cannot cast NaN to an integral value.");
      }
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const int8_t& v) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int16_t& v) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int32_t& v) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int64_t& v) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }
};

template <TypeKind KIND, bool TRUNCATE>
struct Converter<
    KIND,
    std::enable_if_t<KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE, void>,
    TRUNCATE> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static T cast(const From& v) {
    try {
      return folly::to<T>(v);
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(folly::StringPiece v) {
    return cast<folly::StringPiece>(v);
  }

  static T cast(const StringView& v) {
    return cast<folly::StringPiece>(folly::StringPiece(v));
  }

  static T cast(const std::string& v) {
    return cast<std::string>(v);
  }

  static T cast(const bool& v) {
    return cast<bool>(v);
  }

  static T cast(const float& v) {
    return cast<float>(v);
  }

  static T cast(const double& v) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return cast<double>(v);
    }
  }

  static T cast(const int8_t& v) {
    return cast<int8_t>(v);
  }

  static T cast(const int16_t& v) {
    return cast<int16_t>(v);
  }

  // Convert integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int32_t& v) {
    return static_cast<T>(v);
  }

  // Convert large integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int64_t& v) {
    return static_cast<T>(v);
  }

  static T cast(const Date&) {
    VELOX_UNSUPPORTED("Conversion of Date to Real or Double is not supported");
  }

  static T cast(const Timestamp&) {
    VELOX_UNSUPPORTED(
        "Conversion of Timestamp to Real or Double is not supported");
  }
};

template <bool TRUNCATE>
struct Converter<TypeKind::VARBINARY, void, TRUNCATE> {
  // Same semantics of TypeKind::VARCHAR converter.
  template <typename T>
  static std::string cast(const T& val) {
    return Converter<TypeKind::VARCHAR, void, TRUNCATE>::cast(val);
  }
};

template <bool TRUNCATE>
struct Converter<TypeKind::VARCHAR, void, TRUNCATE> {
  template <typename T>
  static std::string cast(const T& val) {
    if constexpr (
        TRUNCATE && (std::is_same_v<T, double> || std::is_same_v<T, double>)) {
      auto stringValue = folly::to<std::string>(val);
      if (stringValue.find(".") == std::string::npos &&
          isdigit(stringValue[stringValue.length() - 1])) {
        stringValue += ".0";
      }
      return stringValue;
    }
    return folly::to<std::string>(val);
  }

  static std::string cast(const Timestamp& val) {
    return val.toString(Timestamp::Precision::kMilliseconds);
  }

  static std::string cast(const bool& val) {
    return val ? "true" : "false";
  }
};

// Allow conversions from string to TIMESTAMP type.
template <>
struct Converter<TypeKind::TIMESTAMP> {
  using T = typename TypeTraits<TypeKind::TIMESTAMP>::NativeType;

  template <typename From>
  static T cast(const From& /* v */) {
    VELOX_UNSUPPORTED("Conversion to Timestamp is not supported");
    return T();
  }

  static T cast(folly::StringPiece v) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const StringView& v) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const std::string& v) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const Date& d) {
    static const int64_t kMillisPerDay{86'400'000};
    return Timestamp::fromMillis(d.days() * kMillisPerDay);
  }
};

// Allow conversions from string to DATE type.
template <bool TRUNCATE>
struct Converter<TypeKind::DATE, void, TRUNCATE> {
  using T = typename TypeTraits<TypeKind::DATE>::NativeType;
  template <typename From>
  static T cast(const From& /* v */) {
    VELOX_UNSUPPORTED("Conversion to Date is not supported");
    return T();
  }

  static T cast(folly::StringPiece v) {
    return fromDateString(v.data(), v.size());
  }

  static T cast(const StringView& v) {
    return fromDateString(v.data(), v.size());
  }

  static T cast(const std::string& v) {
    return fromDateString(v.data(), v.size());
  }

  static T cast(const Timestamp& t) {
    static const int32_t kSecsPerDay{86'400};
    auto seconds = t.getSeconds();
    if (seconds >= 0 || seconds % kSecsPerDay == 0) {
      return Date(seconds / kSecsPerDay);
    }
    // For division with negatives, minus 1 to compensate the discarded
    // fractional part. e.g. -1/86'400 yields 0, yet it should be considered as
    // -1 day.
    return Date(seconds / kSecsPerDay - 1);
  }
};

} // namespace facebook::velox::util
