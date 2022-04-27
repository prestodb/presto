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
  // nullOutput API requires that the user has already set nullOutput to
  // false as default to avoid having to reset it in each cast function for now
  // If in the future we change nullOutput in many functions we can revisit that
  // contract.
  static typename TypeTraits<KIND>::NativeType cast(T val, bool& nullOutput) {
    VELOX_NYI();
  }
};

template <>
struct Converter<TypeKind::BOOLEAN> {
  using T = bool;

  template <typename From>
  static T cast(const From& v, bool& nullOutput) {
    return folly::to<T>(v);
  }

  static T cast(const folly::StringPiece& v, bool& nullOutput) {
    return folly::to<T>(v);
  }

  static T cast(const StringView& v, bool& nullOutput) {
    return folly::to<T>(folly::StringPiece(v));
  }

  static T cast(const std::string& v, bool& nullOutput) {
    return folly::to<T>(v);
  }
};

template <TypeKind KIND, bool TRUNCATE>
struct Converter<
    KIND,
    std::enable_if_t<
        KIND == TypeKind::BOOLEAN || KIND == TypeKind::TINYINT ||
            KIND == TypeKind::SMALLINT || KIND == TypeKind::INTEGER ||
            KIND == TypeKind::BIGINT,
        void>,
    TRUNCATE> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static T cast(const From& v, bool& nullOutput) {
    VELOX_NYI();
  }

  static T convertStringToInt(const folly::StringPiece& v, bool& nullOutput) {
    // Handling boolean target case fist because it is in this scope
    if constexpr (std::is_same_v<T, bool>) {
      return folly::to<T>(v);
    } else {
      // Handling integer target cases
      nullOutput = true;
      bool negative = false;
      T result = 0;
      int index = 0;
      int len = v.size();
      if (len == 0) {
        return -1;
      }
      // Setting negative flag
      if (v[0] == '-') {
        if (len == 1) {
          return -1;
        }
        negative = true;
        index = 1;
      }
      if (negative) {
        for (; index < len; index++) {
          if (!std::isdigit(v[index])) {
            return -1;
          }
          result = result * 10 - (v[index] - '0');
          // Overflow check
          if (result > 0) {
            return -1;
          }
        }
      } else {
        for (; index < len; index++) {
          if (!std::isdigit(v[index])) {
            return -1;
          }
          result = result * 10 + (v[index] - '0');
          // Overflow check
          if (result < 0) {
            return -1;
          }
        }
      }
      // Final result
      nullOutput = false;
      return result;
    }
  }

  static T cast(const folly::StringPiece& v, bool& nullOutput) {
    try {
      if constexpr (TRUNCATE) {
        return convertStringToInt(v, nullOutput);
      } else {
        return folly::to<T>(v);
      }
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const StringView& v, bool& nullOutput) {
    try {
      if constexpr (TRUNCATE) {
        return convertStringToInt(folly::StringPiece(v), nullOutput);
      } else {
        return folly::to<T>(folly::StringPiece(v));
      }
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const std::string& v, bool& nullOutput) {
    try {
      if constexpr (TRUNCATE) {
        return convertStringToInt(v, nullOutput);
      } else {
        return folly::to<T>(v);
      }
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const bool& v, bool& nullOutput) {
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

  static T cast(const float& v, bool& nullOutput) {
    if constexpr (TRUNCATE) {
      if (std::isnan(v)) {
        return 0;
      }
      if (v > LimitType::maxLimit()) {
        return LimitType::max();
      }
      if (v < LimitType::minLimit()) {
        return LimitType::min();
      }
      return LimitType::cast(v);
    } else {
      if (std::isnan(v)) {
        throw std::invalid_argument("Cannot cast NaN to an integral value.");
      }
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const double& v, bool& nullOutput) {
    if constexpr (TRUNCATE) {
      if (std::isnan(v)) {
        return 0;
      }
      if (v > LimitType::maxLimit()) {
        return LimitType::max();
      }
      if (v < LimitType::minLimit()) {
        return LimitType::min();
      }
      return LimitType::cast(v);
    } else {
      if (std::isnan(v)) {
        throw std::invalid_argument("Cannot cast NaN to an integral value.");
      }
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const int8_t& v, bool& nullOutput) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int16_t& v, bool& nullOutput) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int32_t& v, bool& nullOutput) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int64_t& v, bool& nullOutput) {
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
  static T cast(const From& v, bool& nullOutput) {
    try {
      return folly::to<T>(v);
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }

  static T cast(const folly::StringPiece& v, bool& nullOutput) {
    return cast<folly::StringPiece>(v, nullOutput);
  }

  static T cast(const StringView& v, bool& nullOutput) {
    return cast<folly::StringPiece>(folly::StringPiece(v), nullOutput);
  }

  static T cast(const std::string& v, bool& nullOutput) {
    return cast<std::string>(v, nullOutput);
  }

  static T cast(const bool& v, bool& nullOutput) {
    return cast<bool>(v, nullOutput);
  }

  static T cast(const float& v, bool& nullOutput) {
    return cast<float>(v, nullOutput);
  }

  static T cast(const double& v, bool& nullOutput) {
    if constexpr (TRUNCATE) {
      return T(v);
    } else {
      return cast<double>(v, nullOutput);
    }
  }

  static T cast(const int8_t& v, bool& nullOutput) {
    return cast<int8_t>(v, nullOutput);
  }

  static T cast(const int16_t& v, bool& nullOutput) {
    return cast<int16_t>(v, nullOutput);
  }

  // Convert integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int32_t& v, bool& nullOutput) {
    return static_cast<T>(v);
  }

  // Convert large integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int64_t& v, bool& nullOutput) {
    return static_cast<T>(v);
  }
};

template <bool TRUNCATE>
struct Converter<TypeKind::VARCHAR, void, TRUNCATE> {
  template <typename T>
  static std::string cast(const T& val, bool& nullOutput) {
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

  static std::string cast(const bool& val, bool& nullOutput) {
    return val ? "true" : "false";
  }
};

// Allow conversions from string to TIMESTAMP type.
template <>
struct Converter<TypeKind::TIMESTAMP> {
  using T = typename TypeTraits<TypeKind::TIMESTAMP>::NativeType;

  template <typename From>
  static T cast(const From& /* v */, bool& nullOutput) {
    VELOX_NYI();
  }

  static T cast(folly::StringPiece v, bool& nullOutput) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const StringView& v, bool& nullOutput) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const std::string& v, bool& nullOutput) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const Date& d, bool& nullOutput) {
    static const int64_t kMillisPerDay{86'400'000};
    return Timestamp::fromMillis(d.days() * kMillisPerDay);
  }
};

// Allow conversions from string to DATE type.
template <>
struct Converter<TypeKind::DATE> {
  using T = typename TypeTraits<TypeKind::DATE>::NativeType;
  template <typename From>
  static T cast(const From& /* v */, bool& nullOutput) {
    VELOX_NYI();
  }

  static T cast(folly::StringPiece v, bool& nullOutput) {
    return fromDateString(v.data(), v.size());
  }

  static T cast(const StringView& v, bool& nullOutput) {
    return fromDateString(v.data(), v.size());
  }

  static T cast(const std::string& v, bool& nullOutput) {
    return fromDateString(v.data(), v.size());
  }

  static T cast(const Timestamp& t, bool& nullOutput) {
    static const int32_t kSecsPerDay{86'400};
    return Date(t.getSeconds() / kSecsPerDay);
  }
};

} // namespace facebook::velox::util
