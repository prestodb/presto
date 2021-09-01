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
#include "velox/common/base/Exceptions.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"

namespace facebook::velox::util {

template <TypeKind KIND, typename = void, bool TRUNCATE = false>
struct Converter {
  template <typename T>
  static typename TypeTraits<KIND>::NativeType cast(T val) {
    VELOX_NYI();
  }
};

template <>
struct Converter<TypeKind::BOOLEAN> {
  using T = bool;

  template <typename From>
  static T cast(const From& v) {
    VELOX_NYI();
  }

  static T cast(const folly::StringPiece& v) {
    return folly::to<T>(v);
  }

  static T cast(const StringView& v) {
    return folly::to<T>(folly::StringPiece(v));
  }

  static T cast(const std::string& v) {
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
  static T cast(const From& v) {
    VELOX_NYI();
  }

  static T cast(const folly::StringPiece& v) {
    try {
      return folly::to<T>(v);
    } catch (const std::exception& e) {
      throw std::invalid_argument(e.what());
    }
  }

  static T cast(const StringView& v) {
    try {
      return folly::to<T>(folly::StringPiece(v));
    } catch (const std::exception& e) {
      throw std::invalid_argument(e.what());
    }
  }

  static T cast(const std::string& v) {
    try {
      return folly::to<T>(v);
    } catch (const std::exception& e) {
      throw std::invalid_argument(e.what());
    }
  }

  static T cast(const bool& v) {
    return folly::to<T>(v);
  }

  // TODO: implement templated helper method for all floats.
  static T cast(const float& v) {
    if (std::isnan(v)) {
      throw std::invalid_argument("Cannot cast NaN to an integral value.");
    }
    if constexpr (TRUNCATE) {
      VELOX_CHECK(
          std::numeric_limits<T>::min() <= v &&
          v <= std::numeric_limits<T>::max());
      return folly::to<T>(T(v));
    } else {
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const double& v) {
    if (std::isnan(v)) {
      throw std::invalid_argument("Cannot cast NaN to an integral value.");
    }
    if constexpr (TRUNCATE) {
      VELOX_CHECK(
          std::numeric_limits<T>::min() <= v &&
          v <= std::numeric_limits<T>::max());
      return folly::to<T>(T(v));
    } else {
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
      throw std::invalid_argument(e.what());
    }
  }

  static T cast(const folly::StringPiece& v) {
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
    return cast<double>(v);
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
};

template <bool TRUNCATE>
struct Converter<TypeKind::VARCHAR, void, TRUNCATE> {
  template <typename T>
  static std::string cast(const T& val) {
    return folly::to<std::string>(val);
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
    VELOX_NYI();
  }

  static T cast(folly::StringPiece v) {
    return fromTimestampString(v.data(), v.size());
  }

  static T cast(const StringView& v) {
    return fromTimestampString(v);
  }

  static T cast(const std::string& v) {
    return fromTimestampString(v);
  }
};

} // namespace facebook::velox::util
