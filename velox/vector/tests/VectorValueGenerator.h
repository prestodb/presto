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

#include <folly/Random.h>
#include <limits>
#include <optional>
#include <type_traits>

#include "velox/buffer/StringViewBufferHolder.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"

namespace facebook::velox::test {

class VectorValueGenerator {
 private:
  static uint32_t getRand32(
      std::optional<folly::Random::DefaultGenerator>& rng,
      uint32_t max = std::numeric_limits<uint32_t>::max()) {
    if (rng.has_value()) {
      return folly::Random::rand32(max, rng.value());
    } else {
      return folly::Random::rand32(max);
    }
  }

  static uint64_t getRand64(
      std::optional<folly::Random::DefaultGenerator>& rng,
      uint64_t max = std::numeric_limits<uint64_t>::max()) {
    if (rng.has_value()) {
      return folly::Random::rand64(max, rng.value());
    } else {
      return folly::Random::rand64(max);
    }
  }

  static double getRandDouble(
      std::optional<folly::Random::DefaultGenerator>& rng) {
    auto max = static_cast<double>(std::numeric_limits<int32_t>::max());
    auto min = static_cast<double>(std::numeric_limits<int32_t>::min());
    if (rng.has_value()) {
      return folly::Random::randDouble(min, max, rng.value());
    } else {
      return folly::Random::randDouble(min, max);
    }
  }

 public:
  template <typename T>
  static T cardValueOf(
      bool useFullTypeRange,
      std::optional<folly::Random::DefaultGenerator>& rng,
      StringViewBufferHolder& stringViewBufferHolder,
      std::optional<uint32_t> fixedWidthStringSize = std::nullopt) {
    if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
      return useFullTypeRange ? getRand64(rng) : getRand32(rng);
    } else if constexpr (
        std::is_integral<T>::value && !std::is_same_v<T, bool>) {
      // The other integral cases, signed and unsigned
      auto max = std::numeric_limits<T>::max();
      if (!useFullTypeRange) {
        if constexpr (
            std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t>) {
          max = std::numeric_limits<int16_t>::max();
        } else if constexpr (
            std::is_same_v<T, int16_t> || std::is_same_v<T, uint16_t>) {
          max = std::numeric_limits<int8_t>::max();
        } else {
          max = std::numeric_limits<int8_t>::max() / 2;
        }
      }
      return getRand32(rng, max);
    } else if constexpr (std::is_same_v<T, double>) {
      return getRandDouble(rng);
    } else if constexpr (std::is_same_v<T, StringView>) {
      auto str = std::string(
          fixedWidthStringSize.has_value() ? fixedWidthStringSize.value()
                                           : getRand32(rng) % 100 + 1,
          'a' + (getRand32(rng) % 26));
      return stringViewBufferHolder.getOwnedValue(StringView(str));
    } else if constexpr (std::is_same_v<T, bool>) {
      return getRand32(rng) % 2 == 0;
    } else {
      VELOX_UNSUPPORTED("Invalid type");
    }
  }

  template <typename T>
  static T nullValueOf() {
    if constexpr (std::is_arithmetic<T>::value) {
      // integers, double and bool
      return T{};
    } else if constexpr (std::is_same_v<T, StringView>) {
      return StringView();
    } else {
      VELOX_UNSUPPORTED("Invalid type");
    }
  }
};

} // namespace facebook::velox::test
