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

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

// Miscellaneous utilities regarding type, to avoid duplication
// and improve readability in places that have to reason about types.

namespace facebook {
namespace velox {

/**
 * @return true iff the type T can be used in a biased vector
 */
template <typename T>
constexpr bool admitsBias() {
  return std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t> ||
      std::is_same_v<T, int16_t>;
}

/**
 * @return true if the type is integral but not Boolean
 */
template <typename T>
constexpr bool isIntegral() {
  return std::is_integral_v<T> && !std::is_same_v<T, bool>;
}

/**
 * @return true if the type can be used in a dictionary encoding, false
 * otherwise
 */
template <typename T>
constexpr bool admitsDictionary() {
  return std::is_same_v<T, int128_t> || std::is_same_v<T, int64_t> ||
      std::is_same_v<T, double> || std::is_same_v<T, StringView> ||
      std::is_same_v<T, velox::Timestamp>;
}

/**
 * @return true if the type can be used as a MapVector key.
 */
template <typename KeyType>
constexpr bool allowsMapKey() {
  // MapVector only allows simple types with the exception of floating
  // point values.
  return std::is_same_v<KeyType, int8_t> || std::is_same_v<KeyType, uint8_t> ||
      std::is_same_v<KeyType, int16_t> || std::is_same_v<KeyType, uint16_t> ||
      std::is_same_v<KeyType, int32_t> || std::is_same_v<KeyType, uint32_t> ||
      std::is_same_v<KeyType, int64_t> || std::is_same_v<KeyType, uint64_t> ||
      std::is_same_v<KeyType, bool> || std::is_same_v<KeyType, StringView>;
}

/**
 * @return true if delta (absolute difference between max and min values)
 * is small enough to use bias for the given type
 */
template <typename T>
inline bool deltaAllowsBias(uint64_t delta) {
  return false;
}

template <>
inline bool deltaAllowsBias<int64_t>(uint64_t delta) {
  return delta <= std::numeric_limits<uint32_t>::max();
}

template <>
inline bool deltaAllowsBias<int32_t>(uint64_t delta) {
  return delta <= std::numeric_limits<uint16_t>::max();
}

template <>
inline bool deltaAllowsBias<int16_t>(uint64_t delta) {
  return delta <= std::numeric_limits<uint8_t>::max();
}

} // namespace velox
} // namespace facebook
