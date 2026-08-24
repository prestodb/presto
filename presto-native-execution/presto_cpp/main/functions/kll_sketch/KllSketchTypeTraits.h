/*
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

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include "DataSketches/kll_sketch.hpp"
#include "velox/common/base/Exceptions.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/StringView.h"

namespace facebook::presto::functions::kll_sketch {

template <typename T>
struct SketchTypeMapper {
  using type = T;

  static T toSketchType(const T& value) {
    return value;
  }
};

// Java serializes KllItemsSketch<Boolean> with ArrayOfBooleansSerDe
// (bit-packed: 8 booleans per byte, LSB-first). The C++ kll_sketch<bool> uses 1
// byte/bool. kll_sketch::serialize() hard-codes a 1-byte-per-item buffer for
// arithmetic types (including bool) and throws on size mismatch, so a custom
// SerDe cannot fix this. We transcode outside the library: serialize to native,
// replace the items payload with bit-packed bytes; deserialize by expanding
// bit-packed bytes back to 1-byte-per-bool.

namespace detail {
// KLL preamble constants (must match kll_sketch.hpp).
static constexpr size_t KLL_PREAMBLE_SHORT = 8; // empty/single-item
static constexpr size_t KLL_PREAMBLE_FULL =
    20; // multi-item header (before levels)
static constexpr uint8_t KLL_FLAGS_IS_EMPTY = 1u << 0;
static constexpr uint8_t KLL_FLAGS_IS_SINGLE_ITEM = 1u << 2;

// Encodes N 1-byte booleans to bit-packed bytes (LSB-first). Returns ceil(N/8).
inline size_t boolsToBitPacked(const uint8_t* src, size_t N, uint8_t* dst) {
  const size_t numBytes = (N + 7) / 8;
  std::fill(dst, dst + numBytes, 0);
  for (size_t i = 0; i < N; ++i) {
    if (src[i]) {
      dst[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
    }
  }
  return numBytes;
}

// Expands ceil(N/8) bit-packed bytes to N 1-byte booleans (LSB-first).
inline void bitPackedToBools(const uint8_t* src, size_t N, uint8_t* dst) {
  for (size_t i = 0; i < N; ++i) {
    dst[i] = (src[i / 8] >> (i % 8)) & 1u;
  }
}
} // namespace detail

// Serializes kll_sketch<bool> to Java's ArrayOfBooleansSerDe (bit-packed) wire
// format.
inline std::vector<uint8_t> serializeBoolSketch(
    const datasketches::kll_sketch<bool>& sketch) {
  auto native = sketch.serialize();

  if (native.size() < detail::KLL_PREAMBLE_SHORT) {
    return std::vector<uint8_t>(native.begin(), native.end());
  }

  const uint8_t flags = native[3];
  const bool isEmpty = (flags & detail::KLL_FLAGS_IS_EMPTY) != 0;
  const bool isSingleItem = (flags & detail::KLL_FLAGS_IS_SINGLE_ITEM) != 0;

  // Empty/single-item sketches are identical in both formats.
  if (isEmpty || isSingleItem) {
    return std::vector<uint8_t>(native.begin(), native.end());
  }

  if (native.size() < detail::KLL_PREAMBLE_FULL) {
    throw std::logic_error("bool sketch native bytes too short for preamble");
  }
  const uint8_t numLevels = native[18];
  const size_t levelsBytes = static_cast<size_t>(numLevels) * sizeof(uint32_t);
  const size_t itemsStart = detail::KLL_PREAMBLE_FULL + levelsBytes;

  if (native.size() < itemsStart + 2) {
    throw std::logic_error("bool sketch native bytes too short for items");
  }
  const size_t N = native.size() - itemsStart - 2; // subtract min + max
  const size_t packedN = (N + 7) / 8;
  const size_t javaSize = itemsStart + 2 + packedN;
  std::vector<uint8_t> out(javaSize);
  std::memcpy(out.data(), native.data(), itemsStart + 2);
  detail::boolsToBitPacked(
      native.data() + itemsStart + 2, N, out.data() + itemsStart + 2);

  return out;
}

// Deserializes Java's ArrayOfBooleansSerDe (bit-packed) wire format to
// kll_sketch<bool>.
inline datasketches::kll_sketch<bool> deserializeBoolSketch(
    const void* bytes,
    size_t size) {
  if (size < detail::KLL_PREAMBLE_SHORT) {
    throw std::out_of_range("bool sketch bytes too short");
  }
  const auto* b = static_cast<const uint8_t*>(bytes);
  const uint8_t flags = b[3];
  const bool isEmpty = (flags & detail::KLL_FLAGS_IS_EMPTY) != 0;
  const bool isSingleItem = (flags & detail::KLL_FLAGS_IS_SINGLE_ITEM) != 0;

  // Empty/single-item sketches are identical in both formats.
  if (isEmpty || isSingleItem) {
    return datasketches::kll_sketch<bool>::deserialize(bytes, size);
  }

  if (size < detail::KLL_PREAMBLE_FULL) {
    throw std::out_of_range("bool sketch bytes too short for full preamble");
  }
  const uint8_t numLevels = b[18];
  const size_t levelsBytes = static_cast<size_t>(numLevels) * sizeof(uint32_t);
  const size_t itemsStart = detail::KLL_PREAMBLE_FULL + levelsBytes;

  if (size < itemsStart + 2) {
    throw std::out_of_range("bool sketch bytes too short for min/max");
  }

  // numRetained = compute_total_capacity(k, m, numLevels) - levels[0].
  if (size < detail::KLL_PREAMBLE_FULL + levelsBytes) {
    throw std::out_of_range("bool sketch bytes too short for levels");
  }
  uint32_t levels0Val;
  std::memcpy(&levels0Val, b + detail::KLL_PREAMBLE_FULL, sizeof(levels0Val));

  uint16_t k;
  std::memcpy(&k, b + 4, sizeof(k));
  const uint8_t m = b[6];
  const uint32_t capacity =
      datasketches::kll_helper::compute_total_capacity(k, m, numLevels);
  const uint32_t numRetained = capacity - levels0Val;
  const size_t nativeSize = itemsStart + 2 + numRetained;
  std::vector<uint8_t> expanded(nativeSize);
  std::memcpy(expanded.data(), bytes, itemsStart + 2);
  detail::bitPackedToBools(
      b + itemsStart + 2, numRetained, expanded.data() + itemsStart + 2);

  return datasketches::kll_sketch<bool>::deserialize(
      expanded.data(), expanded.size());
}

namespace detail {
// String specializations for StringView (aggregates) and Varchar (scalars).
inline std::string stringViewToString(const velox::StringView& value) {
  return std::string(value.data(), value.size());
}
} // namespace detail

// StringView: used directly by aggregate functions.
template <>
struct SketchTypeMapper<velox::StringView> {
  using type = std::string;

  static std::string toSketchType(const velox::StringView& value) {
    return detail::stringViewToString(value);
  }
};

// Varchar: used by scalar functions. arg_type<Varchar> resolves to StringView
// at runtime.
template <>
struct SketchTypeMapper<velox::Varchar> {
  using type = std::string;

  static std::string toSketchType(const velox::StringView& value) {
    return detail::stringViewToString(value);
  }
};

// Deserializes a KLL sketch, translating std::exception from DataSketches or
// bool transcoding into VELOX_USER_FAIL so corrupt user bytes surface as user
// errors.
template <typename SketchType>
inline datasketches::kll_sketch<SketchType> deserializeSketch(
    const void* data,
    size_t size) {
  try {
    if constexpr (std::is_same_v<SketchType, bool>) {
      return deserializeBoolSketch(data, size);
    } else {
      return datasketches::kll_sketch<SketchType>::deserialize(data, size);
    }
  } catch (const std::out_of_range& e) {
    VELOX_USER_FAIL(
        "Invalid KLL sketch data - buffer out of range: {}", e.what());
  } catch (const std::logic_error& e) {
    VELOX_USER_FAIL(
        "Failed to deserialize KLL sketch - corrupted data or logic error: {}",
        e.what());
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Failed to deserialize KLL sketch: {}", e.what());
  }
}

} // namespace facebook::presto::functions::kll_sketch
