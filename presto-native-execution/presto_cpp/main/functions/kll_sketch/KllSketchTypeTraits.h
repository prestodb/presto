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

// ============================================================================
// Java-compatible boolean KLL sketch serialization
//
// Java serializes KllItemsSketch<Boolean> with ArrayOfBooleansSerDe, which
// bit-packs the items array (8 booleans per byte, LSB-first: item[i] is bit
// (i % 8) of byte (i / 8)).  Single items (min/max) are each 1 byte (0 or 1).
//
// The DataSketches C++ library hard-codes the wire-size calculation for
// arithmetic types as sizeof(T) * count, so there is no way to inject a
// bit-packing serde through the normal template path.  Instead we:
//
//  serialize:   build the sketch with the default serde (1 byte/bool), then
//               transcode the items payload in place — replace the 1-byte-per-
//               bool items section with a bit-packed section and adjust the
//               total size.
//
//  deserialize: expand the bit-packed items payload back to 1-byte-per-bool,
//               then hand the expanded bytes to the standard deserializer.
//
// Wire layout for a multi-item kll_sketch<bool> with N retained items:
//   [0]        preamble_ints (1 byte)
//   [1]        serial_version (1 byte)
//   [2]        family_id (1 byte)       == 15
//   [3]        flags_byte (1 byte)
//   [4-5]      k (uint16_t LE)
//   [6]        m (1 byte)               == 8
//   [7]        unused (1 byte)
//   [8-15]     n (uint64_t LE)
//   [16-17]    min_k (uint16_t LE)
//   [18]       num_levels (1 byte)
//   [19]       unused (1 byte)
//   [20 .. 20 + num_levels*4 - 1]  levels array (num_levels * uint32_t LE)
//   [items_start]  min item (1 byte, 0 or 1)
//   [items_start+1]  max item (1 byte, 0 or 1)
//   [items_start+2 .. items_start+2+N-1]  retained items (N bytes, 1/bool
//   native)
//                                                       or ceil(N/8) bytes
//                                                       (Java)
//
// For a single-item sketch (N==1, SINGLE_ITEM flag set):
//   [0..7]  short preamble (8 bytes)
//   [8]     the single item (1 byte) — same in both formats
//
// Empty sketches have no items and are identical in both formats.
// ============================================================================

namespace detail {
// KLL preamble constants (must match kll_sketch.hpp).
static constexpr size_t KLL_PREAMBLE_SHORT = 8; // empty / single-item
static constexpr size_t KLL_PREAMBLE_FULL =
    20; // multi-item header (before levels)
static constexpr uint8_t KLL_FLAGS_IS_EMPTY = 1u << 0;
static constexpr uint8_t KLL_FLAGS_IS_SINGLE_ITEM = 1u << 2;

// Encode N booleans (1 byte each at src[0..N-1]) to bit-packed bytes at dst.
// Returns number of bytes written = ceil(N/8).
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

// Expand bit-packed bytes at src (ceil(N/8) bytes) to N 1-byte booleans at dst.
inline void bitPackedToBools(const uint8_t* src, size_t N, uint8_t* dst) {
  for (size_t i = 0; i < N; ++i) {
    dst[i] = (src[i / 8] >> (i % 8)) & 1u;
  }
}
} // namespace detail

// Serialize kll_sketch<bool> to bytes compatible with Java's
// KllItemsSketch<Boolean> + ArrayOfBooleansSerDe wire format.
inline std::vector<uint8_t> serializeBoolSketch(
    const datasketches::kll_sketch<bool>& sketch) {
  // Step 1: get the native 1-byte-per-bool serialization.
  auto native = sketch.serialize();

  if (native.size() < detail::KLL_PREAMBLE_SHORT) {
    return std::vector<uint8_t>(native.begin(), native.end());
  }

  const uint8_t flags = native[3];
  const bool isEmpty = (flags & detail::KLL_FLAGS_IS_EMPTY) != 0;
  const bool isSingleItem = (flags & detail::KLL_FLAGS_IS_SINGLE_ITEM) != 0;

  // Empty and single-item sketches: both formats are identical
  // (no bit-packing for single items — 1 byte each in both Java and native).
  if (isEmpty || isSingleItem) {
    return std::vector<uint8_t>(native.begin(), native.end());
  }

  // Multi-item sketch.
  // Read num_levels from byte 18 of the native bytes.
  if (native.size() < detail::KLL_PREAMBLE_FULL) {
    throw std::logic_error("bool sketch native bytes too short for preamble");
  }
  const uint8_t numLevels = native[18];
  const size_t levelsBytes = static_cast<size_t>(numLevels) * sizeof(uint32_t);
  const size_t itemsStart = detail::KLL_PREAMBLE_FULL + levelsBytes;

  // Items layout in native: [min(1)] [max(1)] [retained(N bytes, 1/bool)]
  // The native size must be at least itemsStart + 2 (min + max).
  if (native.size() < itemsStart + 2) {
    throw std::logic_error("bool sketch native bytes too short for items");
  }
  const size_t nativeItemsPayload = native.size() - itemsStart;
  // nativeItemsPayload = 2 (min + max) + N (retained, 1 byte each)
  const size_t N = nativeItemsPayload - 2;

  // Step 2: build the Java-format output.
  // Header (preamble + levels) is identical; only the retained items are
  // packed.
  const size_t packedN = (N + 7) / 8;
  const size_t javaSize = itemsStart + 2 + packedN;
  std::vector<uint8_t> out(javaSize);

  // Copy preamble + levels + min + max unchanged.
  std::memcpy(out.data(), native.data(), itemsStart + 2);

  // Bit-pack the retained items.
  detail::boolsToBitPacked(
      native.data() + itemsStart + 2, // src: retained items (1 byte each)
      N,
      out.data() + itemsStart + 2); // dst: bit-packed

  return out;
}

// Deserialize bytes in Java's KllItemsSketch<Boolean> + ArrayOfBooleansSerDe
// format to a kll_sketch<bool>.
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

  // Empty / single-item: formats are identical, pass through directly.
  if (isEmpty || isSingleItem) {
    return datasketches::kll_sketch<bool>::deserialize(bytes, size);
  }

  // Multi-item: expand bit-packed items to 1-byte-per-bool.
  if (size < detail::KLL_PREAMBLE_FULL) {
    throw std::out_of_range("bool sketch bytes too short for full preamble");
  }
  const uint8_t numLevels = b[18];
  const size_t levelsBytes = static_cast<size_t>(numLevels) * sizeof(uint32_t);
  const size_t itemsStart = detail::KLL_PREAMBLE_FULL + levelsBytes;

  if (size < itemsStart + 2) {
    throw std::out_of_range("bool sketch bytes too short for min/max");
  }

  // Java items payload: [min(1)] [max(1)] [retained: ceil(N/8) bytes]
  const size_t packedBytes = size - itemsStart - 2;
  // Recover N from the sketch's stored n value (bytes 8..15, little-endian).
  uint64_t n;
  std::memcpy(&n, b + 8, sizeof(n));
  // Retained count: read from levels array.
  // levels[0] and levels[numLevels] are stored; retained = levels[numLevels] -
  // levels[0]. But levels[numLevels] is not stored (derived). We'll read
  // levels[0] from the array. Actually, it's easier to derive N from the packed
  // byte count + the sketch's N value. The KLL library reconstructs retained
  // count from levels internally; we just need to expand bit-packed bytes to
  // 1-byte-per-bool. The retained count comes from N via the levels array, but
  // we can derive: packed_bytes = ceil(retained/8), so retained <= packed_bytes
  // * 8.  We use the levels array to get the exact count. levels array:
  // numLevels uint32_t values at offset KLL_PREAMBLE_FULL.
  uint32_t levels0;
  std::memcpy(&levels0, b + detail::KLL_PREAMBLE_FULL, sizeof(levels0));
  // capacity = kll_helper::compute_total_capacity(k, m, numLevels) — not easily
  // available here.  Instead, derive retained from packed_bytes and n:
  // For correctness we read retained from n and the bit-packing:
  // retained items = exactly what the Java encoder packed.  The Java encoder
  // writes ceil(retained/8) bytes and the bit count is retained.  We can
  // recover retained exactly from the levels[numLevels] - levels[0] formula,
  // but that requires knowing capacity.  A simpler approach: read the uint32_t
  // at levels array position numLevels (which the Java encoder did NOT write,
  // but we can derive it from the total byte count since we know packedBytes =
  // ceil(retained/8)).
  //
  // Safest: retained = packedBytes * 8 would over-count.  Instead, we read the
  // compacted n and the stored levels to get the exact retained count via the
  // same formula the C++ library uses after reading levels.
  //
  // Read all numLevels uint32_t level entries.
  if (size < detail::KLL_PREAMBLE_FULL + levelsBytes) {
    throw std::out_of_range("bool sketch bytes too short for levels");
  }
  uint32_t levels0Val;
  std::memcpy(&levels0Val, b + detail::KLL_PREAMBLE_FULL, sizeof(levels0Val));

  // We need levels[numLevels] (the capacity), which is not stored.
  // Compute it the same way the library does:
  //   capacity = kll_helper::compute_total_capacity(k, m, numLevels)
  uint16_t k;
  std::memcpy(&k, b + 4, sizeof(k));
  const uint8_t m = b[6];
  const uint32_t capacity =
      datasketches::kll_helper::compute_total_capacity(k, m, numLevels);
  const uint32_t numRetained = capacity - levels0Val;

  // Build expanded (native-format) bytes: replace packed items with
  // 1-byte-per-bool.
  const size_t nativeSize = itemsStart + 2 + numRetained;
  std::vector<uint8_t> expanded(nativeSize);

  // Copy header (preamble + levels + min + max) unchanged.
  std::memcpy(expanded.data(), bytes, itemsStart + 2);

  // Expand bit-packed retained items to 1 byte each.
  detail::bitPackedToBools(
      b + itemsStart + 2, // src: packed bits
      numRetained,
      expanded.data() + itemsStart + 2); // dst: 1 byte per bool

  return datasketches::kll_sketch<bool>::deserialize(
      expanded.data(), expanded.size());
}

namespace detail {
// Both string specializations (StringView for aggregates, Varchar for scalars).
inline std::string stringViewToString(const velox::StringView& value) {
  return std::string(value.data(), value.size());
}
} // namespace detail

// Specialization for StringView: used by aggregate functions where T is
// velox::StringView directly.
template <>
struct SketchTypeMapper<velox::StringView> {
  using type = std::string;

  static std::string toSketchType(const velox::StringView& value) {
    return detail::stringViewToString(value);
  }
};

// Specialization for Varchar: used by scalar functions where T is
// velox::Varchar (the Velox simple-function type tag). arg_type<Varchar>
// resolves to StringView at runtime, so toSketchType takes a StringView.
template <>
struct SketchTypeMapper<velox::Varchar> {
  using type = std::string;

  static std::string toSketchType(const velox::StringView& value) {
    return detail::stringViewToString(value);
  }
};

} // namespace facebook::presto::functions::kll_sketch
