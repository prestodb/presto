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

#include "velox/common/base/SimdUtil.h"
#include <folly/Preprocessor.h>

namespace facebook::velox::simd {

void gatherBits(
    const uint64_t* bits,
    folly::Range<const int32_t*> indexRange,
    uint64_t* result) {
  constexpr int32_t kStep = xsimd::batch<int32_t>::size;
  const auto size = indexRange.size();
  auto indices = indexRange.data();
  uint8_t* resultPtr = reinterpret_cast<uint8_t*>(result);
  if (FOLLY_LIKELY(size < 5)) {
    uint8_t smallResult = 0;
    for (auto i = 0; i < size; ++i) {
      smallResult |= static_cast<uint8_t>(bits::isBitSet(bits, indices[i]))
          << i;
    }
    *resultPtr = smallResult;
    return;
  }

  int32_t i = 0;
  for (; i + kStep < size; i += kStep) {
    uint16_t flags =
        simd::gather8Bits(bits, xsimd::load_unaligned(indices + i), kStep);
    bits::storeBitsToByte<kStep>(flags, resultPtr, i);
  }
  const auto bitsLeft = size - i;
  if (bitsLeft > 0) {
    uint16_t flags =
        simd::gather8Bits(bits, xsimd::load_unaligned(indices + i), bitsLeft);
    bits::storeBitsToByte<kStep>(flags, resultPtr, i);
  }
}

namespace detail {

alignas(kPadding) int32_t byteSetBits[256][8];
alignas(kPadding) int32_t permute4x64Indices[16][8];

const LeadingMask<int32_t, xsimd::default_arch> leadingMask32;
const LeadingMask<int64_t, xsimd::default_arch> leadingMask64;

const FromBitMask<int32_t, xsimd::default_arch> fromBitMask32;
const FromBitMask<int64_t, xsimd::default_arch> fromBitMask64;

} // namespace detail

namespace {

void initByteSetBits() {
  for (int32_t i = 0; i < 256; ++i) {
    int32_t* entry = detail::byteSetBits[i];
    int32_t fill = 0;
    for (auto b = 0; b < 8; ++b) {
      if (i & (1 << b)) {
        entry[fill++] = b;
      }
    }
    for (; fill < 8; ++fill) {
      entry[fill] = fill;
    }
  }
}

void initPermute4x64Indices() {
  for (int i = 0; i < 16; ++i) {
    int32_t* result = detail::permute4x64Indices[i];
    int32_t fill = 0;
    for (int bit = 0; bit < 4; ++bit) {
      if (i & (1 << bit)) {
        result[fill++] = bit * 2;
        result[fill++] = bit * 2 + 1;
      }
    }
    for (; fill < 8; ++fill) {
      result[fill] = fill;
    }
  }
}

} // namespace

bool initializeSimdUtil() {
  static bool inited = false;
  if (inited) {
    return true;
  }
  initByteSetBits();
  initPermute4x64Indices();
  inited = true;
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_simdConstants) = initializeSimdUtil();

} // namespace facebook::velox::simd
