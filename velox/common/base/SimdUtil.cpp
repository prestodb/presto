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

#include "velox/common/base/SimdUtil.h"
#include <folly/Preprocessor.h>

namespace facebook::velox::simd {

using V32 = Vectors<int32_t>;
using V64 = Vectors<int64_t>;

int64_t V64::int64Masks_[16][4];
int64_t V64::int64LeadingMasks_[4][4];
int32_t V64::int64PermuteIndices_[16][8];
int32_t V32::byteSetBits_[256][8];
int32_t V32::int32LeadingMasks_[8][8];
int32_t V32::int32Masks_[256][8];
int32_t V32::iota_[8] = {0, 1, 2, 3, 4, 5, 6, 7};

void V32::initialize() {
  for (int32_t i = 0; i < 256; ++i) {
    int32_t* entry = byteSetBits_[i];
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
  for (auto i = 0; i < 256; ++i) {
    for (auto bit = 0; bit < 8; ++bit) {
      int32Masks_[i][bit] = (i & (1 << bit)) ? -1 : 0;
    }
  }
  for (auto size = 0; size < Vectors<int32_t>::VSize; ++size) {
    for (auto i = 0; i < size; ++i) {
      int32LeadingMasks_[size][i] = -1;
    }
  }
}

void V64::initialize() {
  for (auto i = 0; i < 16; ++i) {
    int32_t* result = int64PermuteIndices_[i];
    int32_t fill = 0;
    for (auto bit = 0; bit < 4; ++bit) {
      if (i & (1 << bit)) {
        result[fill++] = bit * 2;
        result[fill++] = bit * 2 + 1;
      }
    }
    for (; fill < 8; ++fill) {
      result[fill] = fill;
    }
  }
  for (auto i = 0; i < 16; ++i) {
    for (auto bit = 0; bit < 4; ++bit) {
      int64Masks_[i][bit] = (i & (1 << bit)) ? -1 : 0;
    }
  }
  for (auto size = 0; size < V64::VSize; ++size) {
    for (auto i = 0; i < size; ++i) {
      int64LeadingMasks_[size][i] = -1;
    }
  }
}

bool initializeSimdUtil() {
  static bool inited = false;
  if (inited) {
    return true;
  }
  V32::initialize();
  V64::initialize();
  inited = true;
  return true;
}

int32_t indicesOfSetBits(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    int32_t* result) {
  if (end <= begin) {
    return 0;
  }
  int32_t row = begin & ~63;
  auto originalResult = result;
  int32_t endWord = bits::roundUp(end, 64) / 64;
  auto firstWord = begin / 64;
  for (auto wordIndex = firstWord; wordIndex < endWord; ++wordIndex) {
    uint64_t word = bits[wordIndex];
    if (!word) {
      row += 64;
      continue;
    }
    if (wordIndex == firstWord && begin) {
      word &= bits::highMask(64 - (begin - firstWord * 64));
      if (!word) {
        row += 64;
        continue;
      }
    }
    if (wordIndex == endWord - 1) {
      int32_t lastBits = end - (endWord - 1) * 64;
      if (lastBits < 64) {
        word &= bits::lowMask(lastBits);
        if (!word) {
          break;
        }
      }
    }
    if (result - originalResult < (row >> 2)) {
      do {
        *result++ = __builtin_ctzll(word) + row;
        word = word & (word - 1);
      } while (word);
      row += 64;
    } else {
      for (auto byteCnt = 0; byteCnt < 8; ++byteCnt) {
        uint8_t byte = word;
        word = word >> 8;
        if (byte) {
          __m256si indices = V32::load(&V32::byteSetBits()[byte]);
          V32::store(result, indices + row);
          result += __builtin_popcount(byte);
        }
        row += 8;
      }
    }
  }
  return result - originalResult;
}

static bool FB_ANONYMOUS_VARIABLE(g_simdConstants) = initializeSimdUtil();

} // namespace facebook::velox::simd
