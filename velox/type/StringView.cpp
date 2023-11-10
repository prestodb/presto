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

#include "velox/type/StringView.h"
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox {

namespace {
int32_t linearSearchSimple(
    StringView key,
    const StringView* strings,
    const int32_t* indices,
    int32_t numStrings) {
  if (indices) {
    for (auto i = 0; i < numStrings; ++i) {
      if (strings[indices[i]] == key) {
        return i;
      }
    }
  } else {
    for (auto i = 0; i < numStrings; ++i) {
      if (strings[i] == key) {
        return i;
      }
    }
  }
  return -1;
}

} // namespace

// static
int32_t StringView::linearSearch(
    StringView key,
    const StringView* strings,
    const int32_t* indices,
    int32_t numStrings) {
#if XSIMD_WITH_AVX2
  constexpr int64_t kBatch = xsimd::batch<uint64_t>::size;
  bool isInline = key.isInline();
  bool headOnly = key.size() <= 4;
  const char* body = key.data() + 4;
  int32_t bodySize = key.size() - 4;
  int32_t limit = numStrings & ~(kBatch - 1); // round down to full batches.
  if (indices) {
    uint64_t head = *reinterpret_cast<const uint64_t*>(&key);
    uint64_t inlined = reinterpret_cast<const uint64_t*>(&key)[1];
    xsimd::batch<int32_t, xsimd::sse2> indexVector;

    for (auto i = 0; i < limit; i += kBatch) {
      indexVector = simd::loadGatherIndices<uint64_t, int32_t>(indices + i)
          << 1;
      auto heads =
          simd::gather(reinterpret_cast<const uint64_t*>(strings), indexVector);
      uint16_t hits = simd::toBitMask(heads == head);
      if (LIKELY(!hits)) {
        continue;
      }
      if (headOnly) {
        return i + __builtin_ctz(hits);
      }
      while (hits) {
        auto offset = bits::getAndClearLastSetBit(hits);
        if (isInline ? inlined ==
                    reinterpret_cast<const uint64_t*>(
                           &strings[indices[i + offset]])[1]
                     : simd::memEqualUnsafe(
                           body,
                           strings[indices[i + offset]].data() + 4,
                           bodySize)) {
          return i + offset;
        }
      }
    }
    return linearSearchSimple(
        key, strings, indices + limit, numStrings - limit);
  } else {
    StringView key2[2];
    memcpy(&key2[0], &key, sizeof(key));
    memcpy(&key2[1], &key, sizeof(key));
    auto keyVector = xsimd::load_unaligned(reinterpret_cast<uint64_t*>(&key2));
    for (auto i = 0; i < limit; i += kBatch, strings += kBatch) {
      // Compare  4 StringViews  in 2 loads of 2 each.
      int32_t bits =
          simd::toBitMask(
              xsimd::load_unaligned(
                  reinterpret_cast<const uint64_t*>(strings)) == keyVector) |
          (simd::toBitMask(
               xsimd::load_unaligned(
                   reinterpret_cast<const uint64_t*>(strings + 2)) == keyVector)
           << 4);

      if ((bits & (1 + 4 + 16 + 64)) == 0) {
        // Neither lane 0 or 2 or 4 or 6 hits
        continue;
      }
      int offset = i;
      while (bits) {
        auto low = bits & 3;
        // At least first lane must match.
        if (low & 1) {
          // Both first and second lane match or only first word counts.
          if (low == 3 || headOnly) {
            return offset;
          }
          if (!isInline) {
            if (simd::memEqualUnsafe(
                    body, strings[offset].data() + 4, bodySize)) {
              return offset;
            }
          }
        }
        bits = bits >> 2;
        ++offset;
      }
    }
    return linearSearchSimple(key, strings, nullptr, numStrings - limit);
  }
#else
  return linearSearchSimple(key, strings, indices, numStrings);
#endif
}
} // namespace facebook::velox
