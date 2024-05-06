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

#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::wave {

class CpuBucket {
 public:
#if XSIMD_WITH_SSE2
  using TagVector = xsimd::batch<uint8_t, xsimd::sse2>;
#elif XSIMD_WITH_NEON
  using TagVector = xsimd::batch<uint8_t, xsimd::neon>;
#endif

  auto loadTags() {
#if XSIMD_WITH_SSE2
    return TagVector(_mm_loadu_si128(reinterpret_cast<__m128i const*>(tags_)));
#elif XSIMD_WITH_NEON
    return TagVector(vld1q_u8(tags_));
#endif
  }

  void setTag(int32_t idx, uint8_t tag) {
    tags_[idx] = tag;
  }

  static inline uint16_t matchTags(TagVector tags, uint8_t tag) {
    auto flags = TagVector::broadcast(tag) == tags;
    return simd::toBitMask(flags);
  }

  template <typename T>
  T* load(int32_t idx) {
    uint64_t data = *reinterpret_cast<uint64_t*>(&data_[idx * 6]);
    return reinterpret_cast<T*>(data & 0xffffffffffff);
  }

  void store(uint32_t idx, void* row) {
    auto uptr = reinterpret_cast<uint64_t>(row);
    uint64_t data = *reinterpret_cast<uint64_t*>(&data_[idx * 6]);
    *reinterpret_cast<uint64_t*>(&data_[idx * 6]) =
        (data & 0xffff000000000000) | uptr;
  }

 private:
  uint8_t tags_[16];
  uint8_t data_[128 - 16];
};

struct CpuHashTable {
  CpuHashTable() = default;

  CpuHashTable(int32_t numSlots, int32_t rowBytes) {
    auto numBuckets = bits::nextPowerOfTwo(numSlots) / 16;
    assert(numBuckets > 0);
    sizeMask = numBuckets - 1;
    bucketSpace.resize(numBuckets * sizeof(CpuBucket) + 64);
    buckets = reinterpret_cast<CpuBucket*>(
        bits::roundUp(reinterpret_cast<uint64_t>(bucketSpace.data()), 64));
    rows.resize(rowBytes);
  }

  std::string bucketSpace;

  CpuBucket* buckets;

  int32_t sizeMask;

  // Preallocated space for rows. Do not resize.
  std::string rows;

  // Number of used bytes in 'rows'.
  int32_t spaceUsed{0};

  // Number of entries.
  int32_t size{0};

  template <typename T>
  T* newRow() {
    auto size = sizeof(T);
    if (spaceUsed + size > rows.size()) {
      return nullptr;
    }
    auto row = reinterpret_cast<T*>(rows.data() + spaceUsed);
    spaceUsed += size;
    return row;
  }

  template <typename RowType, typename Ops>
  RowType* find(int64_t key, uint64_t h, Ops ops) const {
    uint8_t tag = 0x80 | (h >> 32);
    int32_t bucketIdx = h & sizeMask;
    for (;;) {
      auto tags = buckets[bucketIdx].loadTags();
      auto hits = CpuBucket::matchTags(tags, tag);
      while (hits) {
        auto idx = bits::getAndClearLastSetBit(hits);
        auto row = buckets[bucketIdx].load<RowType>(idx);
        if (ops.compare1(this, row, key)) {
          return row;
        }
      }
      auto misses = CpuBucket::matchTags(tags, 0);
      if (misses) {
        return nullptr;
      }
      bucketIdx = (1 + bucketIdx) & sizeMask;
    }
  }

  template <typename RowType, typename Ops>
  void updatingProbe(int32_t numRows, HashProbe* probe, Ops ops) {
    for (auto i = 0; i < numRows; ++i) {
      auto h = probe->hashes[i];
      uint8_t tag = 0x80 | (h >> 32);
      auto bucketIdx = h & sizeMask;
      for (;;) {
        auto tags = buckets[bucketIdx].loadTags();
        auto hits = CpuBucket::matchTags(tags, tag);
        while (hits) {
          auto idx = bits::getAndClearLastSetBit(hits);
          auto row = buckets[bucketIdx].load<RowType>(idx);
          if (ops.compare(this, row, i, probe)) {
            ops.update(this, row, i, probe);
            goto done;
          }
        }
        auto misses = CpuBucket::matchTags(tags, 0);
        if (misses) {
          int32_t idx = bits::getAndClearLastSetBit(misses);
          buckets[bucketIdx].setTag(idx, tag);
          auto* newRow = ops.newRow(this, i, probe);
          buckets[bucketIdx].store(idx, newRow);
          ++size;
          ops.update(this, newRow, i, probe);
          break;
        }
        bucketIdx = (bucketIdx + 1) & sizeMask;
      }
    done:;
    }
  }

  void check() {
    for (auto i = 0; i <= sizeMask; ++i) {
      for (auto j = 0; j < 16; j++) {
        auto row = buckets[i].load<char>(j);
        if (!row || (row >= rows.data() && row < rows.data() + rows.size())) {
          continue;
        }
        VELOX_FAIL();
      }
    }
  }
};

} // namespace facebook::velox::wave
