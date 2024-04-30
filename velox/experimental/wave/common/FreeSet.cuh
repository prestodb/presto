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

namespace facebook::velox::wave {

template <typename T, int32_t kSize>
class FreeSet {
 public:
  static constexpr uint32_t kEmpty = ~0;
  static constexpr int32_t kBitSizeMask = (kSize / 64) - 1;
  static constexpr int32_t kSizeMask = kSize - 1;

  void __device__ clear() {
    for (auto i = threadIdx.x; i < kSize; i += blockDim.x) {
      if (i < sizeof(bits_) / sizeof(bits_[0])) {
        bits_[i] = 0;
      }
      items_[i] = kEmpty;
    }
  }

  // Adds an item. Returns true if succeededs.
  bool __device__ put(T item) {
    if (full_) {
      return false;
    }
    auto tid = threadIdx.x + blockDim.x * blockIdx.x;
    auto bitIdx = tid & kBitSizeMask;
    for (auto count = 0; count <= kBitSizeMask; ++count) {
      auto word = ~bits_[bitIdx];
      while (word) {
        auto bit = __ffsll(word);
        --bit;
        if (kEmpty == atomicCAS(&items_[bitIdx * 64 + bit], kEmpty, item)) {
          atomicOr(&bits_[bitIdx], 1UL << bit);
          if (empty_) {
            atomicExch(&empty_, 0);
          }
          return true;
        }
        word &= word - 1;
      }
      bitIdx = bitIdx + 1 & kBitSizeMask;
    }
    atomicExch(&full_, 1);
    return false;
  }

  T __device__ get() {
    if (empty_) {
      return kEmpty;
    }

    auto tid = threadIdx.x + blockDim.x * blockIdx.x;
    auto bitIdx = tid & kBitSizeMask;
    for (auto count = 0; count <= kBitSizeMask; ++count) {
      auto word = bits_[bitIdx];
      while (word) {
        auto bit = __ffsll(word);
        --bit;
        T item = atomicExch(&items_[bitIdx * 64 + bit], kEmpty);
        if (item != kEmpty) {
          atomicAnd(&bits_[bitIdx], ~(1UL << bit));
          if (full_) {
            atomicExch(&full_, 0);
          }
          return item;
        }
        word &= word - 1;
      }
      bitIdx = bitIdx + 1 & kBitSizeMask;
    }
    atomicExch(&empty_, true);
    return kEmpty;
  }

  int32_t full_{0};
  int32_t empty_{1};
  unsigned long long bits_[kBitSizeMask + 1];
  T items_[kSize];
};
} // namespace facebook::velox::wave
