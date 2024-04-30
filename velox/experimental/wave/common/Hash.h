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

#include "velox/experimental/wave/common/StringView.h"

namespace facebook::velox::wave {

template <typename Input, typename Output>
struct Hasher;

class Murmur3 {
 public:
  __device__ __host__ static uint32_t
  hashBytes(const char* data, size_t len, uint32_t seed) {
    auto h1 = seed;
    size_t i = 0;
    for (; i + 4 <= len; i += 4) {
      uint32_t k1;
      memcpy(&k1, data + i, sizeof(uint32_t));
      h1 = mixH1(h1, mixK1(k1));
    }
    for (; i < len; ++i) {
      h1 = mixH1(h1, mixK1(data[i]));
    }
    return fmix(h1, len);
  }

 private:
  __device__ __host__ static uint32_t rotl32(uint32_t a, int shift) {
    return (a << shift) | (a >> (32 - shift));
  }

  __device__ __host__ static uint32_t mixK1(uint32_t k1) {
    k1 *= 0xcc9e2d51;
    k1 = rotl32(k1, 15);
    k1 *= 0x1b873593;
    return k1;
  }

  __device__ __host__ static uint32_t mixH1(uint32_t h1, uint32_t k1) {
    h1 ^= k1;
    h1 = rotl32(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  __device__ __host__ static uint32_t fmix(uint32_t h1, uint32_t length) {
    h1 ^= length;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    return h1;
  }
};

__device__ __host__ inline uint32_t jenkinsRevMix32(uint32_t key) {
  key += (key << 12); // key *= (1 + (1 << 12))
  key ^= (key >> 22);
  key += (key << 4); // key *= (1 + (1 << 4))
  key ^= (key >> 9);
  key += (key << 10); // key *= (1 + (1 << 10))
  key ^= (key >> 2);
  // key *= (1 + (1 << 7)) * (1 + (1 << 12))
  key += (key << 7);
  key += (key << 12);
  return key;
}

__device__ __host__ inline uint32_t twang32From64(uint64_t key) {
  key = (~key) + (key << 18);
  key = key ^ (key >> 31);
  key = key * 21;
  key = key ^ (key >> 11);
  key = key + (key << 6);
  key = key ^ (key >> 22);
  return static_cast<uint32_t>(key);
}

__device__ inline uint64_t hashMix(const uint64_t upper, const uint64_t lower) {
  // Murmur-inspired hashing.
  const uint64_t kMul = 0x9ddfea08eb382d69ULL;
  uint64_t a = (lower ^ upper) * kMul;
  a ^= (a >> 47);
  uint64_t b = (upper ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

template <typename T>
struct IntHasher32 {
  __device__ __host__ uint32_t operator()(T val) const {
    if constexpr (sizeof(T) <= 4) {
      return jenkinsRevMix32(val);
    } else {
      return twang32From64(val);
    }
    __builtin_unreachable();
  }
};

template <>
struct Hasher<StringView, uint32_t> {
  __device__ __host__ uint32_t operator()(StringView val) const {
    return Murmur3::hashBytes(val.data(), val.size(), 42);
  }
};

template <>
struct Hasher<int32_t, uint32_t> : IntHasher32<int32_t> {};

template <>
struct Hasher<int64_t, uint32_t> : IntHasher32<int64_t> {};

} // namespace facebook::velox::wave
