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

#include <stdint.h>

namespace facebook::velox::wave {

constexpr int32_t kWarpThreads = 32;

template <typename T, typename U>
__host__ __device__ constexpr inline T roundUp(T value, U factor) {
  return (value + (factor - 1)) / factor * factor;
}

template <typename T>
constexpr T __device__ __host__ lowMask(int bits) {
  /****
   * NVCC BUG: If the special case for all bits is not in, all modes except -G
   * produce a 0 mask for 32 or 64 bits.
   ****/
  return bits == 8 * sizeof(T) ? ~static_cast<T>(0)
                               : (static_cast<T>(1) << bits) - 1;
}

template <typename T>
constexpr inline __device__ __host__ T highMask(int bits) {
  return lowMask<T>(bits) << ((sizeof(T) * 8) - bits);
}

template <typename T>
inline T* __device__ __host__ addBytes(T* ptr, int bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<char*>(ptr) + bytes);
}

template <typename T>
inline const T* __device__ __host__ addBytes(const T* ptr, int bytes) {
  return reinterpret_cast<const T*>(reinterpret_cast<const char*>(ptr) + bytes);
}

template <typename T>
inline T* __device__ __host__ addCast(void* ptr, int bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<char*>(ptr) + bytes);
}

template <typename T>
inline const T* __device__ __host__ addCast(const void* ptr, int bytes) {
  return reinterpret_cast<const T*>(reinterpret_cast<const char*>(ptr) + bytes);
}

inline unsigned int __device__
deviceScale32(unsigned int n, unsigned int scale) {
  return (static_cast<unsigned long long>(static_cast<unsigned int>(n)) *
          scale) >>
      32;
}

__device__ __forceinline__ unsigned int LaneId() {
  return threadIdx.x % kWarpThreads;
}

/* Log2 included from cub */
/**
 * \brief Statically determine log2(N), rounded up.
 *
 * For example:
 *     Log2<8>::VALUE   // 3
 *     Log2<3>::VALUE   // 2
 */
template <int N, int CURRENT_VAL = N, int COUNT = 0>
struct Log2 {
  /// Static logarithm value
  enum {
    VALUE = Log2<N, (CURRENT_VAL >> 1), COUNT + 1>::VALUE
  }; // Inductive case
};

template <int N, int COUNT>
struct Log2<N, 0, COUNT> {
  enum {
    VALUE = (1 << (COUNT - 1) < N) ? // Base case
        COUNT
                                   : COUNT - 1
  };
};

namespace detail {
inline __device__ bool isLastInWarp() {
  return (threadIdx.x & (kWarpThreads - 1)) == (kWarpThreads - 1);
}
} // namespace detail

} // namespace facebook::velox::wave
