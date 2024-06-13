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

#include <cuda_runtime.h>
#include <cstdint>

/// Utilities header to include in Cuda code for Velox Wave. Do not combine with
/// Velox *.h.n
namespace facebook::velox::wave {

void cudaCheck(cudaError_t err, const char* file, int line);

void cudaCheckFatal(cudaError_t err, const char* file, int line);

#define CUDA_CHECK(e) ::facebook::velox::wave::cudaCheck(e, __FILE__, __LINE__)

#ifndef CUDA_CHECK_FATAL
#define CUDA_CHECK_FATAL(e) \
  ::facebook::velox::wave::cudaCheckFatal(e, __FILE__, __LINE__)
#endif

template <typename T, typename U>
__host__ __device__ constexpr inline T roundUp(T value, U factor) {
  return (value + (factor - 1)) / factor * factor;
}

template <typename T>
constexpr T __device__ __host__ lowMask(int32_t bits) {
  /****
   * NVCC BUG: If the special case for all bits is not in, all modes except -G
   * produce a 0 mask for 32 or 64 bits.
   ****/
  return bits == 8 * sizeof(T) ? ~static_cast<T>(0)
                               : (static_cast<T>(1) << bits) - 1;
}

template <typename T>
constexpr inline __device__ __host__ T highMask(int32_t bits) {
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

__device__ __host__ inline int
memcmp(const void* lhs, const void* rhs, size_t n) {
  auto* a = reinterpret_cast<const uint8_t*>(lhs);
  auto* b = reinterpret_cast<const uint8_t*>(rhs);
  for (size_t i = 0; i < n; ++i) {
    if (int c = (int)a[i] - (int)b[i]) {
      return c;
    }
  }
  return 0;
}

inline uint32_t __device__ deviceScale32(uint32_t n, uint32_t scale) {
  return (static_cast<uint64_t>(static_cast<uint32_t>(n)) * scale) >> 32;
}

struct StreamImpl {
  cudaStream_t stream;
};

bool registerKernel(const char* name, const void* func);

#define REGISTER_KERNEL(name, func)                              \
  namespace {                                                    \
  static bool func##_reg =                                       \
      registerKernel(name, reinterpret_cast<const void*>(func)); \
  }

} // namespace facebook::velox::wave
