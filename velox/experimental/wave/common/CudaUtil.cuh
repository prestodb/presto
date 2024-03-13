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

#define CUDA_CHECK(e) ::facebook::velox::wave::cudaCheck(e, __FILE__, __LINE__)

template <typename T, typename U>
__host__ __device__ constexpr inline T roundUp(T value, U factor) {
  return (value + (factor - 1)) / factor * factor;
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

struct StreamImpl {
  cudaStream_t stream;
};
} // namespace facebook::velox::wave
