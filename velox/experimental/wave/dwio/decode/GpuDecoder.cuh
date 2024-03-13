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
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/dwio/decode/DecodeStep.h"

namespace facebook::velox::wave {

/// Returns the amount of shared memory per thread block for 'op'
int32_t sharedMemorySize(GpuDecode& op);

/// Decode plans passed, one thread block will be running on each plan.
__device__ void decodeNoSharedMemory(GpuDecode* plan);

/// Decode plans passed, this version requires shared memory thus the
/// concurrency level will be lower.
template <int kBlockSize>
__device__ void decodeWithSharedMemory(GpuDecode* plan);

/// Decode plans, initiated from host code.
template <int kBlockSize>
void decodeGlobal(GpuDecode* plan, int numBlocks, cudaStream_t stream = 0);

__host__ __device__ inline bool isSet(const uint8_t* bits, int32_t idx) {
  return bits[idx / 8] & (1 << (idx & 7));
}

__host__ __device__ inline void setBit(uint8_t* bits, uint32_t idx) {
  bits[idx / 8] |= (1 << (idx % 8));
}

__host__ __device__ inline void clearBit(uint8_t* bits, uint32_t idx) {
  static constexpr uint8_t kZeroBitmasks[] = {
      static_cast<uint8_t>(~(1 << 0)),
      static_cast<uint8_t>(~(1 << 1)),
      static_cast<uint8_t>(~(1 << 2)),
      static_cast<uint8_t>(~(1 << 3)),
      static_cast<uint8_t>(~(1 << 4)),
      static_cast<uint8_t>(~(1 << 5)),
      static_cast<uint8_t>(~(1 << 6)),
      static_cast<uint8_t>(~(1 << 7)),
  };
  bits[idx / 8] &= kZeroBitmasks[idx % 8];
}

__host__ __device__ inline void
setBit(uint8_t* bits, uint32_t idx, bool value) {
  value ? setBit(bits, idx) : clearBit(bits, idx);
}

} // namespace facebook::velox::wave

#include "velox/experimental/wave/dwio/decode/GpuDecoder-inl.cuh"
