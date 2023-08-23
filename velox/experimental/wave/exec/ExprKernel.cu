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

#include "velox/experimental/wave/exec/ExprKernel.h"

#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

namespace facebook::velox::wave {

template <typename T>
__device__ inline T opFunc_kPlus(T left, T right) {
  return left + right;
}

template <typename T, typename OpFunc>
__device__ inline void binaryOpKernel(
    OpFunc func,
    IBinary& op,
    int32_t blockBase,
    char* shared,
    BlockStatus* status) {}

__device__ void filterKernel(
    const IFilter& filter,
    int32_t blockBase,
    char* shared,
    int32_t& numRows) {
  auto* flags = filter.flags;
  if (flags->nulls) {
    boolBlockToIndices<kBlockSize>(
        [&]() -> uint8_t {
          return threadIdx.x >= numRows
              ? 0
              : flatValue<uint8_t>(flags->base, blockBase) &
                  flatValue<uint8_t>(flags->nulls, blockBase);
        },
        blockBase,
        filter.indices + blockBase,
        shared,
        numRows);
  } else {
    boolBlockToIndices<kBlockSize>(
        [&]() -> uint8_t {
          return threadIdx.x >= numRows
              ? 0
              : flatValue<uint8_t>(flags->base, blockBase);
        },
        blockBase,
        filter.indices + blockBase,
        shared,
        numRows);
  }
}

__device__ void wrapKernel(IWrap& wrap, int32_t blockBase, int32_t& numRows) {}

#define OP_MIX(op, t) \
  static_cast<OpCode>(static_cast<int32_t>(t) + 8 * static_cast<int32_t>(op))

#define BINARY_TYPES(opCode, OP)                             \
  case OP_MIX(opCode, ScalarType::kInt32):                   \
    binaryOpKernel<int32_t>(                                 \
        [](auto left, auto right) { return left OP right; }, \
        instruction->_.binary,                               \
        blockBase,                                           \
        shared,                                              \
        status);                                             \
    break;

__global__ void waveBaseKernel(
    ThreadBlockProgram** programs,
    int32_t* baseIndices,
    BlockStatus* blockStatusArray) {
  using ScanAlgorithm = cub::BlockScan<int, 256, cub::BLOCK_SCAN_RAKING>;
  extern __shared__ __align__(
      alignof(typename ScanAlgorithm::TempStorage)) char shared[];
  auto* program = programs[blockIdx.x];
  auto* status = &blockStatusArray[blockIdx.x];
  int32_t blockBase = (blockIdx.x - baseIndices[blockIdx.x]) * blockDim.x;
  for (auto i = 0; i < program->numInstructions; ++i) {
    auto instruction = program->instructions[i];
    switch (instruction->opCode) {
      case OpCode::kFilter:
        filterKernel(instruction->_.filter, blockBase, shared, status->numRows);
        break;

      case OpCode::kWrap:
        wrapKernel(instruction->_.wrap, blockBase, status->numRows);
        break;

        BINARY_TYPES(OpCode::kPlus, +);
    }
  }
}

void WaveKernelStream::call(
    Stream* alias,
    int32_t numBlocks,
    ThreadBlockProgram** programs,
    int32_t* baseIndices,
    BlockStatus* status,
    int32_t sharedSize) {
  waveBaseKernel<<<
      numBlocks,
      kBlockSize,
      sharedSize,
      alias ? alias->stream()->stream : stream()->stream>>>(
      programs, baseIndices, status);
}

} // namespace facebook::velox::wave
