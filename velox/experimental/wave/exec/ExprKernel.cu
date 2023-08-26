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
    Operand** operands,
    int32_t blockBase,
    char* shared,
    BlockStatus* status) {
  if (threadIdx.x >= status->numRows) {
    return;
  }
  flatResult<T>(operands, op.result, blockBase, shared) = func(
      getOperand<T>(operands, op.left, blockBase, shared),
      getOperand<T>(operands, op.right, blockBase, shared));
}

__device__ void filterKernel(
    const IFilter& filter,
    Operand** operands,
    int32_t blockBase,
    char* shared,
    int32_t& numRows) {
  auto* flags = operands[filter.flags];
  auto* indices = operands[filter.indices];
  if (flags->nulls) {
    boolBlockToIndices<kBlockSize>(
        [&]() -> uint8_t {
          return threadIdx.x >= numRows
              ? 0
              : flatValue<uint8_t>(flags->base, blockBase) &
                  flatValue<uint8_t>(flags->nulls, blockBase);
        },
        blockBase,
        reinterpret_cast<int32_t*>(indices->base) + blockBase,
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
        reinterpret_cast<int32_t*>(indices->base) + blockBase,
        shared,
        numRows);
  }
}

__device__ void wrapKernel(
    IWrap& wrap,
    Operand** operands,
    int32_t blockBase,
    int32_t& numRows) {}

#define BINARY_TYPES(opCode, OP)                             \
  case OP_MIX(opCode, ScalarType::kInt64):                   \
    binaryOpKernel<int64_t>(                                 \
        [](auto left, auto right) { return left OP right; }, \
        instruction->_.binary,                               \
        operands,                                            \
        blockBase,                                           \
        shared,                                              \
        status);                                             \
    break;

__global__ void waveBaseKernel(
    int32_t* baseIndices,
    int32_t* programIndices,
    ThreadBlockProgram** programs,
    Operand*** programOperands,
    BlockStatus* blockStatusArray) {
  using ScanAlgorithm = cub::BlockScan<int, 256, cub::BLOCK_SCAN_RAKING>;
  extern __shared__ __align__(
      alignof(typename ScanAlgorithm::TempStorage)) char shared[];
  int programIndex = programIndices[blockIdx.x];
  auto* program = programs[programIndex];
  auto* operands = programOperands[programIndex];
  auto* status = &blockStatusArray[blockIdx.x - baseIndices[blockIdx.x]];
  int32_t blockBase = (blockIdx.x - baseIndices[blockIdx.x]) * blockDim.x;
  for (auto i = 0; i < program->numInstructions; ++i) {
    auto instruction = program->instructions[i];
    switch (instruction->opCode) {
      case OpCode::kFilter:
        filterKernel(
            instruction->_.filter,
            operands,
            blockBase,
            shared,
            status->numRows);
        break;

      case OpCode::kWrap:
        wrapKernel(instruction->_.wrap, operands, blockBase, status->numRows);
        break;

        BINARY_TYPES(OpCode::kPlus, +);
    }
  }
}

void WaveKernelStream::call(
    Stream* alias,
    int32_t numBlocks,
    int32_t* bases,
    int32_t* programIdx,
    ThreadBlockProgram** programs,
    Operand*** operands,
    BlockStatus* status,
    int32_t sharedSize) {
  waveBaseKernel<<<
      numBlocks,
      kBlockSize,
      sharedSize,
      alias ? alias->stream()->stream : stream()->stream>>>(
      bases, programIdx, programs, operands, status);
}

} // namespace facebook::velox::wave
