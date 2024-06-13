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

#include <gflags/gflags.h>
#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

DEFINE_bool(kernel_gdb, false, "Run kernels sequentially for debugging");

namespace facebook::velox::wave {

template <typename T>
__device__ inline T opFunc_kPlus(T left, T right) {
  return left + right;
}

template <typename T, typename OpFunc>
__device__ inline void binaryOpKernel(
    OpFunc func,
    IBinary& instr,
    Operand** operands,
    int32_t blockBase,
    char* shared,
    BlockStatus* status) {
  if (threadIdx.x >= status->numRows) {
    return;
  }
  T left;
  T right;
  if (operandOrNull(operands, instr.left, blockBase, shared, left) &&
      operandOrNull(operands, instr.right, blockBase, shared, right)) {
    flatResult<decltype(func(left, right))>(
        operands, instr.result, blockBase, shared) = func(left, right);
  } else {
    resultNull(operands, instr.result, blockBase, shared);
  }
}

__device__ void filterKernel(
    const IFilter& filter,
    Operand** operands,
    int32_t blockBase,
    char* shared,
    int32_t& numRows) {
  auto* flags = operands[filter.flags];
  auto* indices = operands[filter.indices];
  __syncthreads();
  if (flags->nulls) {
    bool256ToIndices<int32_t>(
        [&](int32_t group) -> uint64_t {
          int32_t offset = group * 8;
          int32_t base = blockBase + offset;
          if (offset + 8 <= numRows) {
            return *addCast<uint64_t>(flags->base, base) &
                *addCast<uint64_t>(flags->nulls, base);
          }
          if (offset >= numRows) {
            return 0;
          }
          return lowMask<uint64_t>((offset + 8 - numRows) * 8) &
              *addCast<uint64_t>(flags->base, base) &
              *addCast<uint64_t>(flags->nulls, base);
        },
        blockBase,
        reinterpret_cast<int32_t*>(indices->base) + blockBase,
        numRows,
        shared);
  } else {
    bool256ToIndices<int32_t>(
        [&](int32_t group) -> uint64_t {
          int32_t offset = group * 8;
          int32_t base = blockBase + offset;
          if (offset + 8 <= numRows) {
            return *addCast<uint64_t>(flags->base, base);
          }
          if (offset >= numRows) {
            return 0;
          }
          return lowMask<uint64_t>((numRows - offset) * 8) &
              *addCast<uint64_t>(flags->base, base);
        },
        blockBase,
        reinterpret_cast<int32_t*>(indices->base) + blockBase,
        numRows,
        shared);
  }
}

__device__ void wrapKernel(
    const IWrap& wrap,
    Operand** operands,
    int32_t blockBase,
    int32_t numRows,
    void* shared) {
  Operand* op = operands[wrap.indices];
  auto* filterIndices = reinterpret_cast<int32_t*>(op->base);
  if (filterIndices[blockBase + numRows - 1] == numRows + blockBase - 1) {
    // There is no cardinality change.
    return;
  }

  struct WrapState {
    int32_t* indices;
  };

  auto* state = reinterpret_cast<WrapState*>(shared);
  bool rowActive = threadIdx.x < numRows;
  for (auto column = 0; column < wrap.numColumns; ++column) {
    if (threadIdx.x == 0) {
      auto opIndex = wrap.columns[column];
      auto* op = operands[opIndex];
      int32_t** opIndices = &op->indices[blockBase / kBlockSize];
      if (!*opIndices) {
        *opIndices = filterIndices + blockBase;
        state->indices = nullptr;
      } else {
        state->indices = *opIndices;
      }
    }
    __syncthreads();
    // Every thread sees the decision on thred 0 above.
    if (!state->indices) {
      continue;
    }
    int32_t newIndex;
    if (rowActive) {
      newIndex =
          state->indices[filterIndices[blockBase + threadIdx.x] - blockBase];
    }
    // All threads hit this.
    __syncthreads();
    if (rowActive) {
      state->indices[threadIdx.x] = newIndex;
    }
  }
  __syncthreads();
}

#define BINARY_TYPES(opCode, OP)                             \
  case OP_MIX(opCode, WaveTypeKind::BIGINT):                 \
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
  extern __shared__ __align__(16) char shared[];
  int programIndex = programIndices[blockIdx.x];
  auto* program = programs[programIndex];
  auto* operands = programOperands[programIndex];
  auto* status = &blockStatusArray[blockIdx.x - baseIndices[blockIdx.x]];
  int32_t blockBase = (blockIdx.x - baseIndices[blockIdx.x]) * blockDim.x;
  auto instruction = program->instructions;
  for (;;) {
    switch (instruction->opCode) {
      case OpCode::kReturn:
        __syncthreads();
        return;
      case OpCode::kFilter:
        filterKernel(
            instruction->_.filter,
            operands,
            blockBase,
            shared,
            status->numRows);
        break;

      case OpCode::kWrap:
        wrapKernel(
            instruction->_.wrap, operands, blockBase, status->numRows, shared);
        break;

        BINARY_TYPES(OpCode::kPlus, +);
        BINARY_TYPES(OpCode::kLT, <);
    }
    ++instruction;
  }
}

int32_t instructionSharedMemory(const Instruction& instruction) {
  switch (instruction.opCode) {
    case OpCode::kFilter:
      return (2 + (kBlockSize / kWarpThreads)) * sizeof(int32_t);
    default:
      return 0;
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
  if (FLAGS_kernel_gdb) {
    (alias ? alias : this)->wait();
  }
}
REGISTER_KERNEL("expr", waveBaseKernel);

} // namespace facebook::velox::wave
