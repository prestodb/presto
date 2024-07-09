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

#include <assert.h>
#include <cuda_runtime.h> // @manual

#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/exec/ExprKernel.h"
#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

inline bool __device__ laneActive(ErrorCode code) {
  return static_cast<uint8_t>(code) <=
      static_cast<uint8_t>(ErrorCode::kContinue);
}

template <typename T>
__device__ inline T& flatValue(void* base, int32_t blockBase) {
  return reinterpret_cast<T*>(base)[blockBase + threadIdx.x];
}

/// Returns true if operand is non null. Sets 'value' to the value of the
/// operand.
template <typename T>
__device__ __forceinline__ bool operandOrNull(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    void* shared,
    T& value) {
  if (opIdx > kMinSharedMemIndex) {
    assert(false);
  }
  auto op = operands[opIdx];
  int32_t index = threadIdx.x;
  if (auto indicesInOp = op->indices) {
    auto indices = indicesInOp[blockBase / kBlockSize];
    if (indices) {
      index = indices[index];
    } else {
      index += blockBase;
    }
  } else {
    index = (index + blockBase) & op->indexMask;
  }
  if (op->nulls && op->nulls[index] == kNull) {
    return false;
  }
  value = reinterpret_cast<const T*>(op->base)[index];
  return true;
}

template <typename T>
__device__ inline T getOperand(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    void* shared) {
  if (opIdx > kMinSharedMemIndex) {
    assert(false);
  }
  auto op = operands[opIdx];
  int32_t index = (threadIdx.x + blockBase) & op->indexMask;
  if (auto indicesInOp = op->indices) {
    auto indices = indicesInOp[blockBase / kBlockSize];
    if (indices) {
      index = indices[index];
    }
  }
  return reinterpret_cast<const T*>(op->base)[index];
}

template <typename T>
__device__ inline T value(Operand* op, int32_t blockBase, char* shared) {
  return getOperand<T>(&op, 0, blockBase, shared);
}

template <typename T>
__device__ inline T value(Operand* op, int index) {
  if (auto indicesInOp = op->indices) {
    auto indices = indicesInOp[0];
    if (indices) {
      index = indices[index];
    }
  }
  return reinterpret_cast<const T*>(op->base)[index];
}

/// Sets the lane's result to null for opIdx.
__device__ inline void resultNull(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    void* shared) {
  if (opIdx >= kMinSharedMemIndex) {
    assert(false);
  } else {
    auto* op = operands[opIdx];
    op->nulls[blockBase + threadIdx.x] = kNull;
  }
}

template <typename T>
__device__ inline T& flatResult(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    void* shared) {
  if (opIdx >= kMinSharedMemIndex) {
    assert(false);
  }
  auto* op = operands[opIdx];
  if (op->nulls) {
    op->nulls[blockBase + threadIdx.x] = kNotNull;
  }
  return reinterpret_cast<T*>(op->base)[blockBase + threadIdx.x];
}

template <typename T>
__device__ inline T& flatResult(Operand* op, int32_t blockBase) {
  return flatResult<T>(&op, 0, blockBase, nullptr);
}

#define PROGRAM_PREAMBLE(blockOffset)                                          \
  extern __shared__ char sharedChar[];                                         \
  WaveShared* shared = reinterpret_cast<WaveShared*>(sharedChar);              \
  int programIndex = params.programIdx[blockIdx.x + blockOffset];              \
  auto* program = params.programs[programIndex];                               \
  if (threadIdx.x == 0) {                                                      \
    shared->operands = params.operands[programIndex];                          \
    shared->status = &params.status                                            \
                          [blockIdx.x + blockOffset -                          \
                           params.blockBase[blockIdx.x + blockOffset]];        \
    shared->numRows = shared->status->numRows;                                 \
    shared->blockBase = (blockIdx.x + blockOffset -                            \
                         params.blockBase[blockIdx.x + blockOffset]) *         \
        blockDim.x;                                                            \
    shared->states = params.operatorStates[programIndex];                      \
    shared->stop = false;                                                      \
  }                                                                            \
  __syncthreads();                                                             \
  auto blockBase = shared->blockBase;                                          \
  auto operands = shared->operands;                                            \
  ErrorCode laneStatus;                                                        \
  Instruction* instruction;                                                    \
  if (params.startPC == nullptr) {                                             \
    instruction = program->instructions;                                       \
    laneStatus =                                                               \
        threadIdx.x < shared->numRows ? ErrorCode::kOk : ErrorCode::kInactive; \
  } else {                                                                     \
    instruction = program->instructions + params.startPC[programIndex];        \
    laneStatus = shared->status->errors[threadIdx.x];                          \
  }

#define PROGRAM_EPILOGUE()                          \
  if (threadIdx.x == 0) {                           \
    shared->status->numRows = shared->numRows;      \
  }                                                 \
  shared->status->errors[threadIdx.x] = laneStatus; \
  __syncthreads();

__device__ __forceinline__ void filterKernel(
    const IFilter& filter,
    Operand** operands,
    int32_t blockBase,
    WaveShared* shared,
    ErrorCode& laneStatus) {
  bool isPassed = laneActive(laneStatus);
  if (isPassed) {
    if (!operandOrNull(
            operands, filter.flags, blockBase, &shared->data, isPassed)) {
      isPassed = false;
    }
  }
  uint32_t bits = __ballot_sync(0xffffffff, isPassed);
  if ((threadIdx.x & (kWarpThreads - 1)) == 0) {
    reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x / kWarpThreads] =
        __popc(bits);
  }
  __syncthreads();
  if (threadIdx.x < kWarpThreads) {
    constexpr int32_t kNumWarps = kBlockSize / kWarpThreads;
    int32_t cnt = threadIdx.x < kNumWarps
        ? reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x]
        : 0;
    int32_t sum;
    using Scan = cub::WarpScan<int32_t, kBlockSize / kWarpThreads>;
    Scan(*reinterpret_cast<Scan::TempStorage*>(shared)).ExclusiveSum(cnt, sum);
    if (threadIdx.x < kNumWarps) {
      if (threadIdx.x == kNumWarps - 1) {
        shared->numRows = cnt + sum;
      }
      reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x] = sum;
    }
  }
  __syncthreads();
  if (bits & (1 << (threadIdx.x & (kWarpThreads - 1)))) {
    auto* indices = reinterpret_cast<int32_t*>(operands[filter.indices]->base);
    auto start = blockBase +
        reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x / kWarpThreads];
    auto bit = start +
        __popc(bits & lowMask<uint32_t>(threadIdx.x & (kWarpThreads - 1)));
    indices[bit] = blockBase + threadIdx.x;
  }
  laneStatus =
      threadIdx.x < shared->numRows ? ErrorCode::kOk : ErrorCode::kInactive;
  __syncthreads();
}

__device__ void __forceinline__ wrapKernel(
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

} // namespace facebook::velox::wave
