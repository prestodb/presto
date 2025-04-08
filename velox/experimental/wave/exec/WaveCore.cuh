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

#include "velox/experimental/wave/common/Scan.cuh"
#include "velox/experimental/wave/exec/ExprKernel.h"
#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

template <typename T>
inline T* __device__ gridStatus(const WaveShared* shared, int32_t gridState) {
  return reinterpret_cast<T*>(
      roundUp(
          reinterpret_cast<uintptr_t>(
              &shared->status
                   [shared->numBlocks - (shared->blockBase / kBlockSize)]),
          8) +
      gridState);
}

inline __device__ void setError(
    WaveShared* shared,
    ErrorCode& laneStatus,
    bool insideTry,
    int32_t messageEnum,
    int64_t extra = 0) {
  laneStatus = ErrorCode::kError;
  if (insideTry) {
    return;
  }
  auto* error = gridStatus<KernelError>(shared, 0);
  error->messageEnum = messageEnum;
}

template <typename T>
inline T* __device__
gridStatus(const WaveShared* shared, const InstructionStatus& status) {
  return gridStatus<T>(shared, status.gridState);
}

/// Returns a pointer to the first byte of block level status in the extra
/// status area above the BlockStatus array for the current block.
template <typename T>
inline T* __device__ blockStatus(
    const WaveShared* shared,
    int32_t gridStatusSize,
    int32_t blockStatusOffset,
    int32_t blockStatusSize = sizeof(T)) {
  return reinterpret_cast<T*>(
      roundUp(
          reinterpret_cast<uintptr_t>(shared->status) +
              shared->numBlocks * sizeof(BlockStatus),
          8) +
      gridStatusSize + (blockStatusOffset * shared->numBlocks) +
      (blockStatusSize * (shared->blockBase / kBlockSize)));
}

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
    T& value) {
  auto op = operands[opIdx];
  auto index = threadIdx.x;
  if (auto indicesInOp = op->indices) {
    auto indices = indicesInOp[blockBase / kBlockSize];
    if (indices) {
      index = indices[index];
      if (index == kNullIndex) {
        return false;
      }
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

template <bool kMayWrap, typename T>
bool __device__ __forceinline__ valueOrNull(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    T& value) {
  auto op = operands[opIdx];
  auto index = threadIdx.x;
  if (!kMayWrap) {
    index = (index + blockBase) & op->indexMask;
    if (op->nulls && op->nulls[index] == kNull) {
      return false;
    }
    value = reinterpret_cast<const T*>(op->base)[index];
    return true;
  }
  if (auto indicesInOp = op->indices) {
    auto indices = indicesInOp[blockBase / kBlockSize];
    if (indices) {
      index = indices[index];
      if (index == kNullIndex) {
        return false;
      }
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

template <bool kMayWrap, typename T>
void __device__ __forceinline__ loadValueOrNull(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    T& value,
    uint32_t& nulls) {
  nulls = (nulls & ~(1U << (opIdx & 31))) |
      (static_cast<uint32_t>(
           valueOrNull<kMayWrap>(operands, opIdx, blockBase, value))
       << (opIdx & 31));
}

template <bool kMayWrap, typename T>
T __device__ __forceinline__
nonNullOperand(Operand** operands, OperandIndex opIdx, int32_t blockBase) {
  auto op = operands[opIdx];
  auto index = threadIdx.x;
  if (!kMayWrap) {
    index = (index + blockBase) & op->indexMask;
    return reinterpret_cast<const T*>(op->base)[index];
  }
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
  return reinterpret_cast<const T*>(op->base)[index];
}

bool __device__ __forceinline__
setRegisterNull(uint32_t& flags, int8_t bit, bool isNull) {
  if (isNull) {
    flags &= ~(1 << bit);
  }
  return isNull;
}

bool __device__ __forceinline__ isRegisterNull(uint32_t flags, int8_t bit) {
  return 0 == (flags & (1 << bit));
}

template <typename T>
__device__ inline T&
flatOperand(Operand** operands, OperandIndex opIdx, int32_t blockBase) {
  auto* op = operands[opIdx];
  if (op->nulls) {
    op->nulls[blockBase + threadIdx.x] = kNotNull;
  }
  return reinterpret_cast<T*>(op->base)[blockBase + threadIdx.x];
}

/// Clears 'bit' from 'flags' if notNull is false. Returns true if bit cleared.
bool __device__ __forceinline__
setNullRegister(uint32_t& flags, int8_t bit, bool notNull) {
  if (!notNull) {
    flags &= ~(1 << bit);
  }
  return !notNull;
}

/// Sets the lane's result to null for opIdx.
__device__ inline void
resultNull(Operand** operands, OperandIndex opIdx, int32_t blockBase) {
  auto* op = operands[opIdx];
  op->nulls[blockBase + threadIdx.x] = kNull;
}

__device__ inline void setNull(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    bool isNull) {
  auto* op = operands[opIdx];
  op->nulls[blockBase + threadIdx.x] = isNull ? kNull : kNotNull;
}

template <typename T>
__device__ inline T&
flatResult(Operand** operands, OperandIndex opIdx, int32_t blockBase) {
  auto* op = operands[opIdx];
  if (op->nulls) {
    op->nulls[blockBase + threadIdx.x] = kNotNull;
  }
  return reinterpret_cast<T*>(op->base)[blockBase + threadIdx.x];
}

template <typename T>
__device__ inline T& flatResult(Operand* op, int32_t blockBase) {
  return flatResult<T>(&op, 0, blockBase);
}

#define GENERATED_PREAMBLE(blockOffset)                                        \
  extern __shared__ char sharedChar[];                                         \
  WaveShared* shared = reinterpret_cast<WaveShared*>(sharedChar);              \
  if (threadIdx.x == 0) {                                                      \
    shared->operands = params.operands[0];                                     \
    shared->numBlocks = params.numBlocks;                                      \
    shared->numRowsPerThread = params.numRowsPerThread;                        \
    auto startBlock = blockIdx.x * shared->numRowsPerThread;                   \
    auto branchIdx = startBlock / shared->numBlocks;                           \
    startBlock = startBlock - (shared->numBlocks * branchIdx);                 \
    shared->programIdx = branchIdx;                                            \
    startBlock =                                                               \
        (startBlock / shared->numRowsPerThread) * shared->numRowsPerThread;    \
    int32_t numBlocksAbove = shared->numBlocks - startBlock;                   \
    if (numBlocksAbove < shared->numRowsPerThread) {                           \
      shared->numRowsPerThread = numBlocksAbove;                               \
    }                                                                          \
    shared->status = &params.status[startBlock];                               \
    shared->numRows = shared->status->numRows;                                 \
    shared->blockBase = startBlock * kBlockSize;                               \
    shared->states = params.operatorStates[0];                                 \
    shared->nthBlock = 0;                                                      \
    shared->streamIdx = params.streamIdx;                                      \
    shared->localContinue = false;                                             \
    shared->isContinue = params.startPC != nullptr;                            \
    if (shared->isContinue) {                                                  \
      shared->startLabel = params.startPC[shared->programIdx];                 \
    } else {                                                                   \
      shared->startLabel = -1;                                                 \
    }                                                                          \
    shared->extraWraps = params.extraWraps;                                    \
    shared->numExtraWraps = params.numExtraWraps;                              \
    shared->hasContinue = false;                                               \
    shared->stop = false;                                                      \
  }                                                                            \
  __syncthreads();                                                             \
  int32_t blockBase;                                                           \
  auto operands = shared->operands;                                            \
  ErrorCode laneStatus;                                                        \
  nextBlock:                                                                   \
  blockBase = shared->blockBase;                                               \
  if (!shared->isContinue) {                                                   \
    laneStatus =                                                               \
        threadIdx.x < shared->numRows ? ErrorCode::kOk : ErrorCode::kInactive; \
  } else {                                                                     \
    laneStatus = shared->status->errors[threadIdx.x];                          \
  }

#define PROGRAM_EPILOGUE()                                \
  shared->status->errors[threadIdx.x] = laneStatus;       \
  __syncthreads();                                        \
  if (threadIdx.x == 0) {                                 \
    shared->status->numRows = shared->numRows;            \
    if (++shared->nthBlock >= shared->numRowsPerThread) { \
      shared->stop = true;                                \
    } else {                                              \
      ++shared->status;                                   \
      shared->numRows = shared->status->numRows;          \
      shared->blockBase += kBlockSize;                    \
    }                                                     \
  }                                                       \
  __syncthreads();                                        \
  if (!shared->stop) {                                    \
    goto nextBlock;                                       \
  }

__device__ __forceinline__ void filterKernel(
    bool flag,
    Operand** operands,
    OperandIndex indicesIdx,
    int32_t blockBase,
    WaveShared* shared,
    ErrorCode& laneStatus) {
  bool isPassed = flag && laneActive(laneStatus);
  uint32_t bits = __ballot_sync(0xffffffff, isPassed);
  if ((threadIdx.x & (kWarpThreads - 1)) == 0) {
    reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x / kWarpThreads] =
        __popc(bits);
  }
  __syncthreads();
  if (threadIdx.x < kWarpThreads) {
    constexpr int32_t kNumWarps = kBlockSize / kWarpThreads;
    uint32_t cnt = threadIdx.x < kNumWarps
        ? reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x]
        : 0;
    uint32_t sum;
    using Scan = WarpScan<uint32_t, kBlockSize / kWarpThreads>;
    Scan().exclusiveSum(cnt, sum);
    if (threadIdx.x < kNumWarps) {
      if (threadIdx.x == kNumWarps - 1) {
        shared->numRows = cnt + sum;
      }
      reinterpret_cast<int32_t*>(&shared->data)[threadIdx.x] = sum;
    }
  }
  __syncthreads();
  if (bits & (1 << (threadIdx.x & (kWarpThreads - 1)))) {
    auto* indices = reinterpret_cast<int32_t*>(operands[indicesIdx]->base);
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
    const OperandIndex* wraps,
    int32_t numWraps,
    OperandIndex indicesIdx,
    Operand** operands,
    int32_t blockBase,
    WaveShared* shared) {
  Operand* op = operands[indicesIdx];
  auto* filterIndices = reinterpret_cast<int32_t*>(op->base);
  if (filterIndices[blockBase + shared->numRows - 1] ==
      shared->numRows + blockBase - 1) {
    // There is no cardinality change.
    if (threadIdx.x == 0) {
      auto* op = operands[wraps[0]];
      op->indices[blockBase / kBlockSize] = nullptr;
    }
    __syncthreads();
    return;
  }

  struct WrapState {
    int32_t* indices;
  };

  auto* state = reinterpret_cast<WrapState*>(&shared->data);
  bool rowActive = threadIdx.x < shared->numRows;
  int32_t totalWrap = numWraps + shared->numExtraWraps;
  for (auto column = 0; column < totalWrap; ++column) {
    if (threadIdx.x == 0) {
      auto opIndex = column < numWraps ? wraps[column]
                                       : shared->extraWraps + column - numWraps;
      auto* op = operands[opIndex];
      int32_t** opIndices = &op->indices[blockBase / kBlockSize];
      // If there is no indirection or if this is column 0 whose indirection is
      // inited here, use the filter rows.
      if (!*opIndices || column == 0) {
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

__device__ void __forceinline__ wrapKernel(
    const OperandIndex* wraps,
    int32_t numWraps,
    OperandIndex indicesIdx,
    Operand** operands,
    Operand** newWraps,
    Operand** backup,
    int32_t blockBase,
    WaveShared* shared) {
  Operand* op = operands[indicesIdx];
  auto* filterIndices = reinterpret_cast<int32_t*>(op->base);
  if (filterIndices[blockBase + shared->numRows - 1] ==
      shared->numRows + blockBase - 1) {
    // There is no cardinality change.
    if (threadIdx.x == 0) {
      auto* op = operands[wraps[0]];
      op->indices[blockBase / kBlockSize] = nullptr;
    }
    __syncthreads();
    return;
  }

  struct WrapState {
    int32_t* indices;
  };

  auto* state = reinterpret_cast<WrapState*>(&shared->data);
  bool rowActive = threadIdx.x < shared->numRows;
  int32_t totalWrap = numWraps + shared->numExtraWraps;
  for (auto column = 0; column < totalWrap; ++column) {
    if (threadIdx.x == 0) {
      auto opIndex = column < numWraps ? wraps[column]
                                       : shared->extraWraps + column - numWraps;
      auto* op = operands[opIndex];
      int32_t** opIndices = &op->indices[blockBase / kBlockSize];
      // If there is no indirection or if this is column 0 whose indirection is
      // inited here, use the filter rows.
      if (!*opIndices || column == 0) {
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

__device__ void __forceinline__ wrapKernel(
    OperandIndex first,
    const OperandIndex* wraps,
    const OperandIndex* newIndices,
    const OperandIndex* backups,
    int32_t numWraps,
    OperandIndex indicesIdx,
    WaveShared* shared) {
  auto* operands = shared->operands;
  Operand* op = operands[indicesIdx];
  auto* filterIndices = reinterpret_cast<int32_t*>(op->base);

  struct WrapState {
    int32_t* indices;
    int32_t* newIndices;
  };

  auto* state = reinterpret_cast<WrapState*>(&shared->data);
  bool rowActive = threadIdx.x < shared->numRows;

  if (first != kEmpty) {
    if (threadIdx.x == 0) {
      operands[first]->indices[shared->blockBase / kBlockSize] =
          filterIndices + shared->blockBase;
    }
  }

  for (auto column = 0; column < numWraps; ++column) {
    if (threadIdx.x == 0) {
      auto nthBlock = shared->blockBase / kBlockSize;
      auto opIndex = wraps[column];
      auto* op = operands[opIndex];
      int32_t** opIndices = &op->indices[nthBlock];
      // Record previous indirection
      auto backup =
          reinterpret_cast<int32_t**>(operands[backups[column]]->base);
      backup[nthBlock] = *opIndices;
      if (!*opIndices) {
        *opIndices = filterIndices + shared->blockBase;
        state->indices = nullptr;
      } else {
        state->indices = *opIndices;
        state->newIndices =
            reinterpret_cast<int32_t*>(operands[newIndices[column]]->base);
      }
    }
    __syncthreads();
    // Every thread sees the decision on thred 0 above.
    if (!state->indices) {
      continue;
    }
    int32_t newIndex;
    if (rowActive) {
      newIndex = state->indices
                     [filterIndices[shared->blockBase + threadIdx.x] -
                      shared->blockBase];
      state->newIndices[threadIdx.x] = newIndex;
    }
  }
  __syncthreads();
}

template <typename T>
__device__ T value(Operand* operands, OperandIndex opIdx) {
  // Obsolete signature. call sites must be changed.
  //    assert(false);
  *(long*)0 = 0;
  return T{};
}

} // namespace facebook::velox::wave
