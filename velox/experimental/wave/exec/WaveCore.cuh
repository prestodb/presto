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

#include <cuda_runtime.h> // @manual

#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

template <typename T>
__device__ inline T& flatValue(void* base, int32_t blockBase) {
  return reinterpret_cast<T*>(base)[blockBase + threadIdx.x];
}

__device__ inline bool isNull(Operand* op, int32_t blockBase) {
  return op->nulls == nullptr || !op->nulls[blockBase + threadIdx.x];
}

template <typename T>
__device__ inline T getOperand(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    char* shared) {
  if (opIdx > kMinSharedMemIndex) {
    return reinterpret_cast<T*>(
        shared + opIdx - kMinSharedMemIndex)[blockIdx.x];
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

template <typename T>
__device__ inline T& flatResult(
    Operand** operands,
    OperandIndex opIdx,
    int32_t blockBase,
    char* shared) {
  if (opIdx >= kMinSharedMemIndex) {
    return reinterpret_cast<T*>(
        shared + opIdx - kMinSharedMemIndex)[threadIdx.x];
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

} // namespace facebook::velox::wave
