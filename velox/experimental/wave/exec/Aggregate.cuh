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

namespace facebook::velox::wave {

__device__ __forceinline__ void aggregateKernel(
    const IAggregate& agg,
    WaveShared* shared,
    ErrorCode& laneStatus) {
  auto state =
      reinterpret_cast<DeviceAggregation*>(shared->states[agg.stateIndex]);
  char* row = state->singleRow;
  for (auto i = 0; i < agg.numAggregates; ++i) {
    auto& acc = agg.aggregates[i];
    int64_t value = 0;
    if (laneStatus == ErrorCode::kOk) {
      operandOrNull(
          shared->operands, acc.arg1, shared->blockBase, &shared->data, value);
    }
    using Reduce = cub::WarpReduce<int64_t>;
    auto sum =
        Reduce(*reinterpret_cast<Reduce::TempStorage*>(shared)).Sum(value);
    if ((threadIdx.x & (kWarpThreads - 1)) == 0) {
      auto* data = addCast<unsigned long long>(row, acc.accumulatorOffset);
      atomicAdd(data, static_cast<unsigned long long>(sum));
    }
  }
}

__device__ __forceinline__ void readAggregateKernel(
    const IAggregate& agg,
    WaveShared* shared) {
  if (shared->blockBase > 0) {
    if (threadIdx.x == 0) {
      shared->status->numRows = 0;
    }
    __syncthreads();
    return;
  }
  if (threadIdx.x == 0) {
    auto state =
        reinterpret_cast<DeviceAggregation*>(shared->states[agg.stateIndex]);
    char* row = state->singleRow;
    shared->status->numRows = 1;
    for (auto i = 0; i < agg.numAggregates; ++i) {
      auto& acc = agg.aggregates[i];
      flatResult<int64_t>(
          shared->operands, acc.result, shared->blockBase, &shared->data) =
          *addCast<int64_t>(row, acc.accumulatorOffset);
    }
  }
  __syncthreads();
}

} // namespace facebook::velox::wave
