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
#include "velox/experimental/wave/common/HashTable.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

namespace facebook::velox::wave {

struct SumGroupRow {
  uint32_t lock;
  uint32_t nulls;
  int64_t key;
  int32_t accNulls;
  int64_t sums[20];
};

inline void __device__ increment(int64_t& a, int64_t i) {
  atomicAdd((unsigned long long*)&a, (unsigned long long)i);
}

class SumGroupByOps {
 public:
  __device__ SumGroupByOps(WaveShared* shared, const IAggregate* inst)
      : shared_(shared), inst_(inst) {}

  uint64_t __device__ hash(int32_t i) {
    int64_t key;
    if (operandOrNull(
            shared_->operands,
            *reinterpret_cast<int16_t*>(
                &inst_->aggregates[inst_->numAggregates]),
            shared_->blockBase,
            &shared_->data,
            key)) {
      constexpr uint64_t kMul = 0x9ddfea08eb382d69ULL;
      return kMul * key;
    }
    return 1;
  }

  uint64_t __device__ hashRow(SumGroupRow* row) {
    constexpr uint64_t kMul = 0x9ddfea08eb382d69ULL;
    return kMul * row->key;
  }

  bool __device__ compare(GpuHashTable* table, SumGroupRow* row, int32_t i) {
    int64_t key;
    auto k =
        asDeviceAtomic<int64_t>(&row->key)->load(cuda::memory_order_consume);
    if (operandOrNull(
            shared_->operands,
            *reinterpret_cast<int16_t*>(
                &inst_->aggregates[inst_->numAggregates]),
            shared_->blockBase,
            &shared_->data,
            key)) {
      return k == key;
    }
    return false;
  }

  SumGroupRow* __device__
  newRow(GpuHashTable* table, int32_t partition, int32_t i) {
    auto* allocator = &table->allocators[partition];
    auto row = allocator->allocateRow<SumGroupRow>();
    if (row) {
      for (auto i = 0; i < inst_->numAggregates; ++i) {
        row->sums[i] = 0;
      }
      int64_t k;
      operandOrNull(
          shared_->operands,
          *reinterpret_cast<int16_t*>(&inst_->aggregates[inst_->numAggregates]),
          shared_->blockBase,
          &shared_->data,
          k);
      asDeviceAtomic<int64_t>(&row->key)->store(k, cuda::memory_order_release);
    }
    return row;
  }

  ProbeState __device__ insert(
      GpuHashTable* table,
      int32_t partition,
      GpuBucket* bucket,
      uint32_t misses,
      uint32_t oldTags,
      uint32_t tagWord,
      int32_t i,
      SumGroupRow*& row) {
    if (!row) {
      row = newRow(table, partition, i);
      if (!row) {
        return ProbeState::kNeedSpace;
      }
    }
    auto missShift = __ffs(misses) - 1;
    if (!bucket->addNewTag(tagWord, oldTags, missShift)) {
      return ProbeState::kRetry;
    }
    bucket->store(missShift / 8, row);
    increment(table->numDistinct, 1);
    return ProbeState::kDone;
  }

  void __device__ addHostRetry(int32_t i) {
    shared_->hasContinue = true;
    shared_->status[i / kBlockSize].errors[i & (kBlockSize - 1)] =
        ErrorCode::kInsufficientMemory;
  }

  void __device__
  freeInsertable(GpuHashTable* table, SumGroupRow* row, uint64_t h) {
    int32_t partition = table->partitionIdx(h);
    auto* allocator = &table->allocators[partition];
    allocator->markRowFree(row);
  }

  SumGroupRow* __device__ getExclusive(
      GpuHashTable* table,
      GpuBucket* bucket,
      SumGroupRow* row,
      int32_t hitIdx) {
    return row;
  }

  void __device__ writeDone(SumGroupRow* row) {}

  ProbeState __device__
  update(GpuHashTable* table, GpuBucket* bucket, SumGroupRow* row, int32_t i) {
    int32_t numAggs = inst_->numAggregates;
    for (auto acc = 0; acc < numAggs; ++acc) {
      int64_t x;
      operandOrNull(
          shared_->operands,
          inst_->aggregates[acc].arg1,
          shared_->blockBase,
          &shared_->data,
          x);
      increment(row->sums[acc], x);
    }
    return ProbeState::kDone;
  }

  WaveShared* shared_;
  const IAggregate* inst_;
};

void __device__ __forceinline__ interpretedGroupBy(
    WaveShared* shared,
    DeviceAggregation* deviceAggregation,
    const IAggregate* agg,
    ErrorCode& laneStatus) {
  SumGroupByOps ops(shared, agg);
  auto* table = reinterpret_cast<GpuHashTable*>(deviceAggregation->table);
  if (shared->isContinue) {
    laneStatus = laneStatus == ErrorCode::kInsufficientMemory
        ? ErrorCode::kOk
        : ErrorCode::kInactive;
    shared->status->errors[threadIdx.x] = laneStatus;
  }
  // Reset the return status for this stream.
  if (threadIdx.x == 0) {
    auto status = gridStatus<AggregateReturn>(shared, agg->status);
    status->numDistinct = 0;
  }

  table->updatingProbe<SumGroupRow>(
      threadIdx.x, cub::LaneId(), laneActive(laneStatus), ops);
  __syncthreads();
  laneStatus = shared->status->errors[threadIdx.x];
  if (threadIdx.x == 0 && shared->hasContinue) {
    auto ret = gridStatus<AggregateReturn>(shared, agg->status);
    ret->numDistinct = table->numDistinct;
  }
  __syncthreads();
  if (threadIdx.x == 0 && shared->isContinue) {
    shared->isContinue = false;
  }
  __syncthreads();
}

__device__ __forceinline__ void aggregateKernel(
    const IAggregate& agg,
    WaveShared* shared,
    ErrorCode& laneStatus) {
  auto state =
      reinterpret_cast<DeviceAggregation*>(shared->states[agg.stateIndex]);
  if (agg.numKeys) {
    interpretedGroupBy(shared, state, &agg, laneStatus);
  } else {
    char* row = state->singleRow;
    for (auto i = 0; i < agg.numAggregates; ++i) {
      auto& acc = agg.aggregates[i];
      int64_t value = 0;
      if (laneStatus == ErrorCode::kOk) {
        operandOrNull(
            shared->operands,
            acc.arg1,
            shared->blockBase,
            &shared->data,
            value);
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
}

__device__ __forceinline__ void readAggregateKernel(
    const IAggregate* agg,
    WaveShared* shared) {
  auto state =
      reinterpret_cast<DeviceAggregation*>(shared->states[agg->stateIndex]);
  if (state->resultRowPointers) {
    if (shared->streamIdx >= state->numReadStreams) {
      if (threadIdx.x == 0) {
        shared->status[blockIdx.x].numRows = 0;
      }
    } else {
      auto rowIdx = blockIdx.x * kBlockSize + threadIdx.x + 1;
      auto numRows = state->resultRowPointers[shared->streamIdx][0];
      if (rowIdx <= numRows) {
        int64_t* row = reinterpret_cast<int64_t*>(
            state->resultRowPointers[shared->streamIdx][rowIdx]);
        // Copy keys and accumulators to output.
        auto* keys = reinterpret_cast<OperandIndex*>(
            &agg->aggregates[agg->numAggregates]);
        for (auto i = 0; i < agg->numKeys; ++i) {
          auto opIdx = keys[i];
          auto k = *addCast<int64_t>(row, (i + 1) * sizeof(int64_t));
          flatResult<int64_t>(
              shared->operands, opIdx, shared->blockBase, &shared->data) = k;
        }
        for (auto i = 0; i < agg->numAggregates; ++i) {
          auto& acc = agg->aggregates[i];
          flatResult<int64_t>(
              shared->operands, acc.result, shared->blockBase, &shared->data) =
              *addCast<int64_t>(row, acc.accumulatorOffset);
        }
      }
      if (threadIdx.x == 0) {
        shared->numRows = rowIdx + kBlockSize <= numRows
            ? kBlockSize
            : numRows - blockIdx.x * kBlockSize;
      }
    }
  } else {
    if (shared->blockBase > 0) {
      if (threadIdx.x == 0) {
        shared->numRows = 0;
      }
      __syncthreads();
      return;
    }
    if (threadIdx.x == 0) {
      char* row = state->singleRow;
      shared->status->numRows = 1;
      for (auto i = 0; i < agg->numAggregates; ++i) {
        auto& acc = agg->aggregates[i];
        flatResult<int64_t>(
            shared->operands, acc.result, shared->blockBase, &shared->data) =
            *addCast<int64_t>(row, acc.accumulatorOffset);
      }
    }
  }
  __syncthreads();
}

} // namespace facebook::velox::wave
