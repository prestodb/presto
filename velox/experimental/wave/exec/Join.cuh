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

#include "velox/experimental/wave/common/HashTable.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

namespace facebook::velox::wave {

inline __device__ JoinShared* joinShared(WaveShared* shared) {
  return reinterpret_cast<JoinShared*>(&shared->data);
}

template <int32_t gridStatusSize, int32_t blockStatusOffset>
int64_t __device__ loadJoinNext(WaveShared* shared, ErrorCode& laneStatus) {
  auto* status = blockStatus<HashJoinExpandBlockStatus>(
      shared, gridStatusSize, blockStatusOffset);
  if (threadIdx.x == 0) {
    shared->numRows = 0;
  }
  auto h = reinterpret_cast<int64_t>(status->next[threadIdx.x]);
  laneStatus = h != 0 ? ErrorCode::kOk : ErrorCode::kInactive;
  return h;
}

template <
    typename RowType,
    int32_t indicesIdx,
    int32_t gridstatusOffset,
    int32_t gridStatusSize,
    int32_t blockStatusOffset,
    int32_t hitsIdx>
bool __device__ __forceinline__ joinResult(
    int64_t& hitAsInt,
    bool filterResult,
    bool joinContinue,
    ErrorCode laneStatus,
    WaveShared* shared,
    bool hasDuplicates) {
  RowType* hit =
      reinterpret_cast<RowType*>(laneStatus == ErrorCode::kOk ? hitAsInt : 0);
  if (threadIdx.x == 0) {
    auto* j = joinShared(shared);
    if (hasDuplicates) {
      j->gridStatus =
          gridStatus<HashJoinExpandGridStatus>(shared, gridstatusOffset);
      j->blockStatus = blockStatus<HashJoinExpandBlockStatus>(
          shared, gridStatusSize, blockStatusOffset);
      j->anyNext = 0;
    } else {
      j->gridStatus = nullptr;
      j->blockStatus = nullptr;
    }
  }
  if (!joinContinue) {
    auto nth = exclusiveSum<int32_t, kBlockSize>(
        (hit ? filterResult : 0), &shared->numRows, joinShared(shared)->temp);
    if (hit && filterResult) {
      auto* hits = reinterpret_cast<RowType**>(shared->operands[hitsIdx]->base);
      hits[shared->blockBase + nth] = hit;
      auto* indices =
          reinterpret_cast<int32_t*>(shared->operands[indicesIdx]->base);
      indices[shared->blockBase + nth] = shared->blockBase + threadIdx.x;
    }
    __syncthreads();
    if (threadIdx.x == kBlockSize - 1) {
      shared->numRows = nth + (hit && filterResult);
    }

    if (!hasDuplicates) {
      // syncthreads in caller.
      return false;
    }
    RowType* next = nullptr;
    if (hit) {
      next = *hit->nextPtr();
      hit = next;
      hitAsInt = reinterpret_cast<int64_t>(hit);
    }
    joinShared(shared)->blockStatus->next[threadIdx.x] = next;
    uint32_t flags = __ballot_sync(0xffffffff, next != nullptr);
    if ((threadIdx.x & 31) == 0) {
      if (flags) {
        atomicOr(&joinShared(shared)->anyNext, flags);
        joinShared(shared)->gridStatus->anyContinuable = true;
      }
    }
    __syncthreads();
    if (threadIdx.x == 0) {
      shared->localContinue =
          shared->numRows < kBlockSize - 64 && joinShared(shared)->anyNext;
    }
    __syncthreads();
    // All threads return the same. true if there is space in the output and
    // nexts to look at.
    return shared->localContinue;
  }
  // We come here when there are  places to fill above shared->numRows.
  bool laneFull = false;
  if (hit && filterResult) {
    auto row = atomicAdd(&shared->numRows, 1);
    if (row < kBlockSize) {
      auto* hits = reinterpret_cast<RowType**>(shared->operands[hitsIdx]->base);
      auto* indices =
          reinterpret_cast<int32_t*>(shared->operands[indicesIdx]->base);
      indices[shared->blockBase + row] = shared->blockBase + threadIdx.x;
      hits[shared->blockBase + row] = hit;
    } else {
      laneFull = true;
    }
  }
  // Make sure joinShared is seen on all threads.
  __syncthreads();
  if (!laneFull && hit) {
    auto* next = *hit->nextPtr();
    joinShared(shared)->blockStatus->next[threadIdx.x] = next;
    hitAsInt = reinterpret_cast<int64_t>(next);
  }

  if (threadIdx.x == 0 && shared->numRows > kBlockSize) {
    shared->numRows = kBlockSize;
  }
  uint32_t flags = __ballot_sync(
      0xffffffff,
      joinShared(shared)->blockStatus->next[threadIdx.x] != nullptr);
  if ((threadIdx.x & 31) == 0) {
    if (flags) {
      atomicOr(&joinShared(shared)->anyNext, flags);
    }
  }
  __syncthreads();
  if (threadIdx.x == 0) {
    if (joinShared(shared)->anyNext) {
      joinShared(shared)->gridStatus->anyContinuable = 1;
    }
    shared->localContinue =
        shared->numRows < kBlockSize - 32 && joinShared(shared)->anyNext;
  }
  __syncthreads();
  return shared->localContinue;
}

template <typename RowType, int32_t hitsIdx, typename CopyRow>
void __device__ __forceinline__
joinRow(ErrorCode laneStatus, WaveShared* shared, CopyRow copy) {
  if (laneStatus == ErrorCode::kOk) {
    RowType** hits =
        reinterpret_cast<RowType**>(shared->operands[hitsIdx]->base);
    copy(
        hits[shared->blockBase + threadIdx.x], shared->blockBase + threadIdx.x);
  }
}

} // namespace facebook::velox::wave
