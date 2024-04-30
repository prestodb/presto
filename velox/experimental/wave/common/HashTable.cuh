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

#include <cuda/atomic>
#include <cuda/semaphore>

#include <cub/thread/thread_load.cuh>
#include <cub/util_ptx.cuh>

#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/FreeSet.cuh"
#include "velox/experimental/wave/common/Hash.h"
#include "velox/experimental/wave/common/HashTable.h"

namespace facebook::velox::wave {

#define GPF() *(long*)0 = 0

template <typename T, typename U>
inline __device__ cuda::atomic<T, cuda::thread_scope_device>* asDeviceAtomic(
    U* ptr) {
  return reinterpret_cast<cuda::atomic<T, cuda::thread_scope_device>*>(ptr);
}

template <typename T>
inline bool __device__ atomicTryLock(T* lock) {
  return 0 ==
      asDeviceAtomic<int32_t>(lock)->exchange(1, cuda::memory_order_consume);
}

template <typename T>
inline void __device__ atomicUnlock(T* lock) {
  asDeviceAtomic<int32_t>(lock)->store(0, cuda::memory_order_release);
}

/// Allocator subclass that defines device member functions.
struct RowAllocator : public HashPartitionAllocator {
  template <typename T>
  T* __device__ allocateRow() {
    auto fromFree = getFromFree();
    if (fromFree != kEmpty) {
      ++numFromFree;
      return reinterpret_cast<T*>(base + fromFree);
    }
    auto offset = atomicAdd(&rowOffset, rowSize);

    if (offset + rowSize < cub::ThreadLoad<cub::LOAD_CG>(&stringOffset)) {
      if (!inRange(base + offset)) {
        GPF();
      }
      return reinterpret_cast<T*>(base + offset);
    }
    return nullptr;
  }

  uint32_t __device__ getFromFree() {
    uint32_t item = reinterpret_cast<FreeSet<uint32_t, 1024>*>(freeSet)->get();
    if (item != kEmpty) {
      ++numFromFree;
    }
    return item;
  }

  void __device__ freeRow(void* row) {
    if (!inRange(row)) {
      GPF();
    }
    uint32_t offset = reinterpret_cast<uint64_t>(row) - base;
    numFull += reinterpret_cast<FreeSet<uint32_t, 1024>*>(freeSet)->put(
                   offset) == false;
  }

  template <typename T>
  T* __device__ allocate(int32_t cnt) {
    uint32_t size = sizeof(T) * cnt;
    auto offset = atomicSub(&stringOffset, size);
    if (offset - size > cub::ThreadLoad<cub::LOAD_CG>(&rowOffset)) {
      if (!inRange(base + offset - size)) {
        GPF();
      }
      return reinterpret_cast<T*>(base + offset - size);
    }
    return nullptr;
  }

  template <typename T>
  bool __device__ inRange(T ptr) {
    return reinterpret_cast<uint64_t>(ptr) >= base &&
        reinterpret_cast<uint64_t>(ptr) < base + capacity;
  }
};

inline uint8_t __device__ hashTag(uint64_t h) {
  return 0x80 | (h >> 32);
}

struct GpuBucket : public GpuBucketMembers {
  template <typename RowType>
  inline RowType* __device__ load(int32_t idx) const {
    uint64_t uptr = reinterpret_cast<const uint32_t*>(&data)[idx];
    if (uptr == 0) {
      return nullptr;
    }
    uptr |= static_cast<uint64_t>(data[idx + 8]) << 32;
    return reinterpret_cast<RowType*>(uptr);
  }

  template <typename RowType>
  inline RowType* __device__ loadConsume(int32_t idx) {
    uint64_t uptr =
        asDeviceAtomic<uint32_t>(&data)[idx].load(cuda::memory_order_consume);
    if (uptr == 0) {
      return nullptr;
    }
    uptr |= static_cast<uint64_t>(data[idx + 8]) << 32;
    return reinterpret_cast<RowType*>(uptr);
  }

  template <typename RowType>
  inline RowType* __device__ loadWithWait(int32_t idx) {
    RowType* hit;
    do {
      // It could be somebody inserted the tag but did not fill in the
      // pointer. The pointer is coming in a few clocks.
      hit = loadConsume<RowType>(idx);
    } while (!hit);
    return hit;
  }

  inline void __device__ store(int32_t idx, void* ptr) {
    auto uptr = reinterpret_cast<uint64_t>(ptr);
    data[8 + idx] = uptr >> 32;
    // The high part must be seen if the low part is seen.
    asDeviceAtomic<uint32_t>(&data)[idx].store(
        uptr, cuda::memory_order_release);
  }

  bool __device__ addNewTag(uint8_t tag, uint32_t oldTags, uint8_t tagShift) {
    uint32_t newTags = oldTags | ((static_cast<uint32_t>(tag) << tagShift));
    return (oldTags == atomicCAS(&tags, oldTags, newTags));
  }
};

/// Shared memory state for an updating probe.
struct ProbeShared {
  int32_t* inputRetries;
  int32_t* outputRetries;
  uint32_t numKernelRetries;
  uint32_t numHostRetries;
  int32_t blockBase;
  int32_t blockEnd;
  int32_t numRounds;
  int32_t toDo;
  int32_t done;
  int32_t numUpdated;
  int32_t numTried;

  /// Initializes a probe. Sets outputRetries and clears inputRetries and other
  /// state.
  void __device__ init(HashProbe* probe, int32_t base) {
    inputRetries = nullptr;
    outputRetries = probe->kernelRetries1;
    numKernelRetries = 0;
    numHostRetries = 0;
    blockBase = base;
    toDo = 0;
    done = 0;
    numRounds = 0;
  }

  // Resets retrry count and swaps input and output retries.
  void __device__ nextRound(HashProbe* probe) {
    numKernelRetries = 0;
    if (!inputRetries) {
      // This is after the initial round where there are no input retries.
      inputRetries = outputRetries;
      outputRetries = probe->kernelRetries2;
    } else {
      // swap input and output retries.
      auto temp = outputRetries;
      outputRetries = inputRetries;
      inputRetries = temp;
    }
  }
};

class GpuHashTable : public GpuHashTableBase {
 public:
  static constexpr int32_t kExclusive = 1;

  static int32_t updatingProbeSharedSize() {
    return sizeof(ProbeShared);
  }

  template <typename RowType, typename Ops>
  void __device__ readOnlyProbe(HashProbe* probe, Ops ops) {
    int32_t blockBase = ops.blockBase(probe);
    int32_t end = ops.numRowsInBlock(probe) + blockBase;
    for (auto i = blockBase + threadIdx.x; i < end; i += blockDim.x) {
      auto h = ops.hash(i, probe);
      uint32_t tagWord = hashTag(h);
      tagWord |= tagWord << 8;
      tagWord = tagWord | tagWord << 16;
      auto bucketIdx = h & sizeMask;
      for (;;) {
        GpuBucket* bucket = buckets + bucketIdx;
        auto tags = bucket->tags;
        auto hits = __vcmpeq4(tags, tagWord) & 0x01010101;
        while (hits) {
          auto hitIdx = (__ffs(hits) - 1) / 8;
          auto* hit = bucket->load<RowType>(hitIdx);
          if (ops.compare(this, hit, i, probe)) {
            ops.hit(i, probe, hit);
            goto done;
          }
          hits = hits & (hits - 1);
        }
        if (__vcmpeq4(tags, 0)) {
          ops.miss(i, probe);
          break;
        }
        bucketIdx = (bucketIdx + 1) & sizeMask;
      }
    done:;
    }
  }

  template <typename RowType, typename Ops>
  void __device__ updatingProbe(HashProbe* probe, Ops ops) {
    extern __shared__ __align__(16) char smem[];
    auto* sharedState = reinterpret_cast<ProbeShared*>(smem);
    if (threadIdx.x == 0) {
      sharedState->init(probe, ops.blockBase(probe));
    }
    __syncthreads();
    auto lane = cub::LaneId();
    constexpr int32_t kWarpThreads = 1 << CUB_LOG_WARP_THREADS(0);
    auto warp = threadIdx.x / kWarpThreads;
    int32_t end = ops.numRowsInBlock(probe) + sharedState->blockBase;
    for (auto i = threadIdx.x + sharedState->blockBase; i < end;
         i += blockDim.x) {
      auto start = i & ~(kWarpThreads - 1);
      uint32_t laneMask =
          start + kWarpThreads <= end ? ~0 : lowMask<uint32_t>(end - start);
      auto h = ops.hash(i, probe);
      uint32_t tagWord = hashTag(h);
      tagWord |= tagWord << 8;
      tagWord = tagWord | tagWord << 16;
      auto bucketIdx = h & sizeMask;
      uint32_t misses = 0;
      RowType* hit = nullptr;
      RowType* toInsert = nullptr;
      int32_t hitIdx;
      GpuBucket* bucket;
      uint32_t tags;
      for (;;) {
        bucket = buckets + bucketIdx;
      reprobe:
        tags = asDeviceAtomic<uint32_t>(&bucket->tags)
                   ->load(cuda::memory_order_consume);
        auto hits = __vcmpeq4(tags, tagWord) & 0x01010101;
        while (hits) {
          hitIdx = (__ffs(hits) - 1) / 8;
          auto candidate = bucket->loadWithWait<RowType>(hitIdx);
          if (ops.compare(this, candidate, i, probe)) {
            if (toInsert) {
              freeInsertable(toInsert, h);
            }
            hit = candidate;
            break;
          }
          hits = hits & (hits - 1);
        }
        if (hit) {
          break;
        }
        misses = __vcmpeq4(tags, 0);
        if (misses) {
          auto success = ops.insert(
              this,
              partitionIdx(h),
              bucket,
              misses,
              tags,
              tagWord,
              i,
              probe,
              toInsert);
          if (success == ProbeState::kRetry) {
            goto reprobe;
          }
          if (success == ProbeState::kNeedSpace) {
            addHostRetry(sharedState, i, probe);
          }
          hit = toInsert;
          break;
        }
        bucketIdx = (bucketIdx + 1) & sizeMask;
      }
      // Every lane has a hit, or a nullptr if out of space.
      uint32_t peers =
          __match_any_sync(laneMask, reinterpret_cast<int64_t>(hit));
      if (hit) {
        int32_t leader = (kWarpThreads - 1) - __clz(peers);
        RowType* writable = nullptr;
        if (lane == leader) {
          writable = ops.getExclusive(this, bucket, hit, hitIdx, warp);
        }
        auto toUpdate = peers;
        ProbeState success = ProbeState::kDone;
        while (toUpdate) {
          auto peer = __ffs(toUpdate) - 1;
          auto idxToUpdate = __shfl_sync(peers, i, peer);
          if (lane == leader) {
            if (success == ProbeState::kDone) {
              success = ops.update(this, bucket, writable, idxToUpdate, probe);
            }
            if (success == ProbeState::kNeedSpace) {
              addHostRetry(sharedState, idxToUpdate, probe);
            }
            if (success != ProbeState::kDone) {
              printf("");
            }
          }
          toUpdate &= toUpdate - 1;
        }
        if (lane == leader) {
          ops.writeDone(writable);
        }
      } else {
        printf("");
      }
    }
  }

  template <typename RowType>
  void __device__ freeInsertable(RowType*& row, uint64_t h) {
    allocators[partitionIdx(h)].freeRow(row);
    row = nullptr;
  }

  int32_t __device__ partitionIdx(uint64_t h) const {
    return (h & partitionMask) >> partitionShift;
  }

 private:
  static void __device__
  addHostRetry(ProbeShared* shared, int32_t i, HashProbe* probe) {
    probe->hostRetries
        [shared->blockBase + atomicAdd(&shared->numHostRetries, 1)] = i;
  }
};
} // namespace facebook::velox::wave
