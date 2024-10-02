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
#include <cub/thread/thread_load.cuh>
#include <cub/util_ptx.cuh>
#include "velox/experimental/wave/common/ArenaWithFreeBase.h"
#include "velox/experimental/wave/common/FreeSet.cuh"

namespace facebook::velox::wave {

/// Allocator subclass that defines device member functions.
struct ArenaWithFree : public ArenaWithFreeBase {
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
        assert(false);
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
      assert(false);
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
        assert(false);
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

} // namespace facebook::velox::wave
