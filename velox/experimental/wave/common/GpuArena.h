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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/Cuda.h"

namespace facebook::velox::wave {

/// A contiguous range slab of device or universal memory for
/// backing small allocations. The caller is responsible for
/// serializing access across threads.
class GpuSlab {
 public:
  static constexpr int64_t kMinCapacityBytes = 128 * 1024 * 1024; // 128M

  GpuSlab(void* address, size_t capacityBytes, GpuAllocator* allocator);

  ~GpuSlab();

  // Returns an address for at least 'bytes' of memory inside this slab, nullptr
  // if there is no contiguous run of at least 'bytes'.
  void* allocate(uint64_t bytes);

  /// Frees an area returned by allocate().
  void free(void* address, uint64_t bytes);

  void* address() const {
    return reinterpret_cast<void*>(address_);
  }

  uint64_t byteSize() const {
    return byteSize_;
  }

  const std::map<uint64_t, uint64_t>& freeList() const {
    return freeList_;
  }

  const std::map<uint64_t, std::unordered_set<uint64_t>>& freeLookup() const {
    return freeLookup_;
  }

  uint64_t freeBytes() {
    return freeBytes_;
  }

  bool empty() {
    return freeBytes_ == byteSize_;
  }

  /// Checks internal consistency of this GpuSlab. Returns true if OK. May
  /// return false if there are concurrent alocations and frees during the
  /// consistency check. This is a false positive but not dangerous. This is for
  /// test only
  bool checkConsistency() const;

  /// translate lookup table to a string for debugging purpose only.
  std::string freeLookupStr();

  std::string toString() const;
  // Rounds up size to the next power of 2.
  static uint64_t roundBytes(uint64_t bytes);

 private:
  std::map<uint64_t, uint64_t>::iterator addFreeBlock(
      uint64_t addr,
      uint64_t bytes);

  void removeFromLookup(uint64_t addr, uint64_t bytes);

  void removeFreeBlock(uint64_t addr, uint64_t bytes);

  void removeFreeBlock(std::map<uint64_t, uint64_t>::iterator& itr);

  // Starting address of this slab.
  uint8_t* address_;

  // Total size of this slab.
  const uint64_t byteSize_;

  std::atomic<uint64_t> freeBytes_;

  // A sorted list with each entry mapping from free block address to size of
  // the free block
  std::map<uint64_t, uint64_t> freeList_;

  // A sorted look up structure that stores the block size as key and a set of
  // addresses of that size as value.
  std::map<uint64_t, std::unordered_set<uint64_t>> freeLookup_;

  GpuAllocator* const allocator_;
};

struct ArenaStatus {
  /// Number of allocated Buffers.
  int32_t numBuffers{0};

  /// Sum of capacity of allocated buffers.
  int64_t capacity{};

  /// Currently used bytes. Larger than capacity because of padding.
  int64_t allocatedBytes{0};
};

/// A class that manages a set of GpuSlabs. It is able to adapt itself by
/// growing the number of its managed GpuSlab's when extreme memory
/// fragmentation happens.
class GpuArena {
 public:
  GpuArena(
      uint64_t singleArenaCapacity,
      GpuAllocator* allocator,
      uint64_t standbyCapacity = 0);

  WaveBufferPtr allocateBytes(uint64_t bytes);

  template <typename T>
  WaveBufferPtr allocate(uint64_t items) {
    static_assert(std::is_trivially_destructible_v<T>);
    return allocateBytes(sizeof(T) * items);
  }

  template <typename T>
  T* allocate(int n, WaveBufferPtr& holder) {
    holder = allocate<T>(n);
    return holder->as<T>();
  }

  void free(Buffer* buffer);

  const std::map<uint64_t, std::shared_ptr<GpuSlab>>& slabs() const {
    return arenas_;
  }

  uint64_t maxCapacity() const {
    return maxCapacity_;
  }

  uint64_t totalAllocated() const {
    return totalAllocated_;
  }

  uint64_t numAllocations() const {
    return numAllocations_;
  }

  uint64_t retainedSize() const {
    return capacity_;
  }

  void setSizes(uint64_t arenaSize, uint64_t standbyCapacity) {
    singleArenaCapacity_ = arenaSize;
    standbyCapacity_ = standbyCapacity;
  }

  bool isDevice() const {
    return allocator_->isDevice();
  }

  /// Checks magic numbers and returns the sum of allocated capacity. Actual
  /// sizes are padded to larger.
  ArenaStatus checkBuffers();

  std::string toString() const;

 private:
  // A preallocated array of Buffer handles for memory of 'this'.
  struct Buffers {
    Buffers();
    std::array<Buffer, 1024> buffers;
  };

  // Returns a new reference counting pointer to a new Buffer initialized to
  // 'ptr' and 'size'. 'size' is the size to fre, 'capacity' is the usable size
  // excluding magic numbers and padding.
  WaveBufferPtr getBuffer(void* ptr, size_t capacity, size_t size);

  // Serializes all activity in 'this'.
  std::mutex mutex_;

  // All buffers referencing memory from 'this'. All must be locatable for e.g.
  // compaction.
  std::vector<std::unique_ptr<Buffers>> allBuffers_;

  // Head of Buffer free list.
  Buffer* firstFreeBuffer_{nullptr};

  // Total capacity in all arenas.
  uint64_t capacity_{0};

  // Capacity in bytes for a single GpuSlab managed by this.
  uint64_t singleArenaCapacity_;

  // Lower bound of capacity to keep around even if usage is below this.
  uint64_t standbyCapacity_{0};

  GpuAllocator* const allocator_;

  // A sorted list of GpuSlab by its initial address
  std::map<uint64_t, std::shared_ptr<GpuSlab>> arenas_;

  // All allocations should come from this GpuSlab. When it is no longer able
  // to handle allocations it will be updated to a newly created GpuSlab.
  std::shared_ptr<GpuSlab> currentArena_;

  uint64_t numAllocations_{0};
  uint64_t totalAllocated_{0};
  uint64_t maxCapacity_{0};
};

} // namespace facebook::velox::wave
