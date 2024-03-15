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

#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {
// A set of Allocations holding the fixed width payload
// rows. The Runs are filled to the end except for the last one. This
// is used for iterating over the payload for rehashing, returning
// results etc. This is used via HashStringAllocator for variable length
// allocation for backing ByteStreams for complex objects. In that case, there
// is a current run that is appended to and when this is exhausted a new run is
// started.
class AllocationPool {
 public:
  static constexpr int32_t kMinPages = 16;

  explicit AllocationPool(memory::MemoryPool* pool) : pool_(pool) {}

  ~AllocationPool() {
    clear();
  }

  void clear();

  // Allocate a buffer from this pool, optionally aligned.  The alignment can
  // only be power of 2.
  char* allocateFixed(uint64_t bytes, int32_t alignment = 1);

  // Starts a new run for variable length allocation. The actual size
  // is at least one machine page. Throws std::bad_alloc if no space.
  void newRun(int64_t preferredSize);

  int32_t numRanges() const {
    return allocations_.size() + largeAllocations_.size();
  }

  /// Returns the indexth contiguous range. If the range is a large allocation,
  /// returns the hugepage aligned range of contiguous huge pages in the range.
  /// For the last range, i.e. the one allocations come from, the size is the
  /// distance from start to first byte after last allocation.
  folly::Range<char*> rangeAt(int32_t index) const;

  int64_t currentOffset() const {
    return currentOffset_;
  }

  int64_t allocatedBytes() const {
    return usedBytes_;
  }

  /// Returns the number of bytes allocatable without growing 'this'.
  int64_t freeBytes() const {
    if (largeAllocations_.empty()) {
      return freeAddressableBytes();
    }
    return largeAllocations_.back().size() - currentOffset_;
  }

  // Returns pointer to first unallocated byte in the current run.
  char* firstFreeInRun() {
    VELOX_DCHECK_GT(testingFreeAddressableBytes(), 0);
    return startOfRun_ + currentOffset_;
  }

  // Sets the first free position in the current run.
  void setFirstFreeInRun(const char* firstFree) {
    const auto offset = firstFree - startOfRun_;
    VELOX_CHECK(
        offset >= 0 && offset <= bytesInRun_,
        "Trying to set end of allocation outside of last allocated run");
    currentOffset_ = offset;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

  /// Returns true if 'ptr' is inside the range allocations are made from.
  bool isInCurrentRange(void* ptr) const {
    return reinterpret_cast<char*>(ptr) >= startOfRun_ &&
        reinterpret_cast<char*>(ptr) < startOfRun_ + bytesInRun_;
  }

  int64_t hugePageThreshold() const {
    return hugePageThreshold_;
  }

  /// Sets the size after which 'this' switches to large mmaps with huge pages.
  void setHugePageThreshold(int64_t size) {
    hugePageThreshold_ = size;
  }

  int64_t testingFreeAddressableBytes() const {
    return freeAddressableBytes();
  }

 private:
  static constexpr int64_t kDefaultHugePageThreshold = 256 * 1024;
  static constexpr int64_t kMaxMmapBytes = 512 << 20; // 512 MB

  // Returns the offset from 'startOfRun_' after which the last large
  // allocation must be grown. There are mapped addresses all the way
  // to 'bytesInRun_' ut they are not marked used by the
  // pool/allocator. So use growContiguous() to update this.
  int64_t endOfReservedRun() {
    if (largeAllocations_.empty()) {
      return bytesInRun_;
    }
    return largeAllocations_.back().size();
  }

  // Returns the number of bytes between first unallocated and the end of the
  // addresses mapped in the last Allocation/ContiguousAllocation. This can be
  // larger than the space reported as allocated in 'pool_'.
  int64_t freeAddressableBytes() const {
    return bytesInRun_ - currentOffset_;
  }

  // Increases the reservation in 'pool_' if 'bytesRequested' moves
  // 'currentOffset_' goes past current end of last large allocation, otherwise
  // simply updates 'currentOffset_'.
  void maybeGrowLastAllocation(uint64_t bytesRequested);

  void newRunImpl(memory::MachinePageCount numPages);

  memory::MemoryPool* pool_;
  std::vector<memory::Allocation> allocations_;
  std::vector<memory::ContiguousAllocation> largeAllocations_;

  // Points to the start of the run from which allocations are being made.
  char* startOfRun_{nullptr};

  // Total addressable bytes from 'startOfRun_'. Not all are necessarily
  // declared allocated in 'pool_'. See growLastAllocation().
  int64_t bytesInRun_{0};

  // Offset of first unused byte from 'startOfRun_'.
  int64_t currentOffset_ = 0;

  // Total space returned to users. Size of allocations can be larger specially
  // if mmapped in advance of use.
  int64_t usedBytes_{0};

  // Start using large mmaps with huge pages after 'usedBytes_' exceeds this.
  int64_t hugePageThreshold_{kDefaultHugePageThreshold};
};

} // namespace facebook::velox::memory
