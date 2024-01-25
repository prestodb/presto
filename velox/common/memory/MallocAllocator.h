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
#include "velox/common/memory/MemoryAllocator.h"

DECLARE_bool(velox_memory_leak_check_enabled);

namespace facebook::velox::memory {
/// The implementation of MemoryAllocator using malloc.
class MallocAllocator : public MemoryAllocator {
 public:
  explicit MallocAllocator(size_t capacity);

  ~MallocAllocator() override {
    // TODO: Remove the check when memory leak issue is resolved.
    if (FLAGS_velox_memory_leak_check_enabled) {
      VELOX_CHECK(
          (allocatedBytes_ == 0) && (numAllocated_ == 0) && (numMapped_ == 0),
          "{}",
          toString());
    }
  }

  void registerCache(const std::shared_ptr<Cache>& cache) override {
    VELOX_CHECK_NULL(cache_);
    VELOX_CHECK_NOT_NULL(cache);
    VELOX_CHECK(cache->allocator() == this);
    cache_ = cache;
  }

  Cache* cache() const override {
    return cache_.get();
  }

  Kind kind() const override {
    return kind_;
  }

  size_t capacity() const override {
    return capacity_;
  }

  void freeContiguous(ContiguousAllocation& allocation) override;

  int64_t freeNonContiguous(Allocation& allocation) override;

  bool growContiguousWithoutRetry(
      MachinePageCount increment,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr) override;

  void freeBytes(void* p, uint64_t bytes) noexcept override;

  MachinePageCount unmap(MachinePageCount targetPages) override {
    // NOTE: MallocAllocator doesn't support unmap as it delegates all the
    // memory allocations to std::malloc.
    return 0;
  }

  size_t totalUsedBytes() const override {
    return allocatedBytes_;
  }

  MachinePageCount numAllocated() const override {
    return numAllocated_;
  }

  MachinePageCount numMapped() const override {
    return numMapped_;
  }

  bool checkConsistency() const override;

  std::string toString() const override;

 private:
  bool allocateNonContiguousWithoutRetry(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount minSizeClass = 0) override;

  bool allocateContiguousWithoutRetry(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount maxPages = 0) override;

  bool allocateContiguousImpl(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB,
      MachinePageCount maxPages);

  void freeContiguousImpl(ContiguousAllocation& allocation);

  void* allocateBytesWithoutRetry(uint64_t bytes, uint16_t alignment) override;

  void* allocateZeroFilledWithoutRetry(uint64_t bytes) override;

  // Increment current usage and check current allocator consistency to make
  // sure current usage does not go above 'capacity_'. If it goes above
  // 'capacity_', the increment will not be applied. Returns true if within
  // capacity, false otherwise.
  //
  // NOTE: This method should always be called BEFORE actual allocation.
  inline bool incrementUsage(int64_t bytes) {
    const auto originalBytes = allocatedBytes_.fetch_add(bytes);
    // We don't do the check when capacity_ is 0, meaning unlimited capacity.
    if (capacity_ != 0 && originalBytes + bytes > capacity_) {
      allocatedBytes_.fetch_sub(bytes);
      return false;
    }
    return true;
  }

  // Decrement current usage and check current allocator consistency to make
  // sure current usage does not go below 0. Throws if usage goes below 0.
  //
  // NOTE: This method should always be called AFTER actual free.
  inline void decrementUsage(int64_t bytes) {
    const auto originalBytes = allocatedBytes_.fetch_sub(bytes);
    if (originalBytes - bytes < 0) {
      // In case of inconsistency while freeing memory, do not revert in this
      // case because free is guaranteed to happen.
      VELOX_MEM_ALLOC_ERROR(fmt::format(
          "Trying to free {} bytes, which is larger than current allocated "
          "bytes {}",
          bytes,
          originalBytes))
    }
  }

  const Kind kind_;

  // Capacity in bytes. Total allocation byte is not allowed to exceed this
  // value.
  const size_t capacity_;

  // Current total allocated bytes by this 'MallocAllocator'.
  std::atomic<int64_t> allocatedBytes_{0};

  std::shared_ptr<Cache> cache_;
};
} // namespace facebook::velox::memory
