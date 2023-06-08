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
  explicit MallocAllocator(size_t capacity = 0);

  ~MallocAllocator() override {
    // TODO: Remove the check when memory leak issue is resolved.
    if (FLAGS_velox_memory_leak_check_enabled) {
      VELOX_CHECK(
          (allocatedBytes_ == 0) && (numAllocated_ == 0) && (numMapped_ == 0),
          "{}",
          toString());
    }
  }

  Kind kind() const override {
    return kind_;
  }

  bool allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount minSizeClass = 0) override;

  int64_t freeNonContiguous(Allocation& allocation) override;

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr) override {
    VELOX_CHECK_GT(numPages, 0);
    bool result;
    stats_.recordAllocate(AllocationTraits::pageBytes(numPages), 1, [&]() {
      result = allocateContiguousImpl(
          numPages, collateral, allocation, reservationCB);
    });
    return result;
  }

  void freeContiguous(ContiguousAllocation& allocation) override {
    stats_.recordFree(
        allocation.size(), [&]() { freeContiguousImpl(allocation); });
  }

  void* allocateBytes(uint64_t bytes, uint16_t alignment) override;

  void* allocateZeroFilled(uint64_t bytes) override;

  void freeBytes(void* p, uint64_t bytes) noexcept override;

  MachinePageCount numAllocated() const override {
    return numAllocated_;
  }

  MachinePageCount numMapped() const override {
    return numMapped_;
  }

  Stats stats() const override {
    return stats_;
  }

  bool checkConsistency() const override;

  std::string toString() const override;

 private:
  bool allocateContiguousImpl(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB);

  void freeContiguousImpl(ContiguousAllocation& allocation);

  /// Increment current usage and check current allocator consistency to make
  /// sure current usage does not go above 'capacity_'. If it goes above
  /// 'capacity_', the increment will not be applied. Returns true if within
  /// capacity, false otherwise.
  ///
  /// NOTE: This method should always be called BEFORE actual allocation.
  inline bool incrementUsage(int64_t bytes) {
    const auto originalBytes = allocatedBytes_.fetch_add(bytes);
    // We don't do the check when capacity_ is 0, meaning unlimited capacity.
    if (capacity_ != 0 && originalBytes + bytes > capacity_) {
      allocatedBytes_.fetch_sub(bytes);
      return false;
    }
    return true;
  }

  /// Decrement current usage and check current allocator consistency to make
  /// sure current usage does not go below 0. Throws if usage goes below 0.
  ///
  /// NOTE: This method should always be called AFTER actual free.
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

  /// Capacity in bytes. Total allocation byte is not allowed to exceed this
  /// value. Setting this to 0 means no capacity enforcement.
  const size_t capacity_;

  /// Current total allocated bytes by this 'MallocAllocator'.
  std::atomic<int64_t> allocatedBytes_{0};

  /// Mutex for 'mallocs_'.
  std::mutex mallocsMutex_;

  /// Tracks malloc'd pointers to detect bad frees.
  std::unordered_set<void*> mallocs_;

  Stats stats_;
};
} // namespace facebook::velox::memory
