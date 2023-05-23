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

#include "velox/common/memory/MemoryAllocator.h"

namespace facebook::velox::memory {
/// The implementation of MemoryAllocator using malloc.
class MallocAllocator : public MemoryAllocator {
 public:
  MallocAllocator();

  ~MallocAllocator() override {
    VELOX_CHECK((numAllocated_ == 0) && (numMapped_ == 0), "{}", toString());
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

  const Kind kind_;

  std::mutex mallocsMutex_;
  // Tracks malloc'd pointers to detect bad frees.
  std::unordered_set<void*> mallocs_;
  Stats stats_;
};
} // namespace facebook::velox::memory
