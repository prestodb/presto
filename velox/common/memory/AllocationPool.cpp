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

#include "velox/common/memory/AllocationPool.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryAllocator.h"

namespace facebook::velox {

void AllocationPool::clear() {
  // Trigger Allocation's destructor to free allocated memory
  auto copy = std::move(allocation_);
  allocations_.clear();
  auto copyLarge = std::move(largeAllocations_);
  largeAllocations_.clear();
}

char* AllocationPool::allocateFixed(uint64_t bytes, int32_t alignment) {
  VELOX_CHECK_GT(bytes, 0, "Cannot allocate zero bytes");
  if (availableInRun() >= bytes && alignment == 1) {
    auto* result = currentRun().data<char>() + currentOffset_;
    currentOffset_ += bytes;
    return result;
  }
  VELOX_CHECK_EQ(
      __builtin_popcount(alignment), 1, "Alignment can only be power of 2");

  auto numPages = memory::AllocationTraits::numPages(bytes + alignment - 1);

  // Use contiguous allocations from mapped memory if allocation size is large
  if (numPages > pool_->largestSizeClass()) {
    auto largeAlloc = std::make_unique<memory::ContiguousAllocation>();
    pool_->allocateContiguous(numPages, *largeAlloc);
    largeAllocations_.emplace_back(std::move(largeAlloc));
    auto result = largeAllocations_.back()->data<char>();
    VELOX_CHECK_NOT_NULL(
        result, "Unexpected nullptr for large contiguous allocation");
    // Should be at page boundary and always aligned.
    VELOX_CHECK_EQ(reinterpret_cast<uintptr_t>(result) % alignment, 0);
    return result;
  }

  if (availableInRun() == 0) {
    newRunImpl(numPages);
  } else {
    auto alignedBytes =
        bytes + memory::alignmentPadding(firstFreeInRun(), alignment);
    if (availableInRun() < alignedBytes) {
      newRunImpl(numPages);
    }
  }
  auto run = currentRun();
  currentOffset_ += memory::alignmentPadding(firstFreeInRun(), alignment);
  uint64_t size = run.numBytes();
  VELOX_CHECK_LE(bytes + currentOffset_, size);
  auto* result = run.data<char>() + currentOffset_;
  VELOX_CHECK_EQ(reinterpret_cast<uintptr_t>(result) % alignment, 0);
  currentOffset_ += bytes;
  return result;
}

void AllocationPool::newRunImpl(memory::MachinePageCount numPages) {
  ++currentRun_;
  if (currentRun_ >= allocation_.numRuns()) {
    if (allocation_.numRuns() > 0) {
      allocations_.push_back(
          std::make_unique<memory::Allocation>(std::move(allocation_)));
    }
    pool_->allocateNonContiguous(
        std::max<int32_t>(kMinPages, numPages), allocation_, numPages);
    currentRun_ = 0;
  }
  currentOffset_ = 0;
}

void AllocationPool::newRun(int32_t preferredSize) {
  newRunImpl(memory::AllocationTraits::numPages(preferredSize));
}

} // namespace facebook::velox
