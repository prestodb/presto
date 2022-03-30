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
#include "velox/common/memory/MappedMemory.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MmapAllocator.h"

#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

DECLARE_int32(velox_memory_pool_mb);

namespace facebook::velox::memory {

static constexpr uint64_t kMaxMappedMemory = 128UL * 1024 * 1024;
static constexpr MachinePageCount kCapacity =
    (kMaxMappedMemory / MappedMemory::kPageSize);

class MappedMemoryTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    auto tracker = MemoryUsageTracker::create(
        MemoryUsageConfigBuilder().maxTotalMemory(kMaxMappedMemory).build());
    useMmap_ = GetParam();
    if (useMmap_) {
      MmapAllocatorOptions options = {kMaxMappedMemory};
      mmapAllocator_ = std::make_unique<MmapAllocator>(options);
      MappedMemory::setDefaultInstance(mmapAllocator_.get());
    } else {
      MappedMemory::setDefaultInstance(nullptr);
    }
    instancePtr_ = MappedMemory::getInstance()->addChild(tracker);
    instance_ = instancePtr_.get();
  }

  void TearDown() override {
    MappedMemory::destroyTestOnly();
  }

  bool allocate(int32_t numPages, MappedMemory::Allocation& result) {
    try {
      if (!instance_->allocate(numPages, 0, result)) {
        EXPECT_EQ(result.numRuns(), 0);
        return false;
      }
    } catch (const VeloxException& e) {
      EXPECT_EQ(result.numRuns(), 0);
      return false;
    }
    EXPECT_GE(result.numPages(), numPages);
    initializeContents(result);
    return true;
  }

  void initializeContents(MappedMemory::Allocation& alloc) {
    auto sequence = sequence_.fetch_add(1);
    bool first = true;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      MappedMemory::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * MappedMemory::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          ptr[offset] = reinterpret_cast<void*>(sequence);
          first = false;
        } else {
          ptr[offset] = ptr + offset + sequence;
        }
      }
    }
  }

  void checkContents(MappedMemory::Allocation& alloc) {
    bool first = true;
    long sequence;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      MappedMemory::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * MappedMemory::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          sequence = reinterpret_cast<long>(ptr[offset]);
          first = false;
        } else {
          ASSERT_EQ(ptr[offset], ptr + offset + sequence);
        }
      }
    }
  }

  void initializeContents(MappedMemory::ContiguousAllocation& alloc) {
    long sequence = sequence_.fetch_add(1);
    bool first = true;
    void** ptr = reinterpret_cast<void**>(alloc.data());
    int numWords = alloc.size() / sizeof(void*);
    for (int offset = 0; offset < numWords; offset++) {
      if (first) {
        ptr[offset] = reinterpret_cast<void*>(sequence);
        first = false;
      } else {
        ptr[offset] = ptr + offset + sequence;
      }
    }
  }

  void checkContents(MappedMemory::ContiguousAllocation& alloc) {
    bool first = true;
    long sequence;
    void** ptr = reinterpret_cast<void**>(alloc.data());
    int numWords = alloc.size() / sizeof(void*);
    for (int offset = 0; offset < numWords; offset++) {
      if (first) {
        sequence = reinterpret_cast<long>(ptr[offset]);
        first = false;
      } else {
        ASSERT_EQ(ptr[offset], ptr + offset + sequence);
      }
    }
  }

  void free(MappedMemory::Allocation& alloc) {
    checkContents(alloc);
    instance_->free(alloc);
  }

  void allocateMultiple(
      MachinePageCount numPages,
      int32_t numAllocs,
      std::vector<std::unique_ptr<MappedMemory::Allocation>>& allocations) {
    allocations.clear();
    allocations.reserve(numAllocs);
    allocations.push_back(
        std::make_unique<MappedMemory::Allocation>(instance_));
    bool largeTested = false;
    for (int32_t i = 0; i < numAllocs; ++i) {
      if (allocate(numPages, *allocations.back().get())) {
        allocations.push_back(
            std::make_unique<MappedMemory::Allocation>(instance_));
        int available = kCapacity - instance_->numAllocated();

        // Try large allocations after half the capacity is used.
        if (available <= kCapacity / 2 && !largeTested) {
          largeTested = true;
          MappedMemory::ContiguousAllocation large;
          if (!allocateContiguous(available / 2, nullptr, large)) {
            FAIL() << "Could not allocate half the available";
            return;
          }
          MappedMemory::Allocation small(instance_);
          if (!instance_->allocate(available / 4, 0, small)) {
            FAIL() << "Could not allocate 1/4 of available";
            return;
          }
          // Try to allocate more than available;
          EXPECT_THROW(
              instance_->allocateContiguous(available + 1, &small, large),
              VeloxRuntimeError);

          // Check The failed allocation freed the collateral.
          EXPECT_EQ(small.numPages(), 0);
          EXPECT_EQ(large.numPages(), 0);
          if (!allocateContiguous(available, nullptr, large)) {
            FAIL() << "Could not allocate rest of capacity";
          }
          EXPECT_GE(large.numPages(), available);
          EXPECT_EQ(small.numPages(), 0);
          EXPECT_EQ(kCapacity, instance_->numAllocated());
          if (useMmap_) {
            // The allocator has everything allocated and half mapped, with the
            // other half mapped by the contiguous allocation.
            EXPECT_EQ(kCapacity / 2, instance_->numMapped());
          }
          if (!allocateContiguous(available / 2, nullptr, large)) {
            FAIL()
                << "Could not exchange all of available for half of available";
          }
          EXPECT_GE(large.numPages(), available / 2);
        }
      }
    }
  }

  bool allocateContiguous(
      int numPages,
      MappedMemory::Allocation* FOLLY_NULLABLE collateral,
      MappedMemory::ContiguousAllocation& allocation) {
    bool success =
        instance_->allocateContiguous(numPages, collateral, allocation);
    if (success) {
      initializeContents(allocation);
    }
    return success;
  }

  void free(MappedMemory::ContiguousAllocation& allocation) {
    checkContents(allocation);
    instance_->freeContiguous(allocation);
  }

  void allocateIncreasing(
      MachinePageCount startSize,
      MachinePageCount endSize,
      int32_t repeat,
      std::vector<std::unique_ptr<MappedMemory::Allocation>>& allocations) {
    int32_t hand = 0;
    for (int32_t count = 0; count < repeat;) {
      for (auto size = startSize; size < endSize;
           size += std::max<MachinePageCount>(1, size / 5)) {
        ++count;
        if (!allocate(size, *allocations[hand])) {
          if (!makeSpace(size, allocations, &hand)) {
            // Stop early if other threads have consumed all capacity
            // and there is not enough here to free in to satisfy the
            // allocation.
            return;
          }
        }
        hand = (hand + 1) % allocations.size();
      }
    }
  }

  bool makeSpace(
      int32_t size,
      std::vector<std::unique_ptr<MappedMemory::Allocation>>& allocations,
      int32_t* hand) {
    int numIterations = 0;
    while (kCapacity - instance_->numAllocated() < size) {
      if (allocations[*hand]->numRuns()) {
        free(*allocations[*hand].get());
      }
      *hand = (*hand + 1) % allocations.size();
      if (++numIterations > allocations.size()) {
        // Looked at all of 'allocations' and could not free enough.
        return false;
      }
    }
    return true;
  }

  std::vector<std::unique_ptr<MappedMemory::Allocation>> makeEmptyAllocations(
      int32_t size) {
    std::vector<std::unique_ptr<MappedMemory::Allocation>> allocations;
    allocations.reserve(size);
    for (int32_t i = 0; i < size; i++) {
      allocations.push_back(
          std::make_unique<MappedMemory::Allocation>(instance_));
    }
    return allocations;
  }

  bool useMmap_;
  std::unique_ptr<MmapAllocator> mmapAllocator_;
  std::shared_ptr<MappedMemory> instancePtr_;
  MappedMemory* instance_;
  std::atomic<int32_t> sequence_ = {};
};

TEST_P(MappedMemoryTest, allocationTest) {
  const int32_t kPageSize = MappedMemory::kPageSize;
  MappedMemory::Allocation allocation(instance_);
  uint8_t* pages = reinterpret_cast<uint8_t*>(malloc(kPageSize * 20));
  // We append different pieces of 'pages' to 'allocation'.
  // 4 last pages.
  allocation.append(pages + 16 * kPageSize, 4);
  // 16th page
  allocation.append(pages + 15 * kPageSize, 1);
  // 15 first pages.
  allocation.append(pages, 15);
  EXPECT_EQ(allocation.numRuns(), 3);
  EXPECT_EQ(allocation.numPages(), 20);
  int32_t index;
  int32_t offsetInRun;
  // We look for the pointer of byte 2000 of the 16th page in
  // 'allocation'. This falls on the 11th page of the last run.
  const int32_t offset = 15 * kPageSize + 2000;
  allocation.findRun(offset, &index, &offsetInRun);
  // 3rd run.
  EXPECT_EQ(index, 2);
  EXPECT_EQ(offsetInRun, 10 * kPageSize + 2000);
  EXPECT_EQ(allocation.runAt(1).data(), pages + 15 * kPageSize);

  MappedMemory::Allocation moved(std::move(allocation));
  EXPECT_EQ(allocation.numRuns(), 0);
  EXPECT_EQ(allocation.numPages(), 0);
  EXPECT_EQ(moved.numRuns(), 3);
  EXPECT_EQ(moved.numPages(), 20);

  moved.clear();
  EXPECT_EQ(moved.numRuns(), 0);
  EXPECT_EQ(moved.numPages(), 0);
  ::free(pages);
}

TEST_P(MappedMemoryTest, singleAllocationTest) {
  const std::vector<MachinePageCount>& sizes = instance_->sizeClasses();
  MachinePageCount capacity = kCapacity;
  std::vector<std::unique_ptr<MappedMemory::Allocation>> allocations;
  for (auto& size : sizes) {
    allocateMultiple(size, capacity / size + 10, allocations);
    EXPECT_EQ(allocations.size() - 1, capacity / size);
    EXPECT_TRUE(instance_->checkConsistency());
    EXPECT_GT(instance_->numAllocated(), 0);

    allocations.clear();
    EXPECT_EQ(instance_->numAllocated(), 0);
    if (useMmap_) {
      EXPECT_EQ(instance_->numMapped(), kCapacity);
    }
    EXPECT_TRUE(instance_->checkConsistency());
  }
  for (int32_t i = sizes.size() - 2; i >= 0; --i) {
    auto size = sizes[i];
    allocateMultiple(size, capacity / size + 10, allocations);
    EXPECT_EQ(allocations[0]->numPages(), size);
    EXPECT_EQ(allocations.size() - 1, capacity / size);
    EXPECT_TRUE(instance_->checkConsistency());
    EXPECT_GT(instance_->numAllocated(), 0);

    allocations.clear();
    EXPECT_EQ(instance_->numAllocated(), 0);
    if (useMmap_) {
      EXPECT_EQ(instance_->numMapped(), kCapacity);
    }
    EXPECT_TRUE(instance_->checkConsistency());
  }
}

TEST_P(MappedMemoryTest, increasingSizeTest) {
  std::vector<std::unique_ptr<MappedMemory::Allocation>> allocations =
      makeEmptyAllocations(10'000);
  allocateIncreasing(10, 1'000, 2'000, allocations);
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_GT(instance_->numAllocated(), 0);

  allocations.clear();
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_EQ(instance_->numAllocated(), 0);
}

TEST_P(MappedMemoryTest, increasingSizeWithThreadsTest) {
  const int32_t numThreads = 20;
  std::vector<std::vector<std::unique_ptr<MappedMemory::Allocation>>>
      allocations;
  allocations.reserve(numThreads);
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int32_t i = 0; i < numThreads; ++i) {
    allocations.emplace_back(makeEmptyAllocations(500));
    threads.push_back(std::thread([this, &allocations, i]() {
      allocateIncreasing(10, 1000, 1000, allocations[i]);
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_GT(instance_->numAllocated(), 0);

  allocations.clear();
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_EQ(instance_->numAllocated(), 0);
}

TEST_P(MappedMemoryTest, scopedMemoryUsageTracking) {
  const int32_t numPages = 32;
  {
    auto tracker = MemoryUsageTracker::create();
    auto mappedMemory = instance_->addChild(tracker);

    MappedMemory::Allocation result(mappedMemory.get());

    mappedMemory->allocate(numPages, 0, result);
    EXPECT_GE(result.numPages(), numPages);
    EXPECT_EQ(
        result.numPages() * MappedMemory::kPageSize,
        tracker->getCurrentUserBytes());
    mappedMemory->free(result);
    EXPECT_EQ(0, tracker->getCurrentUserBytes());
  }

  auto tracker = MemoryUsageTracker::create();
  auto mappedMemory = instance_->addChild(tracker);
  {
    MappedMemory::Allocation result1(mappedMemory.get());
    MappedMemory::Allocation result2(mappedMemory.get());
    mappedMemory->allocate(numPages, 0, result1);
    EXPECT_GE(result1.numPages(), numPages);
    EXPECT_EQ(
        result1.numPages() * MappedMemory::kPageSize,
        tracker->getCurrentUserBytes());

    mappedMemory->allocate(numPages, 0, result2);
    EXPECT_GE(result2.numPages(), numPages);
    EXPECT_EQ(
        (result1.numPages() + result2.numPages()) * MappedMemory::kPageSize,
        tracker->getCurrentUserBytes());

    // Since allocations are still valid, usage should not change.
    EXPECT_EQ(
        (result1.numPages() + result2.numPages()) * MappedMemory::kPageSize,
        tracker->getCurrentUserBytes());
  }
  EXPECT_EQ(0, tracker->getCurrentUserBytes());
}

TEST_P(MappedMemoryTest, minSizeClass) {
  auto tracker = MemoryUsageTracker::create();
  auto mappedMemory = instance_->addChild(tracker);

  MappedMemory::Allocation result(mappedMemory.get());

  int32_t sizeClass = mappedMemory->sizeClasses().back();
  int32_t numPages = sizeClass + 1;
  mappedMemory->allocate(numPages, 0, result, nullptr, sizeClass);
  EXPECT_GE(result.numPages(), sizeClass * 2);
  // All runs have to be at least the minimum size.
  for (auto i = 0; i < result.numRuns(); ++i) {
    EXPECT_LE(sizeClass, result.runAt(i).numPages());
  }
  EXPECT_EQ(
      result.numPages() * MappedMemory::kPageSize,
      tracker->getCurrentUserBytes());
  mappedMemory->free(result);
  EXPECT_EQ(0, tracker->getCurrentUserBytes());
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MappedMemoryTests,
    MappedMemoryTest,
    testing::Values(true, false));

} // namespace facebook::velox::memory
