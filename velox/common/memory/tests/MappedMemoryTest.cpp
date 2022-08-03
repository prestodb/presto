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
#include "velox/common/memory/AllocationPool.h"
#include "velox/common/memory/MmapAllocator.h"

#include <thread>

#include <folly/Random.h>
#include <folly/Range.h>
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
      mmapAllocator_ = std::make_shared<MmapAllocator>(options);
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
            // other half mapped by the contiguous allocation. numMapped()
            // includes the contiguous allocation.
            EXPECT_EQ(kCapacity, instance_->numMapped());
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
  std::shared_ptr<MmapAllocator> mmapAllocator_;
  std::shared_ptr<MappedMemory> instancePtr_;
  MappedMemory* instance_;
  std::atomic<int32_t> sequence_ = {};
};

TEST_P(MappedMemoryTest, allocationPoolTest) {
  const size_t kNumLargeAllocPages = instance_->largestSizeClass() * 2;
  AllocationPool pool(instance_);

  pool.allocateFixed(10);
  EXPECT_EQ(pool.numTotalAllocations(), 1);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 10);

  pool.allocateFixed(kNumLargeAllocPages * MappedMemory::kPageSize);
  EXPECT_EQ(pool.numTotalAllocations(), 2);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 10);

  pool.allocateFixed(20);
  EXPECT_EQ(pool.numTotalAllocations(), 2);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 30);

  // Leaving 10 bytes room
  pool.allocateFixed(128 * 4096 - 10);
  EXPECT_EQ(pool.numTotalAllocations(), 3);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 524278);

  pool.allocateFixed(5);
  EXPECT_EQ(pool.numTotalAllocations(), 3);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), (524278 + 5));

  pool.allocateFixed(100);
  EXPECT_EQ(pool.numTotalAllocations(), 4);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 100);
  pool.clear();
}

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
  for (auto i = 0; i < sizes.size(); ++i) {
    auto size = sizes[i];
    allocateMultiple(size, capacity / size + 10, allocations);
    EXPECT_EQ(allocations.size() - 1, capacity / size);
    EXPECT_TRUE(instance_->checkConsistency());
    EXPECT_GT(instance_->numAllocated(), 0);

    allocations.clear();
    EXPECT_EQ(instance_->numAllocated(), 0);

    auto stats = instance_->stats();
    EXPECT_LT(0, stats.sizes[i].clocks());
    EXPECT_GE(stats.sizes[i].totalBytes, capacity * MappedMemory::kPageSize);
    EXPECT_GE(stats.sizes[i].numAllocations, capacity / size);

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

TEST_P(MappedMemoryTest, externalAdvise) {
  if (!useMmap_) {
    return;
  }
  constexpr int32_t kSmallSize = 16;
  constexpr int32_t kLargeSize = 32 * kSmallSize + 1;
  auto instance = dynamic_cast<MmapAllocator*>(MappedMemory::getInstance());
  std::vector<std::unique_ptr<MappedMemory::Allocation>> allocations;
  auto numAllocs = kCapacity / kSmallSize;
  allocations.reserve(numAllocs);
  for (int32_t i = 0; i < numAllocs; ++i) {
    allocations.push_back(std::make_unique<MappedMemory::Allocation>(instance));
    EXPECT_TRUE(allocate(kSmallSize, *allocations.back().get()));
  }
  // We allocated and mapped the capacity. Now free half, leaving the memory
  // still mapped.
  allocations.resize(numAllocs / 2);
  EXPECT_TRUE(instance->checkConsistency());
  EXPECT_EQ(instance->numMapped(), numAllocs * kSmallSize);
  EXPECT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize);
  std::vector<MappedMemory::ContiguousAllocation> large(2);
  EXPECT_TRUE(instance->allocateContiguous(kLargeSize, nullptr, large[0]));
  // The same number are mapped but some got advised away to back the large
  // allocation. One kSmallSize got advised away but not fully used because
  // kLargeSize is not a multiple of kSmallSize.
  EXPECT_EQ(instance->numMapped(), numAllocs * kSmallSize - kSmallSize + 1);
  EXPECT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize + kLargeSize);
  EXPECT_TRUE(instance->allocateContiguous(kLargeSize, nullptr, large[1]));
  large.clear();
  EXPECT_EQ(instance->numAllocated(), allocations.size() * kSmallSize);
  // After freeing 2xkLargeSize, We have unmapped 2*LargeSize at the free and
  // another (kSmallSize - 1 when allocating the first kLargeSize. Of the 15
  // that this unmapped, 1 was taken by the second large alloc. So, the mapped
  // pages is total - (2 * kLargeSize) - 14. The unused unmapped are 15 pages
  // after the first and 14 after the second allocContiguous().
  EXPECT_EQ(
      instance->numMapped(),
      kSmallSize * numAllocs - (2 * kLargeSize) -
          (kSmallSize - (2 * (kLargeSize % kSmallSize))));
  EXPECT_TRUE(instance->checkConsistency());
}

TEST_P(MappedMemoryTest, allocContiguousFail) {
  if (!useMmap_) {
    return;
  }
  // Covers edge cases of
  constexpr int32_t kSmallSize = 16;
  constexpr int32_t kLargeSize = kCapacity / 2;
  auto instance = dynamic_cast<MmapAllocator*>(MappedMemory::getInstance());
  std::vector<std::unique_ptr<MappedMemory::Allocation>> allocations;
  auto numAllocs = kCapacity / kSmallSize;
  int64_t trackedBytes = 0;
  auto trackCallback = [&](int64_t delta) { trackedBytes += delta; };
  allocations.reserve(numAllocs);
  for (int32_t i = 0; i < numAllocs; ++i) {
    allocations.push_back(std::make_unique<MappedMemory::Allocation>(instance));
    EXPECT_TRUE(allocate(kSmallSize, *allocations.back().get()));
  }
  // We allocated and mapped the capacity. Now free half, leaving the memory
  // still mapped.
  allocations.resize(numAllocs / 2);
  EXPECT_TRUE(instance->checkConsistency());
  EXPECT_EQ(instance->numMapped(), numAllocs * kSmallSize);
  EXPECT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize);
  MappedMemory::ContiguousAllocation large;
  EXPECT_TRUE(instance->allocateContiguous(
      kLargeSize / 2, nullptr, large, trackCallback));
  EXPECT_TRUE(instance->checkConsistency());

  // The allocation should go through because there is 1/2 of
  // kLargeSize already in large[0], 1/2 of kLargeSize free and
  // kSmallSize given as collateral. This does not go through because
  // we inject a failure in advising away the collateral.
  instance->injectFailure(MmapAllocator::Failure::kMadvise);
  EXPECT_FALSE(instance->allocateContiguous(
      kLargeSize + kSmallSize, allocations.back().get(), large, trackCallback));
  EXPECT_TRUE(instance->checkConsistency());
  // large and allocations.back() were both freed and nothing was allocated.
  EXPECT_EQ(kSmallSize * (allocations.size() - 1), instance->numAllocated());
  // An extra kSmallSize were freed.
  EXPECT_EQ(-kSmallSize * MappedMemory::kPageSize, trackedBytes);
  // Remove the cleared item from the end.
  allocations.pop_back();

  trackedBytes = 0;
  EXPECT_TRUE(instance->allocateContiguous(
      kLargeSize / 2, nullptr, large, trackCallback));
  instance->injectFailure(MmapAllocator::Failure::kMmap);
  // Should go through because 1/2 of kLargeSize + kSmallSize free and 1/2 of
  // kLargeSize already in large. Fails because mmap after advise away fails.
  EXPECT_FALSE(instance->allocateContiguous(
      kLargeSize + 2 * kSmallSize,
      allocations.back().get(),
      large,
      trackCallback));
  // large and allocations.back() were both freed and nothing was allocated.
  EXPECT_EQ(kSmallSize * (allocations.size() - 1), instance->numAllocated());
  EXPECT_EQ(-kSmallSize * MappedMemory::kPageSize, trackedBytes);
  allocations.pop_back();
  EXPECT_TRUE(instance->checkConsistency());

  trackedBytes = 0;
  EXPECT_TRUE(instance->allocateContiguous(
      kLargeSize / 2, nullptr, large, trackCallback));
  // We succeed without injected failure.
  EXPECT_TRUE(instance->allocateContiguous(
      kLargeSize + 3 * kSmallSize,
      allocations.back().get(),
      large,
      trackCallback));
  EXPECT_EQ(kCapacity, instance->numMapped());
  EXPECT_EQ(kCapacity, instance->numAllocated());
  // Size grew by kLargeSize + 2 * kSmallSize (one kSmallSize item was freed, so
  // no not 3 x kSmallSize).
  EXPECT_EQ(
      (kLargeSize + 2 * kSmallSize) * MappedMemory::kPageSize, trackedBytes);
  EXPECT_TRUE(instance->checkConsistency());
}

TEST_P(MappedMemoryTest, allocateBytes) {
  constexpr int32_t kNumAllocs = 50;
  // Different sizes, including below minimum and above largest size class.
  std::vector<MachinePageCount> sizes = {
      MappedMemory::kMaxMallocBytes / 2,
      100000,
      1000000,
      instance_->sizeClasses().back() * MappedMemory::kPageSize + 100000};
  folly::Random::DefaultGenerator rng;
  rng.seed(1);

  // We fill 'data' with random size allocations. Each is filled with its index
  // in 'data' cast to char.
  std::vector<folly::Range<char*>> data(kNumAllocs);
  for (auto counter = 0; counter < data.size() * 4; ++counter) {
    int32_t index = folly::Random::rand32(rng) % kNumAllocs;
    int32_t bytes = sizes[folly::Random::rand32() % sizes.size()];
    char expected = static_cast<char>(index);
    if (data[index].data()) {
      // If there is pre-existing data, we check that it has not been
      // overwritten.
      for (auto byte : data[index]) {
        EXPECT_EQ(expected, byte);
      }
      instance_->freeBytes(data[index].data(), data[index].size());
    }
    data[index] = folly::Range<char*>(
        reinterpret_cast<char*>(instance_->allocateBytes(bytes)), bytes);
    for (auto& byte : data[index]) {
      byte = expected;
    }
  }
  EXPECT_TRUE(instance_->checkConsistency());
  for (auto& range : data) {
    if (range.data()) {
      instance_->freeBytes(range.data(), range.size());
    }
  }
  auto stats = MappedMemory::allocateBytesStats();
  EXPECT_EQ(0, stats.totalSmall);
  EXPECT_EQ(0, stats.totalInSizeClasses);
  EXPECT_EQ(0, stats.totalLarge);

  EXPECT_EQ(0, instance_->numAllocated());
  EXPECT_TRUE(instance_->checkConsistency());
}

TEST_P(MappedMemoryTest, stlMappedMemoryAllocator) {
  {
    std::vector<double, StlMappedMemoryAllocator<double>> data(
        0, StlMappedMemoryAllocator<double>(instance_));
    // The contiguous size goes to 2MB, covering malloc, size
    // Allocation from classes and ContiguousAllocation outside size
    // classes.
    constexpr int32_t kNumDoubles = 256 * 1024;
    size_t capacity = 0;
    for (auto i = 0; i < kNumDoubles; i++) {
      data.push_back(i);
      if (data.capacity() != capacity) {
        capacity = data.capacity();
        auto stats = MappedMemory::allocateBytesStats();
        EXPECT_EQ(
            capacity * sizeof(double),
            stats.totalSmall + stats.totalInSizeClasses + stats.totalLarge);
      }
    }
    for (auto i = 0; i < kNumDoubles; i++) {
      ASSERT_EQ(i, data[i]);
    }
    EXPECT_EQ(512, instance_->numAllocated());
    auto stats = MappedMemory::allocateBytesStats();
    EXPECT_EQ(0, stats.totalSmall);
    EXPECT_EQ(0, stats.totalInSizeClasses);
    EXPECT_EQ(2 << 20, stats.totalLarge);
  }
  EXPECT_EQ(0, instance_->numAllocated());
  EXPECT_TRUE(instance_->checkConsistency());
  {
    StlMappedMemoryAllocator<int64_t> alloc(instance_);
    EXPECT_THROW(alloc.allocate(1ULL << 62), VeloxException);
    auto p = alloc.allocate(1);
    EXPECT_THROW(alloc.deallocate(p, 1ULL << 62), VeloxException);
    alloc.deallocate(p, 1);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MappedMemoryTests,
    MappedMemoryTest,
    testing::Values(true, false));

} // namespace facebook::velox::memory
