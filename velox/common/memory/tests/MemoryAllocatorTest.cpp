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
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/AllocationPool.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/memory/MmapArena.h"
#include "velox/common/testutil/TestValue.h"

#include <thread>

#include <folly/Random.h>
#include <folly/Range.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

DECLARE_int32(velox_memory_pool_mb);

using namespace facebook::velox::common::testutil;

namespace facebook::velox::memory {

static constexpr uint64_t kMaxMemoryAllocator = 256UL * 1024 * 1024;
static constexpr MachinePageCount kCapacity =
    (kMaxMemoryAllocator / AllocationTraits::kPageSize);

class MemoryAllocatorTest : public testing::TestWithParam<bool> {
 protected:
  static void SetUpTestCase() {
    TestValue::enable();
  }

  void SetUp() override {
    setupAllocator();
  }

  void setupAllocator() {
    pool_.reset();
    MemoryAllocator::testingDestroyInstance();
    useMmap_ = GetParam();
    maxMallocBytes_ = 3072;
    if (useMmap_) {
      MmapAllocator::Options options;
      options.capacity = kMaxMemoryAllocator;
      options.smallAllocationReservePct = 4;
      options.maxMallocBytes = maxMallocBytes_;
      allocator_ = std::make_shared<MmapAllocator>(options);
      auto mmapAllocator = std::dynamic_pointer_cast<MmapAllocator>(allocator_);
      ASSERT_EQ(
          mmapAllocator->capacity(),
          bits::roundUp(
              kMaxMemoryAllocator * (100 - options.smallAllocationReservePct) /
                  100 / AllocationTraits::kPageSize,
              64 * mmapAllocator->sizeClasses().back()));
      MemoryAllocator::setDefaultInstance(allocator_.get());
    } else {
      allocator_ = MemoryAllocator::createDefaultInstance();
      MemoryAllocator::setDefaultInstance(allocator_.get());
    }
    instance_ = MemoryAllocator::getInstance();
    memoryManager_ = std::make_unique<MemoryManager>(IMemoryManager::Options{
        .capacity = kMaxMemory, .allocator = instance_});
    pool_ = memoryManager_->addLeafPool("allocatorTest");
    if (useMmap_) {
      ASSERT_EQ(instance_->kind(), MemoryAllocator::Kind::kMmap);
    } else {
      ASSERT_EQ(instance_->kind(), MemoryAllocator::Kind::kMalloc);
    }
    ASSERT_EQ(
        MemoryAllocator::kindString(static_cast<MemoryAllocator::Kind>(100)),
        "UNKNOWN: 100");
  }

  void TearDown() override {
    MemoryAllocator::testingDestroyInstance();
  }

  bool allocate(int32_t numPages, Allocation& result) {
    try {
      if (!instance_->allocateNonContiguous(numPages, result)) {
        EXPECT_TRUE(result.empty());
        return false;
      }
    } catch (const VeloxException& e) {
      EXPECT_TRUE(result.empty());
      return false;
    }
    EXPECT_GE(result.numPages(), numPages);
    initializeContents(result);
    return true;
  }

  void initializeContents(Allocation& alloc) {
    auto sequence = sequence_.fetch_add(1);
    bool first = true;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      Allocation::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * AllocationTraits::kPageSize / sizeof(void*);
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

  void checkContents(Allocation& alloc) {
    bool first = true;
    long sequence;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      Allocation::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * AllocationTraits::kPageSize / sizeof(void*);
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

  void initializeContents(ContiguousAllocation& alloc) {
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

  void checkContents(ContiguousAllocation& alloc) {
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

  void free(Allocation& alloc) {
    checkContents(alloc);
    instance_->freeNonContiguous(alloc);
  }

  void clearAllocations(std::vector<std::unique_ptr<Allocation>>& allocations) {
    for (auto& allocation : allocations) {
      instance_->freeNonContiguous(*allocation);
    }
    allocations.clear();
  }

  void clearAllocations(std::vector<std::vector<std::unique_ptr<Allocation>>>&
                            allocationsVector) {
    for (auto& allocations : allocationsVector) {
      for (auto& allocation : allocations) {
        instance_->freeNonContiguous(*allocation);
      }
    }
    allocationsVector.clear();
  }

  void shrinkAllocations(
      std::vector<std::unique_ptr<Allocation>>& allocations,
      int32_t reducedSize) {
    while (allocations.size() > reducedSize) {
      instance_->freeNonContiguous(*allocations.back());
      allocations.pop_back();
    }
    ASSERT_EQ(allocations.size(), reducedSize);
  }

  void clearContiguousAllocations(
      std::vector<ContiguousAllocation>& allocations) {
    for (auto& allocation : allocations) {
      instance_->freeContiguous(allocation);
    }
    allocations.clear();
  }

  void allocateMultiple(
      MachinePageCount numPages,
      int32_t numAllocs,
      std::vector<std::unique_ptr<Allocation>>& allocations) {
    clearAllocations(allocations);
    allocations.reserve(numAllocs);
    bool largeTested = false;
    for (int32_t i = 0; i < numAllocs; ++i) {
      auto allocation = std::make_unique<Allocation>();
      if (!allocate(numPages, *allocation)) {
        continue;
      }
      allocations.push_back(std::move(allocation));
      int available = kCapacity - instance_->numAllocated();

      // Try large allocations after half the capacity is used.
      if (available <= kCapacity / 2 && !largeTested) {
        largeTested = true;
        ContiguousAllocation large;
        if (!allocateContiguous(available / 2, nullptr, large)) {
          FAIL() << "Could not allocate half the available";
          return;
        }
        Allocation small;
        if (!instance_->allocateNonContiguous(available / 4, small)) {
          FAIL() << "Could not allocate 1/4 of available";
          return;
        }
        // Try to allocate more than available, and it should fail if we use
        // MmapAllocator which enforces the capacity check.
        if (useMmap_) {
          ASSERT_FALSE(
              instance_->allocateContiguous(available + 1, &small, large));
          ASSERT_TRUE(small.empty());
          ASSERT_TRUE(large.empty());
        } else {
          ASSERT_TRUE(
              instance_->allocateContiguous(available + 1, &small, large));
          ASSERT_TRUE(small.empty());
          ASSERT_FALSE(large.empty());
          instance_->freeContiguous(large);
        }

        // Check the failed allocation freed the collateral.
        ASSERT_EQ(small.numPages(), 0);
        ASSERT_EQ(large.numPages(), 0);
        if (!allocateContiguous(available, nullptr, large)) {
          FAIL() << "Could not allocate rest of capacity";
        }
        ASSERT_GE(large.numPages(), available);
        ASSERT_EQ(small.numPages(), 0);
        ASSERT_EQ(kCapacity, instance_->numAllocated());
        if (useMmap_) {
          // The allocator has everything allocated and half mapped, with the
          // other half mapped by the contiguous allocation. numMapped()
          // includes the contiguous allocation.
          ASSERT_EQ(kCapacity, instance_->numMapped());
        }
        if (!allocateContiguous(available / 2, nullptr, large)) {
          FAIL() << "Could not exchange all of available for half of available";
        }
        ASSERT_GE(large.numPages(), available / 2);
        instance_->freeContiguous(large);
      }
    }
  }

  bool allocateContiguous(
      int numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation) {
    bool success =
        instance_->allocateContiguous(numPages, collateral, allocation);
    if (success) {
      initializeContents(allocation);
    }
    return success;
  }

  void free(ContiguousAllocation& allocation) {
    checkContents(allocation);
    instance_->freeContiguous(allocation);
  }

  void allocateIncreasing(
      MachinePageCount startSize,
      MachinePageCount endSize,
      int32_t repeat,
      std::vector<std::unique_ptr<Allocation>>& allocations) {
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
      std::vector<std::unique_ptr<Allocation>>& allocations,
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

  std::vector<std::unique_ptr<Allocation>> makeEmptyAllocations(int32_t size) {
    std::vector<std::unique_ptr<Allocation>> allocations;
    allocations.reserve(size);
    for (int32_t i = 0; i < size; i++) {
      allocations.push_back(std::make_unique<Allocation>());
    }
    return allocations;
  }

  bool useMmap_;
  int32_t maxMallocBytes_;
  std::shared_ptr<MemoryAllocator> allocator_;
  MemoryAllocator* instance_;
  std::unique_ptr<MemoryManager> memoryManager_;
  std::shared_ptr<MemoryPool> pool_;
  std::atomic<int32_t> sequence_ = {};
};

TEST_P(MemoryAllocatorTest, mmapAllocatorInitTest) {
  if (!useMmap_) {
    return;
  }
  {
    MmapAllocator::Options options;
    options.capacity = kMaxMemoryAllocator;
    options.smallAllocationReservePct = 39;
    options.maxMallocBytes = 2999;
    auto mmapAllocator = std::make_shared<MmapAllocator>(options);
    auto smallAllocationBytes =
        options.capacity * options.smallAllocationReservePct / 100;
    EXPECT_EQ(
        bits::roundUp(
            AllocationTraits::numPages(options.capacity - smallAllocationBytes),
            64 * mmapAllocator->sizeClasses().back()),
        mmapAllocator->capacity());
    EXPECT_EQ(options.maxMallocBytes, mmapAllocator->maxMallocBytes());
    EXPECT_EQ(smallAllocationBytes, mmapAllocator->mallocReservedBytes());
  }
  {
    MmapAllocator::Options options;
    options.capacity = kMaxMemoryAllocator;
    options.smallAllocationReservePct = 39;
    options.maxMallocBytes = 0;
    auto mmapAllocator = std::make_shared<MmapAllocator>(options);
    EXPECT_EQ(
        bits::roundUp(
            AllocationTraits::numPages(kMaxMemoryAllocator),
            64 * mmapAllocator->sizeClasses().back()),
        mmapAllocator->capacity());
    EXPECT_EQ(options.maxMallocBytes, mmapAllocator->maxMallocBytes());
    EXPECT_EQ(0, mmapAllocator->mallocReservedBytes());
  }
  {
    MmapAllocator::Options options;
    options.capacity = 64 * 256 * AllocationTraits::kPageSize - 100;
    options.smallAllocationReservePct = 10;
    options.maxMallocBytes = 3072;
    auto mmapAllocator = std::make_shared<MmapAllocator>(options);
    auto smallAllocationBytes =
        options.capacity * options.smallAllocationReservePct / 100;
    EXPECT_EQ(
        bits::roundUp(
            AllocationTraits::numPages(options.capacity),
            64 * mmapAllocator->sizeClasses().back()),
        mmapAllocator->capacity());
    EXPECT_EQ(options.maxMallocBytes, mmapAllocator->maxMallocBytes());
    EXPECT_EQ(smallAllocationBytes, mmapAllocator->mallocReservedBytes());
  }
}

TEST_P(MemoryAllocatorTest, allocationPoolTest) {
  const size_t kNumLargeAllocPages = instance_->largestSizeClass() * 2;
  AllocationPool pool(pool_.get());

  pool.allocateFixed(10);
  EXPECT_EQ(pool.numTotalAllocations(), 1);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 10);

  pool.allocateFixed(kNumLargeAllocPages * AllocationTraits::kPageSize);
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

  {
    auto old = pool.numLargeAllocations();
    auto bytes = AllocationTraits::kPageSize * instance_->largestSizeClass();
    pool.allocateFixed(bytes);
    ASSERT_EQ(pool.numLargeAllocations(), old);
    auto buf = pool.allocateFixed(bytes, 64);
    ASSERT_EQ(pool.numLargeAllocations(), old + 1);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % 64, 0);
  }

  for (int bytes = 1; bytes < 64; ++bytes) {
    auto buf = pool.allocateFixed(bytes, 64);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % 64, 0);
  }

  {
    // Leaving 10 bytes room
    pool.allocateFixed(128 * 4096 - 10);
    auto old = pool.numSmallAllocations();
    auto buf = pool.allocateFixed(1, 64);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % 64, 0);
    ASSERT_EQ(pool.numSmallAllocations(), old + 1);
  }

  pool.clear();
}

TEST_P(MemoryAllocatorTest, allocationTest) {
  const int32_t kPageSize = AllocationTraits::kPageSize;
  Allocation allocation;
  uint8_t* pages = reinterpret_cast<uint8_t*>(::malloc(kPageSize * 20));
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

  Allocation moved(std::move(allocation));
  ASSERT_TRUE(allocation.empty());
  EXPECT_EQ(allocation.numRuns(), 0);
  EXPECT_EQ(allocation.numPages(), 0);
  EXPECT_EQ(moved.numRuns(), 3);
  EXPECT_EQ(moved.numPages(), 20);

  moved.clear();
  ASSERT_TRUE(moved.empty());
  EXPECT_EQ(moved.numRuns(), 0);
  EXPECT_EQ(moved.numPages(), 0);
  ::free(pages);
}

TEST_P(MemoryAllocatorTest, singleAllocationTest) {
  if (!useMmap_) {
    return;
  }
  const std::vector<MachinePageCount>& sizes = instance_->sizeClasses();
  MachinePageCount capacity = kCapacity;
  std::vector<std::unique_ptr<Allocation>> allocations;
  for (auto i = 0; i < sizes.size(); ++i) {
    auto size = sizes[i];
    allocateMultiple(size, capacity / size + 10, allocations);
    if (useMmap_) {
      ASSERT_EQ(allocations.size(), capacity / size);
    } else {
      // NOTE: the non-mmap allocator doesn't enforce capacity for now.
      ASSERT_EQ(allocations.size(), capacity / size + 10);
    }
    ASSERT_TRUE(instance_->checkConsistency());
    ASSERT_GT(instance_->numAllocated(), 0);

    clearAllocations(allocations);
    ASSERT_EQ(instance_->numAllocated(), 0);

    auto stats = instance_->stats();
    ASSERT_LT(0, stats.sizes[i].clocks());
    ASSERT_GE(
        stats.sizes[i].totalBytes, capacity * AllocationTraits::kPageSize);
    ASSERT_GE(stats.sizes[i].numAllocations, capacity / size);

    if (useMmap_) {
      ASSERT_EQ(instance_->numMapped(), kCapacity);
    }
    ASSERT_TRUE(instance_->checkConsistency());
  }
  for (int32_t i = sizes.size() - 2; i >= 0; --i) {
    auto size = sizes[i];
    allocateMultiple(size, capacity / size + 10, allocations);
    ASSERT_EQ(allocations[0]->numPages(), size);
    if (useMmap_) {
      ASSERT_EQ(allocations.size(), capacity / size);
    } else {
      // NOTE: the non-mmap allocator doesn't enforce capacity for now.
      ASSERT_EQ(allocations.size(), capacity / size + 10);
    }
    ASSERT_TRUE(instance_->checkConsistency());
    ASSERT_GT(instance_->numAllocated(), 0);

    clearAllocations(allocations);
    ASSERT_EQ(instance_->numAllocated(), 0);
    if (useMmap_) {
      ASSERT_EQ(instance_->numMapped(), kCapacity);
    }
    ASSERT_TRUE(instance_->checkConsistency());
  }
}

TEST_P(MemoryAllocatorTest, increasingSizeTest) {
  std::vector<std::unique_ptr<Allocation>> allocations =
      makeEmptyAllocations(10'000);
  allocateIncreasing(10, 1'000, 2'000, allocations);
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_GT(instance_->numAllocated(), 0);

  clearAllocations(allocations);
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_EQ(instance_->numAllocated(), 0);
}

TEST_P(MemoryAllocatorTest, increasingSizeWithThreadsTest) {
  const int32_t numThreads = 20;
  std::vector<std::vector<std::unique_ptr<Allocation>>> allocations;
  allocations.reserve(numThreads);
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int32_t i = 0; i < numThreads; ++i) {
    allocations.emplace_back(makeEmptyAllocations(500));
  }
  for (int32_t i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([this, &allocations, i]() {
      allocateIncreasing(10, 1000, 1000, allocations[i]);
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_GT(instance_->numAllocated(), 0);

  clearAllocations(allocations);
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_EQ(instance_->numAllocated(), 0);
}

TEST_P(MemoryAllocatorTest, minSizeClass) {
  Allocation result;

  int32_t sizeClass = instance_->sizeClasses().back();
  int32_t numPages = sizeClass + 1;
  instance_->allocateNonContiguous(numPages, result, nullptr, sizeClass);
  EXPECT_GE(result.numPages(), sizeClass * 2);
  // All runs have to be at least the minimum size.
  for (auto i = 0; i < result.numRuns(); ++i) {
    EXPECT_LE(sizeClass, result.runAt(i).numPages());
  }
  instance_->freeNonContiguous(result);
}

TEST_P(MemoryAllocatorTest, externalAdvise) {
  if (!useMmap_) {
    return;
  }
  constexpr int32_t kSmallSize = 16;
  constexpr int32_t kLargeSize = 32 * kSmallSize + 1;
  auto instance = dynamic_cast<MmapAllocator*>(MemoryAllocator::getInstance());
  std::vector<std::unique_ptr<Allocation>> allocations;
  auto numAllocs = kCapacity / kSmallSize;
  allocations.reserve(numAllocs);
  for (int32_t i = 0; i < numAllocs; ++i) {
    allocations.push_back(std::make_unique<Allocation>());
    EXPECT_TRUE(allocate(kSmallSize, *allocations.back().get()));
  }
  // We allocated and mapped the capacity. Now free half, leaving the memory
  // still mapped.
  shrinkAllocations(allocations, numAllocs / 2);
  EXPECT_TRUE(instance->checkConsistency());
  EXPECT_EQ(instance->numMapped(), numAllocs * kSmallSize);
  EXPECT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize);
  std::vector<ContiguousAllocation> larges(2);
  EXPECT_TRUE(instance->allocateContiguous(kLargeSize, nullptr, larges[0]));
  // The same number are mapped but some got advised away to back the large
  // allocation. One kSmallSize got advised away but not fully used because
  // kLargeSize is not a multiple of kSmallSize.
  EXPECT_EQ(instance->numMapped(), numAllocs * kSmallSize - kSmallSize + 1);
  EXPECT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize + kLargeSize);
  EXPECT_TRUE(instance->allocateContiguous(kLargeSize, nullptr, larges[1]));
  clearContiguousAllocations(larges);
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
  clearAllocations(allocations);
  EXPECT_TRUE(instance->checkConsistency());
}

TEST_P(MemoryAllocatorTest, nonContiguousFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, injectedFailure:{}",
          numOldPages,
          numNewPages,
          injectedFailure);
    }
  } testSettings[] = {// Cap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {200, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      // Allocate failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {200, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(
        fmt::format("{}, useMmap:{}", testData.debugString(), useMmap_));
    if ((testData.injectedFailure !=
         MemoryAllocator::InjectedFailure::kAllocate) &&
        !useMmap_) {
      // Non-Allocate failure injection only applies for MmapAllocator.
      continue;
    }
    setupAllocator();
    Allocation allocation;
    if (testData.numOldPages > 0) {
      allocator_->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    allocator_->testingSetFailureInjection(testData.injectedFailure, true);
    ASSERT_FALSE(
        allocator_->allocateNonContiguous(testData.numNewPages, allocation));
    ASSERT_EQ(allocator_->numAllocated(), 0);
    allocator_->testingClearFailureInjection();
  }
  ASSERT_TRUE(allocator_->checkConsistency());
}

TEST_P(MemoryAllocatorTest, allocContiguous) {
  struct {
    MachinePageCount nonContiguousPages;
    MachinePageCount oldContiguousPages;
    MachinePageCount newContiguousPages;

    std::string debugString() const {
      return fmt::format(
          "nonContiguousPages:{} oldContiguousPages:{} newContiguousPages:{}",
          nonContiguousPages,
          oldContiguousPages,
          newContiguousPages);
    }
  } testSettings[] = {
      {100, 100, 200},
      {100, 200, 200},
      {200, 100, 200},
      {200, 100, 400},
      {0, 100, 100},
      {0, 200, 100},
      {0, 100, 200},
      {100, 0, 100},
      {200, 0, 100},
      {100, 0, 200}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format("{} useMmap{}", testData.debugString(), useMmap_));
    setupAllocator();
    const MachinePageCount nonContiguousPages = 100;
    Allocation allocation;
    if (testData.nonContiguousPages != 0) {
      instance_->allocateNonContiguous(testData.nonContiguousPages, allocation);
    }
    ContiguousAllocation contiguousAllocation;
    if (testData.oldContiguousPages != 0) {
      instance_->allocateContiguous(
          testData.oldContiguousPages, nullptr, contiguousAllocation);
    }
    instance_->allocateContiguous(
        testData.newContiguousPages, &allocation, contiguousAllocation);
    ASSERT_EQ(instance_->numAllocated(), testData.newContiguousPages);

    if (useMmap_) {
      if (testData.nonContiguousPages > 0) {
        // Mmap allocator doesn't free mapped pages count for the old
        // non-contiguous allocation.
        ASSERT_GE(
            instance_->numMapped(),
            testData.newContiguousPages + testData.nonContiguousPages);
      } else {
        ASSERT_EQ(instance_->numMapped(), testData.newContiguousPages);
      }
      auto mappedAllocator = dynamic_cast<MmapAllocator*>(allocator_.get());
      ASSERT_EQ(
          mappedAllocator->numExternalMapped(), testData.newContiguousPages);
    } else {
      ASSERT_EQ(instance_->numMapped(), testData.newContiguousPages);
    }

    instance_->freeContiguous(contiguousAllocation);

    if (useMmap_) {
      ASSERT_EQ(instance_->numAllocated(), 0);
      if (testData.nonContiguousPages > 0) {
        ASSERT_GE(instance_->numMapped(), testData.nonContiguousPages);
      } else {
        ASSERT_EQ(instance_->numMapped(), 0);
      }
      auto mappedAllocator = dynamic_cast<MmapAllocator*>(allocator_.get());
      ASSERT_EQ(mappedAllocator->numExternalMapped(), 0);
    } else {
      ASSERT_EQ(instance_->numMapped(), 0);
      ASSERT_EQ(instance_->numAllocated(), 0);
    }
    ASSERT_TRUE(instance_->checkConsistency());
  }
}

TEST_P(MemoryAllocatorTest, allocContiguousFail) {
  struct {
    MachinePageCount nonContiguousPages;
    MachinePageCount oldContiguousPages;
    MachinePageCount newContiguousPages;
    MemoryAllocator::InjectedFailure injectedFailure;

    std::string debugString() const {
      return fmt::format(
          "nonContiguousPages:{} oldContiguousPages:{} newContiguousPages:{} injectedFailure:{}",
          nonContiguousPages,
          oldContiguousPages,
          newContiguousPages,
          injectedFailure);
    }
  } testSettings[] = {
      {200, 100, 400, MemoryAllocator::InjectedFailure::kCap},
      {0, 100, 200, MemoryAllocator::InjectedFailure::kCap},
      {100, 0, 200, MemoryAllocator::InjectedFailure::kCap},
      {100, 100, 200, MemoryAllocator::InjectedFailure::kMmap},
      {100, 200, 200, MemoryAllocator::InjectedFailure::kMmap},
      {200, 100, 200, MemoryAllocator::InjectedFailure::kMmap},
      {200, 100, 400, MemoryAllocator::InjectedFailure::kMmap},
      {0, 100, 100, MemoryAllocator::InjectedFailure::kMmap},
      {0, 200, 100, MemoryAllocator::InjectedFailure::kMmap},
      {0, 100, 200, MemoryAllocator::InjectedFailure::kMmap},
      {100, 0, 100, MemoryAllocator::InjectedFailure::kMmap},
      {200, 0, 100, MemoryAllocator::InjectedFailure::kMmap},
      {100, 0, 200, MemoryAllocator::InjectedFailure::kMmap},
      {100, 100, 200, MemoryAllocator::InjectedFailure::kMadvise},
      {200, 100, 200, MemoryAllocator::InjectedFailure::kMadvise},
      {200, 100, 400, MemoryAllocator::InjectedFailure::kMadvise},
      {0, 100, 200, MemoryAllocator::InjectedFailure::kMadvise},
      {100, 0, 100, MemoryAllocator::InjectedFailure::kMadvise},
      {200, 0, 100, MemoryAllocator::InjectedFailure::kMadvise},
      {100, 0, 200, MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    if ((testData.injectedFailure !=
         MemoryAllocator::InjectedFailure::kAllocate) &&
        !useMmap_) {
      continue;
    }
    SCOPED_TRACE(
        fmt::format("{} useMmap {}", testData.debugString(), useMmap_));
    setupAllocator();
    const MachinePageCount nonContiguousPages = 100;
    Allocation allocation;
    if (testData.nonContiguousPages != 0) {
      instance_->allocateNonContiguous(testData.nonContiguousPages, allocation);
    }
    ContiguousAllocation contiguousAllocation;
    if (testData.oldContiguousPages != 0) {
      instance_->allocateContiguous(
          testData.oldContiguousPages, nullptr, contiguousAllocation);
    }
    ASSERT_EQ(
        instance_->numAllocated(),
        testData.oldContiguousPages + testData.nonContiguousPages);

    instance_->testingSetFailureInjection(testData.injectedFailure, true);

    ASSERT_FALSE(instance_->allocateContiguous(
        testData.newContiguousPages, &allocation, contiguousAllocation));
    ASSERT_EQ(instance_->numAllocated(), 0);

    if (useMmap_) {
      // Mmap allocator doesn't free mapped pages count for the old
      // non-contiguous allocation.
      ASSERT_EQ(instance_->numMapped(), testData.nonContiguousPages);
      auto mappedAllocator = dynamic_cast<MmapAllocator*>(allocator_.get());
      ASSERT_EQ(mappedAllocator->numExternalMapped(), 0);
    } else {
      ASSERT_EQ(instance_->numMapped(), 0);
    }
    ASSERT_TRUE(instance_->checkConsistency());
  }
}

TEST_P(MemoryAllocatorTest, allocateBytes) {
  constexpr int32_t kNumAllocs = 50;
  // Different sizes, including below minimum and above largest size class.
  std::vector<MachinePageCount> sizes = {
      (size_t)(maxMallocBytes_ / 2),
      100000,
      1000000,
      instance_->sizeClasses().back() * AllocationTraits::kPageSize + 100000};
  folly::Random::DefaultGenerator rng;
  rng.seed(1);

  // We fill 'data' with random size allocations. Each is filled with its index
  // in 'data' cast to char.
  std::vector<folly::Range<char*>> data(kNumAllocs);
  uint64_t expectedNumMallocBytes = 0;
  for (auto counter = 0; counter < data.size() * 4; ++counter) {
    int32_t index = folly::Random::rand32(rng) % kNumAllocs;
    int32_t bytes = sizes[folly::Random::rand32() % sizes.size()];
    char expected = static_cast<char>(index);
    if (data[index].data()) {
      // If there is pre-existing data, we check that it has not been
      // overwritten.
      for (auto byte : data[index]) {
        ASSERT_EQ(expected, byte);
      }
      int32_t freeSize = data[index].size();
      instance_->freeBytes(data[index].data(), freeSize);
      if (useMmap_ && freeSize == sizes[0]) {
        expectedNumMallocBytes -= freeSize;
        ASSERT_EQ(
            expectedNumMallocBytes,
            ((MmapAllocator*)instance_)->numMallocBytes());
      }
    }
    data[index] = folly::Range<char*>(
        reinterpret_cast<char*>(instance_->allocateBytes(bytes)), bytes);
    if (useMmap_ && bytes == sizes[0]) {
      expectedNumMallocBytes += bytes;
      ASSERT_EQ(
          expectedNumMallocBytes,
          ((MmapAllocator*)instance_)->numMallocBytes());
    }
    for (auto& byte : data[index]) {
      byte = expected;
    }
  }
  ASSERT_TRUE(instance_->checkConsistency());
  for (auto& range : data) {
    if (range.data()) {
      int32_t bytes = range.size();
      instance_->freeBytes(range.data(), bytes);
      if (useMmap_ && bytes == sizes[0]) {
        expectedNumMallocBytes -= bytes;
        ASSERT_EQ(
            expectedNumMallocBytes,
            ((MmapAllocator*)instance_)->numMallocBytes());
      }
    }
  }

  ASSERT_EQ(0, instance_->numAllocated());
  ASSERT_TRUE(instance_->checkConsistency());
}

TEST_P(MemoryAllocatorTest, allocateBytesWithAlignment) {
  struct {
    uint64_t allocateBytes;
    uint16_t alignment;
    bool expectSuccess;
    std::string debugString() const {
      return fmt::format(
          "allocateBytes:{} alignment:{}, expectSuccess:{}",
          allocateBytes,
          alignment,
          expectSuccess);
    }
  } testSettings[] = {
      {AllocationTraits::kPageSize / 5,
       MemoryAllocator::kMinAlignment + 1,
       false},
      {AllocationTraits::kPageSize / 4,
       MemoryAllocator::kMaxAlignment + 1,
       false},
      {AllocationTraits::kPageSize / 5,
       MemoryAllocator::kMaxAlignment * 2,
       false},
      {AllocationTraits::kPageSize / 4,
       MemoryAllocator::kMaxAlignment * 2,
       false},
      {AllocationTraits::kPageSize / 5, MemoryAllocator::kMaxAlignment, false},
      {AllocationTraits::kPageSize, MemoryAllocator::kMaxAlignment + 1, false},
      {AllocationTraits::kPageSize, MemoryAllocator::kMaxAlignment * 2, false},
      {AllocationTraits::kPageSize, MemoryAllocator::kMaxAlignment, true},
      {AllocationTraits::kPageSize, MemoryAllocator::kMaxAlignment / 2, true},
      {AllocationTraits::kPageSize * 2, MemoryAllocator::kMaxAlignment, true},
      {AllocationTraits::kPageSize * 2,
       MemoryAllocator::kMaxAlignment / 2,
       true},
      {MemoryAllocator::kMaxAlignment, MemoryAllocator::kMaxAlignment, true},
      {MemoryAllocator::kMaxAlignment / 2,
       MemoryAllocator::kMaxAlignment / 2,
       true},
      {MemoryAllocator::kMaxAlignment / 2,
       MemoryAllocator::kMinAlignment,
       true},
      {MemoryAllocator::kMaxAlignment / 2,
       MemoryAllocator::kMinAlignment - 1,
       false},
      {MemoryAllocator::kMaxAlignment / 2, 0, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(
        fmt::format("UseMmap: {}, {}", useMmap_, testData.debugString()));

    if (testData.expectSuccess) {
      auto* ptr =
          instance_->allocateBytes(testData.allocateBytes, testData.alignment);
      ASSERT_NE(ptr, nullptr);
      if (testData.alignment > MemoryAllocator::kMinAlignment) {
        ASSERT_EQ(reinterpret_cast<uint64_t>(ptr) % testData.alignment, 0);
      }
      instance_->freeBytes(ptr, testData.allocateBytes);
    } else {
      EXPECT_ANY_THROW(
          instance_->allocateBytes(testData.allocateBytes, testData.alignment));
    }
    ASSERT_TRUE(instance_->checkConsistency());
  }
}

TEST_P(MemoryAllocatorTest, allocateZeroFilled) {
  constexpr int32_t kNumAllocs = 50;
  // Different sizes, including below minimum and above largest size class.
  const std::vector<MachinePageCount> sizes = {
      (size_t)(maxMallocBytes_ / 2),
      100000,
      1000000,
      instance_->sizeClasses().back() * AllocationTraits::kPageSize + 100000};
  const std::vector<uint64_t> alignments = {
      8, 16, 32, MemoryAllocator::kMaxAlignment};
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
        ASSERT_EQ(expected, byte);
      }
      instance_->freeBytes(data[index].data(), data[index].size());
    }
    uint16_t alignment =
        alignments[folly::Random::rand32() % alignments.size()];
    if (bytes % alignment != 0) {
      alignment = 0;
    }
    data[index] = folly::Range<char*>(
        reinterpret_cast<char*>(instance_->allocateZeroFilled(bytes)), bytes);
    for (auto& byte : data[index]) {
      ASSERT_EQ(byte, 0);
      byte = expected;
    }
  }
  ASSERT_TRUE(instance_->checkConsistency());
  for (auto& range : data) {
    if (range.data()) {
      instance_->freeBytes(range.data(), range.size());
    }
  }

  ASSERT_EQ(0, instance_->numAllocated());
  ASSERT_TRUE(instance_->checkConsistency());
}

TEST_P(MemoryAllocatorTest, StlMemoryAllocator) {
  {
    std::vector<double, StlAllocator<double>> data(
        0, StlAllocator<double>(*pool_));
    // The contiguous size goes to 2MB, covering malloc, size
    // Allocation from classes and ContiguousAllocation outside size
    // classes.
    constexpr int32_t kNumDoubles = 256 * 1024;
    size_t capacity = 0;
    for (auto i = 0; i < kNumDoubles; i++) {
      data.push_back(i);
    }
    for (auto i = 0; i < kNumDoubles; i++) {
      ASSERT_EQ(i, data[i]);
    }
    if (useMmap_) {
      EXPECT_EQ(512, instance_->numAllocated());
    } else {
      EXPECT_EQ(0, instance_->numAllocated());
    }
  }
  EXPECT_EQ(0, instance_->numAllocated());
  EXPECT_TRUE(instance_->checkConsistency());
  {
    StlAllocator<int64_t> alloc(*pool_);
    EXPECT_THROW(alloc.allocate(1ULL << 62), VeloxException);
    auto p = alloc.allocate(1);
    EXPECT_THROW(alloc.deallocate(p, 1ULL << 62), VeloxException);
    alloc.deallocate(p, 1);
  }
}

TEST_P(MemoryAllocatorTest, badNonContiguousAllocation) {
  // Set the num of pages to allocate exceeds one PageRun limit.
  constexpr MachinePageCount kNumPages =
      Allocation::PageRun::kMaxPagesInRun + 1;
  std::unique_ptr<Allocation> allocation(new Allocation());
  ASSERT_THROW(
      instance_->allocateNonContiguous(kNumPages, *allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(instance_->allocateNonContiguous(kNumPages - 1, *allocation));
  instance_->freeNonContiguous(*allocation);
}

TEST_P(MemoryAllocatorTest, allocation) {
  const MachinePageCount kNumPages = 133;
  const MachinePageCount kMinClassSize = 20;
  auto allocation = std::make_unique<Allocation>();
  ASSERT_TRUE(allocation->empty());
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_EQ(allocation->numPages(), 0);
  ASSERT_EQ(allocation->numRuns(), 0);
  ASSERT_THROW(
      instance_->allocateNonContiguous(0, *allocation, nullptr, kMinClassSize),
      VeloxRuntimeError);
  ASSERT_TRUE(instance_->allocateNonContiguous(
      kNumPages, *allocation, nullptr, kMinClassSize));
  ASSERT_TRUE(!allocation->empty());
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_GT(allocation->numPages(), kNumPages);
  ASSERT_GT(allocation->numRuns(), 0);
  {
    Allocation movedAllocation = std::move(*allocation);
    ASSERT_TRUE(allocation->empty());
    ASSERT_TRUE(!movedAllocation.empty());
    *allocation = std::move(movedAllocation);
    ASSERT_TRUE(!allocation->empty());
    ASSERT_TRUE(movedAllocation.empty());
  }
  ASSERT_DEATH(allocation.reset(), "");
  instance_->freeNonContiguous(*allocation);
  ASSERT_TRUE(allocation->empty());
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_EQ(allocation->numPages(), 0);
  ASSERT_EQ(allocation->numRuns(), 0);
  uint8_t* fakePtr = reinterpret_cast<uint8_t*>(allocation.get());
  allocation->append(fakePtr, kNumPages);
  ASSERT_EQ(allocation->numRuns(), 1);
  ASSERT_EQ(allocation->numPages(), kNumPages);
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_TRUE(!allocation->empty());
  allocation->setPool(pool_.get());
  ASSERT_EQ(allocation->pool(), pool_.get());
  {
    Allocation movedAllocation = std::move(*allocation);
    ASSERT_TRUE(allocation->empty());
    ASSERT_TRUE(!movedAllocation.empty());
    ASSERT_EQ(movedAllocation.pool(), pool_.get());
    *allocation = std::move(movedAllocation);
    ASSERT_TRUE(!allocation->empty());
    ASSERT_TRUE(movedAllocation.empty());
    ASSERT_EQ(allocation->pool(), pool_.get());
  }
  ASSERT_THROW(allocation->setPool(pool_.get()), VeloxRuntimeError);
  ASSERT_TRUE(!allocation->empty());
  allocation->clear();
  ASSERT_TRUE(allocation->empty());
  ASSERT_EQ(allocation->numPages(), 0);
  ASSERT_EQ(allocation->numRuns(), 0);
  ASSERT_EQ(allocation->pool(), nullptr);
  allocation->setPool(pool_.get());
  ASSERT_THROW(allocation->setPool(pool_.get()), VeloxRuntimeError);
  ASSERT_DEATH(allocation.reset(), "");
  ASSERT_THROW(allocation->empty(), VeloxRuntimeError);
  allocation->clear();
}

TEST_P(MemoryAllocatorTest, contiguousAllocation) {
  const MachinePageCount kNumPages = instance_->largestSizeClass() + 1;
  auto allocation = std::make_unique<ContiguousAllocation>();
  ASSERT_TRUE(allocation->empty());
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_EQ(allocation->numPages(), 0);
  ASSERT_THROW(
      instance_->allocateContiguous(0, nullptr, *allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(instance_->allocateContiguous(kNumPages, nullptr, *allocation));
  ASSERT_TRUE(!allocation->empty());
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_EQ(allocation->numPages(), kNumPages);
  {
    ContiguousAllocation movedAllocation = std::move(*allocation);
    ASSERT_TRUE(allocation->empty());
    ASSERT_TRUE(!movedAllocation.empty());
    *allocation = std::move(movedAllocation);
    ASSERT_TRUE(!allocation->empty());
    ASSERT_TRUE(movedAllocation.empty());
  }
  ASSERT_DEATH(allocation.reset(), "");
  instance_->freeContiguous(*allocation);
  ASSERT_TRUE(allocation->empty());
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_EQ(allocation->numPages(), 0);
  uint8_t* fakePtr = reinterpret_cast<uint8_t*>(allocation.get());
  allocation->set(fakePtr, kNumPages * AllocationTraits::kPageSize);
  ASSERT_EQ(allocation->numPages(), kNumPages);
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_TRUE(!allocation->empty());
  allocation->setPool(pool_.get());
  ASSERT_EQ(allocation->pool(), pool_.get());
  {
    ContiguousAllocation movedAllocation = std::move(*allocation);
    ASSERT_TRUE(allocation->empty());
    ASSERT_TRUE(!movedAllocation.empty());
    ASSERT_EQ(movedAllocation.pool(), pool_.get());
    *allocation = std::move(movedAllocation);
    ASSERT_TRUE(!allocation->empty());
    ASSERT_TRUE(movedAllocation.empty());
    ASSERT_EQ(allocation->pool(), pool_.get());
  }
  ASSERT_THROW(allocation->setPool(pool_.get()), VeloxRuntimeError);
  ASSERT_TRUE(!allocation->empty());
  allocation->clear();
  ASSERT_TRUE(allocation->empty());
  ASSERT_EQ(allocation->numPages(), 0);
  ASSERT_EQ(allocation->pool(), nullptr);
  allocation->setPool(pool_.get());
  ASSERT_THROW(allocation->setPool(pool_.get()), VeloxRuntimeError);
  ASSERT_DEATH(allocation.reset(), "");
  ASSERT_THROW(allocation->empty(), VeloxRuntimeError);
  allocation->clear();
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MemoryAllocatorTests,
    MemoryAllocatorTest,
    testing::ValuesIn({false, true}));

class MmapArenaTest : public testing::Test {
 public:
  // 32 MB arena space
  static constexpr uint64_t kArenaCapacityBytes = 1l << 25;

 protected:
  void SetUp() override {
    rng_.seed(1);
  }

  void* allocateAndPad(MmapArena* arena, uint64_t bytes) {
    void* buffer = arena->allocate(bytes);
    memset(buffer, 0xff, bytes);
    return buffer;
  }

  void unpadAndFree(MmapArena* arena, void* buffer, uint64_t bytes) {
    memset(buffer, 0x00, bytes);
    arena->free(buffer, bytes);
  }

  uint64_t randomPowTwo(uint64_t lowerBound, uint64_t upperBound) {
    lowerBound = bits::nextPowerOfTwo(lowerBound);
    auto attemptedUpperBound = bits::nextPowerOfTwo(upperBound);
    upperBound = attemptedUpperBound == upperBound ? upperBound
                                                   : attemptedUpperBound / 2;
    uint64_t moveSteps;
    if (lowerBound == 0) {
      uint64_t one = 1;
      moveSteps =
          (folly::Random::rand64(
               bits::countLeadingZeros(one) + 1 -
                   bits::countLeadingZeros(upperBound),
               rng_) +
           1);
      return moveSteps == 0 ? 0 : (1l << (moveSteps - 1));
    }
    moveSteps =
        (folly::Random::rand64(
             bits::countLeadingZeros(lowerBound) -
                 bits::countLeadingZeros(upperBound),
             rng_) +
         1);
    return lowerBound << moveSteps;
  }

  folly::Random::DefaultGenerator rng_;
};

TEST_F(MmapArenaTest, basic) {
  // 0 Byte lower bound for revealing edge cases.
  const uint64_t kAllocLowerBound = 0;

  // 1 KB upper bound
  const uint64_t kAllocUpperBound = 1l << 10;
  std::unique_ptr<MmapArena> arena =
      std::make_unique<MmapArena>(kArenaCapacityBytes);
  memset(arena->address(), 0x00, kArenaCapacityBytes);

  std::unordered_map<uint64_t, uint64_t> allocations;

  // First phase allocate only
  for (size_t i = 0; i < 1000; i++) {
    auto bytes = randomPowTwo(kAllocLowerBound, kAllocUpperBound);
    allocations.emplace(
        reinterpret_cast<uint64_t>(allocateAndPad(arena.get(), bytes)), bytes);
  }
  EXPECT_TRUE(arena->checkConsistency());

  // Second phase alloc and free called in an interleaving way
  for (size_t i = 0; i < 10000; i++) {
    auto bytes = randomPowTwo(kAllocLowerBound, kAllocUpperBound);
    allocations.emplace(
        reinterpret_cast<uint64_t>(allocateAndPad(arena.get(), bytes)), bytes);

    auto itrToFree = allocations.begin();
    auto bytesFree = itrToFree->second;
    unpadAndFree(
        arena.get(), reinterpret_cast<void*>(itrToFree->first), bytesFree);
    allocations.erase(itrToFree);
  }
  EXPECT_TRUE(arena->checkConsistency());

  // Third phase free only
  auto itr = allocations.begin();
  while (itr != allocations.end()) {
    auto bytes = itr->second;
    unpadAndFree(arena.get(), reinterpret_cast<void*>(itr->first), bytes);
    itr++;
  }
  EXPECT_TRUE(arena->checkConsistency());
}

TEST_F(MmapArenaTest, managedMmapArenas) {
  {
    // Test natural growing of ManagedMmapArena
    std::unique_ptr<ManagedMmapArenas> managedArenas =
        std::make_unique<ManagedMmapArenas>(kArenaCapacityBytes);
    EXPECT_EQ(managedArenas->arenas().size(), 1);
    void* alloc1 = managedArenas->allocate(kArenaCapacityBytes);
    EXPECT_EQ(managedArenas->arenas().size(), 1);
    void* alloc2 = managedArenas->allocate(kArenaCapacityBytes);
    EXPECT_EQ(managedArenas->arenas().size(), 2);

    managedArenas->free(alloc2, kArenaCapacityBytes);
    EXPECT_EQ(managedArenas->arenas().size(), 2);
    managedArenas->free(alloc1, kArenaCapacityBytes);
    EXPECT_EQ(managedArenas->arenas().size(), 1);
  }

  {
    // Test growing of ManagedMmapArena due to fragmentation
    std::unique_ptr<ManagedMmapArenas> managedArenas =
        std::make_unique<ManagedMmapArenas>(kArenaCapacityBytes);
    const uint64_t kNumAllocs = 128;
    const uint64_t kAllocSize = kArenaCapacityBytes / kNumAllocs;
    std::vector<uint64_t> evenAllocAddresses;
    for (int i = 0; i < kNumAllocs; i++) {
      auto* allocResult = managedArenas->allocate(kAllocSize);
      if (i % 2 == 0) {
        evenAllocAddresses.emplace_back(
            reinterpret_cast<uint64_t>(allocResult));
      }
    }
    EXPECT_EQ(managedArenas->arenas().size(), 1);

    // Free every other allocations so that the single MmapArena is fragmented
    // that it can no longer handle allocations of size larger than kAllocSize
    for (auto address : evenAllocAddresses) {
      managedArenas->free(reinterpret_cast<void*>(address), kAllocSize);
    }

    managedArenas->allocate(kAllocSize * 2);
    EXPECT_EQ(managedArenas->arenas().size(), 2);
  }
}

TEST_F(MmapArenaTest, managedMmapArenasFree) {
  struct {
    std::vector<uint64_t> allocSizes;
    std::vector<int> freeIndexes;
    std::vector<uint64_t> postFreeAllocSizes;
    std::vector<uint64_t> postAllocFeeeIndexes;
    int expectedNumOfAreanas;

    std::string debugString() const {
      return fmt::format(
          "allocSizes:{} freeIndexes:{} postFreeAllocSizes:{} postAllocFeeeIndexes:{} expectedNumOfAreanas:{}",
          folly::join(',', allocSizes),
          folly::join(',', freeIndexes),
          folly::join(',', postFreeAllocSizes),
          folly::join(',', postAllocFeeeIndexes),
          expectedNumOfAreanas);
    }
  } testSettings[] = {
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {1},
       {kArenaCapacityBytes / 4},
       {0, 2, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {0},
       {kArenaCapacityBytes / 4},
       {1, 2, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {0},
       {kArenaCapacityBytes / 2},
       {1, 2, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {1},
       {kArenaCapacityBytes / 2},
       {0, 2, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {1, 2},
       {kArenaCapacityBytes / 2},
       {0, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {0, 1},
       {kArenaCapacityBytes / 2},
       {2, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {0, 2},
       {kArenaCapacityBytes / 2},
       {1, 3},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {0, 3},
       {kArenaCapacityBytes / 2},
       {2, 1},
       1},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {1},
       {kArenaCapacityBytes / 2},
       {0, 3},
       2},
      {{kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4,
        kArenaCapacityBytes / 4},
       {0},
       {kArenaCapacityBytes / 2},
       {1, 3},
       2}};
  struct Buffer {
    void* buffer;
    uint64_t length;
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::unique_ptr<ManagedMmapArenas> managedArenas =
        std::make_unique<ManagedMmapArenas>(kArenaCapacityBytes);
    std::vector<Buffer> buffers;
    buffers.reserve(
        testData.allocSizes.size() + testData.postFreeAllocSizes.size());
    for (const auto& allocSize : testData.allocSizes) {
      buffers.push_back(Buffer{managedArenas->allocate(allocSize), allocSize});
    }
    for (const auto& freeIndex : testData.freeIndexes) {
      managedArenas->free(buffers[freeIndex].buffer, buffers[freeIndex].length);
      buffers[freeIndex].buffer = nullptr;
    }
    for (const auto& allocSize : testData.postFreeAllocSizes) {
      buffers.push_back(Buffer{managedArenas->allocate(allocSize), allocSize});
    }
    for (const auto& freeIndex : testData.postAllocFeeeIndexes) {
      managedArenas->free(buffers[freeIndex].buffer, buffers[freeIndex].length);
      buffers[freeIndex].buffer = nullptr;
    }
    ASSERT_EQ(managedArenas->arenas().size(), testData.expectedNumOfAreanas);
  }
}

TEST_F(MmapArenaTest, managedMmapArenasFreeError) {
  {
    std::unique_ptr<ManagedMmapArenas> managedArenas =
        std::make_unique<ManagedMmapArenas>(kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 1);
    void* alloc1 = managedArenas->allocate(kArenaCapacityBytes / 2);
    void* alloc2 = managedArenas->allocate(kArenaCapacityBytes / 2);
    ASSERT_EQ(managedArenas->arenas().size(), 1);
    managedArenas->free(alloc1, kArenaCapacityBytes / 2);
    ASSERT_ANY_THROW(managedArenas->free(alloc1, kArenaCapacityBytes / 2));
    managedArenas->free(alloc2, kArenaCapacityBytes / 2);
    ASSERT_ANY_THROW(managedArenas->free(alloc2, kArenaCapacityBytes / 2));
  }
  {
    std::unique_ptr<ManagedMmapArenas> managedArenas =
        std::make_unique<ManagedMmapArenas>(kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 1);
    void* alloc1 = managedArenas->allocate(kArenaCapacityBytes);
    void* alloc2 = managedArenas->allocate(kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 2);
    managedArenas->free(alloc1, kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 1);
    ASSERT_ANY_THROW(managedArenas->free(alloc1, kArenaCapacityBytes));
    managedArenas->free(alloc2, kArenaCapacityBytes);
    ASSERT_ANY_THROW(managedArenas->free(alloc2, kArenaCapacityBytes));
  }
}
} // namespace facebook::velox::memory
