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

static constexpr uint64_t kMaxMemoryAllocator = 128UL * 1024 * 1024;
static constexpr MachinePageCount kCapacity =
    (kMaxMemoryAllocator / MemoryAllocator::kPageSize);

// The class leverage memory usage tracker to track the memory usage.
class MockMemoryAllocator final : public MemoryAllocator {
 public:
  void* allocateBytes(
      uint64_t bytes,
      uint16_t alignment = 0,
      uint64_t maxMallocSize = kMaxMallocBytes) override {
    return allocator_->allocateBytes(bytes, alignment, maxMallocSize);
  }

  void freeBytes(
      void* FOLLY_NONNULL p,
      uint64_t size,
      uint64_t maxMallocSize = kMaxMallocBytes) noexcept override {
    allocator_->freeBytes(p, size, maxMallocSize);
  }

  MockMemoryAllocator(
      MemoryAllocator* FOLLY_NONNULL allocator,
      std::shared_ptr<MemoryUsageTracker> tracker)
      : allocator_(allocator), tracker_(std::move(tracker)) {}

  bool allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback /*unused*/ = nullptr,
      MachinePageCount minSizeClass = 0) override {
    freeNonContiguous(out);
    return allocator_->allocateNonContiguous(
        numPages,
        out,
        [this](int64_t allocBytes, bool preAllocate) {
          if (tracker_ != nullptr) {
            tracker_->update(preAllocate ? allocBytes : -allocBytes);
          }
        },
        minSizeClass);
  }

  int64_t freeNonContiguous(Allocation& allocation) override {
    const int64_t freed = allocator_->freeNonContiguous(allocation);
    if (tracker_) {
      tracker_->update(-freed);
    }
    return freed;
  }

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      ReservationCallback /*unused*/ = nullptr) override {
    return allocator_->allocateContiguous(
        numPages,
        collateral,
        allocation,
        [this](int64_t allocBytes, bool preAlloc) {
          if (tracker_ != nullptr) {
            tracker_->update(preAlloc ? allocBytes : -allocBytes);
          }
        });
  }

  void freeContiguous(ContiguousAllocation& allocation) override {
    const int64_t size = allocation.size();
    allocator_->freeContiguous(allocation);
    if (tracker_ != nullptr) {
      tracker_->update(-size);
    }
  }

  bool checkConsistency() const override {
    return allocator_->checkConsistency();
  }

  const std::vector<MachinePageCount>& sizeClasses() const override {
    return allocator_->sizeClasses();
  }

  MachinePageCount numAllocated() const override {
    return allocator_->numAllocated();
  }

  MachinePageCount numMapped() const override {
    return allocator_->numMapped();
  }

  Stats stats() const override {
    return allocator_->stats();
  }

 private:
  MemoryAllocator* FOLLY_NONNULL allocator_;
  std::shared_ptr<MemoryUsageTracker> tracker_;
};

struct TestParam {
  bool useMmap;
  // If true, use MockMemoryAllocator to tracker the memory usage through the
  // memory usage tracker.
  bool hasMemoryTracker;

  TestParam(bool _useMmap, bool _hasMemoryTracker)
      : useMmap(_useMmap), hasMemoryTracker(_hasMemoryTracker) {}

  std::string toString() const {
    return fmt::format(
        "useMmap{} hasMemoryTracker{}", useMmap, hasMemoryTracker);
  }
};

class MemoryAllocatorTest : public testing::TestWithParam<TestParam> {
 public:
  static const std::vector<TestParam> getTestParams() {
    std::vector<TestParam> params;
    params.push_back({true, true});
    params.push_back({true, false});
    params.push_back({false, true});
    params.push_back({false, false});
    return params;
  }

 protected:
  static void SetUpTestCase() {
    TestValue::enable();
  }

  void SetUp() override {
    MemoryAllocator::testingDestroyInstance();
    useMmap_ = GetParam().useMmap;
    if (useMmap_) {
      MmapAllocatorOptions options;
      options.capacity = kMaxMemoryAllocator;
      mmapAllocator_ = std::make_shared<MmapAllocator>(options);
      MemoryAllocator::setDefaultInstance(mmapAllocator_.get());
    } else {
      MemoryAllocator::setDefaultInstance(nullptr);
    }
    hasMemoryTracker_ = GetParam().hasMemoryTracker;
    if (hasMemoryTracker_) {
      memoryUsageTracker_ =
          MemoryUsageTracker::create(MemoryUsageConfigBuilder()
                                         .maxTotalMemory(kMaxMemoryAllocator)
                                         .build());
      mockAllocator_ = std::make_shared<MockMemoryAllocator>(
          MemoryAllocator::getInstance(), memoryUsageTracker_);
      MemoryAllocator::setDefaultInstance(mockAllocator_.get());
    }
    instance_ = MemoryAllocator::getInstance();
    IMemoryManager::Options options;
    options.capacity = kMaxMemory;
    options.allocator = instance_;
    memoryManager_ = std::make_unique<MemoryManager>(options);
    pool_ = memoryManager_->getChild();
  }

  void TearDown() override {
    MemoryAllocator::testingDestroyInstance();
  }

  bool allocate(int32_t numPages, MemoryAllocator::Allocation& result) {
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

  void initializeContents(MemoryAllocator::Allocation& alloc) {
    auto sequence = sequence_.fetch_add(1);
    bool first = true;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      MemoryAllocator::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * MemoryAllocator::kPageSize / sizeof(void*);
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

  void checkContents(MemoryAllocator::Allocation& alloc) {
    bool first = true;
    long sequence;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      MemoryAllocator::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * MemoryAllocator::kPageSize / sizeof(void*);
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

  void initializeContents(MemoryAllocator::ContiguousAllocation& alloc) {
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

  void checkContents(MemoryAllocator::ContiguousAllocation& alloc) {
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

  void free(MemoryAllocator::Allocation& alloc) {
    checkContents(alloc);
    instance_->freeNonContiguous(alloc);
  }

  void clearAllocations(
      std::vector<std::unique_ptr<MemoryAllocator::Allocation>>& allocations) {
    for (auto& allocation : allocations) {
      instance_->freeNonContiguous(*allocation);
    }
    allocations.clear();
  }

  void clearAllocations(
      std::vector<std::vector<std::unique_ptr<MemoryAllocator::Allocation>>>&
          allocationsVector) {
    for (auto& allocations : allocationsVector) {
      for (auto& allocation : allocations) {
        instance_->freeNonContiguous(*allocation);
      }
    }
    allocationsVector.clear();
  }

  void shrinkAllocations(
      std::vector<std::unique_ptr<MemoryAllocator::Allocation>>& allocations,
      int32_t reducedSize) {
    while (allocations.size() > reducedSize) {
      instance_->freeNonContiguous(*allocations.back());
      allocations.pop_back();
    }
    ASSERT_EQ(allocations.size(), reducedSize);
  }

  void clearContiguousAllocations(
      std::vector<MemoryAllocator::ContiguousAllocation>& allocations) {
    for (auto& allocation : allocations) {
      instance_->freeContiguous(allocation);
    }
    allocations.clear();
  }

  void allocateMultiple(
      MachinePageCount numPages,
      int32_t numAllocs,
      std::vector<std::unique_ptr<MemoryAllocator::Allocation>>& allocations) {
    clearAllocations(allocations);
    allocations.reserve(numAllocs);
    // allocations.push_back(std::make_unique<MemoryAllocator::Allocation>());
    bool largeTested = false;
    for (int32_t i = 0; i < numAllocs; ++i) {
      auto allocation = std::make_unique<MemoryAllocator::Allocation>();
      if (!allocate(numPages, *allocation)) {
        continue;
      }
      allocations.push_back(std::move(allocation));
      int available = kCapacity - instance_->numAllocated();

      // Try large allocations after half the capacity is used.
      if (available <= kCapacity / 2 && !largeTested) {
        largeTested = true;
        MemoryAllocator::ContiguousAllocation large;
        if (!allocateContiguous(available / 2, nullptr, large)) {
          FAIL() << "Could not allocate half the available";
          return;
        }
        MemoryAllocator::Allocation small;
        if (!instance_->allocateNonContiguous(available / 4, small)) {
          FAIL() << "Could not allocate 1/4 of available";
          return;
        }
        // Try to allocate more than available, and it should fail if we use
        // MmapAllocator which enforces the capacity check.
        if (hasMemoryTracker_) {
          ASSERT_THROW(
              instance_->allocateContiguous(available + 1, &small, large),
              VeloxRuntimeError);
        } else {
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
        }

        // Check The failed allocation freed the collateral.
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
      MemoryAllocator::Allocation* FOLLY_NULLABLE collateral,
      MemoryAllocator::ContiguousAllocation& allocation) {
    bool success =
        instance_->allocateContiguous(numPages, collateral, allocation);
    if (success) {
      initializeContents(allocation);
    }
    return success;
  }

  void free(MemoryAllocator::ContiguousAllocation& allocation) {
    checkContents(allocation);
    instance_->freeContiguous(allocation);
  }

  void allocateIncreasing(
      MachinePageCount startSize,
      MachinePageCount endSize,
      int32_t repeat,
      std::vector<std::unique_ptr<MemoryAllocator::Allocation>>& allocations) {
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
      std::vector<std::unique_ptr<MemoryAllocator::Allocation>>& allocations,
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

  std::vector<std::unique_ptr<MemoryAllocator::Allocation>>
  makeEmptyAllocations(int32_t size) {
    std::vector<std::unique_ptr<MemoryAllocator::Allocation>> allocations;
    allocations.reserve(size);
    for (int32_t i = 0; i < size; i++) {
      allocations.push_back(std::make_unique<MemoryAllocator::Allocation>());
    }
    return allocations;
  }

  bool useMmap_;
  bool hasMemoryTracker_;
  std::shared_ptr<MemoryUsageTracker> memoryUsageTracker_;
  std::shared_ptr<MmapAllocator> mmapAllocator_;
  std::shared_ptr<MockMemoryAllocator> mockAllocator_;
  MemoryAllocator* instance_;
  std::unique_ptr<MemoryManager> memoryManager_;
  std::shared_ptr<MemoryPool> pool_;
  std::atomic<int32_t> sequence_ = {};
};

TEST_P(MemoryAllocatorTest, allocationPoolTest) {
  const size_t kNumLargeAllocPages = instance_->largestSizeClass() * 2;
  AllocationPool pool(pool_.get());

  pool.allocateFixed(10);
  EXPECT_EQ(pool.numTotalAllocations(), 1);
  EXPECT_EQ(pool.currentRunIndex(), 0);
  EXPECT_EQ(pool.currentOffset(), 10);

  pool.allocateFixed(kNumLargeAllocPages * MemoryAllocator::kPageSize);
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

TEST_P(MemoryAllocatorTest, allocationTest) {
  const int32_t kPageSize = MemoryAllocator::kPageSize;
  MemoryAllocator::Allocation allocation;
  uint8_t* pages = reinterpret_cast<uint8_t*>(::malloc(kPageSize * 20));
  // We append different pieces of 'pages' to 'allocation'.
  // 4 last pages.
  allocation.append(pages + 16 * kPageSize, 4);
  // 16th page
  allocation.append(pages + 15 * kPageSize, 1);
  // 15 first pages.
  allocation.append(pages, 15);
  ASSERT_EQ(allocation.numRuns(), 3);
  ASSERT_EQ(allocation.numPages(), 20);
  int32_t index;
  int32_t offsetInRun;
  // We look for the pointer of byte 2000 of the 16th page in
  // 'allocation'. This falls on the 11th page of the last run.
  const int32_t offset = 15 * kPageSize + 2000;
  allocation.findRun(offset, &index, &offsetInRun);
  // 3rd run.
  ASSERT_EQ(index, 2);
  ASSERT_EQ(offsetInRun, 10 * kPageSize + 2000);
  ASSERT_EQ(allocation.runAt(1).data(), pages + 15 * kPageSize);

  MemoryAllocator::Allocation moved(std::move(allocation));
  ASSERT_TRUE(allocation.empty());
  ASSERT_EQ(allocation.numRuns(), 0);
  ASSERT_EQ(allocation.numPages(), 0);
  ASSERT_EQ(moved.numRuns(), 3);
  ASSERT_EQ(moved.numPages(), 20);

  moved.clear();
  ASSERT_TRUE(moved.empty());
  ASSERT_EQ(moved.numRuns(), 0);
  ASSERT_EQ(moved.numPages(), 0);
  ::free(pages);
}

TEST_P(MemoryAllocatorTest, singleAllocationTest) {
  if (!useMmap_) {
    return;
  }
  const std::vector<MachinePageCount>& sizes = instance_->sizeClasses();
  MachinePageCount capacity = kCapacity;
  std::vector<std::unique_ptr<MemoryAllocator::Allocation>> allocations;
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
    EXPECT_EQ(instance_->numAllocated(), 0);

    auto stats = instance_->stats();
    EXPECT_LT(0, stats.sizes[i].clocks());
    ASSERT_GE(stats.sizes[i].totalBytes, capacity * MemoryAllocator::kPageSize)
        << i << " size: " << size;
    ASSERT_GE(stats.sizes[i].numAllocations, capacity / size)
        << i << " size: " << size;

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
  std::vector<std::unique_ptr<MemoryAllocator::Allocation>> allocations =
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
  std::vector<std::vector<std::unique_ptr<MemoryAllocator::Allocation>>>
      allocations;
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

TEST_P(MemoryAllocatorTest, allocationWithMemoryUsageTracking) {
  if (!hasMemoryTracker_) {
    GTEST_SKIP();
  }
  const int32_t numPages = 32;
  {
    MemoryAllocator::Allocation result;
    instance_->allocateNonContiguous(numPages, result);
    ASSERT_GE(result.numPages(), numPages);
    ASSERT_EQ(
        result.numPages() * MemoryAllocator::kPageSize,
        memoryUsageTracker_->getCurrentUserBytes());
    instance_->freeNonContiguous(result);
    ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);
  }

  {
    MemoryAllocator::Allocation result1;
    MemoryAllocator::Allocation result2;
    instance_->allocateNonContiguous(numPages, result1);
    ASSERT_GE(result1.numPages(), numPages);
    ASSERT_EQ(
        result1.numPages() * MemoryAllocator::kPageSize,
        memoryUsageTracker_->getCurrentUserBytes());

    instance_->allocateNonContiguous(numPages, result2);
    ASSERT_GE(result2.numPages(), numPages);
    ASSERT_EQ(
        (result1.numPages() + result2.numPages()) * MemoryAllocator::kPageSize,
        memoryUsageTracker_->getCurrentUserBytes());

    // Since allocations are still valid, usage should not change.
    ASSERT_EQ(
        (result1.numPages() + result2.numPages()) * MemoryAllocator::kPageSize,
        memoryUsageTracker_->getCurrentUserBytes());
    instance_->freeNonContiguous(result1);
    instance_->freeNonContiguous(result2);
  }
  ASSERT_EQ(0, memoryUsageTracker_->getCurrentUserBytes());
}

TEST_P(MemoryAllocatorTest, minSizeClass) {
  MemoryAllocator::Allocation result;

  int32_t sizeClass = instance_->sizeClasses().back();
  int32_t numPages = sizeClass + 1;
  instance_->allocateNonContiguous(numPages, result, nullptr, sizeClass);
  ASSERT_GE(result.numPages(), sizeClass * 2);
  // All runs have to be at least the minimum size.
  for (auto i = 0; i < result.numRuns(); ++i) {
    ASSERT_LE(sizeClass, result.runAt(i).numPages());
  }
  instance_->freeNonContiguous(result);
}

TEST_P(MemoryAllocatorTest, externalAdvise) {
  if (!useMmap_ || hasMemoryTracker_) {
    GTEST_SKIP();
  }
  constexpr int32_t kSmallSize = 16;
  constexpr int32_t kLargeSize = 32 * kSmallSize + 1;
  auto instance = dynamic_cast<MmapAllocator*>(MemoryAllocator::getInstance());
  std::vector<std::unique_ptr<MemoryAllocator::Allocation>> allocations;
  auto numAllocs = kCapacity / kSmallSize;
  allocations.reserve(numAllocs);
  for (int32_t i = 0; i < numAllocs; ++i) {
    allocations.push_back(std::make_unique<MemoryAllocator::Allocation>());
    ASSERT_TRUE(allocate(kSmallSize, *allocations.back().get()));
  }
  // We allocated and mapped the capacity. Now free half, leaving the memory
  // still mapped.
  shrinkAllocations(allocations, numAllocs / 2);
  ASSERT_TRUE(instance->checkConsistency());
  ASSERT_EQ(instance->numMapped(), numAllocs * kSmallSize);
  ASSERT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize);
  std::vector<MemoryAllocator::ContiguousAllocation> larges(2);
  ASSERT_TRUE(instance->allocateContiguous(kLargeSize, nullptr, larges[0]));
  // The same number are mapped but some got advised away to back the large
  // allocation. One kSmallSize got advised away but not fully used because
  // kLargeSize is not a multiple of kSmallSize.
  ASSERT_EQ(instance->numMapped(), numAllocs * kSmallSize - kSmallSize + 1);
  ASSERT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize + kLargeSize);
  ASSERT_TRUE(instance->allocateContiguous(kLargeSize, nullptr, larges[1]));
  clearContiguousAllocations(larges);
  ASSERT_EQ(instance->numAllocated(), allocations.size() * kSmallSize);
  // After freeing 2xkLargeSize, We have unmapped 2*LargeSize at the free and
  // another (kSmallSize - 1 when allocating the first kLargeSize. Of the 15
  // that this unmapped, 1 was taken by the second large alloc. So, the mapped
  // pages is total - (2 * kLargeSize) - 14. The unused unmapped are 15 pages
  // after the first and 14 after the second allocContiguous().
  ASSERT_EQ(
      instance->numMapped(),
      kSmallSize * numAllocs - (2 * kLargeSize) -
          (kSmallSize - (2 * (kLargeSize % kSmallSize))));
  ASSERT_TRUE(instance->checkConsistency());
  clearAllocations(allocations);
  ASSERT_TRUE(instance->checkConsistency());
}

TEST_P(MemoryAllocatorTest, allocContiguousFail) {
  if (!useMmap_ || hasMemoryTracker_) {
    GTEST_SKIP();
  }
  // Covers edge cases of
  constexpr int32_t kSmallSize = 16;
  constexpr int32_t kLargeSize = kCapacity / 2;
  auto instance = dynamic_cast<MmapAllocator*>(MemoryAllocator::getInstance());
  std::vector<std::unique_ptr<MemoryAllocator::Allocation>> allocations;
  auto numAllocs = kCapacity / kSmallSize;
  int64_t trackedBytes = 0;
  auto trackCallback = [&](int64_t delta, bool preAlloc) {
    trackedBytes += preAlloc ? delta : -delta;
  };
  allocations.reserve(numAllocs);
  for (int32_t i = 0; i < numAllocs; ++i) {
    allocations.push_back(std::make_unique<MemoryAllocator::Allocation>());
    ASSERT_TRUE(allocate(kSmallSize, *allocations.back().get()));
  }
  // We allocated and mapped the capacity. Now free half, leaving the memory
  // still mapped.
  shrinkAllocations(allocations, numAllocs / 2);
  ASSERT_TRUE(instance->checkConsistency());
  ASSERT_EQ(instance->numMapped(), numAllocs * kSmallSize);
  ASSERT_EQ(instance->numAllocated(), numAllocs / 2 * kSmallSize);
  MemoryAllocator::ContiguousAllocation large;
  ASSERT_TRUE(instance->allocateContiguous(
      kLargeSize / 2, nullptr, large, trackCallback));
  ASSERT_TRUE(instance->checkConsistency());

  // The allocation should go through because there is 1/2 of
  // kLargeSize already in large[0], 1/2 of kLargeSize free and
  // kSmallSize given as collateral. This does not go through because
  // we inject a failure in advising away the collateral.
  instance->testingInjectFailure(MmapAllocator::Failure::kMadvise);
  ASSERT_FALSE(instance->allocateContiguous(
      kLargeSize + kSmallSize, allocations.back().get(), large, trackCallback));
  ASSERT_TRUE(instance->checkConsistency());
  // large and allocations.back() were both freed and nothing was allocated.
  ASSERT_EQ(kSmallSize * (allocations.size() - 1), instance->numAllocated());
  // An extra kSmallSize were freed.
  ASSERT_EQ(-kSmallSize * MemoryAllocator::kPageSize, trackedBytes);
  // Remove the cleared item from the end.
  allocations.pop_back();

  trackedBytes = 0;
  ASSERT_TRUE(instance->allocateContiguous(
      kLargeSize / 2, nullptr, large, trackCallback));
  instance->testingInjectFailure(MmapAllocator::Failure::kMmap);
  // Should go through because 1/2 of kLargeSize + kSmallSize free and 1/2 of
  // kLargeSize already in large. Fails because mmap after advise away fails.
  ASSERT_FALSE(instance->allocateContiguous(
      kLargeSize + 2 * kSmallSize,
      allocations.back().get(),
      large,
      trackCallback));
  // large and allocations.back() were both freed and nothing was allocated.
  ASSERT_EQ(kSmallSize * (allocations.size() - 1), instance->numAllocated());
  ASSERT_EQ(-kSmallSize * MemoryAllocator::kPageSize, trackedBytes);
  allocations.pop_back();
  ASSERT_TRUE(instance->checkConsistency());

  trackedBytes = 0;
  ASSERT_TRUE(instance->allocateContiguous(
      kLargeSize / 2, nullptr, large, trackCallback));
  // We succeed without injected failure.
  ASSERT_TRUE(instance->allocateContiguous(
      kLargeSize + 3 * kSmallSize,
      allocations.back().get(),
      large,
      trackCallback));
  ASSERT_EQ(kCapacity, instance->numMapped());
  ASSERT_EQ(kCapacity, instance->numAllocated());
  // Size grew by kLargeSize + 2 * kSmallSize (one kSmallSize item was freed, so
  // no not 3 x kSmallSize).
  ASSERT_EQ(
      (kLargeSize + 2 * kSmallSize) * MemoryAllocator::kPageSize, trackedBytes);
  ASSERT_TRUE(instance->checkConsistency());
  instance_->freeContiguous(large);
  ASSERT_TRUE(instance->checkConsistency());
  clearAllocations(allocations);
  ASSERT_TRUE(instance->checkConsistency());
}

TEST_P(MemoryAllocatorTest, allocateBytes) {
  constexpr int32_t kNumAllocs = 50;
  MemoryAllocator::testingClearAllocateBytesStats();
  // Different sizes, including below minimum and above largest size class.
  std::vector<MachinePageCount> sizes = {
      MemoryAllocator::kMaxMallocBytes / 2,
      100000,
      1000000,
      instance_->sizeClasses().back() * MemoryAllocator::kPageSize + 100000};
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
    data[index] = folly::Range<char*>(
        reinterpret_cast<char*>(instance_->allocateBytes(bytes)), bytes);
    for (auto& byte : data[index]) {
      byte = expected;
    }
  }
  ASSERT_TRUE(instance_->checkConsistency());
  auto stats = MemoryAllocator::allocateBytesStats();
  if (!useMmap_) {
    ASSERT_LT(0, stats.totalSmall);
  }
  for (auto& range : data) {
    if (range.data()) {
      instance_->freeBytes(range.data(), range.size());
    }
  }
  stats = MemoryAllocator::allocateBytesStats();
  ASSERT_EQ(0, stats.totalSmall);
  ASSERT_EQ(0, stats.totalInSizeClasses);
  ASSERT_EQ(0, stats.totalLarge);

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
      {MemoryAllocator::kPageSize, MemoryAllocator::kMinAlignment / 2, false},
      {MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment / 10,
       false},
      {MemoryAllocator::kPageSize / 4,
       MemoryAllocator::kMaxAlignment / 10,
       false},
      {MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment * 2,
       false},
      {MemoryAllocator::kPageSize / 4,
       MemoryAllocator::kMaxAlignment * 2,
       false},
      {MemoryAllocator::kPageSize / 5, MemoryAllocator::kMaxAlignment, false},
      {MemoryAllocator::kPageSize, MemoryAllocator::kMaxAlignment / 10, false},
      {MemoryAllocator::kPageSize, MemoryAllocator::kMaxAlignment * 2, false},
      {MemoryAllocator::kPageSize, MemoryAllocator::kMaxAlignment, true},
      {MemoryAllocator::kPageSize, MemoryAllocator::kMaxAlignment / 2, true},
      {MemoryAllocator::kPageSize * 2, MemoryAllocator::kMaxAlignment, true},
      {MemoryAllocator::kPageSize * 2,
       MemoryAllocator::kMaxAlignment / 2,
       true},
      {MemoryAllocator::kMaxAlignment, MemoryAllocator::kMaxAlignment, true},
      {MemoryAllocator::kMaxAlignment / 2,
       MemoryAllocator::kMaxAlignment / 2,
       true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(
        fmt::format("UseMmap: {}, {}", useMmap_, testData.debugString()));

    MemoryAllocator::testingClearAllocateBytesStats();
    if (testData.expectSuccess) {
      auto* ptr =
          instance_->allocateBytes(testData.allocateBytes, testData.alignment);
      ASSERT_NE(ptr, nullptr);
      if (testData.alignment != 0) {
        ASSERT_EQ(reinterpret_cast<uint64_t>(ptr) % testData.alignment, 0);
      }
      const auto stats = MemoryAllocator::allocateBytesStats();
      if (useMmap_ &&
          testData.allocateBytes >= MemoryAllocator::kMaxMallocBytes) {
        ASSERT_EQ(0, stats.totalSmall);
        ASSERT_EQ(
            testData.allocateBytes,
            stats.totalInSizeClasses + stats.totalLarge);
        ASSERT_LT(0, instance_->numAllocated());
      } else {
        ASSERT_EQ(testData.allocateBytes, stats.totalSmall);
        ASSERT_EQ(0, stats.totalInSizeClasses);
        ASSERT_EQ(0, stats.totalLarge);
        ASSERT_EQ(0, instance_->numAllocated());
      };
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
  MemoryAllocator::testingClearAllocateBytesStats();
  // Different sizes, including below minimum and above largest size class.
  const std::vector<MachinePageCount> sizes = {
      MemoryAllocator::kMaxMallocBytes / 2,
      100000,
      1000000,
      instance_->sizeClasses().back() * MemoryAllocator::kPageSize + 100000};
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
        reinterpret_cast<char*>(
            instance_->allocateZeroFilled(bytes, alignment)),
        bytes);
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
  auto stats = MemoryAllocator::allocateBytesStats();
  ASSERT_EQ(0, stats.totalSmall);
  ASSERT_EQ(0, stats.totalInSizeClasses);
  ASSERT_EQ(0, stats.totalLarge);

  ASSERT_EQ(0, instance_->numAllocated());
  ASSERT_TRUE(instance_->checkConsistency());
}

TEST_P(MemoryAllocatorTest, reallocateBytes) {
  struct {
    uint64_t oldBytes;
    uint64_t newBytes;
    uint16_t alignment;
    bool expectSuccess;
    std::string debugString() const {
      return fmt::format(
          "oldBytes:{}, newBytes:{}, alignment:{}, expectSuccess:{}",
          oldBytes,
          newBytes,
          alignment,
          expectSuccess);
    }
  } testSettings[] = {
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment / 10,
       false},
      {MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kMaxAlignment / 10,
       false},
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment,
       false},
      {MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kMaxAlignment,
       false},
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment,
       false},
      {MemoryAllocator::kPageSize / 3,
       MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment,
       false},
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment,
       false},
      {MemoryAllocator::kPageSize * 2,
       MemoryAllocator::kPageSize,
       MemoryAllocator::kMaxAlignment / 5,
       false},
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kMaxAlignment * 2,
       false},
      {MemoryAllocator::kPageSize / 5,
       MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kMaxAlignment * 2,
       false},
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize,
       MemoryAllocator::kMaxAlignment,
       true},
      {MemoryAllocator::kPageSize * 2,
       MemoryAllocator::kPageSize,
       MemoryAllocator::kMaxAlignment,
       true},
      {MemoryAllocator::kPageSize / 7,
       MemoryAllocator::kPageSize,
       MemoryAllocator::kMaxAlignment / 2,
       true},
      {MemoryAllocator::kPageSize * 2,
       MemoryAllocator::kPageSize,
       MemoryAllocator::kMaxAlignment / 2,
       true},
      {MemoryAllocator::kPageSize,
       MemoryAllocator::kPageSize * 2,
       MemoryAllocator::kMaxAlignment / 2,
       true},
      {MemoryAllocator::kPageSize * 4,
       MemoryAllocator::kPageSize * 2,
       MemoryAllocator::kMaxAlignment / 2,
       true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(
        fmt::format("UseMmap: {}, {}", useMmap_, testData.debugString()));

    MemoryAllocator::testingClearAllocateBytesStats();
    auto* oldPtr = instance_->allocateBytes(testData.oldBytes);
    char* data = reinterpret_cast<char*>(oldPtr);
    const char value = 'o';
    for (int32_t i = 0; i < testData.oldBytes; ++i) {
      data[i] = value;
    }
    if (testData.expectSuccess) {
      auto* newPtr = instance_->reallocateBytes(
          oldPtr, testData.oldBytes, testData.newBytes, testData.alignment);
      ASSERT_NE(newPtr, nullptr);
      ASSERT_NE(oldPtr, newPtr);
      if (testData.alignment != 0) {
        ASSERT_EQ(reinterpret_cast<uint64_t>(newPtr) % testData.alignment, 0);
      }
      data = reinterpret_cast<char*>(newPtr);
      for (int32_t i = 0; i < std::min(testData.newBytes, testData.oldBytes);
           ++i) {
        ASSERT_EQ(data[i], value);
      }
      instance_->freeBytes(newPtr, testData.newBytes);
    } else {
      EXPECT_ANY_THROW(instance_->reallocateBytes(
          oldPtr, testData.oldBytes, testData.newBytes, testData.alignment));
      instance_->freeBytes(oldPtr, testData.oldBytes);
    }
    ASSERT_TRUE(instance_->checkConsistency());
  }
}

TEST_P(MemoryAllocatorTest, StlMemoryAllocator) {
  {
    std::vector<double, StlMemoryAllocator<double>> data(
        0, StlMemoryAllocator<double>(instance_));
    // The contiguous size goes to 2MB, covering malloc, size
    // Allocation from classes and ContiguousAllocation outside size
    // classes.
    constexpr int32_t kNumDoubles = 256 * 1024;
    size_t capacity = 0;
    for (auto i = 0; i < kNumDoubles; i++) {
      data.push_back(i);
      if (data.capacity() != capacity) {
        capacity = data.capacity();
        auto stats = MemoryAllocator::allocateBytesStats();
        ASSERT_EQ(
            capacity * sizeof(double),
            stats.totalSmall + stats.totalInSizeClasses + stats.totalLarge);
      }
    }
    for (auto i = 0; i < kNumDoubles; i++) {
      ASSERT_EQ(i, data[i]);
    }
    auto stats = MemoryAllocator::allocateBytesStats();
    if (useMmap_) {
      ASSERT_EQ(0, stats.totalSmall);
      ASSERT_EQ(0, stats.totalInSizeClasses);
      ASSERT_EQ(2 << 20, stats.totalLarge);
      ASSERT_EQ(512, instance_->numAllocated());
    } else {
      ASSERT_EQ(2 << 20, stats.totalSmall);
      ASSERT_EQ(0, stats.totalInSizeClasses);
      ASSERT_EQ(0, stats.totalLarge);
      ASSERT_EQ(0, instance_->numAllocated());
    };
  }
  ASSERT_EQ(0, instance_->numAllocated());
  ASSERT_TRUE(instance_->checkConsistency());
  {
    StlMemoryAllocator<int64_t> alloc(instance_);
    ASSERT_THROW(alloc.allocate(1ULL << 62), VeloxException);
    auto p = alloc.allocate(1);
    ASSERT_THROW(alloc.deallocate(p, 1ULL << 62), VeloxException);
    alloc.deallocate(p, 1);
  }
}

DEBUG_ONLY_TEST_P(
    MemoryAllocatorTest,
    nonContiguousScopedMemoryAllocatorAllocationFailure) {
  if (!memoryUsageTracker_) {
    GTEST_SKIP();
  }
  ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);

  const std::string testValueStr = useMmap_
      ? "facebook::velox::memory::MmapAllocator::allocateNonContiguous"
      : "facebook::velox::memory::MemoryAllocatorImpl::allocateNonContiguous";
  std::atomic<bool> testingInjectFailureOnce{true};
  SCOPED_TESTVALUE_SET(
      testValueStr, std::function<void(bool*)>([&](bool* testFlag) {
        if (!testingInjectFailureOnce.exchange(false)) {
          return;
        }
        if (useMmap_) {
          *testFlag = false;
        } else {
          *testFlag = true;
        }
      }));

  constexpr MachinePageCount kAllocSize = 8;
  std::unique_ptr<MemoryAllocator::Allocation> allocation(
      new MemoryAllocator::Allocation());
  ASSERT_FALSE(instance_->allocateNonContiguous(kAllocSize, *allocation));
  ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);
  ASSERT_TRUE(instance_->allocateNonContiguous(kAllocSize, *allocation));
  ASSERT_GT(memoryUsageTracker_->getCurrentUserBytes(), 0);
  instance_->freeNonContiguous(*allocation);
  ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);
}

TEST_P(MemoryAllocatorTest, contiguousScopedMemoryAllocatorAllocationFailure) {
  if (!useMmap_ || !hasMemoryTracker_) {
    // This test doesn't apply for MemoryAllocatorImpl which doesn't have memory
    // allocation failure rollback code path.
    GTEST_SKIP();
  }
  std::vector<MmapAllocator::Failure> failureTypes(
      {MmapAllocator::Failure::kMadvise, MmapAllocator::Failure::kMmap});
  for (const auto& failure : failureTypes) {
    mmapAllocator_->testingInjectFailure(failure);
    ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);

    constexpr MachinePageCount kAllocSize = 8;
    std::unique_ptr<MemoryAllocator::ContiguousAllocation> allocation(
        new MemoryAllocator::ContiguousAllocation());
    ASSERT_FALSE(
        instance_->allocateContiguous(kAllocSize, nullptr, *allocation));
    ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);
    mmapAllocator_->testingInjectFailure(MmapAllocator::Failure::kNone);
    ASSERT_TRUE(
        instance_->allocateContiguous(kAllocSize, nullptr, *allocation));
    ASSERT_GT(memoryUsageTracker_->getCurrentUserBytes(), 0);
    instance_->freeContiguous(*allocation);
    ASSERT_EQ(memoryUsageTracker_->getCurrentUserBytes(), 0);
  }
}

TEST_P(MemoryAllocatorTest, allocation) {
  const MachinePageCount kNumPages = 133;
  const MachinePageCount kMinClassSize = 20;
  auto allocation = std::make_unique<MemoryAllocator::Allocation>();
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
    MemoryAllocator::Allocation movedAllocation = std::move(*allocation);
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
    MemoryAllocator::Allocation movedAllocation = std::move(*allocation);
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
  auto allocation = std::make_unique<MemoryAllocator::ContiguousAllocation>();
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
    MemoryAllocator::ContiguousAllocation movedAllocation =
        std::move(*allocation);
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
  allocation->set(fakePtr, kNumPages * MemoryAllocator::kPageSize);
  ASSERT_EQ(allocation->numPages(), kNumPages);
  ASSERT_EQ(allocation->pool(), nullptr);
  ASSERT_TRUE(!allocation->empty());
  allocation->setPool(pool_.get());
  ASSERT_EQ(allocation->pool(), pool_.get());
  {
    MemoryAllocator::ContiguousAllocation movedAllocation =
        std::move(*allocation);
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
    testing::ValuesIn(MemoryAllocatorTest::getTestParams()));

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
    ASSERT_EQ(managedArenas->arenas().size(), 1);
    void* alloc1 = managedArenas->allocate(kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 1);
    void* alloc2 = managedArenas->allocate(kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 2);

    managedArenas->free(alloc2, kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 2);
    managedArenas->free(alloc1, kArenaCapacityBytes);
    ASSERT_EQ(managedArenas->arenas().size(), 1);
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
    ASSERT_EQ(managedArenas->arenas().size(), 1);

    // Free every other allocations so that the single MmapArena is fragmented
    // that it can no longer handle allocations of size larger than kAllocSize
    for (auto address : evenAllocAddresses) {
      managedArenas->free(reinterpret_cast<void*>(address), kAllocSize);
    }

    managedArenas->allocate(kAllocSize * 2);
    ASSERT_EQ(managedArenas->arenas().size(), 2);
  }
}
} // namespace facebook::velox::memory
