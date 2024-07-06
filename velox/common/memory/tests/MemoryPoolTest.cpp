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

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_memory_pool_debug_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);

using namespace ::testing;
using namespace facebook::velox::cache;
using namespace facebook::velox::common::testutil;

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;
constexpr int64_t GB = 1024L * MB;

namespace facebook {
namespace velox {
namespace memory {

struct TestParam {
  bool useMmap;
  bool useCache;
  bool threadSafe;

  TestParam(bool _useMmap, bool _useCache, bool _threadSafe)
      : useMmap(_useMmap), useCache(_useCache), threadSafe(_threadSafe) {}

  std::string toString() const {
    return fmt::format(
        "useMmap{} useCache{} threadSafe{}", useMmap, useCache, threadSafe);
  }
};

class MemoryPoolTest : public testing::TestWithParam<TestParam> {
 public:
  static const std::vector<TestParam> getTestParams() {
    std::vector<TestParam> params;
    params.push_back({true, true, false});
    params.push_back({true, false, false});
    params.push_back({false, true, false});
    params.push_back({false, false, false});
    params.push_back({true, true, true});
    params.push_back({true, false, true});
    params.push_back({false, true, true});
    params.push_back({false, false, true});
    return params;
  }

 protected:
  static constexpr uint64_t kDefaultCapacity = 8 * GB; // 8GB
  static void SetUpTestCase() {
    SharedArbitrator::registerFactory();
    FLAGS_velox_memory_leak_check_enabled = true;
    TestValue::enable();
  }

  MemoryPoolTest()
      : useMmap_(GetParam().useMmap),
        useCache_(GetParam().useCache),
        isLeafThreadSafe_(GetParam().threadSafe) {}

  void SetUp() override {
    // For duration of the test, make a local MmapAllocator that will not be
    // seen by any other test.
    setupMemory();
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;
  }

  void setupMemory(
      MemoryManagerOptions options = {
          .allocatorCapacity = kDefaultCapacity,
          .arbitratorCapacity = kDefaultCapacity,
          .arbitratorReservedCapacity = 1LL << 30}) {
    options.useMmapAllocator = useMmap_;
    manager_ = std::make_shared<MemoryManager>(options);
    if (useCache_) {
      cache_ = AsyncDataCache::create(manager_->allocator());
    }
  }

  void TearDown() override {
    if (useCache_) {
      cache_->shutdown();
    }
  }

  void reset() {
    TearDown();
    SetUp();
  }

  std::shared_ptr<MemoryManager> getMemoryManager() {
    return manager_;
  }

  void abortPool(MemoryPool* pool) {
    try {
      VELOX_FAIL("Manual MemoryPool Abortion");
    } catch (const VeloxException&) {
      pool->abort(std::current_exception());
    }
  }

  const int32_t maxMallocBytes_ = 3072;
  const bool useMmap_;
  const bool useCache_;
  const bool isLeafThreadSafe_;
  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<MemoryManager> manager_;
  std::shared_ptr<AsyncDataCache> cache_;
  MemoryReclaimer::Stats stats_;
};

TEST_P(MemoryPoolTest, ctor) {
  constexpr uint16_t kAlignment = 64;
  setupMemory({.alignment = 64, .allocatorCapacity = kDefaultCapacity});
  MemoryManager& manager = *getMemoryManager();
  const int64_t capacity = 4 * GB;
  auto root = manager.addRootPool("Ctor", 4 * GB);
  ASSERT_EQ(root->kind(), MemoryPool::Kind::kAggregate);
  ASSERT_EQ(root->usedBytes(), 0);
  ASSERT_EQ(root->reservedBytes(), 0);
  ASSERT_EQ(root->parent(), nullptr);
  ASSERT_EQ(root->root(), root.get());
  ASSERT_EQ(root->capacity(), capacity);

  {
    auto fakeRoot = std::make_shared<MemoryPoolImpl>(
        &manager,
        "fake_root",
        MemoryPool::Kind::kAggregate,
        nullptr,
        nullptr,
        nullptr,
        nullptr);
    // We can't construct an aggregate memory pool with non-thread safe.
    ASSERT_ANY_THROW(std::make_shared<MemoryPoolImpl>(
        &manager,
        "fake_root",
        MemoryPool::Kind::kAggregate,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        MemoryPool::Options{.threadSafe = false}));
    ASSERT_EQ("fake_root", fakeRoot->name());
    ASSERT_EQ(
        static_cast<MemoryPoolImpl*>(root.get())->testingAllocator(),
        fakeRoot->testingAllocator());
    ASSERT_EQ(0, fakeRoot->usedBytes());
    ASSERT_EQ(fakeRoot->parent(), nullptr);
  }
  {
    auto child = root->addLeafChild("child", isLeafThreadSafe_);
    ASSERT_EQ(child->parent(), root.get());
    ASSERT_EQ(child->root(), root.get());
    ASSERT_EQ(child->capacity(), capacity);
    auto& favoriteChild = dynamic_cast<MemoryPoolImpl&>(*child);
    ASSERT_EQ("child", favoriteChild.name());
    ASSERT_EQ(
        static_cast<MemoryPoolImpl*>(root.get())->testingAllocator(),
        favoriteChild.testingAllocator());
    ASSERT_EQ(favoriteChild.usedBytes(), 0);
  }
  {
    auto aggregateChild = root->addAggregateChild("aggregateChild");
    ASSERT_EQ(aggregateChild->parent(), root.get());
    ASSERT_EQ(aggregateChild->root(), root.get());
    ASSERT_EQ(aggregateChild->capacity(), capacity);
    auto grandChild = aggregateChild->addLeafChild("child", isLeafThreadSafe_);
    ASSERT_EQ(grandChild->parent(), aggregateChild.get());
    ASSERT_EQ(grandChild->root(), root.get());
    ASSERT_EQ(grandChild->capacity(), capacity);
  }
  // Check we can't create a memory pool with zero max capacity.
  VELOX_ASSERT_THROW(
      manager.addRootPool("rootWithZeroMaxCapacity", 0),
      "Memory pool rootWithZeroMaxCapacity max capacity can't be zero");
}

TEST_P(MemoryPoolTest, addChild) {
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool("root");
  ASSERT_EQ(root->parent(), nullptr);

  ASSERT_EQ(0, root->getChildCount());
  auto childOne = root->addLeafChild("child_one", isLeafThreadSafe_);
  auto childTwo = root->addAggregateChild("child_two");

  std::vector<MemoryPool*> nodes{};
  ASSERT_EQ(2, root->getChildCount());
  root->visitChildren([&nodes](MemoryPool* child) {
    nodes.emplace_back(child);
    return true;
  });
  ASSERT_THAT(
      nodes, UnorderedElementsAreArray({childOne.get(), childTwo.get()}));

  // Child pool name collision.
  ASSERT_THROW(root->addAggregateChild("child_one"), VeloxRuntimeError);
  ASSERT_EQ(root->getChildCount(), 2);

  constexpr int64_t kChunkSize{128};
  void* buff = childOne->allocate(kChunkSize);
  // Add child when 'reservedBytes != 0', in which case 'usedBytes()' will call
  // 'visitChildren()'.
  VELOX_ASSERT_THROW(root->addAggregateChild("child_one"), "");
  childOne->free(buff, kChunkSize);
  ASSERT_EQ(root->getChildCount(), 2);

  childOne.reset();
  ASSERT_EQ(root->getChildCount(), 1);
  childOne = root->addLeafChild("child_one", isLeafThreadSafe_);
  ASSERT_EQ(root->getChildCount(), 2);
  ASSERT_EQ(root->treeMemoryUsage(), "root usage 0B reserved 0B peak 1.00MB\n");
  ASSERT_EQ(
      root->treeMemoryUsage(true), "root usage 0B reserved 0B peak 1.00MB\n");
  const std::string treeUsageWithEmptyPool = root->treeMemoryUsage(false);
  ASSERT_THAT(
      treeUsageWithEmptyPool,
      testing::HasSubstr("root usage 0B reserved 0B peak 1.00MB\n"));
  ASSERT_THAT(
      treeUsageWithEmptyPool,
      testing::HasSubstr("child_one usage 0B reserved 0B peak 0B\n"));
  ASSERT_THAT(
      treeUsageWithEmptyPool,
      testing::HasSubstr("child_two usage 0B reserved 0B peak 0B\n"));
}

TEST_P(MemoryPoolTest, dropChild) {
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool("root");
  ASSERT_EQ(root->parent(), nullptr);

  ASSERT_EQ(root->getChildCount(), 0);
  auto childOne = root->addLeafChild("child_one", isLeafThreadSafe_);
  ASSERT_EQ(childOne->parent(), root.get());
  auto childTwo = root->addAggregateChild("child_two");
  ASSERT_EQ(childTwo->parent(), root.get());
  ASSERT_EQ(root->getChildCount(), 2);

  childOne.reset();
  ASSERT_EQ(root->getChildCount(), 1);

  // Remove invalid address.
  childTwo.reset();
  ASSERT_EQ(root->getChildCount(), 0);

  // Check parent pool is alive until all the children has been destroyed.
  auto child = root->addAggregateChild("child");
  ASSERT_EQ(child->parent(), root.get());
  auto* rawChild = child.get();
  auto grandChild1 = child->addLeafChild("grandChild1", isLeafThreadSafe_);
  ASSERT_EQ(grandChild1->parent(), child.get());
  ASSERT_THROW(child->addAggregateChild("grandChild1"), VeloxRuntimeError);
  auto grandChild2 = child->addLeafChild("grandChild2", isLeafThreadSafe_);
  ASSERT_EQ(grandChild2->parent(), child.get());
  ASSERT_EQ(1, root->getChildCount());
  ASSERT_EQ(2, child->getChildCount());
  ASSERT_EQ(0, grandChild1->getChildCount());
  ASSERT_EQ(0, grandChild2->getChildCount());
  child.reset();
  ASSERT_EQ(1, root->getChildCount());
  ASSERT_EQ(2, rawChild->getChildCount());
  grandChild1.reset();
  ASSERT_EQ(1, root->getChildCount());
  ASSERT_EQ(1, rawChild->getChildCount());
  grandChild2.reset();
  ASSERT_EQ(0, root->getChildCount());
}

MachinePageCount numPagesNeeded(
    const MmapAllocator* mmapAllocator,
    MachinePageCount numPages) {
  auto& sizeClasses = mmapAllocator->sizeClasses();
  if (numPages > sizeClasses.back()) {
    return numPages;
  }
  for (auto& sizeClass : sizeClasses) {
    if (sizeClass >= numPages) {
      return sizeClass;
    }
  }
  VELOX_UNREACHABLE();
}

void testMmapMemoryAllocation(
    int64_t capacity,
    MachinePageCount allocPages,
    size_t allocCount,
    bool threadSafe) {
  MemoryManager manager{
      {.allocatorCapacity = capacity, .useMmapAllocator = true}};
  const auto kPageSize = 4 * KB;

  auto root = manager.addRootPool();
  auto child = root->addLeafChild("elastic_quota", threadSafe);

  std::vector<void*> allocations;
  uint64_t totalPageAllocated = 0;
  uint64_t totalPageMapped = 0;
  auto* mmapAllocator = static_cast<MmapAllocator*>(manager.allocator());
  const auto pageIncrement = numPagesNeeded(mmapAllocator, allocPages);
  const auto isSizeClassAlloc =
      allocPages <= mmapAllocator->sizeClasses().back();
  const auto byteSize = allocPages * kPageSize;
  const std::string buffer(byteSize, 'x');
  for (size_t i = 0; i < allocCount; i++) {
    void* allocResult = nullptr;
    ASSERT_NO_THROW(allocResult = child->allocate(byteSize));
    ASSERT_TRUE(allocResult != nullptr);

    // Write data to let mapped address to be backed by physical memory
    memcpy(allocResult, buffer.data(), byteSize);
    allocations.emplace_back(allocResult);
    totalPageAllocated += pageIncrement;
    totalPageMapped += pageIncrement;
    ASSERT_EQ(mmapAllocator->numAllocated(), totalPageAllocated);
    ASSERT_EQ(
        isSizeClassAlloc ? mmapAllocator->numMapped()
                         : mmapAllocator->numExternalMapped(),
        totalPageMapped);
  }
  for (size_t i = 0; i < allocCount; i++) {
    ASSERT_NO_THROW(child->free(allocations[i], byteSize));
    totalPageAllocated -= pageIncrement;
    ASSERT_EQ(mmapAllocator->numAllocated(), totalPageAllocated);
    if (isSizeClassAlloc) {
      ASSERT_EQ(mmapAllocator->numMapped(), totalPageMapped);
    } else {
      totalPageMapped -= pageIncrement;
      ASSERT_EQ(mmapAllocator->numExternalMapped(), totalPageMapped);
    }
  }
}

TEST_P(MemoryPoolTest, smallMmapMemoryAllocation) {
  testMmapMemoryAllocation(8 * GB, 6, 100, isLeafThreadSafe_);
}

TEST_P(MemoryPoolTest, bigMmapMemoryAllocation) {
  testMmapMemoryAllocation(8 * GB, 256 + 56, 20, isLeafThreadSafe_);
}

// Mainly tests how it updates the memory usage in memory pool.
TEST_P(MemoryPoolTest, allocTest) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();

  auto child = root->addLeafChild("allocTest", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};

  void* oneChunk = child->allocate(kChunkSize);
  ASSERT_EQ(reinterpret_cast<uint64_t>(oneChunk) % child->alignment(), 0);
  ASSERT_EQ(kChunkSize, child->usedBytes());
  ASSERT_EQ(kChunkSize, child->stats().peakBytes);

  void* threeChunks = child->allocate(3 * kChunkSize);
  ASSERT_EQ(4 * kChunkSize, child->usedBytes());
  ASSERT_EQ(4 * kChunkSize, child->stats().peakBytes);

  child->free(threeChunks, 3 * kChunkSize);
  ASSERT_EQ(kChunkSize, child->usedBytes());
  ASSERT_EQ(4 * kChunkSize, child->stats().peakBytes);

  child->free(oneChunk, kChunkSize);
  ASSERT_EQ(0, child->usedBytes());
  ASSERT_EQ(4 * kChunkSize, child->stats().peakBytes);
}

TEST_P(MemoryPoolTest, usedBytes) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();

  auto child1 = root->addLeafChild("usedBytes1", isLeafThreadSafe_);
  auto child2 = root->addLeafChild("usedBytes2", isLeafThreadSafe_);

  const int64_t kChunkSize{128};

  void* buf1 = child1->allocate(kChunkSize);
  void* buf2 = child2->allocate(kChunkSize);
  ASSERT_EQ(child1->reservedBytes(), 1 << 20);
  ASSERT_EQ(child1->usedBytes(), kChunkSize);
  ASSERT_EQ(child2->reservedBytes(), 1 << 20);
  ASSERT_EQ(child2->usedBytes(), kChunkSize);

  ASSERT_EQ(root->reservedBytes(), 2 << 20);
  ASSERT_EQ(root->usedBytes(), 2 * kChunkSize);

  child1->free(buf1, kChunkSize);
  ASSERT_EQ(root->reservedBytes(), 1 << 20);
  ASSERT_EQ(root->usedBytes(), kChunkSize);
  child2->free(buf2, kChunkSize);
  ASSERT_EQ(root->reservedBytes(), 0);
  ASSERT_EQ(root->usedBytes(), 0);
}

TEST_P(MemoryPoolTest, DISABLED_memoryLeakCheck) {
  gflags::FlagSaver flagSaver;
  testing::FLAGS_gtest_death_test_style = "fast";
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();

  auto child = root->addLeafChild("elastic_quota", isLeafThreadSafe_);
  const int64_t kChunkSize{32L * MB};
  void* oneChunk = child->allocate(kChunkSize);
  FLAGS_velox_memory_leak_check_enabled = true;
  ASSERT_DEATH(child.reset(), "");
  child->free(oneChunk, kChunkSize);
}

TEST_P(MemoryPoolTest, growFailures) {
  auto manager = getMemoryManager();
  // Grow beyond limit.
  {
    auto poolWithoutLimit = manager->addRootPool("poolWithoutLimit");
    ASSERT_EQ(poolWithoutLimit->capacity(), kMaxMemory);
    ASSERT_EQ(poolWithoutLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithoutLimit->reservedBytes(), 0);
    VELOX_ASSERT_THROW(
        poolWithoutLimit->grow(1, 0), "Can't grow with unlimited capacity");
    ASSERT_EQ(poolWithoutLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithoutLimit->reservedBytes(), 0);
    VELOX_ASSERT_THROW(
        poolWithoutLimit->grow(1, 1'000), "Can't grow with unlimited capacity");
    ASSERT_EQ(poolWithoutLimit->usedBytes(), 0);
  }
  {
    const int64_t capacity = 4 * GB;
    auto poolWithLimit = manager->addRootPool("poolWithLimit", capacity);
    ASSERT_EQ(poolWithLimit->capacity(), capacity);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->shrink(poolWithLimit->reservedBytes()), capacity);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_TRUE(poolWithLimit->grow(capacity / 2, 0));
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_FALSE(poolWithLimit->grow(capacity, 0));
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->capacity(), capacity / 2);
    ASSERT_FALSE(poolWithLimit->grow(capacity, 1'000));
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
  }

  // Insufficient capacity for new reservation.
  {
    const int64_t capacity = 4 * GB;
    auto poolWithLimit = manager->addRootPool("poolWithLimit", capacity);
    ASSERT_EQ(poolWithLimit->capacity(), capacity);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->shrink(poolWithLimit->capacity()), capacity);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->capacity(), 0);

    ASSERT_FALSE(poolWithLimit->grow(capacity / 2, capacity));
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->capacity(), 0);

    ASSERT_FALSE(poolWithLimit->grow(0, capacity));
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
    ASSERT_EQ(poolWithLimit->capacity(), 0);
    ASSERT_EQ(poolWithLimit->reservedBytes(), 0);
    ASSERT_EQ(poolWithLimit->usedBytes(), 0);
  }
}

TEST_P(MemoryPoolTest, grow) {
  auto manager = getMemoryManager();
  const int64_t capacity = 4 * GB;
  auto root = manager->addRootPool("grow", capacity);
  root->shrink(capacity / 2);
  ASSERT_EQ(root->capacity(), capacity / 2);

  auto leaf = root->addLeafChild("leafPool");
  void* buf = leaf->allocate(1 * MB);
  ASSERT_EQ(root->capacity(), capacity / 2);
  ASSERT_EQ(root->reservedBytes(), 1 * MB);

  ASSERT_TRUE(root->grow(0, 2 * MB));
  ASSERT_EQ(root->reservedBytes(), 3 * MB);
  ASSERT_EQ(root->capacity(), capacity / 2);

  ASSERT_TRUE(root->grow(0, 4 * MB));
  ASSERT_EQ(root->reservedBytes(), 7 * MB);
  ASSERT_EQ(root->capacity(), capacity / 2);

  ASSERT_TRUE(root->grow(1 * MB, 2 * MB));
  ASSERT_EQ(root->reservedBytes(), 9 * MB);
  ASSERT_EQ(root->capacity(), capacity / 2 + 1 * MB);

  ASSERT_TRUE(root->grow(6 * MB, 4 * MB));
  ASSERT_EQ(root->reservedBytes(), 13 * MB);
  ASSERT_EQ(root->capacity(), capacity / 2 + 7 * MB);

  static_cast<MemoryPoolImpl*>(root.get())->testingSetReservation(1 * MB);
  leaf->free(buf, 1 * MB);
}

TEST_P(MemoryPoolTest, releasableMemory) {
  struct TestParam {
    int64_t usedBytes;
    int64_t reservedBytes;
  };
  std::vector<TestParam> testParams{
      {2345, 98760},
      {1, 1024},
      {4096, 4096},
      {1 * MB, 16 * MB},
      {6 * MB, 7 * MB},
      {123 * MB, 200 * MB},
      {100 * MB, 50 * MB}};
  auto root = getMemoryManager()->addRootPool("releasableMemory", 4 * GB);
  for (auto i = 0; i < testParams.size() - 1; i++) {
    auto leaf0 = root->addLeafChild("leafPool-0");
    leaf0->maybeReserve(testParams[i].reservedBytes);
    void* buffer0 = leaf0->allocate(testParams[i].usedBytes);
    const auto reservedBytes0 = leaf0->reservedBytes();
    const auto releasableBytes0 = leaf0->releasableReservation();

    auto leaf1 = root->addLeafChild("leafPool-1");
    leaf1->maybeReserve(testParams[i + 1].reservedBytes);
    void* buffer1 = leaf1->allocate(testParams[i + 1].usedBytes);
    const auto reservedBytes1 = leaf1->reservedBytes();
    const auto releasableBytes1 = leaf1->releasableReservation();

    const auto releasableBytesRoot = root->releasableReservation();
    const auto reservedBytesRoot = root->reservedBytes();
    ASSERT_EQ(releasableBytesRoot, releasableBytes0 + releasableBytes1);

    leaf0->release();
    ASSERT_EQ(reservedBytes0 - leaf0->reservedBytes(), releasableBytes0);
    ASSERT_EQ(reservedBytesRoot - root->reservedBytes(), releasableBytes0);
    leaf1->release();
    ASSERT_EQ(reservedBytes1 - leaf1->reservedBytes(), releasableBytes1);
    ASSERT_EQ(reservedBytesRoot - root->reservedBytes(), releasableBytesRoot);

    leaf0->free(buffer0, testParams[i].usedBytes);
    leaf1->free(buffer1, testParams[i + 1].usedBytes);
  }
}

TEST_P(MemoryPoolTest, ReallocTestSameSize) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();

  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};

  // Realloc the same size.
  void* oneChunk = pool->allocate(kChunkSize);
  ASSERT_EQ(kChunkSize, pool->usedBytes());
  ASSERT_EQ(kChunkSize, pool->stats().peakBytes);

  void* anotherChunk = pool->reallocate(oneChunk, kChunkSize, kChunkSize);
  ASSERT_EQ(kChunkSize, pool->usedBytes());
  ASSERT_EQ(2 * kChunkSize, pool->stats().peakBytes);

  pool->free(anotherChunk, kChunkSize);
  ASSERT_EQ(0, pool->usedBytes());
  ASSERT_EQ(2 * kChunkSize, pool->stats().peakBytes);
}

TEST_P(MemoryPoolTest, ReallocTestHigher) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();

  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};
  // Realloc higher.
  void* oneChunk = pool->allocate(kChunkSize);
  EXPECT_EQ(kChunkSize, pool->usedBytes());
  EXPECT_EQ(kChunkSize, pool->stats().peakBytes);

  void* threeChunks = pool->reallocate(oneChunk, kChunkSize, 3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool->usedBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);

  pool->free(threeChunks, 3 * kChunkSize);
  EXPECT_EQ(0, pool->usedBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);
}

TEST_P(MemoryPoolTest, ReallocTestLower) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();
  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};
  // Realloc lower.
  void* threeChunks = pool->allocate(3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool->usedBytes());
  EXPECT_EQ(3 * kChunkSize, pool->stats().peakBytes);

  void* oneChunk = pool->reallocate(threeChunks, 3 * kChunkSize, kChunkSize);
  EXPECT_EQ(kChunkSize, pool->usedBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);

  pool->free(oneChunk, kChunkSize);
  EXPECT_EQ(0, pool->usedBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);
}

TEST_P(MemoryPoolTest, allocateZeroFilled) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();

  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const std::vector<int64_t> numEntriesVector({1, 2, 10});
  const std::vector<int64_t> sizeEachVector({1, 117, 2467});
  std::vector<void*> allocationPtrs;
  std::vector<int64_t> allocationSizes;
  for (const auto& numEntries : numEntriesVector) {
    for (const auto& sizeEach : sizeEachVector) {
      SCOPED_TRACE(
          fmt::format("numEntries{}, sizeEach{}", numEntries, sizeEach));
      void* ptr = pool->allocateZeroFilled(numEntries, sizeEach);
      uint8_t* bytes = reinterpret_cast<uint8_t*>(ptr);
      for (int32_t i = 0; i < numEntries * sizeEach; ++i) {
        ASSERT_EQ(bytes[i], 0);
      }
      allocationPtrs.push_back(ptr);
      allocationSizes.push_back(numEntries * sizeEach);
    }
  }
  for (int32_t i = 0; i < allocationPtrs.size(); ++i) {
    pool->free(allocationPtrs[i], allocationSizes[i]);
  }
  ASSERT_EQ(0, pool->usedBytes());
}

TEST_P(MemoryPoolTest, alignmentCheck) {
  std::vector<uint16_t> alignments = {
      0,
      MemoryAllocator::kMinAlignment,
      MemoryAllocator::kMinAlignment * 2,
      MemoryAllocator::kMaxAlignment};
  for (const auto& alignment : alignments) {
    SCOPED_TRACE(fmt::format("alignment:{}", alignment));
    setupMemory(
        {.alignment = alignment, .allocatorCapacity = kDefaultCapacity});
    auto manager = getMemoryManager();
    auto pool = manager->addLeafPool("alignmentCheck");
    ASSERT_EQ(
        pool->alignment(),
        alignment == 0 ? MemoryAllocator::kMinAlignment : alignment);
    const int32_t kTestIterations = 10;
    for (int32_t i = 0; i < 10; ++i) {
      const int64_t bytesToAlloc = 1 + folly::Random::rand32() % (1 * MB);
      void* ptr = pool->allocate(bytesToAlloc);
      if (alignment != 0) {
        ASSERT_EQ(reinterpret_cast<uint64_t>(ptr) % alignment, 0);
      }
      pool->free(ptr, bytesToAlloc);
    }
    ASSERT_EQ(0, pool->usedBytes());
  }
}

TEST_P(MemoryPoolTest, memoryCapExceptions) {
  const uint64_t kMaxCap = 128L * MB;
  setupMemory(
      {.allocatorCapacity = kMaxCap,
       .arbitratorCapacity = kMaxCap,
       .arbitratorReservedCapacity = kMaxCap / 2});
  auto manager = getMemoryManager();
  // Capping memory pool.
  {
    auto root = manager->addRootPool("MemoryCapExceptions", kMaxCap);
    auto pool = root->addLeafChild("static_quota", isLeafThreadSafe_);
    {
      ASSERT_EQ(0, pool->usedBytes());
      try {
        pool->allocate(129L * MB);
      } catch (const velox::VeloxRuntimeError& ex) {
        ASSERT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
        ASSERT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
        ASSERT_TRUE(ex.isRetriable());
        ASSERT_EQ(
            "Exceeded memory pool cap of 128.00MB with max 128.00MB when "
            "requesting 136.00MB, memory manager cap is 128.00MB, requestor "
            "'static_quota' with current usage 0B\nMemoryCapExceptions usage "
            "0B reserved 0B peak 0B\n",
            ex.message());
      }
    }
  }

  // Capping allocator.
  {
    auto root =
        manager->addRootPool("MemoryCapExceptions", 2 * manager->capacity());
    auto pool = root->addLeafChild("static_quota", isLeafThreadSafe_);
    {
      ASSERT_EQ(0, pool->usedBytes());
      try {
        pool->allocate(manager->capacity() + 1);
      } catch (const velox::VeloxRuntimeError& ex) {
        ASSERT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
        ASSERT_EQ(error_code::kMemAllocError.c_str(), ex.errorCode());
        ASSERT_TRUE(ex.isRetriable());
        if (useMmap_) {
          if (useCache_) {
            ASSERT_EQ(
                fmt::format(
                    "allocate failed with 128.00MB from Memory Pool["
                    "static_quota LEAF root[MemoryCapExceptions] "
                    "parent[MemoryCapExceptions] MMAP track-usage {}]<max "
                    "capacity 256.00MB capacity 256.00MB used 0B available 0B "
                    "reservation [used 0B, reserved 0B, min 0B] counters [allocs "
                    "1, frees 0, reserves 0, releases 0, collisions 0])> Failed to"
                    " evict from cache state: AsyncDataCache:\nCache size: 0B "
                    "tinySize: 0B large size: 0B\nCache entries: 0 read pins: "
                    "0 write pins: 0 pinned shared: 0B pinned exclusive: 0B\n "
                    "num write wait: 0 empty entries: 0\nCache access miss: 0 "
                    "hit: 0 hit bytes: 0B eviction: 0 savable eviction: 0 eviction checks: 0 "
                    "aged out: 0 stales: 0\nPrefetch entries: 0 bytes: 0B\nAlloc Megaclocks 0\n"
                    "Allocated pages: 0 cached pages: 0\n",
                    isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"),
                ex.message());
          } else {
            ASSERT_EQ(
                fmt::format(
                    "allocate failed with 128.00MB from Memory Pool["
                    "static_quota LEAF root[MemoryCapExceptions] "
                    "parent[MemoryCapExceptions] MMAP track-usage {}]<max "
                    "capacity 256.00MB capacity 256.00MB used 0B available 0B "
                    "reservation [used 0B, reserved 0B, min 0B] counters [allocs "
                    "1, frees 0, reserves 0, releases 0, collisions 0])> "
                    "Exceeded memory allocator limit when allocating 32769 "
                    "new pages for total allocation of 32769 pages, the memory"
                    " allocator capacity is 32768 pages",
                    isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"),
                ex.message());
          }
        } else {
          if (useCache_) {
            ASSERT_EQ(
                fmt::format(
                    "allocate failed with 128.00MB from Memory Pool"
                    "[static_quota LEAF root[MemoryCapExceptions] "
                    "parent[MemoryCapExceptions] MALLOC track-usage {}]"
                    "<max capacity 256.00MB capacity 256.00MB used 0B available "
                    "0B reservation [used 0B, reserved 0B, min 0B] counters "
                    "[allocs 1, frees 0, reserves 0, releases 0, collisions 0])>"
                    " Failed to evict from cache state: AsyncDataCache:\nCache "
                    "size: 0B tinySize: 0B large size: 0B\nCache entries: 0 "
                    "read pins: 0 write pins: 0 pinned shared: 0B pinned "
                    "exclusive: 0B\n num write wait: 0 empty entries: 0\nCache "
                    "access miss: 0 hit: 0 hit bytes: 0B eviction: 0 savable eviction: 0 eviction "
                    "checks: 0 aged out: 0 stales: 0\nPrefetch entries: 0 bytes: 0B\nAlloc Megaclocks"
                    " 0\nAllocated pages: 0 cached pages: 0\n",
                    isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"),
                ex.message());
          } else {
            ASSERT_EQ(
                fmt::format(
                    "allocate failed with 128.00MB from Memory Pool"
                    "[static_quota LEAF root[MemoryCapExceptions] "
                    "parent[MemoryCapExceptions] MALLOC track-usage {}]"
                    "<max capacity 256.00MB capacity 256.00MB used 0B available "
                    "0B reservation [used 0B, reserved 0B, min 0B] counters "
                    "[allocs 1, frees 0, reserves 0, releases 0, collisions 0])>"
                    " Failed to allocateBytes 128.00MB: Exceeded memory "
                    "allocator limit of 128.00MB",
                    isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"),
                ex.message());
          }
        }
      }
    }
  }
}

TEST(MemoryPoolTest, GetAlignment) {
  {
    MemoryManagerOptions options;
    options.allocatorCapacity = kMaxMemory;
    EXPECT_EQ(
        MemoryAllocator::kMaxAlignment,
        MemoryManager{options}.addRootPool()->alignment());
  }
  {
    MemoryManagerOptions options;
    options.allocatorCapacity = kMaxMemory;
    options.alignment = 64;
    MemoryManager manager{options};
    EXPECT_EQ(64, manager.addRootPool()->alignment());
  }
}

TEST_P(MemoryPoolTest, MemoryManagerGlobalCap) {
  setupMemory(
      {.allocatorCapacity = 32L * MB,
       .arbitratorCapacity = 32L * MB,
       .arbitratorReservedCapacity = 16L * MB});
  auto manager = getMemoryManager();
  const auto kAllocCap = manager->capacity();
  auto root = manager->addRootPool();
  auto pool = root->addAggregateChild("unbounded");
  auto child = pool->addLeafChild("unbounded", isLeafThreadSafe_);
  void* oneChunk = child->allocate(kAllocCap);
  ASSERT_EQ(root->reservedBytes(), kAllocCap);
  EXPECT_THROW(child->allocate(kAllocCap), velox::VeloxRuntimeError);
  ASSERT_EQ(root->reservedBytes(), kAllocCap);
  EXPECT_THROW(
      child->reallocate(oneChunk, kAllocCap, 2 * kAllocCap),
      velox::VeloxRuntimeError);
  ASSERT_EQ(root->reservedBytes(), kAllocCap);
  child->free(oneChunk, kAllocCap);
}

// Tests how child updates itself and its parent's memory usage
// and what it returns for reservedBytes()/getMaxBytes and
// with memoryUsageTracker.
TEST_P(MemoryPoolTest, childUsageTest) {
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool();
  auto pool = root->addAggregateChild("main_pool");

  auto verifyUsage = [](std::vector<std::shared_ptr<MemoryPool>>& tree,
                        std::vector<int> usedBytes,
                        std::vector<int> maxBytes) {
    ASSERT_TRUE(
        tree.size() == usedBytes.size() && tree.size() == maxBytes.size());
    for (unsigned i = 0, e = tree.size(); i != e; ++i) {
      EXPECT_EQ(tree[i]->usedBytes(), usedBytes[i]) << i;
      EXPECT_EQ(tree[i]->stats().peakBytes, maxBytes[i]) << i;
    }
  };

  // Create the following MemoryPool tree.
  //              p0
  //              |
  //      +-------+--------+
  //      |                |
  //     p1                p2
  //      |                |
  //  +------+         +---+---+
  // p3      p4       p5       p6
  //
  std::vector<std::shared_ptr<MemoryPool>> tree;
  tree.push_back(pool->addAggregateChild("p0"));

  // first level: p1, p2.
  tree.push_back(tree[0]->addAggregateChild("p1"));
  tree.push_back(tree[0]->addAggregateChild("p2"));

  // second level: p3, p4, p5, p6.
  tree.push_back(tree[1]->addLeafChild("p3", isLeafThreadSafe_));
  tree.push_back(tree[1]->addLeafChild("p4", isLeafThreadSafe_));
  tree.push_back(tree[2]->addLeafChild("p5", isLeafThreadSafe_));
  tree.push_back(tree[2]->addLeafChild("p6", isLeafThreadSafe_));

  verifyUsage(tree, {0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0});

  void* p3Chunk0 = tree[3]->allocate(16);
  verifyUsage(
      tree, {64, 64, 0, 64, 0, 0, 0}, {1048576, 1048576, 0, 64, 0, 0, 0});
  void* p5Chunk0 = tree[5]->allocate(64);
  verifyUsage(
      tree,
      {128, 64, 64, 64, 0, 64, 0},
      {2097152, 1048576, 1048576, 64, 0, 64, 0});

  tree[3]->free(p3Chunk0, 16);
  verifyUsage(
      tree,
      {64, 0, 64, 0, 0, 64, 0},
      {2097152, 1048576, 1048576, 64, 0, 64, 0});

  tree[5]->free(p5Chunk0, 64);
  verifyUsage(
      tree, {0, 0, 0, 0, 0, 0, 0}, {2097152, 1048576, 1048576, 64, 0, 64, 0});

  // Release all memory pool->
  tree.clear();

  std::vector<int64_t> expectedReservedBytes({0, 0, 0, 0, 0, 0, 0});
  std::vector<int64_t> expectedMaxBytes({128, 64, 64, 64, 0, 64, 0});

  // Verify the stats still holds the correct stats.
  for (unsigned i = 0, e = tree.size(); i != e; ++i) {
    ASSERT_GE(tree[i]->reservedBytes(), expectedReservedBytes[i]);
    ASSERT_GE(tree[i]->stats().peakBytes, expectedMaxBytes[i]);
  }
}

TEST_P(MemoryPoolTest, getPreferredSize) {
  MemoryManager& manager = *getMemoryManager();
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());

  // size < 8
  EXPECT_EQ(8, pool.preferredSize(1));
  EXPECT_EQ(8, pool.preferredSize(2));
  EXPECT_EQ(8, pool.preferredSize(4));
  EXPECT_EQ(8, pool.preferredSize(7));
  // size >=8, pick 2^k or 1.5 * 2^k
  EXPECT_EQ(8, pool.preferredSize(8));
  EXPECT_EQ(24, pool.preferredSize(24));
  EXPECT_EQ(32, pool.preferredSize(25));
  EXPECT_EQ(1024 * 1536, pool.preferredSize(1024 * 1024 + 1));
  EXPECT_EQ(1024 * 1024 * 2, pool.preferredSize(1024 * 1536 + 1));
}

TEST_P(MemoryPoolTest, getPreferredSizeOverflow) {
  MemoryManager& manager = *getMemoryManager();
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());

  EXPECT_EQ(1ULL << 32, pool.preferredSize((1ULL << 32) - 1));
  EXPECT_EQ(1ULL << 63, pool.preferredSize((1ULL << 62) - 1 + (1ULL << 62)));
}

TEST_P(MemoryPoolTest, allocatorOverflow) {
  MemoryManager& manager = *getMemoryManager();
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());
  StlAllocator<int64_t> alloc(pool);
  EXPECT_THROW(alloc.allocate(1ULL << 62), VeloxException);
  EXPECT_THROW(alloc.deallocate(nullptr, 1ULL << 62), VeloxException);
}

TEST_P(MemoryPoolTest, contiguousAllocate) {
  auto manager = getMemoryManager();
  auto pool = manager->addLeafPool("contiguousAllocate");
  const auto largestSizeClass = manager->allocator()->largestSizeClass();
  struct {
    MachinePageCount numAllocPages;
    std::string debugString() const {
      return fmt::format("numAllocPages:{}", numAllocPages);
    }
  } testSettings[] = {
      {largestSizeClass},
      {largestSizeClass + 1},
      {largestSizeClass / 10},
      {1},
      {largestSizeClass * 2},
      {largestSizeClass * 3 + 1}};
  std::vector<ContiguousAllocation> allocations;
  const char c('M');
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ContiguousAllocation allocation;
    ASSERT_TRUE(allocation.empty());
    pool->allocateContiguous(testData.numAllocPages, allocation);
    ASSERT_FALSE(allocation.empty());
    ASSERT_EQ(allocation.pool(), pool.get());
    ASSERT_EQ(allocation.numPages(), testData.numAllocPages);
    ASSERT_EQ(
        allocation.size(),
        testData.numAllocPages * AllocationTraits::kPageSize);
    for (int32_t i = 0; i < allocation.size(); ++i) {
      allocation.data()[i] = c;
    }
    allocations.push_back(std::move(allocation));
  }
  // Verify data.
  for (auto& allocation : allocations) {
    for (int32_t i = 0; i < allocation.size(); ++i) {
      ASSERT_EQ(allocation.data()[i], c);
    }
  }
  allocations.clear();

  // Random tests.
  const int32_t numIterations = 10;
  const MachinePageCount kMaxAllocationPages = 32 << 10; // Total 128MB
  int32_t numAllocatedPages = 0;
  for (int32_t i = 0; i < numIterations; ++i) {
    const MachinePageCount pagesToAllocate =
        1 + folly::Random().rand32() % kMaxAllocationPages;
    ContiguousAllocation allocation;
    if (folly::Random().oneIn(2) && !allocations.empty()) {
      const int32_t freeAllocationIdx =
          folly::Random().rand32() % allocations.size();
      allocation = std::move(allocations[freeAllocationIdx]);
      numAllocatedPages -= allocation.numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.erase(allocations.begin() + freeAllocationIdx);
    }
    const MachinePageCount minSizeClass = folly::Random().oneIn(4)
        ? 0
        : std::min(
              manager->allocator()->largestSizeClass(),
              folly::Random().rand32() % kMaxAllocationPages);
    pool->allocateContiguous(pagesToAllocate, allocation);
    numAllocatedPages += allocation.numPages();
    for (int32_t j = 0; j < allocation.size(); ++j) {
      allocation.data()[j] = c;
    }
    allocations.push_back(std::move(allocation));
    while (numAllocatedPages > kMaxAllocationPages) {
      numAllocatedPages -= allocations.back().numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.pop_back();
    }
  }
  // Verify data.
  for (auto& allocation : allocations) {
    for (int32_t i = 0; i < allocation.size(); ++i) {
      ASSERT_EQ(allocation.data()[i], c);
    }
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateExceedLimit) {
  const auto memCapacity = (int64_t)(AllocationTraits::pageBytes(1 << 10));
  setupMemory(
      {.allocatorCapacity = memCapacity,
       .arbitratorCapacity = memCapacity,
       .arbitratorReservedCapacity = memCapacity / 2});
  auto manager = getMemoryManager();
  const auto kMemoryCapBytes = manager->capacity();
  const auto kMaxNumPages = AllocationTraits::numPages(kMemoryCapBytes);
  auto root =
      manager->addRootPool("contiguousAllocateExceedLimit", kMemoryCapBytes);
  auto pool = root->addLeafChild("child", isLeafThreadSafe_);
  ContiguousAllocation allocation;
  pool->allocateContiguous(kMaxNumPages, allocation);
  ASSERT_THROW(
      pool->allocateContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
  ASSERT_THROW(
      pool->allocateContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
}

TEST_P(MemoryPoolTest, badContiguousAllocation) {
  auto manager = getMemoryManager();
  auto pool = manager->addLeafPool("badContiguousAllocation");
  constexpr MachinePageCount kAllocSize = 8;
  ContiguousAllocation allocation;
  ASSERT_THROW(pool->allocateContiguous(0, allocation), VeloxRuntimeError);
}

TEST_P(MemoryPoolTest, nonContiguousAllocate) {
  auto manager = getMemoryManager();
  auto pool = manager->addLeafPool("nonContiguousAllocate");
  const auto& sizeClasses = manager->allocator()->sizeClasses();
  for (const auto& sizeClass : sizeClasses) {
    SCOPED_TRACE(fmt::format("sizeClass:{}", sizeClass));
    struct {
      MachinePageCount numAllocPages;
      MachinePageCount minSizeClass;
      std::string debugString() const {
        return fmt::format(
            "numAllocPages:{}, minSizeClass:{}", numAllocPages, minSizeClass);
      }
    } testSettings[] = {
        {sizeClass, 0},
        {sizeClass, 1},
        {sizeClass, 10},
        {sizeClass, sizeClass},
        {sizeClass, sizeClass + 1},
        {sizeClass, sizeClass * 2},
        {sizeClass + 10, 0},
        {sizeClass + 10, 1},
        {sizeClass + 10, 10},
        {sizeClass + 10, sizeClass},
        {sizeClass + 10, sizeClass + 1},
        {sizeClass + 10, sizeClass * 2},
        {sizeClass * 2, 0},
        {sizeClass * 2, 1},
        {sizeClass * 2, 10},
        {sizeClass * 2, sizeClass},
        {sizeClass * 2, sizeClass + 1},
        {sizeClass * 2, sizeClass + 10},
        {sizeClass * 2, sizeClass * 2},
        {sizeClass * 2, sizeClass * 2 + 1},
        {sizeClass * 2, sizeClass * 2 * 2}};
    std::vector<Allocation> allocations;
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      Allocation allocation;
      ASSERT_TRUE(allocation.empty());
      pool->allocateNonContiguous(
          testData.numAllocPages,
          allocation,
          std::min(
              testData.minSizeClass, manager->allocator()->largestSizeClass()));
      ASSERT_FALSE(allocation.empty());
      ASSERT_EQ(allocation.pool(), pool.get());
      ASSERT_GT(allocation.numRuns(), 0);
      ASSERT_GE(allocation.numPages(), testData.numAllocPages);
    }
  }

  // Random tests.
  const int32_t numIterations = 100;
  const MachinePageCount kMaxAllocationPages = 32 << 10; // Total 128MB
  int32_t numAllocatedPages = 0;
  std::vector<Allocation> allocations;
  for (int32_t i = 0; i < numIterations; ++i) {
    const MachinePageCount pagesToAllocate =
        1 + folly::Random().rand32() % kMaxAllocationPages;
    Allocation allocation;
    if (folly::Random().oneIn(2) && !allocations.empty()) {
      const int32_t freeAllocationIdx =
          folly::Random().rand32() % allocations.size();
      allocation = std::move(allocations[freeAllocationIdx]);
      numAllocatedPages -= allocation.numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.erase(allocations.begin() + freeAllocationIdx);
    }
    const MachinePageCount minSizeClass = folly::Random().oneIn(4)
        ? 0
        : std::min(
              manager->allocator()->largestSizeClass(),
              folly::Random().rand32() % kMaxAllocationPages);
    pool->allocateNonContiguous(pagesToAllocate, allocation, minSizeClass);
    numAllocatedPages += allocation.numPages();
    allocations.push_back(std::move(allocation));
    while (numAllocatedPages > kMaxAllocationPages) {
      numAllocatedPages -= allocations.back().numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.pop_back();
    }
  }
}

TEST_P(MemoryPoolTest, allocationFailStats) {
  setupMemory(
      {.allocatorCapacity = 16 * KB,
       .allocationSizeThresholdWithReservation = false,
       .arbitratorCapacity = 16 * KB,
       .arbitratorReservedCapacity = 16 * KB,
       .memoryPoolReservedCapacity = 16 * KB});
  auto manager = getMemoryManager();
  auto pool = manager->addLeafPool("allocationFailStats");
  auto allocatorCapacity = manager->capacity();

  EXPECT_THROW(pool->allocate(allocatorCapacity + 1), VeloxException);
  EXPECT_EQ(1, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  auto* buffer = pool->allocate(256);
  EXPECT_EQ(2, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  EXPECT_THROW(
      pool->reallocate(buffer, 256, allocatorCapacity + 1), VeloxException);
  EXPECT_EQ(3, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  EXPECT_THROW(
      pool->allocateZeroFilled(1, allocatorCapacity + 1), VeloxException);
  EXPECT_EQ(4, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  // Free to reset to 0 allocation
  pool->free(buffer, 256);
  EXPECT_EQ(1, pool->stats().numFrees);

  Allocation allocation;
  EXPECT_THROW(
      pool->allocateNonContiguous(
          AllocationTraits::numPages(allocatorCapacity) + 1, allocation, 1),
      VeloxException);
  EXPECT_EQ(5, pool->stats().numAllocs);
  EXPECT_EQ(1, pool->stats().numFrees);

  pool->allocateNonContiguous(2 /* 8KB */, allocation, 1);
  EXPECT_EQ(6, pool->stats().numAllocs);
  EXPECT_EQ(1, pool->stats().numFrees);

  EXPECT_THROW(
      pool->allocateNonContiguous(
          AllocationTraits::numPages(allocatorCapacity) + 1, allocation, 1),
      VeloxException);
  EXPECT_EQ(7, pool->stats().numAllocs);
  EXPECT_EQ(2, pool->stats().numFrees);

  pool->allocateNonContiguous(3 /* 12KB */, allocation, 1);
  EXPECT_EQ(8, pool->stats().numAllocs);
  EXPECT_EQ(2, pool->stats().numFrees);

  // Free to reset to 0 allocation
  pool->freeNonContiguous(allocation);
  EXPECT_EQ(3, pool->stats().numFrees);

  ContiguousAllocation contiguousAllocation;
  EXPECT_THROW(
      pool->allocateContiguous(
          AllocationTraits::numPages(allocatorCapacity) + 1,
          contiguousAllocation),
      VeloxException);
  EXPECT_EQ(9, pool->stats().numAllocs);
  EXPECT_EQ(3, pool->stats().numFrees);

  pool->allocateContiguous(2 /* 8KB */, contiguousAllocation);
  EXPECT_EQ(10, pool->stats().numAllocs);
  EXPECT_EQ(3, pool->stats().numFrees);

  pool->allocateContiguous(4 /* 16KB */, contiguousAllocation);
  EXPECT_EQ(11, pool->stats().numAllocs);
  EXPECT_EQ(4, pool->stats().numFrees);

  EXPECT_THROW(
      pool->allocateContiguous(
          AllocationTraits::numPages(allocatorCapacity) + 1,
          contiguousAllocation),
      VeloxException);
  EXPECT_EQ(12, pool->stats().numAllocs);
  EXPECT_EQ(5, pool->stats().numFrees);

  pool->freeContiguous(contiguousAllocation);
  EXPECT_EQ(6, pool->stats().numFrees);
}

TEST_P(MemoryPoolTest, nonContiguousAllocateWithOldAllocation) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool("nonContiguousAllocateWithOldAllocation");
  auto pool = root->addLeafChild(
      "nonContiguousAllocateWithOldAllocation", isLeafThreadSafe_);
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}", numOldPages, numNewPages);
    }
  } testSettings[] = {
      {0, 100},
      {0, Allocation::PageRun::kMaxPagesInRun / 2},
      {0, Allocation::PageRun::kMaxPagesInRun},
      {100, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun},
      {200, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun - 1},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun / 2}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    pool->allocateNonContiguous(testData.numNewPages, allocation);
    ASSERT_GE(allocation.numPages(), testData.numNewPages);
  }
}

TEST_P(MemoryPoolTest, persistentNonContiguousAllocateFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, injectedFailure:{}",
          static_cast<uint64_t>(numOldPages),
          static_cast<uint64_t>(numNewPages),
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
    SCOPED_TRACE(fmt::format(
        "{}, useMmap:{}, useCache:{}",
        testData.debugString(),
        useMmap_,
        useCache_));
    if ((testData.injectedFailure !=
         MemoryAllocator::InjectedFailure::kAllocate) &&
        !useMmap_) {
      // Non-Allocate failure injection only applies for MmapAllocator.
      continue;
    }
    reset();

    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "persistentNonContiguousAllocateFailure", isLeafThreadSafe_);
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    manager->allocator()->testingSetFailureInjection(
        testData.injectedFailure, true);
    ASSERT_THROW(
        pool->allocateNonContiguous(testData.numNewPages, allocation),
        VeloxRuntimeError);
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, transientNonContiguousAllocateFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MachinePageCount expectedAllocatedPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, expectedAllocatedPages:{}, injectedFailure:{}",
          numOldPages,
          numNewPages,
          expectedAllocatedPages,
          injectedFailure);
    }
  } testSettings[] = {
      // Cap failure injection.
      {0, 100, 100, MemoryAllocator::InjectedFailure::kCap},
      {0,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kCap},
      {0,
       Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kCap},
      {100, 100, 100, MemoryAllocator::InjectedFailure::kCap},
      {Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kCap},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kCap},
      {200, 100, 100, MemoryAllocator::InjectedFailure::kCap},
      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kCap},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun - 1,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kCap},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kCap},
      // Allocate failure injection.
      {0, 100, 100, MemoryAllocator::InjectedFailure::kAllocate},
      {0,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kAllocate},
      {0,
       Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kCap},
      {100, 100, 100, MemoryAllocator::InjectedFailure::kAllocate},
      {Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kAllocate},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kAllocate},
      {200, 100, 100, MemoryAllocator::InjectedFailure::kAllocate},
      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kAllocate},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun - 1,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kAllocate},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kAllocate},
      // Madvise failure injection.
      {0, 100, 100, MemoryAllocator::InjectedFailure::kMadvise},
      {0,
       Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2,
       MemoryAllocator::InjectedFailure::kMadvise},
      {0,
       Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun + 1,
       MemoryAllocator::InjectedFailure::kMadvise},
      {200, 100, 100, MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{}, useMmap:{}, useCache:{}",
        testData.debugString(),
        useMmap_,
        useCache_));
    if ((testData.injectedFailure !=
         MemoryAllocator::InjectedFailure::kAllocate) &&
        !useMmap_) {
      // Non-Allocate failure injection only applies for MmapAllocator.
      continue;
    }
    reset();
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "transientNonContiguousAllocateFailure", isLeafThreadSafe_);
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    manager->allocator()->testingSetFailureInjection(testData.injectedFailure);
    if (useCache_) {
      pool->allocateNonContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.expectedAllocatedPages);
    } else {
      ASSERT_THROW(
          pool->allocateNonContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateWithOldAllocation) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();
  auto pool = root->addLeafChild(
      "contiguousAllocateWithOldAllocation", isLeafThreadSafe_);
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}", numOldPages, numNewPages);
    }
  } testSettings[] = {
      {0, 100},
      {0, Allocation::PageRun::kMaxPagesInRun / 2},
      {0, Allocation::PageRun::kMaxPagesInRun},
      {100, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun * 2},
      {200, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun - 1},
      {Allocation::PageRun::kMaxPagesInRun * 2,
       Allocation::PageRun::kMaxPagesInRun}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    ASSERT_EQ(allocation.numPages(), testData.numOldPages);
    pool->allocateContiguous(testData.numNewPages, allocation);
    ASSERT_EQ(allocation.numPages(), testData.numNewPages);
  }
}

TEST_P(MemoryPoolTest, persistentContiguousAllocateFailure) {
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
                      // Mmap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {200, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {100, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (!useMmap_) {
      // No failure injections supported for contiguous allocation of
      // MallocAllocator.
      continue;
    }
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "persistentContiguousAllocateFailure", isLeafThreadSafe_);
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    manager->allocator()->testingSetFailureInjection(
        testData.injectedFailure, true);
    ASSERT_EQ(allocation.numPages(), testData.numOldPages);
    if ((testData.numOldPages >= testData.numNewPages) &&
        testData.injectedFailure != MemoryAllocator::InjectedFailure::kMmap) {
      pool->allocateContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.numNewPages);
    } else {
      ASSERT_THROW(
          pool->allocateContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, transientContiguousAllocateFailure) {
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
                      // Mmap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {200, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {100, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{}, useCache:{} , useMmap:{}",
        testData.debugString(),
        useCache_,
        useMmap_));
    if (!useMmap_) {
      // No failure injections supported for contiguous allocation of
      // MallocAllocator.
      continue;
    }
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "transientContiguousAllocateFailure", isLeafThreadSafe_);
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    manager->allocator()->testingSetFailureInjection(testData.injectedFailure);
    ASSERT_EQ(allocation.numPages(), testData.numOldPages);
    // NOTE: AsyncDataCache will retry on the transient memory allocation
    // failures from the underlying allocator.
    if (useCache_ ||
        ((testData.numOldPages >= testData.numNewPages) &&
         testData.injectedFailure != MemoryAllocator::InjectedFailure::kMmap)) {
      pool->allocateContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.numNewPages);
    } else {
      ASSERT_THROW(
          pool->allocateContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateExceedMemoryPoolLimit) {
  const MachinePageCount kMaxNumPages = 1 << 10;
  const auto kMemoryCapBytes = kMaxNumPages * AllocationTraits::kPageSize;
  setupMemory(
      {.allocatorCapacity = 1 << 30,
       .arbitratorCapacity = 1 << 30,
       .arbitratorReservedCapacity = 128 * MB});
  auto manager = getMemoryManager();
  auto root =
      manager->addRootPool("contiguousAllocateExceedLimit", kMemoryCapBytes);
  auto pool =
      root->addLeafChild("contiguousAllocateExceedLimit", isLeafThreadSafe_);

  ContiguousAllocation allocation1;
  pool->allocateContiguous(kMaxNumPages, allocation1);
  ASSERT_FALSE(allocation1.empty());
  ContiguousAllocation allocation2;
  ASSERT_THROW(
      pool->allocateContiguous(2 * kMaxNumPages, allocation2),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation2.empty());
  pool->freeContiguous(allocation1);
  pool->allocateContiguous(kMaxNumPages, allocation2);
  ASSERT_FALSE(allocation2.empty());
}

TEST_P(MemoryPoolTest, persistentContiguousGrowAllocateFailure) {
  struct {
    MachinePageCount numInitialPages;
    MachinePageCount numGrowPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string expectedErrorMessage;
    std::string debugString() const {
      return fmt::format(
          "numInitialPages:{}, numGrowPages:{}, injectedFailure:{}, expectedErrorMessage:{}",
          numInitialPages,
          numGrowPages,
          injectedFailure,
          expectedErrorMessage);
    }
  } testSettings[] = {// Cap failure injection.
                      {10,
                       100,
                       MemoryAllocator::InjectedFailure::kCap,
                       "growContiguous failed with 100 pages from Memory Pool"},
                      {100,
                       10,
                       MemoryAllocator::InjectedFailure::kCap,
                       "growContiguous failed with 10 pages from Memory Pool"},
                      // Mmap failure injection.
                      {10,
                       100,
                       MemoryAllocator::InjectedFailure::kMmap,
                       "growContiguous failed with 100 pages from Memory Pool"},
                      {100,
                       10,
                       MemoryAllocator::InjectedFailure::kMmap,
                       "growContiguous failed with 10 pages from Memory Pool"},
                      // Madvise failure injection.
                      {10,
                       100,
                       MemoryAllocator::InjectedFailure::kMadvise,
                       "growContiguous failed with 100 pages from Memory Pool"},
                      {100,
                       10,
                       MemoryAllocator::InjectedFailure::kMadvise,
                       "growContiguous failed with 10 pages from Memory Pool"}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (!useMmap_) {
      // No failure injections supported for contiguous allocation of
      // MallocAllocator.
      continue;
    }
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "persistentContiguousGrowAllocateFailure", isLeafThreadSafe_);
    ContiguousAllocation allocation;
    pool->allocateContiguous(
        testData.numInitialPages,
        allocation,
        testData.numGrowPages + testData.numInitialPages);
    manager->allocator()->testingSetFailureInjection(
        testData.injectedFailure, true);
    ASSERT_EQ(allocation.numPages(), testData.numInitialPages);
    ASSERT_EQ(
        allocation.maxSize(),
        AllocationTraits::pageBytes(
            testData.numInitialPages + testData.numGrowPages));
    VELOX_ASSERT_THROW(
        pool->growContiguous(testData.numGrowPages, allocation),
        testData.expectedErrorMessage);
    ASSERT_EQ(allocation.numPages(), testData.numInitialPages);
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, transientContiguousGrowAllocateFailure) {
  struct {
    MachinePageCount numInitialPages;
    MachinePageCount numGrowPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numInitialPages:{}, numGrowPages:{}, injectedFailure:{}",
          numInitialPages,
          numGrowPages,
          injectedFailure);
    }
  } testSettings[] = {// Cap failure injection.
                      {10, 100, MemoryAllocator::InjectedFailure::kCap},
                      {100, 10, MemoryAllocator::InjectedFailure::kCap},
                      // Mmap failure injection.
                      {10, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {100, 10, MemoryAllocator::InjectedFailure::kMmap},
                      // Madvise failure injection.
                      {10, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {100, 10, MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{}, useCache:{} , useMmap:{}",
        testData.debugString(),
        useCache_,
        useMmap_));
    if (!useMmap_) {
      // No failure injections supported for contiguous allocation of
      // MallocAllocator.
      continue;
    }
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "transientContiguousGrowAllocateFailure", isLeafThreadSafe_);
    ContiguousAllocation allocation;
    pool->allocateContiguous(
        testData.numInitialPages,
        allocation,
        testData.numGrowPages + testData.numInitialPages);
    manager->allocator()->testingSetFailureInjection(
        testData.injectedFailure, false);
    ASSERT_EQ(allocation.numPages(), testData.numInitialPages);
    ASSERT_EQ(
        allocation.maxSize(),
        AllocationTraits::pageBytes(
            testData.numInitialPages + testData.numGrowPages));
    // NOTE: AsyncDataCache will retry on the transient memory allocation
    // failures from the underlying allocator.
    if (useCache_) {
      pool->growContiguous(testData.numGrowPages, allocation);
      ASSERT_EQ(
          allocation.numPages(),
          testData.numInitialPages + testData.numGrowPages);
      ASSERT_EQ(
          allocation.maxSize(),
          AllocationTraits::pageBytes(
              testData.numInitialPages + testData.numGrowPages));
    } else {
      ASSERT_THROW(
          pool->growContiguous(testData.numInitialPages, allocation),
          VeloxRuntimeError);
      ASSERT_EQ(allocation.numPages(), testData.numInitialPages);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateGrowExceedMemoryPoolLimit) {
  const MachinePageCount kMaxNumPages = 1 << 10;
  const auto kMemoryCapBytes = kMaxNumPages * AllocationTraits::kPageSize;
  setupMemory(
      {.allocatorCapacity = 1 << 30,
       .arbitratorCapacity = 1 << 30,
       .arbitratorReservedCapacity = 128 * MB});
  auto manager = getMemoryManager();
  auto root = manager->addRootPool(
      "contiguousAllocateGrowExceedMemoryPoolLimit", kMemoryCapBytes);
  auto pool = root->addLeafChild(
      "contiguousAllocateGrowExceedMemoryPoolLimit", isLeafThreadSafe_);

  ContiguousAllocation allocation;
  pool->allocateContiguous(kMaxNumPages / 2, allocation, kMaxNumPages * 2);
  ASSERT_EQ(allocation.numPages(), kMaxNumPages / 2);
  ASSERT_EQ(
      allocation.maxSize(), AllocationTraits::pageBytes(kMaxNumPages * 2));
  VELOX_ASSERT_THROW(
      pool->growContiguous(kMaxNumPages, allocation),
      "Exceeded memory pool cap");
  ASSERT_EQ(allocation.numPages(), kMaxNumPages / 2);
}

TEST_P(MemoryPoolTest, nonContiguousAllocationBounds) {
  auto manager = getMemoryManager();
  auto pool = manager->addLeafPool("nonContiguousAllocationBounds");
  Allocation allocation;
  // Bad zero page allocation size.
  ASSERT_THROW(pool->allocateNonContiguous(0, allocation), VeloxRuntimeError);

  // Set the num of pages to allocate exceeds one PageRun limit.
  constexpr MachinePageCount kNumPages =
      Allocation::PageRun::kMaxPagesInRun + 1;
  pool->allocateNonContiguous(kNumPages, allocation);
  ASSERT_GE(allocation.numPages(), kNumPages);
  pool->freeNonContiguous(allocation);
  pool->allocateNonContiguous(kNumPages - 1, allocation);
  ASSERT_GE(allocation.numPages(), kNumPages - 1);
  pool->freeNonContiguous(allocation);
  pool->allocateNonContiguous(
      Allocation::PageRun::kMaxPagesInRun * 2, allocation);
  ASSERT_GE(allocation.numPages(), Allocation::PageRun::kMaxPagesInRun * 2);
  pool->freeNonContiguous(allocation);
}

TEST_P(MemoryPoolTest, nonContiguousAllocateExceedLimit) {
  const int64_t kMemoryCapBytes = AllocationTraits::pageBytes(1 << 10);
  setupMemory(
      {.allocatorCapacity = kMemoryCapBytes,
       .useMmapAllocator = useMmap_,
       .arbitratorCapacity = kMemoryCapBytes,
       .arbitratorReservedCapacity = kMemoryCapBytes / 2});
  auto manager = getMemoryManager();
  const MachinePageCount kMaxNumPages =
      AllocationTraits::numPages(kMemoryCapBytes);

  auto root =
      manager->addRootPool("nonContiguousAllocateExceedLimit", kMemoryCapBytes);
  auto pool =
      root->addLeafChild("nonContiguousAllocateExceedLimit", isLeafThreadSafe_);
  Allocation allocation;
  pool->allocateNonContiguous(kMaxNumPages, allocation);
  ASSERT_THROW(
      pool->allocateNonContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
  ASSERT_THROW(
      pool->allocateNonContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
  pool->allocateNonContiguous(kMaxNumPages, allocation);
}

TEST_P(MemoryPoolTest, nonContiguousAllocateError) {
  auto manager = getMemoryManager();
  auto pool = manager->addLeafPool("nonContiguousAllocateError");
  manager->allocator()->testingSetFailureInjection(
      MemoryAllocator::InjectedFailure::kAllocate, true);
  constexpr MachinePageCount kAllocSize = 8;
  std::unique_ptr<Allocation> allocation(new Allocation());
  ASSERT_THROW(
      pool->allocateNonContiguous(kAllocSize, *allocation), VeloxRuntimeError);
  ASSERT_TRUE(allocation->empty());
  manager->allocator()->testingClearFailureInjection();
  pool->allocateNonContiguous(kAllocSize, *allocation);
  pool->freeNonContiguous(*allocation);
  ASSERT_TRUE(allocation->empty());
}

TEST_P(MemoryPoolTest, mmapAllocatorCapAllocationError) {
  if (!useMmap_) {
    return;
  }
  struct {
    int64_t allocateBytes;
    bool expectedFailure;
    bool persistentErrorInjection;
    std::string debugString() const {
      return fmt::format(
          "allocateBytes {}, expectFailure {}, persistentErrorInjection {}",
          allocateBytes,
          expectedFailure,
          persistentErrorInjection);
    }
  } testSettings[] = {// NOTE: the failure injection only applies for
                      // allocations that are not delegated to malloc.
                      {maxMallocBytes_ - 1, false, false},
                      {maxMallocBytes_, false, false},
                      {maxMallocBytes_ + 1, true, false},
                      {maxMallocBytes_ - 1, false, true},
                      {maxMallocBytes_, false, true},
                      {maxMallocBytes_ + 1, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory();
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "mmapAllocatorCapAllocationError", isLeafThreadSafe_);

    manager->allocator()->testingSetFailureInjection(
        MemoryAllocator::InjectedFailure::kCap,
        testData.persistentErrorInjection);
    // Async data cache will retry transient memory allocation failure.
    if (!testData.expectedFailure ||
        (useCache_ && !testData.persistentErrorInjection)) {
      void* buffer = pool->allocate(testData.allocateBytes);
      pool->free(buffer, testData.allocateBytes);
    } else {
      ASSERT_THROW(pool->allocate(testData.allocateBytes), VeloxRuntimeError);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, mmapAllocatorCapAllocationZeroFilledError) {
  if (!useMmap_) {
    return;
  }

  struct {
    int64_t numEntries;
    int64_t sizeEach;
    bool expectedFailure;
    bool persistentErrorInjection;
    std::string debugString() const {
      return fmt::format(
          "numEntries {}, sizeEach {}, expectFailure {}, persistentErrorInjection {}",
          numEntries,
          sizeEach,
          expectedFailure,
          persistentErrorInjection);
    }
  } testSettings[] = {// NOTE: the failure injection only applies for
                      // allocations that are not delegated to malloc.
                      {maxMallocBytes_ - 1, 1, false, false},
                      {maxMallocBytes_, 1, false, false},
                      {maxMallocBytes_ + 1, 1, true, false},
                      {maxMallocBytes_ - 1, 1, false, true},
                      {maxMallocBytes_, 1, false, true},
                      {maxMallocBytes_ + 1, 1, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory();
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "mmapAllocatorCapAllocationZeroFilledError", isLeafThreadSafe_);

    manager->allocator()->testingSetFailureInjection(
        MemoryAllocator::InjectedFailure::kCap,
        testData.persistentErrorInjection);
    // Async data cache will retry transient memory allocation failure.
    if (!testData.expectedFailure ||
        (useCache_ && !testData.persistentErrorInjection)) {
      void* buffer =
          pool->allocateZeroFilled(testData.numEntries, testData.sizeEach);
      pool->free(buffer, testData.numEntries * testData.sizeEach);
    } else {
      ASSERT_THROW(
          pool->allocateZeroFilled(testData.numEntries, testData.sizeEach),
          VeloxRuntimeError);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, mmapAllocatorCapReallocateError) {
  if (!useMmap_) {
    return;
  }
  struct {
    int64_t allocateBytes;
    bool expectedFailure;
    bool persistentErrorInjection;
    std::string debugString() const {
      return fmt::format(
          "allocateBytes {}, expectFailure {}, persistentErrorInjection {}",
          allocateBytes,
          expectedFailure,
          persistentErrorInjection);
    }
  } testSettings[] = {// NOTE: the failure injection only applies for
                      // allocations that are not delegated to malloc.
                      {maxMallocBytes_ - 1, false, false},
                      {maxMallocBytes_, false, false},
                      {maxMallocBytes_ + 1, true, false},
                      {maxMallocBytes_ - 1, false, true},
                      {maxMallocBytes_, false, true},
                      {maxMallocBytes_ + 1, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory();
    auto manager = getMemoryManager();
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "mmapAllocatorCapReallocateError", isLeafThreadSafe_);

    manager->allocator()->testingSetFailureInjection(
        MemoryAllocator::InjectedFailure::kCap,
        testData.persistentErrorInjection);
    // Async data cache will retry transient memory allocation failure.
    if (!testData.expectedFailure ||
        (useCache_ && !testData.persistentErrorInjection)) {
      void* buffer = pool->reallocate(nullptr, 0, testData.allocateBytes);
      pool->free(buffer, testData.allocateBytes);
    } else {
      ASSERT_THROW(
          pool->reallocate(nullptr, 0, testData.allocateBytes),
          VeloxRuntimeError);
    }
    manager->allocator()->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, validCheck) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool();
  ASSERT_ANY_THROW(root->allocate(100));
  ASSERT_ANY_THROW(root->reallocate(static_cast<void*>(this), 100, 300));
  ASSERT_ANY_THROW(root->allocateZeroFilled(100, 100));
  ASSERT_ANY_THROW(root->free(static_cast<void*>(this), 100));
  {
    Allocation out;
    ASSERT_ANY_THROW(root->allocateNonContiguous(100, out));
    ASSERT_ANY_THROW(root->freeNonContiguous(out));
  }
  {
    ContiguousAllocation out;
    ASSERT_ANY_THROW(root->allocateContiguous(100, out));
    ASSERT_ANY_THROW(root->freeContiguous(out));
  }
  auto child = root->addLeafChild("validCheck");
  ASSERT_ANY_THROW(child->addLeafChild("validCheck", isLeafThreadSafe_));
  ASSERT_ANY_THROW(child->addAggregateChild("validCheck"));
}

// Class used to test operations on MemoryPool.
class MemoryPoolTester {
 public:
  MemoryPoolTester(int32_t id, int64_t maxMemory, memory::MemoryPool& pool)
      : id_(id), maxMemory_(maxMemory), pool_(pool) {}

  ~MemoryPoolTester() {
    for (auto& allocation : contiguousAllocations_) {
      pool_.freeContiguous(allocation);
    }
    for (auto& allocation : nonContiguiusAllocations_) {
      pool_.freeNonContiguous(allocation);
    }
    for (auto& bufferEntry : allocBuffers_) {
      pool_.free(bufferEntry.first, bufferEntry.second);
    }
    if (reservedBytes_ != 0) {
      pool_.release();
    }
  }

  void run() {
    const int32_t op = folly::Random().rand32() % 4;
    switch (op) {
      case 0: {
        // Allocate buffers.
        if ((folly::Random().rand32() % 2) && !allocBuffers_.empty()) {
          pool_.free(allocBuffers_.back().first, allocBuffers_.back().second);
          allocBuffers_.pop_back();
        } else {
          const int64_t allocateBytes =
              folly::Random().rand32() % (maxMemory_ / 64);
          try {
            void* buffer = pool_.allocate(allocateBytes);
            VELOX_CHECK_NOT_NULL(buffer);
            allocBuffers_.push_back({buffer, allocateBytes});
          } catch (VeloxException& e) {
            // Ignore memory limit exception.
            ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
            return;
          }
        }
        break;
      }
      case 2: {
        // Contiguous allocations.
        if ((folly::Random().rand32() % 2) && !contiguousAllocations_.empty()) {
          pool_.freeContiguous(contiguousAllocations_.back());
          contiguousAllocations_.pop_back();
        } else {
          int64_t allocatePages = std::max<int64_t>(
              1,
              folly::Random().rand32() % Allocation::PageRun::kMaxPagesInRun);
          ContiguousAllocation allocation;
          if (folly::Random().oneIn(2) && !contiguousAllocations_.empty()) {
            allocation = std::move(contiguousAllocations_.back());
            contiguousAllocations_.pop_back();
            if (folly::Random().oneIn(2)) {
              allocatePages = std::max<int64_t>(1, allocation.numPages() - 4);
            }
          }
          try {
            pool_.allocateContiguous(allocatePages, allocation);
            contiguousAllocations_.push_back(std::move(allocation));
          } catch (VeloxException& e) {
            // Ignore memory limit exception.
            ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
            return;
          }
        }
        break;
      }
      case 1: {
        // Non-contiguous allocations.
        if ((folly::Random().rand32() % 2) &&
            !nonContiguiusAllocations_.empty()) {
          pool_.freeNonContiguous(nonContiguiusAllocations_.back());
          nonContiguiusAllocations_.pop_back();
        } else {
          const int64_t allocatePages = std::max<int64_t>(
              1,
              folly::Random().rand32() % Allocation::PageRun::kMaxPagesInRun);
          Allocation allocation;
          try {
            pool_.allocateNonContiguous(allocatePages, allocation);
            nonContiguiusAllocations_.push_back(std::move(allocation));
          } catch (VeloxException& e) {
            // Ignore memory limit exception.
            ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
            return;
          }
        }
        break;
      }
      case 3: {
        // maybe reserve.
        if (reservedBytes_ == 0) {
          const uint64_t reservedBytes =
              folly::Random().rand32() % (maxMemory_ / 32);
          if (pool_.maybeReserve(reservedBytes)) {
            reservedBytes_ = reservedBytes;
          }
        } else {
          pool_.release();
          reservedBytes_ = 0;
        }
        break;
      }
    }
  }

 private:
  const int32_t id_;
  const int64_t maxMemory_;
  memory::MemoryPool& pool_;
  uint64_t reservedBytes_{0};
  std::vector<ContiguousAllocation> contiguousAllocations_;
  std::vector<Allocation> nonContiguiusAllocations_;
  std::vector<std::pair<void*, uint64_t>> allocBuffers_;
};

TEST_P(MemoryPoolTest, concurrentUpdateToDifferentPools) {
  constexpr int64_t kMaxMemory = 10 * GB;
  MemoryManager& manager = *getMemoryManager();
  auto root =
      manager.addRootPool("concurrentUpdateToDifferentPools", kMaxMemory);
  const int32_t kNumThreads = 5;
  // Create one memory tracker per each thread.
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int32_t i = 0; i < kNumThreads; ++i) {
    childPools.push_back(root->addLeafChild(
        fmt::format("{}", i), i % 2 ? isLeafThreadSafe_ : !isLeafThreadSafe_));
  }

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 1'000;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      MemoryPoolTester tester(i, kMaxMemory, *childPools[i]);
      for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
        tester.run();
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  ASSERT_EQ(root->availableReservation(), 0);
  for (auto& child : childPools) {
    ASSERT_EQ(child->usedBytes(), 0);
    child->release();
    ASSERT_EQ(child->reservedBytes(), 0);
    ASSERT_EQ(child->availableReservation(), 0);
    ASSERT_EQ(child->usedBytes(), 0);
    ASSERT_LE(child->stats().peakBytes, child->stats().cumulativeBytes);
  }
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  childPools.clear();
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  ASSERT_EQ(root->stats().usedBytes, 0);
}

TEST_P(MemoryPoolTest, concurrentUpdatesToTheSamePool) {
  FLAGS_velox_memory_pool_debug_enabled = true;
  if (!isLeafThreadSafe_) {
    return;
  }
  constexpr int64_t kMaxMemory = 8 * GB;
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool();

  const int32_t kNumThreads = 4;
  const int32_t kNumChildPools = 2;
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int32_t i = 0; i < kNumChildPools; ++i) {
    childPools.push_back(root->addLeafChild(fmt::format("{}", i)));
  }

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 5'00;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      MemoryPoolTester tester(i, kMaxMemory, *childPools[i % kNumChildPools]);
      for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
        tester.run();
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  ASSERT_EQ(root->availableReservation(), 0);
  for (auto& child : childPools) {
    ASSERT_EQ(child->usedBytes(), 0);
    child->release();
    ASSERT_EQ(child->reservedBytes(), 0);
    ASSERT_EQ(child->availableReservation(), 0);
    ASSERT_EQ(child->usedBytes(), 0);
    ASSERT_LE(child->stats().peakBytes, child->stats().cumulativeBytes);
  }
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  childPools.clear();
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  ASSERT_EQ(root->stats().usedBytes, 0);
}

TEST_P(MemoryPoolTest, concurrentUpdateToSharedPools) {
  // under some conditions bug.
  constexpr int64_t kMaxMemory = 10 * GB;
  MemoryManager& manager = *getMemoryManager();
  const int32_t kNumThreads = FLAGS_velox_memory_num_shared_leaf_pools;

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 200;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      auto& sharedPool = manager.deprecatedSharedLeafPool();
      MemoryPoolTester tester(i, kMaxMemory, sharedPool);
      for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
        tester.run();
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  for (auto pool : manager.testingSharedLeafPools()) {
    EXPECT_EQ(pool->usedBytes(), 0);
  }
}

TEST_P(MemoryPoolTest, concurrentPoolStructureAccess) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1234);
  constexpr int64_t kMaxMemory = 8 * GB;
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool();
  std::atomic<int64_t> poolId{0};
  std::mutex lock;
  std::vector<std::shared_ptr<MemoryPool>> pools;
  const int32_t kNumThreads = 20;
  const int32_t kNumOpsPerThread = 1'000;
  const std::string kPoolNamePrefix("concurrent");

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      for (int32_t op = 0; op < kNumOpsPerThread; ++op) {
        std::shared_ptr<MemoryPool> pool;
        {
          std::lock_guard<std::mutex> l(lock);
          if (pools.empty() || folly::Random().oneIn(5)) {
            auto pool = root->addLeafChild(
                fmt::format("{}{}", kPoolNamePrefix, poolId++));
            pools.push_back(pool);
            continue;
          }
          const auto idx = folly::Random().rand32() % pools.size();
          if (folly::Random().oneIn(3)) {
            pools.erase(pools.begin() + idx);
            continue;
          }
          pool = pools[idx];
        }
        VELOX_CHECK_NOT_NULL(pool);

        if (pool->kind() == MemoryPool::Kind::kAggregate &&
            !folly::Random().oneIn(3)) {
          const std::string name =
              fmt::format("{}{}", kPoolNamePrefix, poolId++);
          auto childPool = folly::Random().oneIn(4)
              ? pool->addLeafChild(
                    name,
                    folly::Random().oneIn(2) ? isLeafThreadSafe_
                                             : !isLeafThreadSafe_)
              : pool->addAggregateChild(name);
          std::lock_guard<std::mutex> l(lock);
          pools.push_back(std::move(childPool));
          continue;
        }

        pool->visitChildren([&](MemoryPool* pool) {
          VELOX_CHECK_EQ(
              pool->name().compare(0, kPoolNamePrefix.size(), kPoolNamePrefix),
              0);
          return true;
        });
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  pools.clear();
  ASSERT_EQ(root->getChildCount(), 0);
}

TEST(MemoryPoolTest, visitChildren) {
  MemoryManagerOptions options;
  options.allocatorCapacity = kMaxMemory;
  MemoryManager manager{options};
  auto root = manager.addRootPool("root");

  const int numChildren = 10;
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int i = 0; i < numChildren; ++i) {
    childPools.push_back(root->addLeafChild(std::to_string(i)));
  }

  std::vector<int> stopSums;
  stopSums.push_back(0);
  stopSums.push_back(numChildren + 1);
  stopSums.push_back(1);
  stopSums.push_back(numChildren / 2);
  stopSums.push_back(numChildren);
  for (const auto& stopSum : stopSums) {
    int sum = 0;
    root->visitChildren([&](MemoryPool* /*unused*/) {
      ++sum;
      return !(sum == stopSum);
    });
    if (stopSum < 1 || stopSum > numChildren) {
      ASSERT_EQ(sum, numChildren);
    } else {
      ASSERT_EQ(sum, stopSum);
    }
  }

  // Verify there is no deadlock when access back its parent node.
  root->visitChildren([&](MemoryPool* /*unused*/) {
    auto child = root->addAggregateChild("DeadlockDetection");
    return true;
  });
}

TEST(MemoryPoolTest, debugMode) {
  FLAGS_velox_memory_pool_debug_enabled = true;
  constexpr int64_t kMaxMemory = 10 * GB;
  constexpr int64_t kNumIterations = 100;
  const std::vector<int64_t> kAllocSizes = {128, 8 * KB, 2 * MB};
  const auto checkAllocs =
      [](const std::unordered_map<uint64_t, MemoryPoolImpl::AllocationRecord>&
             records,
         uint64_t size) {
        for (const auto& pair : records) {
          EXPECT_EQ(pair.second.size, size);
        }
      };

  MemoryManagerOptions options;
  options.allocatorCapacity = kMaxMemory;
  options.debugEnabled = true;
  MemoryManager manager{options};
  auto pool = manager.addRootPool("root")->addLeafChild("child");
  const auto& allocRecords = std::dynamic_pointer_cast<MemoryPoolImpl>(pool)
                                 ->testingDebugAllocRecords();
  std::vector<void*> smallAllocs;
  for (int32_t i = 0; i < kNumIterations; i++) {
    smallAllocs.push_back(pool->allocate(kAllocSizes[0]));
  }
  EXPECT_EQ(allocRecords.size(), kNumIterations);
  checkAllocs(allocRecords, kAllocSizes[0]);
  for (int32_t i = 0; i < kNumIterations; i++) {
    pool->free(smallAllocs[i], kAllocSizes[0]);
  }
  EXPECT_EQ(allocRecords.size(), 0);

  std::vector<Allocation> mediumAllocs;
  for (int32_t i = 0; i < kNumIterations; i++) {
    Allocation out;
    pool->allocateNonContiguous(
        AllocationTraits::numPages(kAllocSizes[1]), out);
    mediumAllocs.push_back(std::move(out));
  }
  EXPECT_EQ(allocRecords.size(), kNumIterations);
  checkAllocs(allocRecords, kAllocSizes[1]);
  for (int32_t i = 0; i < kNumIterations; i++) {
    pool->freeNonContiguous(mediumAllocs[i]);
  }
  EXPECT_EQ(allocRecords.size(), 0);

  std::vector<ContiguousAllocation> largeAllocs;
  for (int32_t i = 0; i < kNumIterations; i++) {
    ContiguousAllocation out;
    pool->allocateContiguous(AllocationTraits::numPages(kAllocSizes[2]), out);
    largeAllocs.push_back(std::move(out));
  }
  EXPECT_EQ(allocRecords.size(), kNumIterations);
  checkAllocs(allocRecords, kAllocSizes[2]);
  for (int32_t i = 0; i < kNumIterations; i++) {
    pool->freeContiguous(largeAllocs[i]);
  }
  EXPECT_EQ(allocRecords.size(), 0);
}

TEST(MemoryPoolTest, debugModeWithFilter) {
  constexpr int64_t kMaxMemory = 10 * GB;
  constexpr int64_t kNumIterations = 100;
  const std::vector<int64_t> kAllocSizes = {128, 8 * KB, 2 * MB};
  const std::vector<bool> debugEnabledSet{true, false};
  for (const auto& debugEnabled : debugEnabledSet) {
    MemoryManager manager{
        {.debugEnabled = debugEnabled, .allocatorCapacity = kMaxMemory}};

    // leaf child created from MemoryPool, not match filter
    MemoryPoolImpl::setDebugPoolNameRegex("NO-MATCH");
    auto root0 = manager.addRootPool("root0");
    auto pool0 = root0->addLeafChild("PartialAggregation.0.0");
    auto* buffer0 = pool0->allocate(1 * KB);
    EXPECT_TRUE(std::dynamic_pointer_cast<MemoryPoolImpl>(pool0)
                    ->testingDebugAllocRecords()
                    .empty());
    pool0->free(buffer0, 1 * KB);

    // leaf child created from MemoryPool, match filter
    MemoryPoolImpl::setDebugPoolNameRegex(".*PartialAggregation.*");
    auto root1 = manager.addRootPool("root1");
    auto pool1 = root1->addLeafChild("PartialAggregation.0.1");
    auto* buffer1 = pool1->allocate(1 * KB);
    if (!debugEnabled) {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool1)
              ->testingDebugAllocRecords()
              .size(),
          0);
    } else {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool1)
              ->testingDebugAllocRecords()
              .size(),
          1);
    }
    pool1->free(buffer1, 1 * KB);

    // old pool should not be affected by updated filter
    buffer0 = pool0->allocate(1 * KB);
    EXPECT_TRUE(std::dynamic_pointer_cast<MemoryPoolImpl>(pool0)
                    ->testingDebugAllocRecords()
                    .empty());
    pool0->free(buffer0, 1 * KB);

    // leaf child created from MemoryPool, match filter
    MemoryPoolImpl::setDebugPoolNameRegex(".*OrderBy.*");
    auto root2 = manager.addRootPool("root2");
    auto pool2 = root2->addLeafChild("OrderBy.0.0");
    auto* buffer2 = pool2->allocate(1 * KB);
    if (!debugEnabled) {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool2)
              ->testingDebugAllocRecords()
              .size(),
          0);
    } else {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool2)
              ->testingDebugAllocRecords()
              .size(),
          1);
    }
    pool2->free(buffer2, 1 * KB);

    // leaf child created from aggr MemoryPool, match filter
    auto intPool = root2->addAggregateChild("AGG-Pool");
    auto pool3 = intPool->addLeafChild("OrderBy.0.1");
    auto* buffer3 = pool3->allocate(1 * KB);
    if (!debugEnabled) {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool3)
              ->testingDebugAllocRecords()
              .size(),
          0);
    } else {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool3)
              ->testingDebugAllocRecords()
              .size(),
          1);
    }
    pool3->free(buffer3, 1 * KB);

    // leaf child created from MemoryManager, not match filter
    auto pool4 = manager.addLeafPool("Arbitrator.0.0");
    auto* buffer4 = pool4->allocate(1 * KB);
    EXPECT_TRUE(std::dynamic_pointer_cast<MemoryPoolImpl>(pool4)
                    ->testingDebugAllocRecords()
                    .empty());
    pool4->free(buffer4, 1 * KB);

    // leaf child created from MemoryManager, match filter
    MemoryPoolImpl::setDebugPoolNameRegex(".*Arbitrator.*");
    auto pool5 = manager.addLeafPool("Arbitrator.0.1");
    auto* buffer5 = pool5->allocate(1 * KB);
    if (!debugEnabled) {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool5)
              ->testingDebugAllocRecords()
              .size(),
          0);
    } else {
      EXPECT_EQ(
          std::dynamic_pointer_cast<MemoryPoolImpl>(pool5)
              ->testingDebugAllocRecords()
              .size(),
          1);
    }
    pool5->free(buffer5, 1 * KB);
  }
}

TEST_P(MemoryPoolTest, shrinkAndGrowAPIs) {
  MemoryManager& manager = *getMemoryManager();
  std::vector<uint64_t> capacities = {kMaxMemory, 128 * MB};
  const int allocationSize = 8 * MB;
  for (const auto& capacity : capacities) {
    SCOPED_TRACE(fmt::format("capacity {}", succinctBytes(capacity)));
    auto rootPool = manager.addRootPool("shrinkAPIs.Root", capacity);
    auto aggregationPool = rootPool->addAggregateChild("shrinkAPIs.Aggregate");
    auto leafPool = aggregationPool->addLeafChild("shrinkAPIs");
    ASSERT_EQ(rootPool->capacity(), capacity);
    ASSERT_EQ(leafPool->capacity(), capacity);
    ASSERT_EQ(aggregationPool->capacity(), capacity);
    if (capacity == 0 || capacity == kMaxMemory) {
      ASSERT_EQ(rootPool->freeBytes(), 0);
      ASSERT_EQ(leafPool->freeBytes(), 0);
      ASSERT_EQ(aggregationPool->freeBytes(), 0);
    } else {
      ASSERT_EQ(rootPool->freeBytes(), capacity);
      ASSERT_EQ(leafPool->freeBytes(), capacity);
      ASSERT_EQ(aggregationPool->freeBytes(), capacity);
    }
    if (capacity == 0) {
      VELOX_ASSERT_THROW(leafPool->allocate(allocationSize), "");
      ASSERT_EQ(leafPool->shrink(0), 0);
      ASSERT_EQ(leafPool->shrink(allocationSize), 0);
      continue;
    }
    void* buffer = leafPool->allocate(allocationSize);
    if (capacity == kMaxMemory) {
      ASSERT_EQ(rootPool->freeBytes(), 0);
      ASSERT_EQ(leafPool->freeBytes(), 0);
      ASSERT_EQ(aggregationPool->freeBytes(), 0);
      VELOX_ASSERT_THROW(leafPool->shrink(0), "");
      VELOX_ASSERT_THROW(leafPool->shrink(allocationSize), "");
      VELOX_ASSERT_THROW(leafPool->shrink(kMaxMemory), "");
      VELOX_ASSERT_THROW(aggregationPool->shrink(0), "");
      VELOX_ASSERT_THROW(aggregationPool->shrink(allocationSize), "");
      VELOX_ASSERT_THROW(aggregationPool->shrink(kMaxMemory), "");
      VELOX_ASSERT_THROW(rootPool->shrink(0), "");
      VELOX_ASSERT_THROW(rootPool->shrink(allocationSize), "");
      VELOX_ASSERT_THROW(rootPool->shrink(kMaxMemory), "");
      leafPool->free(buffer, allocationSize);
      continue;
    }
    ASSERT_EQ(rootPool->freeBytes(), capacity - allocationSize);
    ASSERT_EQ(leafPool->freeBytes(), capacity - allocationSize);
    ASSERT_EQ(aggregationPool->freeBytes(), capacity - allocationSize);

    ASSERT_EQ(leafPool->shrink(allocationSize), allocationSize);

    ASSERT_EQ(leafPool->capacity(), capacity - allocationSize);
    ASSERT_EQ(aggregationPool->capacity(), capacity - allocationSize);
    ASSERT_EQ(rootPool->capacity(), capacity - allocationSize);

    ASSERT_EQ(leafPool->freeBytes(), capacity - 2 * allocationSize);
    ASSERT_EQ(aggregationPool->freeBytes(), capacity - 2 * allocationSize);
    ASSERT_EQ(rootPool->freeBytes(), capacity - 2 * allocationSize);

    ASSERT_EQ(aggregationPool->shrink(), capacity - 2 * allocationSize);
    ASSERT_EQ(leafPool->capacity(), allocationSize);
    ASSERT_EQ(aggregationPool->capacity(), allocationSize);
    ASSERT_EQ(rootPool->capacity(), allocationSize);

    ASSERT_EQ(leafPool->freeBytes(), 0);
    ASSERT_EQ(aggregationPool->freeBytes(), 0);
    ASSERT_EQ(rootPool->freeBytes(), 0);

    ASSERT_EQ(leafPool->shrink(), 0);
    ASSERT_EQ(aggregationPool->shrink(), 0);
    ASSERT_EQ(rootPool->shrink(), 0);

    leafPool->free(buffer, allocationSize);

    ASSERT_EQ(leafPool->capacity(), allocationSize);
    ASSERT_EQ(aggregationPool->capacity(), allocationSize);
    ASSERT_EQ(rootPool->capacity(), allocationSize);

    ASSERT_EQ(leafPool->freeBytes(), allocationSize);
    ASSERT_EQ(aggregationPool->freeBytes(), allocationSize);
    ASSERT_EQ(rootPool->freeBytes(), allocationSize);

    ASSERT_EQ(leafPool->shrink(allocationSize / 2), allocationSize / 2);
    ASSERT_EQ(leafPool->capacity(), allocationSize / 2);
    ASSERT_EQ(aggregationPool->capacity(), allocationSize / 2);
    ASSERT_EQ(rootPool->capacity(), allocationSize / 2);

    ASSERT_EQ(leafPool->shrink(), allocationSize / 2);
    ASSERT_EQ(aggregationPool->shrink(), 0);
    ASSERT_EQ(rootPool->shrink(), 0);

    ASSERT_EQ(leafPool->capacity(), 0);
    ASSERT_EQ(aggregationPool->capacity(), 0);
    ASSERT_EQ(rootPool->capacity(), 0);

    ASSERT_EQ(leafPool->freeBytes(), 0);
    ASSERT_EQ(aggregationPool->freeBytes(), 0);
    ASSERT_EQ(rootPool->freeBytes(), 0);

    ASSERT_EQ(leafPool->shrink(allocationSize), 0);
    ASSERT_EQ(aggregationPool->shrink(allocationSize), 0);
    ASSERT_EQ(rootPool->shrink(allocationSize), 0);

    const int step = 10;
    for (int i = 0; i < step; ++i) {
      const int expectedCapacity = (i + 1) * allocationSize;
      if (i % 3 == 0) {
        ASSERT_TRUE(leafPool->grow(allocationSize, 0));
        ASSERT_EQ(leafPool->capacity(), expectedCapacity);
      } else if (i % 3 == 1) {
        ASSERT_TRUE(aggregationPool->grow(allocationSize, 0));
        ASSERT_EQ(leafPool->capacity(), expectedCapacity);
      } else {
        ASSERT_TRUE(rootPool->grow(allocationSize, 0));
        ASSERT_EQ(leafPool->capacity(), expectedCapacity);
      }
      ASSERT_EQ(leafPool->capacity(), expectedCapacity);
      ASSERT_EQ(aggregationPool->capacity(), expectedCapacity);
      ASSERT_EQ(rootPool->capacity(), expectedCapacity);

      ASSERT_EQ(leafPool->freeBytes(), expectedCapacity);
      ASSERT_EQ(aggregationPool->freeBytes(), expectedCapacity);
      ASSERT_EQ(rootPool->freeBytes(), expectedCapacity);
    }
  }
}

TEST_P(MemoryPoolTest, memoryReclaimerSetCheck) {
  auto manager = getMemoryManager();
  // Valid use case: parent has memory reclaimer but child doesn't set.
  {
    auto root =
        manager->addRootPool("", kMaxMemory, memory::MemoryReclaimer::create());
    // Can't set more tha once.
    VELOX_ASSERT_THROW(
        root->setReclaimer(memory::MemoryReclaimer::create()), "");
    VELOX_ASSERT_THROW(root->setReclaimer(nullptr), "");
    auto aggrChild = root->addAggregateChild("aggregateChild");
    aggrChild->setReclaimer(memory::MemoryReclaimer::create());
    VELOX_ASSERT_THROW(
        aggrChild->setReclaimer(memory::MemoryReclaimer::create()), "");
    // Can't set empty reclaimer.
    VELOX_ASSERT_THROW(aggrChild->setReclaimer(nullptr), "");
    auto leafChild = root->addLeafChild("leafChild");
    leafChild->setReclaimer(memory::MemoryReclaimer::create());
    VELOX_ASSERT_THROW(
        leafChild->setReclaimer(memory::MemoryReclaimer::create()), "");
    VELOX_ASSERT_THROW(leafChild->setReclaimer(nullptr), "");
  }
  // Valid use case: both parent and child set memory reclaimer.
  {
    auto root =
        manager->addRootPool("", kMaxMemory, memory::MemoryReclaimer::create());
    auto aggrChild = root->addAggregateChild("aggregateChild");
    auto leafChild = root->addLeafChild("leafChild");
  }
  // Valid use case: both parent and child don't set memory reclaimer.
  {
    auto root = manager->addRootPool("", kMaxMemory);
    auto aggrChild = root->addAggregateChild("aggregateChild");
    auto leafChild = root->addLeafChild("leafChild");
    root->setReclaimer(memory::MemoryReclaimer::create());
    VELOX_ASSERT_THROW(aggrChild->setReclaimer(nullptr), "");
    aggrChild->setReclaimer(memory::MemoryReclaimer::create());
    VELOX_ASSERT_THROW(leafChild->setReclaimer(nullptr), "");
    leafChild->setReclaimer(memory::MemoryReclaimer::create());
  }
  // Invalid use case: parent has no memory reclaimer but child set.
  {
    auto root = manager->addRootPool("", kMaxMemory);
    VELOX_ASSERT_THROW(
        root->addAggregateChild(
            "aggregateChild", memory::MemoryReclaimer::create()),
        "");
    VELOX_ASSERT_THROW(
        root->addLeafChild(
            "leafChild",
            folly::Random::oneIn(2),
            memory::MemoryReclaimer::create()),
        "");
  }
}

TEST_P(MemoryPoolTest, reclaimAPIsWithDefaultReclaimer) {
  MemoryManager& manager = *getMemoryManager();
  struct {
    int numChildren;
    int numGrandchildren;
    bool doAllocation;
    bool hasReclaimer;

    std::string debugString() const {
      return fmt::format(
          "numChildren {} numGrandchildren {} doAllocation{} hasReclaimer {}",
          numChildren,
          numGrandchildren,
          doAllocation,
          hasReclaimer);
    }
  } testSettings[] = {
      {0, 0, false, true},     {0, 0, false, false},   {1, 0, false, true},
      {1, 0, false, false},    {100, 0, false, true},  {100, 0, false, false},
      {1, 100, false, true},   {1, 100, false, false}, {10, 100, false, true},
      {10, 100, false, false}, {100, 1, true, true},   {100, 1, true, false},
      {0, 0, true, true},      {0, 0, true, false},    {1, 0, true, true},
      {1, 0, true, false},     {100, 0, true, true},   {100, 0, true, false},
      {1, 100, true, true},    {1, 100, true, false},  {10, 100, true, true},
      {10, 100, true, false},  {100, 1, true, true},   {100, 1, true, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::vector<std::shared_ptr<MemoryPool>> pools;
    auto pool = manager.addRootPool(
        "shrinkAPIs",
        kMaxMemory,
        testData.hasReclaimer ? memory::MemoryReclaimer::create() : nullptr);
    pools.push_back(pool);

    struct Allocation {
      void* buffer;
      size_t size;
      MemoryPool* pool;
    };
    std::vector<Allocation> allocations;
    for (int i = 0; i < testData.numChildren; ++i) {
      const bool isLeaf = testData.numGrandchildren == 0 ? true : false;
      auto childPool = isLeaf
          ? pool->addLeafChild(
                std::to_string(i),
                isLeafThreadSafe_,
                testData.hasReclaimer ? memory::MemoryReclaimer::create()
                                      : nullptr)
          : pool->addAggregateChild(
                std::to_string(i),
                testData.hasReclaimer ? memory::MemoryReclaimer::create()
                                      : nullptr);
      pools.push_back(childPool);
      for (int j = 0; j < testData.numGrandchildren; ++j) {
        auto grandChild = childPool->addLeafChild(
            std::to_string(j),
            isLeafThreadSafe_,
            testData.hasReclaimer ? memory::MemoryReclaimer::create()
                                  : nullptr);
        pools.push_back(grandChild);
      }
    }
    for (auto& pool : pools) {
      if (testData.hasReclaimer) {
        ASSERT_NE(pool->reclaimer(), nullptr);
      } else {
        ASSERT_EQ(pool->reclaimer(), nullptr);
      }
      if (pool->kind() == MemoryPool::Kind::kLeaf) {
        const size_t size = 1 + folly::Random::rand32(rng_) % 1024;
        void* buffer = pool->allocate(size);
        allocations.push_back({buffer, size, pool.get()});
      }
    }
    for (auto& pool : pools) {
      ASSERT_FALSE(pool->reclaimableBytes().has_value());
      ASSERT_EQ(pool->reclaim(0, 0, stats_), 0);
      ASSERT_EQ(pool->reclaim(100, 0, stats_), 0);
      ASSERT_EQ(pool->reclaim(kMaxMemory, 0, stats_), 0);
    }
    for (const auto& allocation : allocations) {
      allocation.pool->free(allocation.buffer, allocation.size);
    }
  }
}

TEST_P(MemoryPoolTest, usageTrackerOptionTest) {
  auto manager = getMemoryManager();
  auto root =
      manager->addRootPool("usageTrackerOptionTest", kMaxMemory, nullptr);
  ASSERT_TRUE(root->trackUsage());
  auto child = root->addLeafChild("usageTrackerOptionTest", isLeafThreadSafe_);
  ASSERT_TRUE(child->trackUsage());
  ASSERT_EQ(child->threadSafe(), isLeafThreadSafe_);
  ASSERT_TRUE(root->threadSafe());
  ASSERT_EQ(
      child->toString(),
      fmt::format(
          "Memory Pool[usageTrackerOptionTest LEAF root[usageTrackerOptionTest] parent[usageTrackerOptionTest] {} track-usage {}]<unlimited max capacity unlimited capacity used 0B available 0B reservation [used 0B, reserved 0B, min 0B] counters [allocs 0, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC",
          isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"));
  ASSERT_EQ(
      root->toString(),
      fmt::format(
          "Memory Pool[usageTrackerOptionTest AGGREGATE root[usageTrackerOptionTest] parent[null] {} track-usage thread-safe]<unlimited max capacity unlimited capacity used 0B available 0B reservation [used 0B, reserved 0B, min 0B] counters [allocs 0, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC"));
}

TEST_P(MemoryPoolTest, statsAndToString) {
  auto manager = getMemoryManager();
  auto root = manager->addRootPool("stats", 4 * GB);
  ASSERT_TRUE(root->threadSafe());
  auto leafChild1 = root->addLeafChild("leaf-child1", isLeafThreadSafe_);
  auto aggregateChild = root->addAggregateChild("aggregate-child");
  ASSERT_TRUE(aggregateChild->threadSafe());
  auto leafChild2 =
      aggregateChild->addLeafChild("leaf-child2", isLeafThreadSafe_);
  const int bufferSize = 1024;
  void* buf1 = leafChild1->allocate(bufferSize);
  ASSERT_EQ(
      leafChild1->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      leafChild1->toString(),
      fmt::format(
          "Memory Pool[leaf-child1 LEAF root[stats] parent[stats] {} track-usage {}]<max capacity 4.00GB capacity 4.00GB used 1.00KB available 1023.00KB reservation [used 1.00KB, reserved 1.00MB, min 0B] counters [allocs 1, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC",
          isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"));
  ASSERT_EQ(
      leafChild2->stats().toString(),
      "usedBytes:0B reservedBytes:0B peakBytes:0B cumulativeBytes:0B numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      leafChild1->toString(),
      fmt::format(
          "Memory Pool[leaf-child1 LEAF root[stats] parent[stats] {} track-usage {}]<max capacity 4.00GB capacity 4.00GB used 1.00KB available 1023.00KB reservation [used 1.00KB, reserved 1.00MB, min 0B] counters [allocs 1, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC",
          isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"));
  ASSERT_EQ(
      aggregateChild->stats().toString(),
      "usedBytes:0B reservedBytes:0B peakBytes:0B cumulativeBytes:0B numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      root->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00MB cumulativeBytes:1.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  void* buf2 = leafChild2->allocate(bufferSize);
  ASSERT_EQ(
      leafChild1->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      leafChild2->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      aggregateChild->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00MB cumulativeBytes:1.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      root->stats().toString(),
      "usedBytes:2.00KB reservedBytes:2.00MB peakBytes:2.00MB cumulativeBytes:2.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  leafChild1->free(buf1, bufferSize);
  ASSERT_EQ(
      leafChild1->stats().toString(),
      "usedBytes:0B reservedBytes:0B peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:1 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      leafChild2->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      aggregateChild->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:1.00MB cumulativeBytes:1.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  ASSERT_EQ(
      root->stats().toString(),
      "usedBytes:1.00KB reservedBytes:1.00MB peakBytes:2.00MB cumulativeBytes:2.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0 numCapacityGrowths:0");
  leafChild2->free(buf2, bufferSize);
  std::vector<void*> bufs;
  for (int i = 0; i < 10; ++i) {
    bufs.push_back(leafChild1->allocate(bufferSize));
  }
  ASSERT_EQ(root->stats().numAllocs, 0);
  ASSERT_EQ(root->stats().numFrees, 0);
  ASSERT_EQ(root->stats().numCollisions, 0);
  ASSERT_EQ(root->stats().numReclaims, 0);
  ASSERT_EQ(root->stats().peakBytes, 2097152);
  ASSERT_EQ(root->peakBytes(), 2097152);
  ASSERT_EQ(root->stats().cumulativeBytes, 3145728);
  ASSERT_EQ(root->stats().usedBytes, 10240);
  ASSERT_EQ(root->stats().numCapacityGrowths, 0);
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 1);
  ASSERT_EQ(leafChild1->stats().usedBytes, 10240);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().numReserves, 0);
  ASSERT_EQ(leafChild1->stats().numReleases, 0);
  ASSERT_EQ(leafChild1->stats().numCapacityGrowths, 0);
  for (auto* buf : bufs) {
    leafChild1->free(buf, bufferSize);
  }
  ASSERT_EQ(root->stats().numAllocs, 0);
  ASSERT_EQ(root->stats().numFrees, 0);
  ASSERT_EQ(root->stats().numCollisions, 0);
  ASSERT_EQ(root->stats().numReclaims, 0);
  ASSERT_EQ(root->stats().peakBytes, 2097152);
  ASSERT_EQ(root->peakBytes(), 2097152);
  ASSERT_EQ(root->stats().cumulativeBytes, 3145728);
  ASSERT_EQ(root->stats().usedBytes, 0);
  ASSERT_EQ(root->stats().numCapacityGrowths, 0);
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 11);
  ASSERT_EQ(leafChild1->stats().usedBytes, 0);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().numReserves, 0);
  ASSERT_EQ(leafChild1->stats().numReleases, 0);
  ASSERT_EQ(leafChild1->stats().numCapacityGrowths, 0);
  leafChild1->maybeReserve(bufferSize);
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 11);
  ASSERT_EQ(leafChild1->stats().usedBytes, 0);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->peakBytes(), 10240);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().numReserves, 1);
  ASSERT_EQ(leafChild1->stats().numReleases, 0);
  ASSERT_EQ(leafChild1->stats().numCapacityGrowths, 0);
  leafChild1->release();
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 11);
  ASSERT_EQ(leafChild1->stats().usedBytes, 0);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->peakBytes(), 10240);
  ASSERT_EQ(leafChild1->stats().numReserves, 1);
  ASSERT_EQ(leafChild1->stats().numReleases, 1);
  ASSERT_EQ(leafChild1->stats().numCapacityGrowths, 0);
}

struct Buffer {
  void* data;
  size_t length;
};

TEST_P(MemoryPoolTest, memoryUsageUpdateCheck) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  //  setupMemory({.allocatorCapacity = kMaxSize});
  setupMemory(
      {.allocatorCapacity = kMaxSize,
       .allocationSizeThresholdWithReservation = false,
       .arbitratorCapacity = kMaxSize,
       .arbitratorReservedCapacity = 128 << 20});

  auto manager = getMemoryManager();
  auto root = manager->addRootPool("memoryUsageUpdate", kMaxSize);

  auto child1 = root->addLeafChild("child1", isLeafThreadSafe_);
  auto child2 = root->addLeafChild("child2", isLeafThreadSafe_);

  ASSERT_THROW(child1->allocate(2 * kMaxSize), VeloxRuntimeError);

  ASSERT_EQ(root->stats().usedBytes, 0);
  ASSERT_EQ(root->stats().reservedBytes, 0);
  ASSERT_EQ(root->stats().cumulativeBytes, 0);
  ASSERT_EQ(root->reservedBytes(), 0);

  std::vector<Buffer> buffers;
  buffers.emplace_back(Buffer{child1->allocate(1000), 1000});
  // The memory pool do alignment internally.
  ASSERT_EQ(child1->stats().usedBytes, 1024);
  ASSERT_EQ(root->stats().reservedBytes, MB);
  ASSERT_EQ(child1->usedBytes(), 1024);
  ASSERT_EQ(child1->reservedBytes(), MB);
  ASSERT_EQ(child1->stats().reservedBytes, MB);
  ASSERT_EQ(child1->stats().cumulativeBytes, 1024);
  ASSERT_EQ(root->reservedBytes(), MB);
  ASSERT_EQ(root->stats().cumulativeBytes, MB);
  ASSERT_EQ(MB - 1024, child1->availableReservation());

  buffers.emplace_back(Buffer{child1->allocate(1000), 1000});
  ASSERT_EQ(child1->stats().usedBytes, 2048);
  ASSERT_EQ(child1->usedBytes(), 2048);
  ASSERT_EQ(child1->stats().reservedBytes, MB);
  ASSERT_EQ(root->reservedBytes(), MB);
  ASSERT_EQ(root->stats().usedBytes, 2048);
  ASSERT_EQ(root->stats().cumulativeBytes, MB);
  ASSERT_EQ(root->stats().reservedBytes, MB);

  buffers.emplace_back(Buffer{child1->allocate(MB), MB});
  ASSERT_EQ(child1->stats().usedBytes, 2048 + MB);
  ASSERT_EQ(child1->stats().reservedBytes, 2 * MB);
  ASSERT_EQ(child1->usedBytes(), 2048 + MB);
  ASSERT_EQ(root->reservedBytes(), 2 * MB);
  ASSERT_EQ(root->stats().usedBytes, 2048 + MB);
  ASSERT_EQ(root->stats().cumulativeBytes, 2 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 2 * MB);

  buffers.emplace_back(Buffer{child1->allocate(100 * MB), 100 * MB});
  ASSERT_EQ(child1->usedBytes(), 2048 + 101 * MB);
  ASSERT_EQ(child1->stats().usedBytes, 2048 + 101 * MB);
  ASSERT_EQ(child1->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(child1->reservedBytes(), 104 * MB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->usedBytes());
  // Larger sizes round up to next 8MB.
  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().usedBytes, 2048 + 101 * MB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[0].data, buffers[0].length);
  ASSERT_EQ(child1->usedBytes(), 1024 + 101 * MB);
  ASSERT_EQ(child1->stats().usedBytes, 1024 + 101 * MB);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * MB);
  ASSERT_EQ(child1->reservedBytes(), 104 * MB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->usedBytes());
  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().usedBytes, 1024 + 101 * MB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[2].data, buffers[2].length);
  ASSERT_EQ(child1->usedBytes(), 1024 + 100 * MB);
  ASSERT_EQ(child1->stats().usedBytes, 1024 + 100 * MB);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * MB);
  ASSERT_EQ(child1->reservedBytes(), 104 * MB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->usedBytes());
  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().usedBytes, 1024 + 100 * MB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[3].data, buffers[3].length);
  ASSERT_EQ(child1->usedBytes(), 1024);
  ASSERT_EQ(child1->stats().usedBytes, 1024);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * MB);
  ASSERT_EQ(child1->reservedBytes(), MB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->usedBytes());
  ASSERT_EQ(root->reservedBytes(), MB);
  ASSERT_EQ(root->stats().usedBytes, 1024);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[1].data, buffers[1].length);
  ASSERT_EQ(child1->usedBytes(), 0);
  ASSERT_EQ(child1->stats().usedBytes, 0);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * MB);
  ASSERT_EQ(child1->reservedBytes(), 0);
  ASSERT_EQ(child1->availableReservation(), 0);
  ASSERT_EQ(root->reservedBytes(), 0);
  ASSERT_EQ(root->stats().usedBytes, 0);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 0);
  ASSERT_EQ(root->availableReservation(), 0);

  ASSERT_EQ(root->stats().numAllocs, 0);
  ASSERT_EQ(root->stats().numFrees, 0);
  ASSERT_EQ(root->stats().numReserves, 0);
  ASSERT_EQ(root->stats().numReleases, 0);
  ASSERT_EQ(root->stats().numCollisions, 0);
  ASSERT_EQ(root->stats().numReclaims, 0);
  ASSERT_EQ(root->stats().numShrinks, 0);

  ASSERT_EQ(child1->stats().numAllocs, 5);
  ASSERT_EQ(child1->stats().numFrees, 4);
  ASSERT_EQ(child1->stats().numReserves, 0);
  ASSERT_EQ(child1->stats().numReleases, 0);
  ASSERT_EQ(child1->stats().numCollisions, 0);
  ASSERT_EQ(child1->stats().numReclaims, 0);
  ASSERT_EQ(child1->stats().numShrinks, 0);
}

TEST_P(MemoryPoolTest, maybeReserve) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  setupMemory(
      {.allocatorCapacity = kMaxSize,
       .arbitratorCapacity = kMaxSize,
       .arbitratorReservedCapacity = kMaxSize / 8});
  auto manager = getMemoryManager();
  auto root = manager->addRootPool("reserve", kMaxSize);

  auto child = root->addLeafChild("reserve", isLeafThreadSafe_);

  ASSERT_THROW(child->allocate(2 * kMaxSize), VeloxRuntimeError);

  child->maybeReserve(100 * MB);
  // The reservation child shows up as a reservation on the child and as an
  // allocation on the parent.
  ASSERT_EQ(child->usedBytes(), 0);
  ASSERT_EQ(child->stats().usedBytes, 0);
  ASSERT_EQ(child->stats().cumulativeBytes, 0);
  ASSERT_EQ(child->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(child->availableReservation(), 104 * MB);

  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().usedBytes, 0);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  std::vector<Buffer> buffers;
  buffers.emplace_back(Buffer{child->allocate(60 * MB), 60 * MB});
  ASSERT_EQ(child->usedBytes(), 60 * MB);
  ASSERT_EQ(child->stats().usedBytes, 60 * MB);
  ASSERT_EQ(child->reservedBytes(), 104 * MB);
  ASSERT_EQ(child->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->usedBytes());
  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  buffers.emplace_back(Buffer{child->allocate(70 * MB), 70 * MB});
  ASSERT_EQ(child->usedBytes(), 130 * MB);
  ASSERT_EQ(child->stats().usedBytes, 130 * MB);
  ASSERT_EQ(child->reservedBytes(), 136 * MB);
  ASSERT_EQ(child->stats().reservedBytes, 136 * MB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->usedBytes());
  // Extended and rounded up the reservation to then next 8MB.
  ASSERT_EQ(root->reservedBytes(), 136 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 136 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child->free(buffers[0].data, buffers[0].length);
  ASSERT_EQ(child->usedBytes(), 70 * MB);
  ASSERT_EQ(child->stats().usedBytes, 70 * MB);
  // Extended and rounded up the reservation to then next 8MB.
  ASSERT_EQ(child->reservedBytes(), 104 * MB);
  ASSERT_EQ(child->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->usedBytes());
  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child->free(buffers[1].data, buffers[1].length);

  // The reservation goes down to the explicitly made reservation.
  ASSERT_EQ(child->usedBytes(), 0);
  ASSERT_EQ(child->stats().usedBytes, 0);
  ASSERT_EQ(child->reservedBytes(), 104 * MB);
  ASSERT_EQ(child->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->usedBytes());
  ASSERT_EQ(root->reservedBytes(), 104 * MB);
  ASSERT_EQ(root->stats().reservedBytes, 104 * MB);
  ASSERT_EQ(root->availableReservation(), 0);

  child->release();
  ASSERT_EQ(child->usedBytes(), 0);
  ASSERT_EQ(child->stats().usedBytes, 0);
  ASSERT_EQ(child->reservedBytes(), 0);
  ASSERT_EQ(child->stats().reservedBytes, 0);
  ASSERT_EQ(child->availableReservation(), 0);
  ASSERT_EQ(root->reservedBytes(), 0);
  ASSERT_EQ(root->availableReservation(), 0);
  ASSERT_EQ(root->stats().reservedBytes, 0);

  ASSERT_EQ(root->stats().numAllocs, 0);
  ASSERT_EQ(root->stats().numFrees, 0);
  ASSERT_EQ(root->stats().numReserves, 0);
  ASSERT_EQ(root->stats().numReleases, 0);
  ASSERT_EQ(root->stats().numCollisions, 0);
  ASSERT_EQ(root->stats().numReclaims, 0);
  ASSERT_EQ(root->stats().numShrinks, 0);

  ASSERT_EQ(child->stats().numAllocs, 3);
  ASSERT_EQ(child->stats().numFrees, 2);
  ASSERT_EQ(child->stats().numReserves, 1);
  ASSERT_EQ(child->stats().numReleases, 1);
  ASSERT_EQ(child->stats().numCollisions, 0);
  ASSERT_EQ(child->stats().numReclaims, 0);
  ASSERT_EQ(child->stats().numShrinks, 0);
}

TEST_P(MemoryPoolTest, maybeReserveFailWithAbort) {
  constexpr int64_t kMaxSize = 1 * GB; // 1GB
  setupMemory(
      {.allocatorCapacity = kMaxSize,
       .arbitratorCapacity = kMaxSize,
       .arbitratorReservedCapacity = kMaxSize / 8,
       .arbitratorKind = "SHARED"});
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool(
      "maybeReserveFailWithAbort", kMaxSize, MemoryReclaimer::create());
  auto child = root->addLeafChild("maybeReserveFailWithAbort");
  // maybeReserve returns false if reservation fails.
  ASSERT_FALSE(child->maybeReserve(2 * kMaxSize));
  // maybeReserve throws if reservation fails and the memory pool is aborted.
  abortPool(child.get());
  ASSERT_TRUE(child->aborted());
  ASSERT_TRUE(root->aborted());
  VELOX_ASSERT_THROW(
      child->maybeReserve(2 * kMaxSize), "Manual MemoryPool Abortion");
}

DEBUG_ONLY_TEST_P(MemoryPoolTest, raceBetweenFreeAndFailedAllocation) {
  if (!isLeafThreadSafe_) {
    return;
  }
  setupMemory(
      {.allocatorCapacity = 1 * GB,
       .arbitratorCapacity = 1 * GB,
       .arbitratorReservedCapacity = 128 * MB});
  auto manager = getMemoryManager();
  auto root = manager->addRootPool("grow", 64 * MB);
  auto child = root->addLeafChild("grow", isLeafThreadSafe_);
  void* buffer1 = child->allocate(17 * MB);
  ASSERT_EQ(child->capacity(), 64 * MB);
  ASSERT_EQ(child->reservedBytes(), 20 * MB);
  int reservationAttempts{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(MemoryPool*)>([&](MemoryPool* /*unused*/) {
        ++reservationAttempts;
        // On the first reservation attempt for the second buffer allocation,
        // trigger to free the first allocated buffer which will cause the
        // first reservation attempt fails. The quantized reservation size of
        // the first attempt is 16MB which requires 20MB after the first
        // buffer free.
        if (reservationAttempts == 1) {
          // Inject to free the first allocated buffer while the
          child->free(buffer1, 17 * MB);
          return;
        }
        // On the second reservation attempt for the second buffer allocation,
        // reduce the memory pool's capacity to trigger the memory pool
        // capacity exceeded exception error which might leave unused
        // reservation bytes but zero used reservation if we don't do the
        // cleanup properly.
        if (reservationAttempts == 2) {
          static_cast<MemoryPoolImpl*>(root.get())->testingSetCapacity(16 * MB);
          return;
        }
        VELOX_UNREACHABLE("Unexpected code path");
      }));
  ASSERT_ANY_THROW(child->allocate(19 * MB));
}

TEST_P(MemoryPoolTest, quantizedSize) {
  struct {
    uint64_t inputSize;
    uint64_t quantizedSize;

    std::string debugString() const {
      return fmt::format(
          "inputSize {} quantizedSize {}",
          succinctBytes(inputSize),
          succinctBytes(quantizedSize));
    }
  } testSettings[] = {
      {0, 0},
      {1, MB},
      {KB, MB},
      {MB / 2, MB},
      {MB, MB},
      {MB + 1, 2 * MB},
      {3 * MB, 3 * MB},
      {3 * MB + 1, 4 * MB},
      {11 * MB, 11 * MB},
      {15 * MB + 1, 16 * MB},
      {16 * MB, 16 * MB},
      {16 * MB + 1, 20 * MB},
      {17 * MB + 1, 20 * MB},
      {23 * MB + 1, 24 * MB},
      {30 * MB + 1, 32 * MB},
      {64 * MB - 1, 64 * MB},
      {64 * MB, 64 * MB},
      {64 * MB + 1, 72 * MB},
      {80 * MB - 1, 80 * MB},
      {80 * MB + 1, 88 * MB},
      {88 * MB, 88 * MB},
      {GB + 1, GB + 8 * MB}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_EQ(
        MemoryPool::quantizedSize(testData.inputSize), testData.quantizedSize);
  }
}

namespace {
class MockMemoryReclaimer : public MemoryReclaimer {
 public:
  static std::unique_ptr<MockMemoryReclaimer> create(bool doThrow) {
    return std::unique_ptr<MockMemoryReclaimer>(
        new MockMemoryReclaimer(doThrow));
  }

  void abort(MemoryPool* pool, const std::exception_ptr& error) override {
    if (doThrow_) {
      VELOX_MEM_POOL_ABORTED("Memory pool aborted");
    }
  }

 private:
  explicit MockMemoryReclaimer(bool doThrow) : doThrow_(doThrow) {}

  const bool doThrow_;
};
} // namespace

TEST_P(MemoryPoolTest, abortAPI) {
  MemoryManager& manager = *getMemoryManager();
  std::vector<uint64_t> capacities = {kMaxMemory, 128 * MB};
  for (const auto& capacity : capacities) {
    SCOPED_TRACE(fmt::format("capacity {}", succinctBytes(capacity)));
    {
      auto rootPool = manager.addRootPool("abortAPI", capacity);
      ASSERT_FALSE(rootPool->aborted());
      VELOX_ASSERT_THROW(abortPool(rootPool.get()), "");
      ASSERT_FALSE(rootPool->aborted());
    }
    // The root memory pool with no child pool and default memory reclaimer.
    {
      auto rootPool =
          manager.addRootPool("abortAPI", capacity, MemoryReclaimer::create());
      ASSERT_FALSE(rootPool->aborted());
      {
        abortPool(rootPool.get());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(rootPool->aborted());
      {
        VELOX_ASSERT_THROW(
            abortPool(rootPool.get()),
            "Trying to set another abort error on an already aborted pool.");
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(rootPool->aborted());
    }
    // The root memory pool with child pools and default memory reclaimer.
    {
      auto rootPool =
          manager.addRootPool("abortAPI", capacity, MemoryReclaimer::create());
      ASSERT_FALSE(rootPool->aborted());
      auto leafPool = rootPool->addLeafChild(
          "leafAbortAPI", true, MemoryReclaimer::create());
      ASSERT_FALSE(leafPool->aborted());
      {
        VELOX_ASSERT_THROW(abortPool(leafPool.get()), "");
        ASSERT_TRUE(leafPool->aborted());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(leafPool->aborted());
      ASSERT_TRUE(rootPool->aborted());
      auto aggregatePool = rootPool->addAggregateChild(
          "aggregateAbortAPI", MemoryReclaimer::create());
      ASSERT_TRUE(aggregatePool->aborted());
      {
        VELOX_ASSERT_THROW(abortPool(aggregatePool.get()), "");
        ASSERT_TRUE(aggregatePool->aborted());
        ASSERT_TRUE(leafPool->aborted());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(aggregatePool->aborted());
      ASSERT_TRUE(leafPool->aborted());
      ASSERT_TRUE(rootPool->aborted());
      {
        VELOX_ASSERT_THROW(abortPool(rootPool.get()), "");
        ASSERT_TRUE(aggregatePool->aborted());
        ASSERT_TRUE(leafPool->aborted());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(aggregatePool->aborted());
      ASSERT_TRUE(leafPool->aborted());
      ASSERT_TRUE(rootPool->aborted());
    }
    // The root memory pool with no child pool and memory reclaimer support at
    // leaf.
    {
      auto rootPool =
          manager.addRootPool("abortAPI", capacity, MemoryReclaimer::create());
      ASSERT_FALSE(rootPool->aborted());
      auto leafPool = rootPool->addLeafChild(
          "leafAbortAPI", true, MockMemoryReclaimer::create(false));
      ASSERT_FALSE(leafPool->aborted());
      {
        abortPool(leafPool.get());
        ASSERT_TRUE(leafPool->aborted());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(leafPool->aborted());
      ASSERT_TRUE(rootPool->aborted());
      auto aggregatePool = rootPool->addAggregateChild(
          "aggregateAbortAPI", MemoryReclaimer::create());
      ASSERT_TRUE(aggregatePool->aborted());
      {
        VELOX_ASSERT_THROW(
            abortPool(aggregatePool.get()),
            "Trying to set another abort error on an already aborted pool.");
        ASSERT_TRUE(aggregatePool->aborted());
        ASSERT_TRUE(leafPool->aborted());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(aggregatePool->aborted());
      ASSERT_TRUE(leafPool->aborted());
      ASSERT_TRUE(rootPool->aborted());
      {
        VELOX_ASSERT_THROW(
            abortPool(rootPool.get()),
            "Trying to set another abort error on an already aborted pool.");
        ASSERT_TRUE(aggregatePool->aborted());
        ASSERT_TRUE(leafPool->aborted());
        ASSERT_TRUE(rootPool->aborted());
      }
      ASSERT_TRUE(aggregatePool->aborted());
      ASSERT_TRUE(leafPool->aborted());
      ASSERT_TRUE(rootPool->aborted());
    }
  }
}

TEST_P(MemoryPoolTest, abort) {
  MemoryManager& manager = *getMemoryManager();
  int64_t capacity = 4 * MB;
  // Abort throw from root.
  {
    auto rootPool = manager.addRootPool(
        "abort", capacity, MockMemoryReclaimer::create(true));
    ASSERT_FALSE(rootPool->aborted());
    VELOX_ASSERT_THROW(abortPool(rootPool.get()), "");
    ASSERT_TRUE(rootPool->aborted());
  }
  // Abort throw from leaf pool.
  {
    auto rootPool =
        manager.addRootPool("abort", capacity, MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    auto aggregatePool = rootPool->addAggregateChild(
        "aggregateAbort", MemoryReclaimer::create());
    ASSERT_FALSE(aggregatePool->aborted());
    auto leafPool = rootPool->addLeafChild(
        "leafAbort", true, MockMemoryReclaimer::create(true));
    ASSERT_FALSE(leafPool->aborted());
    VELOX_ASSERT_THROW(abortPool(leafPool.get()), "");
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    VELOX_ASSERT_THROW(abortPool(aggregatePool.get()), "");
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    VELOX_ASSERT_THROW(abortPool(rootPool.get()), "");
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
  }
  // Abort throw from aggregate pool.
  {
    auto rootPool =
        manager.addRootPool("abort", capacity, MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    auto aggregatePool = rootPool->addAggregateChild(
        "aggregateAbort", MemoryReclaimer::create());
    ASSERT_FALSE(aggregatePool->aborted());
    auto leafPool = rootPool->addLeafChild(
        "leafAbort", true, MockMemoryReclaimer::create(true));
    ASSERT_FALSE(leafPool->aborted());
    VELOX_ASSERT_THROW(abortPool(leafPool.get()), "");
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    VELOX_ASSERT_THROW(abortPool(aggregatePool.get()), "");
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    VELOX_ASSERT_THROW(abortPool(rootPool.get()), "");
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
  }
  // Abort from leaf with future wait.
  {
    auto rootPool =
        manager.addRootPool("abort", capacity, MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    auto aggregatePool = rootPool->addAggregateChild(
        "aggregateAbort", MemoryReclaimer::create());
    ASSERT_FALSE(aggregatePool->aborted());
    auto leafPool = rootPool->addLeafChild(
        "leafAbort", true, MockMemoryReclaimer::create(false));
    ASSERT_FALSE(leafPool->aborted());

    abortPool(leafPool.get());
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    ASSERT_TRUE(rootPool->aborted());
  }
  // Abort from aggregate with future wait.
  {
    auto rootPool =
        manager.addRootPool("abort", capacity, MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    auto aggregatePool = rootPool->addAggregateChild(
        "aggregateAbort", MemoryReclaimer::create());
    ASSERT_FALSE(aggregatePool->aborted());
    auto leafPool = rootPool->addLeafChild(
        "leafAbort", true, MockMemoryReclaimer::create(false));
    ASSERT_FALSE(leafPool->aborted());

    abortPool(aggregatePool.get());
    ASSERT_TRUE(leafPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    ASSERT_TRUE(rootPool->aborted());
  }
  // Abort from root with future wait.
  {
    auto rootPool =
        manager.addRootPool("abort", capacity, MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    auto aggregatePool = rootPool->addAggregateChild(
        "aggregateAbort", MemoryReclaimer::create());
    ASSERT_FALSE(aggregatePool->aborted());
    auto leafPool = rootPool->addLeafChild(
        "leafAbort", true, MockMemoryReclaimer::create(false));
    ASSERT_FALSE(leafPool->aborted());

    abortPool(rootPool.get());
    ASSERT_TRUE(rootPool->aborted());
    ASSERT_TRUE(aggregatePool->aborted());
    ASSERT_TRUE(rootPool->aborted());
  }
  // Allocation from an aborted memory pool.
  std::vector<bool> hasReclaimers = {false, true};
  for (bool hasReclaimer : hasReclaimers) {
    SCOPED_TRACE(fmt::format("hasReclaimer {}", hasReclaimer));
    {
      auto rootPool = manager.addRootPool(
          "abort",
          capacity,
          hasReclaimer ? MemoryReclaimer::create() : nullptr);
      ASSERT_FALSE(rootPool->aborted());
      auto aggregatePool = rootPool->addAggregateChild(
          "aggregateAbort", hasReclaimer ? MemoryReclaimer::create() : nullptr);
      ASSERT_FALSE(aggregatePool->aborted());
      auto leafPool = rootPool->addLeafChild(
          "leafAbort",
          true,
          hasReclaimer ? MockMemoryReclaimer::create(false) : nullptr);
      ASSERT_FALSE(leafPool->aborted());

      // Allocate some buffer from leaf.
      void* buf1 = leafPool->allocate(128);
      ASSERT_EQ(leafPool->usedBytes(), 128);

      // Abort the pool.
      ContinueFuture future;
      if (!hasReclaimer) {
        VELOX_ASSERT_THROW(abortPool(leafPool.get()), "");
        VELOX_ASSERT_THROW(abortPool(aggregatePool.get()), "");
        VELOX_ASSERT_THROW(abortPool(rootPool.get()), "");
        ASSERT_FALSE(leafPool->aborted());
        ASSERT_FALSE(aggregatePool->aborted());
        ASSERT_FALSE(rootPool->aborted());
        leafPool->free(buf1, 128);
        buf1 = leafPool->allocate(capacity / 2);
        leafPool->free(buf1, capacity / 2);
        continue;
      } else {
        abortPool(leafPool.get());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(aggregatePool->aborted());
      ASSERT_TRUE(rootPool->aborted());

      // Allocate more buffer to trigger reservation increment at the root.
      { VELOX_ASSERT_THROW(leafPool->allocate(capacity / 2), ""); }
      // Allocate more buffer to trigger memory arbitration at the root.
      { VELOX_ASSERT_THROW(leafPool->allocate(capacity * 2), ""); }
      // Allocate without trigger memory reservation increment.
      void* buf2 = leafPool->allocate(128);
      ASSERT_EQ(leafPool->usedBytes(), 256);
      leafPool->free(buf1, 128);
      leafPool->free(buf2, 128);
      ASSERT_EQ(leafPool->usedBytes(), 0);
      ASSERT_EQ(leafPool->capacity(), capacity);
    }
  }
}

TEST_P(MemoryPoolTest, overuseUnderArbitration) {
  constexpr int64_t kMaxSize = 128 * MB; // 1GB
  setupMemory(
      {.allocatorCapacity = kMaxSize,
       .arbitratorCapacity = kMaxSize,
       .arbitratorReservedCapacity = 4 * MB,
       .arbitratorKind = "SHARED"});
  MemoryManager& manager = *getMemoryManager();
  auto root = manager.addRootPool(
      "overuseUnderArbitration", kMaxSize, MemoryReclaimer::create());
  auto child = root->addLeafChild("overuseUnderArbitration");
  // maybeReserve returns false if reservation fails.
  ASSERT_FALSE(child->maybeReserve(2 * kMaxSize));
  ASSERT_EQ(child->usedBytes(), 0);
  ASSERT_EQ(child->reservedBytes(), 0);
  ScopedMemoryArbitrationContext scopedMemoryArbitration(child.get());
  ASSERT_TRUE(underMemoryArbitration());
  ASSERT_TRUE(child->maybeReserve(2 * kMaxSize));
  ASSERT_EQ(child->usedBytes(), 0);
  ASSERT_EQ(child->reservedBytes(), 2 * kMaxSize);
  child->release();
  ASSERT_EQ(child->usedBytes(), 0);
  ASSERT_EQ(child->reservedBytes(), 0);
}

TEST_P(MemoryPoolTest, allocationWithCoveredCollateral) {
  // Verify that the memory pool's reservation is correctly updated when an
  // allocation call is attempted with collateral that covers the allocation
  // (that is, the collateral is larger than the requested allocation).
  auto manager = getMemoryManager();
  auto root = manager->addRootPool("root", kMaxMemory, nullptr);
  ASSERT_TRUE(root->trackUsage());
  auto pool =
      root->addLeafChild("allocationWithCoveredCollateral", isLeafThreadSafe_);
  ASSERT_TRUE(pool->trackUsage());
  // Check non-contiguous allocation.
  ASSERT_EQ(pool->reservedBytes(), 0);
  Allocation allocation;
  pool->allocateNonContiguous(100, allocation);
  auto prevReservedBytes = pool->usedBytes();
  pool->allocateNonContiguous(50, allocation);
  ASSERT_LT(pool->usedBytes(), prevReservedBytes);
  pool->freeNonContiguous(allocation);

  // Check contiguous allocation.
  ContiguousAllocation contiguousAllocation;
  pool->allocateContiguous(100, contiguousAllocation);
  prevReservedBytes = pool->usedBytes();
  pool->allocateContiguous(50, contiguousAllocation);
  ASSERT_LT(pool->usedBytes(), prevReservedBytes);
  pool->freeContiguous(contiguousAllocation);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MemoryPoolTestSuite,
    MemoryPoolTest,
    testing::ValuesIn(MemoryPoolTest::getTestParams()));

} // namespace memory
} // namespace velox
} // namespace facebook
