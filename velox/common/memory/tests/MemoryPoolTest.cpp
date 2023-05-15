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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/testutil/TestValue.h"

DECLARE_bool(velox_memory_leak_check_enabled);
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
  static void SetUpTestCase() {
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
    const uint64_t kCapacity = 8UL << 30;
    if (useMmap_) {
      MmapAllocator::Options opts{8UL << 30};
      allocator_ = std::make_shared<MmapAllocator>(opts);
      if (useCache_) {
        cache_ =
            std::make_shared<AsyncDataCache>(allocator_, kCapacity, nullptr);
        MemoryAllocator::setDefaultInstance(cache_.get());
      } else {
        MemoryAllocator::setDefaultInstance(allocator_.get());
      }
    } else {
      allocator_ = MemoryAllocator::createDefaultInstance();
      if (useCache_) {
        cache_ =
            std::make_shared<AsyncDataCache>(allocator_, kCapacity, nullptr);
        MemoryAllocator::setDefaultInstance(cache_.get());
      } else {
        MemoryAllocator::setDefaultInstance(allocator_.get());
      }
    }
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;
  }

  void TearDown() override {
    allocator_->testingClearFailureInjection();
    MmapAllocator::setDefaultInstance(nullptr);
  }

  void reset() {
    TearDown();
    SetUp();
  }

  std::shared_ptr<IMemoryManager> getMemoryManager(int64_t quota) {
    return std::make_shared<MemoryManager>(
        IMemoryManager::Options{.capacity = quota});
  }

  std::shared_ptr<IMemoryManager> getMemoryManager(
      const IMemoryManager::Options& options) {
    return std::make_shared<MemoryManager>(options);
  }

  const int32_t maxMallocBytes_ = 3072;
  const bool useMmap_;
  const bool useCache_;
  const bool isLeafThreadSafe_;
  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<MemoryAllocator> allocator_;
  std::shared_ptr<AsyncDataCache> cache_;
};

TEST_P(MemoryPoolTest, Ctor) {
  constexpr uint16_t kAlignment = 64;
  MemoryManager manager{{.alignment = kAlignment, .capacity = 8 * GB}};
  const int64_t capacity = 4 * GB;
  auto root = manager.addRootPool("Ctor", 4 * GB);
  ASSERT_EQ(root->kind(), MemoryPool::Kind::kAggregate);
  ASSERT_EQ(root->currentBytes(), 0);
  ASSERT_EQ(root->parent(), nullptr);
  ASSERT_EQ(root->root(), root.get());
  ASSERT_EQ(root->capacity(), capacity);

  {
    auto fakeRoot = std::make_shared<MemoryPoolImpl>(
        &manager, "fake_root", MemoryPool::Kind::kAggregate, nullptr);
    // We can't construct an aggregate memory pool with non-thread safe.
    ASSERT_ANY_THROW(std::make_shared<MemoryPoolImpl>(
        &manager,
        "fake_root",
        MemoryPool::Kind::kAggregate,
        nullptr,
        nullptr,
        nullptr,
        MemoryPool::Options{.threadSafe = false}));
    ASSERT_EQ("fake_root", fakeRoot->name());
    ASSERT_EQ(
        static_cast<MemoryPoolImpl*>(root.get())->testingAllocator(),
        fakeRoot->testingAllocator());
    ASSERT_EQ(0, fakeRoot->currentBytes());
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
    ASSERT_EQ(favoriteChild.currentBytes(), 0);
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
}

TEST_P(MemoryPoolTest, AddChild) {
  MemoryManager manager{};
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
  childOne.reset();
  ASSERT_EQ(root->getChildCount(), 1);
  childOne = root->addLeafChild("child_one", isLeafThreadSafe_);
  ASSERT_EQ(root->getChildCount(), 2);
}

TEST_P(MemoryPoolTest, dropChild) {
  MemoryManager manager{};
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
    const MmapAllocator* mmapAllocator,
    MachinePageCount allocPages,
    size_t allocCount,
    bool threadSafe) {
  MemoryManager manager({.capacity = 8 * GB});
  const auto kPageSize = 4 * KB;

  auto root = manager.addRootPool();
  auto child = root->addLeafChild("elastic_quota", threadSafe);

  std::vector<void*> allocations;
  uint64_t totalPageAllocated = 0;
  uint64_t totalPageMapped = 0;
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

TEST_P(MemoryPoolTest, SmallMmapMemoryAllocation) {
  MmapAllocator::Options options;
  options.capacity = 8 * GB;
  auto mmapAllocator = std::make_shared<memory::MmapAllocator>(options);
  MemoryAllocator::setDefaultInstance(mmapAllocator.get());
  testMmapMemoryAllocation(mmapAllocator.get(), 6, 100, isLeafThreadSafe_);
  MemoryAllocator::setDefaultInstance(nullptr);
}

TEST_P(MemoryPoolTest, BigMmapMemoryAllocation) {
  MmapAllocator::Options options;
  options.capacity = 8 * GB;
  auto mmapAllocator = std::make_shared<memory::MmapAllocator>(options);
  MemoryAllocator::setDefaultInstance(mmapAllocator.get());
  testMmapMemoryAllocation(
      mmapAllocator.get(),
      mmapAllocator->sizeClasses().back() + 56,
      20,
      isLeafThreadSafe_);
  MemoryAllocator::setDefaultInstance(nullptr);
}

// Mainly tests how it updates the memory usage in Memorypool->
TEST_P(MemoryPoolTest, AllocTest) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->addRootPool();

  auto child = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};

  void* oneChunk = child->allocate(kChunkSize);
  ASSERT_EQ(reinterpret_cast<uint64_t>(oneChunk) % child->alignment(), 0);
  ASSERT_EQ(kChunkSize, child->currentBytes());
  ASSERT_EQ(kChunkSize, child->stats().peakBytes);

  void* threeChunks = child->allocate(3 * kChunkSize);
  ASSERT_EQ(4 * kChunkSize, child->currentBytes());
  ASSERT_EQ(4 * kChunkSize, child->stats().peakBytes);

  child->free(threeChunks, 3 * kChunkSize);
  ASSERT_EQ(kChunkSize, child->currentBytes());
  ASSERT_EQ(4 * kChunkSize, child->stats().peakBytes);

  child->free(oneChunk, kChunkSize);
  ASSERT_EQ(0, child->currentBytes());
  ASSERT_EQ(4 * kChunkSize, child->stats().peakBytes);
}

TEST_P(MemoryPoolTest, DISABLED_memoryLeakCheck) {
  gflags::FlagSaver flagSaver;
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->addRootPool();

  auto child = root->addLeafChild("elastic_quota", isLeafThreadSafe_);
  const int64_t kChunkSize{32L * MB};
  void* oneChunk = child->allocate(kChunkSize);
  FLAGS_velox_memory_leak_check_enabled = true;
  ASSERT_DEATH(child.reset(), "");
  child->free(oneChunk, kChunkSize);
}

TEST_P(MemoryPoolTest, ReallocTestSameSize) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->addRootPool();

  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};

  // Realloc the same size.

  void* oneChunk = pool->allocate(kChunkSize);
  ASSERT_EQ(kChunkSize, pool->currentBytes());
  ASSERT_EQ(kChunkSize, pool->stats().peakBytes);

  void* anotherChunk = pool->reallocate(oneChunk, kChunkSize, kChunkSize);
  ASSERT_EQ(kChunkSize, pool->currentBytes());
  ASSERT_EQ(2 * kChunkSize, pool->stats().peakBytes);

  pool->free(anotherChunk, kChunkSize);
  ASSERT_EQ(0, pool->currentBytes());
  ASSERT_EQ(2 * kChunkSize, pool->stats().peakBytes);
}

TEST_P(MemoryPoolTest, ReallocTestHigher) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->addRootPool();

  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};
  // Realloc higher.
  void* oneChunk = pool->allocate(kChunkSize);
  EXPECT_EQ(kChunkSize, pool->currentBytes());
  EXPECT_EQ(kChunkSize, pool->stats().peakBytes);

  void* threeChunks = pool->reallocate(oneChunk, kChunkSize, 3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool->currentBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);

  pool->free(threeChunks, 3 * kChunkSize);
  EXPECT_EQ(0, pool->currentBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);
}

TEST_P(MemoryPoolTest, ReallocTestLower) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->addRootPool();
  auto pool = root->addLeafChild("elastic_quota", isLeafThreadSafe_);

  const int64_t kChunkSize{32L * MB};
  // Realloc lower.
  void* threeChunks = pool->allocate(3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool->currentBytes());
  EXPECT_EQ(3 * kChunkSize, pool->stats().peakBytes);

  void* oneChunk = pool->reallocate(threeChunks, 3 * kChunkSize, kChunkSize);
  EXPECT_EQ(kChunkSize, pool->currentBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);

  pool->free(oneChunk, kChunkSize);
  EXPECT_EQ(0, pool->currentBytes());
  EXPECT_EQ(4 * kChunkSize, pool->stats().peakBytes);
}

TEST_P(MemoryPoolTest, allocateZeroFilled) {
  auto manager = getMemoryManager(8 * GB);
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
  ASSERT_EQ(0, pool->currentBytes());
}

TEST_P(MemoryPoolTest, alignmentCheck) {
  std::vector<uint16_t> alignments = {
      0,
      MemoryAllocator::kMinAlignment,
      MemoryAllocator::kMinAlignment * 2,
      MemoryAllocator::kMaxAlignment};
  for (const auto& alignment : alignments) {
    SCOPED_TRACE(fmt::format("alignment:{}", alignment));
    IMemoryManager::Options options;
    options.capacity = 8 * GB;
    options.alignment = alignment;
    auto manager =
        getMemoryManager({.alignment = alignment, .capacity = 8 * GB});
    auto pool = manager->addLeafPool("alignmentCheck");
    ASSERT_EQ(
        pool->alignment(),
        alignment == 0 ? MemoryAllocator::kMinAlignment : alignment);
    const int32_t kTestIterations = 10;
    for (int32_t i = 0; i < 10; ++i) {
      const int64_t bytesToAlloc = 1 + folly::Random::rand32() % (1 << 20);
      void* ptr = pool->allocate(bytesToAlloc);
      if (alignment != 0) {
        ASSERT_EQ(reinterpret_cast<uint64_t>(ptr) % alignment, 0);
      }
      pool->free(ptr, bytesToAlloc);
    }
    ASSERT_EQ(0, pool->currentBytes());
  }
}

TEST_P(MemoryPoolTest, MemoryCapExceptions) {
  MemoryManager manager{{.capacity = 127L * MB}};
  // Capping memory pool.
  {
    auto root = manager.addRootPool("MemoryCapExceptions", manager.capacity());
    auto pool = root->addLeafChild("static_quota", isLeafThreadSafe_);
    {
      ASSERT_EQ(0, pool->currentBytes());
      try {
        pool->allocate(128L * MB);
      } catch (const velox::VeloxRuntimeError& ex) {
        ASSERT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
        ASSERT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
        ASSERT_TRUE(ex.isRetriable());
        ASSERT_EQ(
            "\nExceeded memory pool cap of 127.00MB when requesting 128.00MB, memory manager cap is 127.00MB\nMemoryCapExceptions usage 0B peak 0B\n\nFailed memory pool: static_quota: 0B\n",
            ex.message());
      }
    }
  }

  // Capping memory manager.
  {
    auto root =
        manager.addRootPool("MemoryCapExceptions", 2 * manager.capacity());
    auto pool = root->addLeafChild("static_quota", isLeafThreadSafe_);
    {
      ASSERT_EQ(0, pool->currentBytes());
      try {
        pool->allocate(manager.capacity() + 1);
      } catch (const velox::VeloxRuntimeError& ex) {
        ASSERT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
        ASSERT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
        ASSERT_TRUE(ex.isRetriable());
        ASSERT_EQ(
            "\nExceeded memory manager cap of 127.00MB when requesting 127.00MB, memory pool cap is 254.00MB\nMemoryCapExceptions usage 0B peak 128.00MB\n\nFailed memory pool: static_quota: 0B\n",
            ex.message());
      }
    }
  }
}

TEST(MemoryPoolTest, GetAlignment) {
  {
    EXPECT_EQ(
        MemoryAllocator::kMaxAlignment,
        MemoryManager{{.capacity = 32 * MB}}.addRootPool()->alignment());
  }
  {
    MemoryManager manager{{.alignment = 64, .capacity = 32 * MB}};
    EXPECT_EQ(64, manager.addRootPool()->alignment());
  }
}

TEST_P(MemoryPoolTest, MemoryManagerGlobalCap) {
  MemoryManager manager{{.capacity = 32 * MB}};

  auto root = manager.addRootPool();
  auto pool = root->addAggregateChild("unbounded");
  auto child = pool->addLeafChild("unbounded", isLeafThreadSafe_);
  void* oneChunk = child->allocate(32L * MB);
  ASSERT_EQ(root->currentBytes(), 33554432L);
  EXPECT_THROW(child->allocate(32L * MB), velox::VeloxRuntimeError);
  ASSERT_EQ(root->currentBytes(), 33554432L);
  EXPECT_THROW(
      child->reallocate(oneChunk, 32L * MB, 64L * MB),
      velox::VeloxRuntimeError);
  ASSERT_EQ(root->currentBytes(), 33554432L);
  child->free(oneChunk, 32L * MB);
}

// Tests how child updates itself and its parent's memory usage
// and what it returns for currentBytes()/getMaxBytes and
// with memoryUsageTracker.
TEST_P(MemoryPoolTest, childUsageTest) {
  MemoryManager manager{{.capacity = 8 * GB}};
  auto root = manager.addRootPool();
  auto pool = root->addAggregateChild("main_pool");

  auto verifyUsage = [](std::vector<std::shared_ptr<MemoryPool>>& tree,
                        std::vector<int> currentBytes,
                        std::vector<int> maxBytes) {
    ASSERT_TRUE(
        tree.size() == currentBytes.size() && tree.size() == maxBytes.size());
    for (unsigned i = 0, e = tree.size(); i != e; ++i) {
      EXPECT_EQ(tree[i]->currentBytes(), currentBytes[i]) << i;
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
      tree,
      {1048576, 1048576, 0, 64, 0, 0, 0},
      {1048576, 1048576, 0, 64, 0, 0, 0});
  void* p5Chunk0 = tree[5]->allocate(64);
  verifyUsage(
      tree,
      {2097152, 1048576, 1048576, 64, 0, 64, 0},
      {2097152, 1048576, 1048576, 64, 0, 64, 0});

  tree[3]->free(p3Chunk0, 16);
  verifyUsage(
      tree,
      {1048576, 0, 1048576, 0, 0, 64, 0},
      {2097152, 1048576, 1048576, 64, 0, 64, 0});

  tree[5]->free(p5Chunk0, 64);
  verifyUsage(
      tree, {0, 0, 0, 0, 0, 0, 0}, {2097152, 1048576, 1048576, 64, 0, 64, 0});

  // Release all memory pool->
  tree.clear();

  std::vector<int64_t> expectedCurrentBytes({0, 0, 0, 0, 0, 0, 0});
  std::vector<int64_t> expectedMaxBytes({128, 64, 64, 64, 0, 64, 0});

  // Verify the stats still holds the correct stats.
  for (unsigned i = 0, e = tree.size(); i != e; ++i) {
    ASSERT_GE(tree[i]->currentBytes(), expectedCurrentBytes[i]);
    ASSERT_GE(tree[i]->stats().peakBytes, expectedMaxBytes[i]);
  }
}

TEST_P(MemoryPoolTest, getPreferredSize) {
  MemoryManager manager;
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
  MemoryManager manager;
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());

  EXPECT_EQ(1ULL << 32, pool.preferredSize((1ULL << 32) - 1));
  EXPECT_EQ(1ULL << 63, pool.preferredSize((1ULL << 62) - 1 + (1ULL << 62)));
}

TEST_P(MemoryPoolTest, allocatorOverflow) {
  MemoryManager manager;
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());
  StlAllocator<int64_t> alloc(pool);
  EXPECT_THROW(alloc.allocate(1ULL << 62), VeloxException);
  EXPECT_THROW(alloc.deallocate(nullptr, 1ULL << 62), VeloxException);
}

TEST_P(MemoryPoolTest, contiguousAllocate) {
  auto manager = getMemoryManager(8 * GB);
  auto pool = manager->addLeafPool("contiguousAllocate");
  const auto largestSizeClass =
      MemoryAllocator::getInstance()->largestSizeClass();
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
  const int32_t numIterations = 100;
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
              MemoryAllocator::getInstance()->largestSizeClass(),
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
  const MachinePageCount kMaxNumPages = 1 << 10;
  const auto kMemoryCapBytes = kMaxNumPages * AllocationTraits::kPageSize;
  auto manager = getMemoryManager(kMemoryCapBytes);
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
  auto manager = getMemoryManager(8 * GB);
  auto pool = manager->addLeafPool("badContiguousAllocation");
  constexpr MachinePageCount kAllocSize = 8;
  ContiguousAllocation allocation;
  ASSERT_THROW(pool->allocateContiguous(0, allocation), VeloxRuntimeError);
}

TEST_P(MemoryPoolTest, nonContiguousAllocate) {
  auto manager = getMemoryManager(8 * GB);
  auto pool = manager->addLeafPool("nonContiguousAllocate");
  const auto& sizeClasses = MemoryAllocator::getInstance()->sizeClasses();
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
              testData.minSizeClass,
              MemoryAllocator::getInstance()->largestSizeClass()));
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
              MemoryAllocator::getInstance()->largestSizeClass(),
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
  auto manager = getMemoryManager(16 * KB);
  auto pool = manager->addLeafPool("nonContiguousAllocateFail");

  EXPECT_THROW(pool->allocate(32 * KB), VeloxException);
  EXPECT_EQ(1, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  auto* buffer = pool->allocate(256);
  EXPECT_EQ(2, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  EXPECT_THROW(pool->reallocate(buffer, 256, 32 * KB), VeloxException);
  EXPECT_EQ(3, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  EXPECT_THROW(pool->allocateZeroFilled(32, 1 * KB), VeloxException);
  EXPECT_EQ(4, pool->stats().numAllocs);
  EXPECT_EQ(0, pool->stats().numFrees);

  // Free to reset to 0 allocation
  pool->free(buffer, 256);
  EXPECT_EQ(1, pool->stats().numFrees);

  Allocation allocation;
  EXPECT_THROW(pool->allocateNonContiguous(32, allocation, 1), VeloxException);
  EXPECT_EQ(5, pool->stats().numAllocs);
  EXPECT_EQ(1, pool->stats().numFrees);

  pool->allocateNonContiguous(2 /* 8KB */, allocation, 1);
  EXPECT_EQ(6, pool->stats().numAllocs);
  EXPECT_EQ(1, pool->stats().numFrees);

  EXPECT_THROW(
      pool->allocateNonContiguous(8 /* 32KB */, allocation, 1), VeloxException);
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
      pool->allocateContiguous(8 /* 32KB */, contiguousAllocation),
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
      pool->allocateContiguous(8 /* 32KB */, contiguousAllocation),
      VeloxException);
  EXPECT_EQ(12, pool->stats().numAllocs);
  EXPECT_EQ(5, pool->stats().numFrees);

  pool->freeContiguous(contiguousAllocation);
  EXPECT_EQ(6, pool->stats().numFrees);
}

TEST_P(MemoryPoolTest, nonContiguousAllocateWithOldAllocation) {
  auto manager = getMemoryManager(8 * GB);
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

    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "persistentNonContiguousAllocateFailure", isLeafThreadSafe_);
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    allocator_->testingSetFailureInjection(testData.injectedFailure, true);
    ASSERT_THROW(
        pool->allocateNonContiguous(testData.numNewPages, allocation),
        VeloxRuntimeError);
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, transientNonContiguousAllocateFailure) {
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
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "transientNonContiguousAllocateFailure", isLeafThreadSafe_);
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    allocator_->testingSetFailureInjection(testData.injectedFailure);
    if (useCache_) {
      pool->allocateNonContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.numNewPages);
    } else {
      ASSERT_THROW(
          pool->allocateNonContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateWithOldAllocation) {
  auto manager = getMemoryManager(8 * GB);
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
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "persistentContiguousAllocateFailure", isLeafThreadSafe_);
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    allocator_->testingSetFailureInjection(testData.injectedFailure, true);
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
    allocator_->testingClearFailureInjection();
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
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "transientContiguousAllocateFailure", isLeafThreadSafe_);
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    allocator_->testingSetFailureInjection(testData.injectedFailure);
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
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, badNonContiguousAllocation) {
  auto manager = getMemoryManager(8 * GB);
  auto pool = manager->addLeafPool("badNonContiguousAllocation");
  Allocation allocation;
  // Bad zero page allocation size.
  ASSERT_THROW(pool->allocateNonContiguous(0, allocation), VeloxRuntimeError);

  // Set the num of pages to allocate exceeds one PageRun limit.
  constexpr MachinePageCount kNumPages =
      Allocation::PageRun::kMaxPagesInRun + 1;
  ASSERT_THROW(
      pool->allocateNonContiguous(kNumPages, allocation), VeloxRuntimeError);
  pool->allocateNonContiguous(kNumPages - 1, allocation);
  ASSERT_GE(allocation.numPages(), kNumPages - 1);
  pool->freeNonContiguous(allocation);
}

TEST_P(MemoryPoolTest, nonContiguousAllocateExceedLimit) {
  const MachinePageCount kMaxNumPages = 1 << 10;
  const auto kMemoryCapBytes = kMaxNumPages * AllocationTraits::kPageSize;
  auto manager = getMemoryManager(kMemoryCapBytes);
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
}

TEST_P(MemoryPoolTest, nonContiguousAllocateError) {
  auto manager = getMemoryManager(8 * GB);
  auto pool = manager->addLeafPool("nonContiguousAllocateError");
  allocator_->testingSetFailureInjection(
      MemoryAllocator::InjectedFailure::kAllocate, true);
  constexpr MachinePageCount kAllocSize = 8;
  std::unique_ptr<Allocation> allocation(new Allocation());
  ASSERT_THROW(
      pool->allocateNonContiguous(kAllocSize, *allocation), VeloxRuntimeError);
  ASSERT_TRUE(allocation->empty());
  allocator_->testingClearFailureInjection();
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
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "mmapAllocatorCapAllocationError", isLeafThreadSafe_);

    allocator_->testingSetFailureInjection(
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
    allocator_->testingClearFailureInjection();
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
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "mmapAllocatorCapAllocationZeroFilledError", isLeafThreadSafe_);

    allocator_->testingSetFailureInjection(
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
    allocator_->testingClearFailureInjection();
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
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->addRootPool();
    auto pool = root->addLeafChild(
        "mmapAllocatorCapReallocateError", isLeafThreadSafe_);

    allocator_->testingSetFailureInjection(
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
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, validCheck) {
  auto manager = getMemoryManager(8 * GB);
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
  MemoryManager manager{{.capacity = kMaxMemory}};
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
    ASSERT_EQ(child->currentBytes(), 0);
    child->release();
    ASSERT_EQ(child->reservedBytes(), 0);
    ASSERT_EQ(child->availableReservation(), 0);
    ASSERT_EQ(child->currentBytes(), 0);
    ASSERT_LE(child->stats().peakBytes, child->stats().cumulativeBytes);
  }
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  childPools.clear();
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  ASSERT_EQ(root->stats().currentBytes, 0);
}

TEST_P(MemoryPoolTest, concurrentUpdatesToTheSamePool) {
  if (!isLeafThreadSafe_) {
    return;
  }
  constexpr int64_t kMaxMemory = 8 * GB;
  MemoryManager manager{{.capacity = kMaxMemory}};
  auto root = manager.addRootPool();

  const int32_t kNumThreads = 5;
  const int32_t kNumChildPools = 2;
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int32_t i = 0; i < kNumChildPools; ++i) {
    childPools.push_back(root->addLeafChild(fmt::format("{}", i)));
  }

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 1'000;
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
    ASSERT_EQ(child->currentBytes(), 0);
    child->release();
    ASSERT_EQ(child->reservedBytes(), 0);
    ASSERT_EQ(child->availableReservation(), 0);
    ASSERT_EQ(child->currentBytes(), 0);
    ASSERT_LE(child->stats().peakBytes, child->stats().cumulativeBytes);
  }
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  childPools.clear();
  ASSERT_LE(root->stats().peakBytes, root->stats().cumulativeBytes);
  ASSERT_EQ(root->stats().currentBytes, 0);
}

TEST_P(MemoryPoolTest, concurrentUpdateToSharedPools) {
  constexpr int64_t kMaxMemory = 10 * GB;
  MemoryManager manager{{.capacity = kMaxMemory}};
  const int32_t kNumThreads = FLAGS_velox_memory_num_shared_leaf_pools;

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 1'000;
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
    EXPECT_EQ(pool->currentBytes(), 0);
  }
}

TEST_P(MemoryPoolTest, concurrentPoolStructureAccess) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1234);
  constexpr int64_t kMaxMemory = 8 * GB;
  MemoryManager manager{{.capacity = kMaxMemory}};
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
  MemoryManager manager{};
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

TEST_P(MemoryPoolTest, shrinkAndGrowAPIs) {
  MemoryManager manager;
  std::vector<uint64_t> capacities = {kMaxMemory, 0, 128 * MB};
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
      ASSERT_ANY_THROW(leafPool->allocate(allocationSize));
      ASSERT_EQ(leafPool->shrink(0), 0);
      ASSERT_EQ(leafPool->shrink(allocationSize), 0);
      continue;
    }
    void* buffer = leafPool->allocate(allocationSize);
    if (capacity == kMaxMemory) {
      ASSERT_EQ(rootPool->freeBytes(), 0);
      ASSERT_EQ(leafPool->freeBytes(), 0);
      ASSERT_EQ(aggregationPool->freeBytes(), 0);
      ASSERT_ANY_THROW(leafPool->shrink(0));
      ASSERT_ANY_THROW(leafPool->shrink(allocationSize));
      ASSERT_ANY_THROW(leafPool->shrink(kMaxMemory));
      ASSERT_ANY_THROW(aggregationPool->shrink(0));
      ASSERT_ANY_THROW(aggregationPool->shrink(allocationSize));
      ASSERT_ANY_THROW(aggregationPool->shrink(kMaxMemory));
      ASSERT_ANY_THROW(rootPool->shrink(0));
      ASSERT_ANY_THROW(rootPool->shrink(allocationSize));
      ASSERT_ANY_THROW(rootPool->shrink(kMaxMemory));
      leafPool->free(buffer, allocationSize);
      ASSERT_ANY_THROW(leafPool->grow(allocationSize));
      ASSERT_ANY_THROW(aggregationPool->grow(allocationSize));
      ASSERT_ANY_THROW(rootPool->grow(allocationSize));
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
        ASSERT_EQ(leafPool->grow(allocationSize), expectedCapacity);
      } else if (i % 3 == 1) {
        ASSERT_EQ(aggregationPool->grow(allocationSize), expectedCapacity);
      } else {
        ASSERT_EQ(rootPool->grow(allocationSize), expectedCapacity);
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
  auto manager = getMemoryManager(kMaxMemory);
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
  MemoryManager manager;
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
      uint64_t reclaimableBytes{100};
      ASSERT_FALSE(pool->reclaimableBytes(reclaimableBytes));
      ASSERT_EQ(reclaimableBytes, 0);
      ASSERT_EQ(pool->reclaim(0), 0);
      ASSERT_EQ(pool->reclaim(100), 0);
      ASSERT_EQ(pool->reclaim(kMaxMemory), 0);
    }
    for (const auto& allocation : allocations) {
      allocation.pool->free(allocation.buffer, allocation.size);
    }
  }
}

TEST_P(MemoryPoolTest, usageTrackerOptionTest) {
  auto manager = getMemoryManager(8 * GB);
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
          "Memory Pool[usageTrackerOptionTest LEAF {} track-usage {}]<unlimited capacity used 0B available 0B reservation [used 0B, reserved 0B, min 0B] counters [allocs 0, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC",
          isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"));
  ASSERT_EQ(
      root->toString(),
      fmt::format(
          "Memory Pool[usageTrackerOptionTest AGGREGATE {} track-usage thread-safe]<unlimited capacity used 0B available 0B reservation [used 0B, reserved 0B, min 0B] counters [allocs 0, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC"));
}

TEST_P(MemoryPoolTest, statsAndToString) {
  auto manager = getMemoryManager(8 * GB);
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
      "currentBytes:1.00KB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      leafChild1->toString(),
      fmt::format(
          "Memory Pool[leaf-child1 LEAF {} track-usage {}]<capacity 4.00GB used 1.00KB available 1023.00KB reservation [used 1.00KB, reserved 1.00MB, min 0B] counters [allocs 1, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC",
          isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"));
  ASSERT_EQ(
      leafChild2->stats().toString(),
      "currentBytes:0B peakBytes:0B cumulativeBytes:0B numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      leafChild1->toString(),
      fmt::format(
          "Memory Pool[leaf-child1 LEAF {} track-usage {}]<capacity 4.00GB used 1.00KB available 1023.00KB reservation [used 1.00KB, reserved 1.00MB, min 0B] counters [allocs 1, frees 0, reserves 0, releases 0, collisions 0])>",
          useMmap_ ? "MMAP" : "MALLOC",
          isLeafThreadSafe_ ? "thread-safe" : "non-thread-safe"));
  ASSERT_EQ(
      aggregateChild->stats().toString(),
      "currentBytes:0B peakBytes:0B cumulativeBytes:0B numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      root->stats().toString(),
      "currentBytes:1.00MB peakBytes:1.00MB cumulativeBytes:1.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  void* buf2 = leafChild2->allocate(bufferSize);
  ASSERT_EQ(
      leafChild1->stats().toString(),
      "currentBytes:1.00KB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      leafChild2->stats().toString(),
      "currentBytes:1.00KB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      aggregateChild->stats().toString(),
      "currentBytes:1.00MB peakBytes:1.00MB cumulativeBytes:1.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      root->stats().toString(),
      "currentBytes:2.00MB peakBytes:2.00MB cumulativeBytes:2.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  leafChild1->free(buf1, bufferSize);
  ASSERT_EQ(
      leafChild1->stats().toString(),
      "currentBytes:0B peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:1 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      leafChild2->stats().toString(),
      "currentBytes:1.00KB peakBytes:1.00KB cumulativeBytes:1.00KB numAllocs:1 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      aggregateChild->stats().toString(),
      "currentBytes:1.00MB peakBytes:1.00MB cumulativeBytes:1.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
  ASSERT_EQ(
      root->stats().toString(),
      "currentBytes:1.00MB peakBytes:2.00MB cumulativeBytes:2.00MB numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numShrinks:0 numReclaims:0 numCollisions:0");
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
  ASSERT_EQ(root->stats().currentBytes, 1048576);
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 1);
  ASSERT_EQ(leafChild1->stats().currentBytes, 10240);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().numReserves, 0);
  ASSERT_EQ(leafChild1->stats().numReleases, 0);
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
  ASSERT_EQ(root->stats().currentBytes, 0);
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 11);
  ASSERT_EQ(leafChild1->stats().currentBytes, 0);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().numReserves, 0);
  ASSERT_EQ(leafChild1->stats().numReleases, 0);
  leafChild1->maybeReserve(bufferSize);
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 11);
  ASSERT_EQ(leafChild1->stats().currentBytes, 0);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->peakBytes(), 10240);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().numReserves, 1);
  ASSERT_EQ(leafChild1->stats().numReleases, 0);
  leafChild1->release();
  ASSERT_EQ(leafChild1->stats().numAllocs, 11);
  ASSERT_EQ(leafChild1->stats().numFrees, 11);
  ASSERT_EQ(leafChild1->stats().currentBytes, 0);
  ASSERT_EQ(leafChild1->stats().cumulativeBytes, 11264);
  ASSERT_EQ(leafChild1->stats().peakBytes, 10240);
  ASSERT_EQ(leafChild1->peakBytes(), 10240);
  ASSERT_EQ(leafChild1->stats().numReserves, 1);
  ASSERT_EQ(leafChild1->stats().numReleases, 1);
}

struct Buffer {
  void* data;
  size_t length;
};

TEST_P(MemoryPoolTest, memoryUsageUpdateCheck) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto manager = getMemoryManager(kMaxSize);
  auto root = manager->addRootPool("memoryUsageUpdate", kMaxSize);

  auto child1 = root->addLeafChild("child1", isLeafThreadSafe_);
  auto child2 = root->addLeafChild("child2", isLeafThreadSafe_);

  ASSERT_THROW(child1->allocate(2 * kMaxSize), VeloxRuntimeError);

  ASSERT_EQ(root->stats().currentBytes, 0);
  ASSERT_EQ(root->stats().cumulativeBytes, 0);
  ASSERT_EQ(root->reservedBytes(), 0);

  std::vector<Buffer> buffers;
  buffers.emplace_back(Buffer{child1->allocate(1000), 1000});
  // The memory pool do alignment internally.
  ASSERT_EQ(child1->stats().currentBytes, 1024);
  ASSERT_EQ(child1->currentBytes(), 1024);
  ASSERT_EQ(child1->reservedBytes(), kMB);
  ASSERT_EQ(child1->stats().cumulativeBytes, 1024);
  ASSERT_EQ(root->currentBytes(), kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, kMB);
  ASSERT_EQ(kMB - 1024, child1->availableReservation());

  buffers.emplace_back(Buffer{child1->allocate(1000), 1000});
  ASSERT_EQ(child1->stats().currentBytes, 2048);
  ASSERT_EQ(child1->currentBytes(), 2048);
  ASSERT_EQ(root->currentBytes(), kMB);
  ASSERT_EQ(root->stats().currentBytes, kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, kMB);

  buffers.emplace_back(Buffer{child1->allocate(kMB), kMB});
  ASSERT_EQ(child1->stats().currentBytes, 2048 + kMB);
  ASSERT_EQ(child1->currentBytes(), 2048 + kMB);
  ASSERT_EQ(root->currentBytes(), 2 * kMB);
  ASSERT_EQ(root->stats().currentBytes, 2 * kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, 2 * kMB);

  buffers.emplace_back(Buffer{child1->allocate(100 * kMB), 100 * kMB});
  ASSERT_EQ(child1->currentBytes(), 2048 + 101 * kMB);
  ASSERT_EQ(child1->stats().currentBytes, 2048 + 101 * kMB);
  ASSERT_EQ(child1->reservedBytes(), 104 * kMB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->currentBytes());
  // Larger sizes round up to next 8MB.
  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->stats().currentBytes, 104 * kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[0].data, buffers[0].length);
  ASSERT_EQ(child1->currentBytes(), 1024 + 101 * kMB);
  ASSERT_EQ(child1->stats().currentBytes, 1024 + 101 * kMB);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * kMB);
  ASSERT_EQ(child1->reservedBytes(), 104 * kMB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->currentBytes());
  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->stats().currentBytes, 104 * kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[2].data, buffers[2].length);
  ASSERT_EQ(child1->currentBytes(), 1024 + 100 * kMB);
  ASSERT_EQ(child1->stats().currentBytes, 1024 + 100 * kMB);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * kMB);
  ASSERT_EQ(child1->reservedBytes(), 104 * kMB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->currentBytes());
  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->stats().currentBytes, 104 * kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[3].data, buffers[3].length);
  ASSERT_EQ(child1->currentBytes(), 1024);
  ASSERT_EQ(child1->stats().currentBytes, 1024);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * kMB);
  ASSERT_EQ(child1->reservedBytes(), kMB);
  ASSERT_EQ(
      child1->availableReservation(),
      child1->reservedBytes() - child1->currentBytes());
  ASSERT_EQ(root->currentBytes(), kMB);
  ASSERT_EQ(root->stats().currentBytes, kMB);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child1->free(buffers[1].data, buffers[1].length);
  ASSERT_EQ(child1->currentBytes(), 0);
  ASSERT_EQ(child1->stats().currentBytes, 0);
  ASSERT_EQ(child1->stats().cumulativeBytes, 2048 + 101 * kMB);
  ASSERT_EQ(child1->reservedBytes(), 0);
  ASSERT_EQ(child1->availableReservation(), 0);
  ASSERT_EQ(root->currentBytes(), 0);
  ASSERT_EQ(root->stats().currentBytes, 0);
  ASSERT_EQ(root->stats().cumulativeBytes, 104 * kMB);
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
  constexpr int64_t kMB = 1 << 20;
  auto manager = getMemoryManager(kMaxSize);
  auto root = manager->addRootPool("reserve", kMaxSize);

  auto child = root->addLeafChild("reserve", isLeafThreadSafe_);

  ASSERT_THROW(child->allocate(2 * kMaxSize), VeloxRuntimeError);

  child->maybeReserve(100 * kMB);
  // The reservation child shows up as a reservation on the child and as an
  // allocation on the parent.
  ASSERT_EQ(child->currentBytes(), 0);
  ASSERT_EQ(child->stats().currentBytes, 0);
  ASSERT_EQ(child->stats().cumulativeBytes, 0);
  ASSERT_EQ(child->availableReservation(), 104 * kMB);

  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->stats().currentBytes, 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  std::vector<Buffer> buffers;
  buffers.emplace_back(Buffer{child->allocate(60 * kMB), 60 * kMB});
  ASSERT_EQ(child->currentBytes(), 60 * kMB);
  ASSERT_EQ(child->stats().currentBytes, 60 * kMB);
  ASSERT_EQ(child->reservedBytes(), 104 * kMB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->currentBytes());
  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  buffers.emplace_back(Buffer{child->allocate(70 * kMB), 70 * kMB});
  ASSERT_EQ(child->currentBytes(), 130 * kMB);
  ASSERT_EQ(child->stats().currentBytes, 130 * kMB);
  ASSERT_EQ(child->reservedBytes(), 136 * kMB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->currentBytes());
  // Extended and rounded up the reservation to then next 8MB.
  ASSERT_EQ(root->currentBytes(), 136 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child->free(buffers[0].data, buffers[0].length);
  ASSERT_EQ(child->currentBytes(), 70 * kMB);
  ASSERT_EQ(child->stats().currentBytes, 70 * kMB);
  // Extended and rounded up the reservation to then next 8MB.
  ASSERT_EQ(child->reservedBytes(), 104 * kMB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->currentBytes());
  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child->free(buffers[1].data, buffers[1].length);

  // The reservation goes down to the explicitly made reservation.
  ASSERT_EQ(child->currentBytes(), 0);
  ASSERT_EQ(child->stats().currentBytes, 0);
  ASSERT_EQ(child->reservedBytes(), 104 * kMB);
  ASSERT_EQ(
      child->availableReservation(),
      child->reservedBytes() - child->currentBytes());
  ASSERT_EQ(root->currentBytes(), 104 * kMB);
  ASSERT_EQ(root->availableReservation(), 0);

  child->release();
  ASSERT_EQ(child->currentBytes(), 0);
  ASSERT_EQ(child->stats().currentBytes, 0);
  ASSERT_EQ(child->reservedBytes(), 0);
  ASSERT_EQ(child->availableReservation(), 0);
  ASSERT_EQ(root->currentBytes(), 0);
  ASSERT_EQ(root->availableReservation(), 0);

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

// Model implementation of a GrowCallback.
bool grow(int64_t size, int64_t hardLimit, MemoryPool& pool) {
  static std::mutex mutex;
  // The calls from different threads on the same tracker must be serialized.
  std::lock_guard<std::mutex> l(mutex);
  // The total includes the allocation that exceeded the limit. This function's
  // job is to raise the limit to >= current + size.
  auto current = pool.reservedBytes();
  auto limit = pool.capacity();
  if (current + size <= limit) {
    // No need to increase. It could be another thread already
    // increased the cap far enough while this thread was waiting to
    // enter the lock_guard.
    return true;
  }
  if (current + size > hardLimit) {
    // The caller will revert the allocation that called this and signal an
    // error.
    return false;
  }
  // We set the new limit to be the requested size.
  static_cast<MemoryPoolImpl*>(&pool)->testingSetCapacity(current + size);
  return true;
}

DEBUG_ONLY_TEST_P(MemoryPoolTest, raceBetweenFreeAndFailedAllocation) {
  if (!isLeafThreadSafe_) {
    return;
  }
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto manager = getMemoryManager(kMaxSize);
  auto root = manager->addRootPool("grow", 64 * kMB);
  auto child = root->addLeafChild("grow", isLeafThreadSafe_);
  void* buffer1 = child->allocate(17 * kMB);
  ASSERT_EQ(child->capacity(), 64 * kMB);
  ASSERT_EQ(child->reservedBytes(), 20 * kMB);
  int reservationAttempts{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(MemoryPool*)>([&](MemoryPool* /*unused*/) {
        ++reservationAttempts;
        // On the first reservation attempt for the second buffer allocation,
        // trigger to free the first allocated buffer which will cause the first
        // reservation attempt fails. The quantized reservation size of the
        // first attempt is 16MB which requires 20MB after the first buffer
        // free.
        if (reservationAttempts == 1) {
          // Inject to free the first allocated buffer while the
          child->free(buffer1, 17 * kMB);
          return;
        }
        // On the second reservation attempt for the second buffer allocation,
        // reduce the memory pool's capacity to trigger the memory pool capacity
        // exceeded exception error which might leave unused reservation bytes
        // but zero used reservation if we don't do the cleanup properly.
        if (reservationAttempts == 2) {
          static_cast<MemoryPoolImpl*>(root.get())
              ->testingSetCapacity(16 * kMB);
          return;
        }
        VELOX_UNREACHABLE("Unexpected code path");
      }));
  ASSERT_ANY_THROW(child->allocate(19 * kMB));
}

TEST_P(MemoryPoolTest, quantizedSize) {
  const uint64_t kKB = 1 << 10;
  const uint64_t kMB = kKB << 10;
  const uint64_t kGB = kMB << 10;
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
      {1, kMB},
      {kKB, kMB},
      {kMB / 2, kMB},
      {kMB, kMB},
      {kMB + 1, 2 * kMB},
      {3 * kMB, 3 * kMB},
      {3 * kMB + 1, 4 * kMB},
      {11 * kMB, 11 * kMB},
      {15 * kMB + 1, 16 * kMB},
      {16 * kMB, 16 * kMB},
      {16 * kMB + 1, 20 * kMB},
      {17 * kMB + 1, 20 * kMB},
      {23 * kMB + 1, 24 * kMB},
      {30 * kMB + 1, 32 * kMB},
      {64 * kMB - 1, 64 * kMB},
      {64 * kMB, 64 * kMB},
      {64 * kMB + 1, 72 * kMB},
      {80 * kMB - 1, 80 * kMB},
      {80 * kMB + 1, 88 * kMB},
      {88 * kMB, 88 * kMB},
      {kGB + 1, kGB + 8 * kMB}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_EQ(
        MemoryPool::quantizedSize(testData.inputSize), testData.quantizedSize);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MemoryPoolTestSuite,
    MemoryPoolTest,
    testing::ValuesIn(MemoryPoolTest::getTestParams()));

} // namespace memory
} // namespace velox
} // namespace facebook
