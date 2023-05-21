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

#include <gtest/gtest.h>

#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"

DECLARE_int32(velox_memory_num_shared_leaf_pools);

using namespace ::testing;

namespace facebook {
namespace velox {
namespace memory {

namespace {
constexpr folly::StringPiece kDefaultRootName{"__default_root__"};

MemoryManager& toMemoryManager(IMemoryManager& manager) {
  return *static_cast<MemoryManager*>(&manager);
}
} // namespace

TEST(MemoryManagerTest, Ctor) {
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools;
  {
    MemoryManager manager{};
    ASSERT_EQ(manager.numPools(), 0);
    ASSERT_EQ(manager.capacity(), kMaxMemory);
    ASSERT_EQ(0, manager.getTotalBytes());
    ASSERT_EQ(manager.alignment(), MemoryAllocator::kMaxAlignment);
    ASSERT_EQ(manager.testingDefaultRoot().alignment(), manager.alignment());
    ASSERT_EQ(manager.arbitrator(), nullptr);
  }
  {
    MemoryManager manager{{.capacity = 8L * 1024 * 1024}};
    ASSERT_EQ(8L * 1024 * 1024, manager.capacity());
    ASSERT_EQ(manager.numPools(), 0);
    ASSERT_EQ(0, manager.getTotalBytes());
    ASSERT_EQ(manager.testingDefaultRoot().alignment(), manager.alignment());
  }
  {
    MemoryManager manager{{.alignment = 0, .capacity = 8L * 1024 * 1024}};

    ASSERT_EQ(manager.alignment(), MemoryAllocator::kMinAlignment);
    ASSERT_EQ(manager.testingDefaultRoot().alignment(), manager.alignment());
    // TODO: replace with root pool memory tracker quota check.
    ASSERT_EQ(kSharedPoolCount, manager.testingDefaultRoot().getChildCount());
    ASSERT_EQ(8L * 1024 * 1024, manager.capacity());
    ASSERT_EQ(0, manager.getTotalBytes());
  }
  { ASSERT_ANY_THROW(MemoryManager manager{{.capacity = -1}}); }
  {
    IMemoryManager::Options options;
    options.capacity = 32L << 30;
    options.arbitratorConfig.kind = MemoryArbitrator::Kind::kShared;
    // The arbitrator capacity will be overridden by the memory manager's
    // capacity.
    options.arbitratorConfig.capacity = folly::Random::rand32();
    MemoryManager manager{options};
    auto* arbitrator = manager.arbitrator();
    ASSERT_EQ(arbitrator->kind(), MemoryArbitrator::Kind::kShared);
    ASSERT_EQ(arbitrator->stats().maxCapacityBytes, 32L << 30);
  }
}

TEST(MemoryManagerTest, addPool) {
  MemoryManager manager{};

  auto rootPool = manager.addRootPool("duplicateRootPool", kMaxMemory);
  { ASSERT_ANY_THROW(manager.addRootPool("duplicateRootPool", kMaxMemory)); }
  auto threadSafeLeafPool = manager.addLeafPool("leafPool", true);
  auto nonThreadSafeLeafPool = manager.addLeafPool("duplicateLeafPool", true);
  { ASSERT_ANY_THROW(manager.addLeafPool("duplicateLeafPool")); }
}

TEST(MemoryManagerTest, defaultMemoryManager) {
  auto& managerA = toMemoryManager(defaultMemoryManager());
  auto& managerB = toMemoryManager(defaultMemoryManager());
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools;
  ASSERT_EQ(managerA.numPools(), 0);
  ASSERT_EQ(managerA.testingDefaultRoot().getChildCount(), kSharedPoolCount);
  ASSERT_EQ(managerB.numPools(), 0);
  ASSERT_EQ(managerB.testingDefaultRoot().getChildCount(), kSharedPoolCount);

  auto child1 = managerA.addLeafPool("child_1");
  ASSERT_EQ(child1->parent()->name(), managerA.testingDefaultRoot().name());
  auto child2 = managerB.addLeafPool("child_2");
  ASSERT_EQ(child2->parent()->name(), managerA.testingDefaultRoot().name());
  EXPECT_EQ(
      kSharedPoolCount + 2, managerA.testingDefaultRoot().getChildCount());
  EXPECT_EQ(
      kSharedPoolCount + 2, managerB.testingDefaultRoot().getChildCount());
  ASSERT_EQ(managerA.numPools(), 2);
  ASSERT_EQ(managerB.numPools(), 2);
  auto pool = managerB.addRootPool();
  ASSERT_EQ(managerA.numPools(), 3);
  ASSERT_EQ(managerB.numPools(), 3);
  ASSERT_EQ(
      managerA.toString(),
      "Memory Manager[capacity 8388608.00TB alignment 64B usedBytes 0B number of pools 3\nList of root pools:\n\t__default_root__\n\tdefault_root_0\n]");
  ASSERT_EQ(
      managerB.toString(),
      "Memory Manager[capacity 8388608.00TB alignment 64B usedBytes 0B number of pools 3\nList of root pools:\n\t__default_root__\n\tdefault_root_0\n]");
  child1.reset();
  EXPECT_EQ(
      kSharedPoolCount + 1, managerA.testingDefaultRoot().getChildCount());
  child2.reset();
  EXPECT_EQ(kSharedPoolCount, managerB.testingDefaultRoot().getChildCount());
  ASSERT_EQ(managerA.numPools(), 1);
  ASSERT_EQ(managerB.numPools(), 1);
  pool.reset();
  ASSERT_EQ(managerA.numPools(), 0);
  ASSERT_EQ(managerB.numPools(), 0);
  ASSERT_EQ(
      managerA.toString(),
      "Memory Manager[capacity 8388608.00TB alignment 64B usedBytes 0B number of pools 0\nList of root pools:\n\t__default_root__\n]");
  ASSERT_EQ(
      managerB.toString(),
      "Memory Manager[capacity 8388608.00TB alignment 64B usedBytes 0B number of pools 0\nList of root pools:\n\t__default_root__\n]");
}

TEST(MemoryHeaderTest, addDefaultLeafMemoryPool) {
  auto& manager = toMemoryManager(defaultMemoryManager());
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools;
  ASSERT_EQ(manager.testingDefaultRoot().getChildCount(), kSharedPoolCount);
  {
    auto poolA = addDefaultLeafMemoryPool();
    ASSERT_EQ(poolA->kind(), MemoryPool::Kind::kLeaf);
    auto poolB = addDefaultLeafMemoryPool();
    ASSERT_EQ(poolB->kind(), MemoryPool::Kind::kLeaf);
    EXPECT_EQ(
        kSharedPoolCount + 2, manager.testingDefaultRoot().getChildCount());
    {
      auto poolC = addDefaultLeafMemoryPool();
      ASSERT_EQ(poolC->kind(), MemoryPool::Kind::kLeaf);
      EXPECT_EQ(
          kSharedPoolCount + 3, manager.testingDefaultRoot().getChildCount());
      {
        auto poolD = addDefaultLeafMemoryPool();
        ASSERT_EQ(poolD->kind(), MemoryPool::Kind::kLeaf);
        EXPECT_EQ(
            kSharedPoolCount + 4, manager.testingDefaultRoot().getChildCount());
      }
      EXPECT_EQ(
          kSharedPoolCount + 3, manager.testingDefaultRoot().getChildCount());
    }
    EXPECT_EQ(
        kSharedPoolCount + 2, manager.testingDefaultRoot().getChildCount());
  }
  EXPECT_EQ(kSharedPoolCount, manager.testingDefaultRoot().getChildCount());

  auto namedPool = addDefaultLeafMemoryPool("namedPool");
  ASSERT_EQ(namedPool->name(), "namedPool");
}

TEST(MemoryManagerTest, memoryPoolManagement) {
  const int alignment = 32;
  IMemoryManager::Options options;
  options.alignment = alignment;
  MemoryManager manager{options};
  ASSERT_EQ(manager.numPools(), 0);
  const int numPools = 100;
  std::vector<std::shared_ptr<MemoryPool>> userRootPools;
  std::vector<std::shared_ptr<MemoryPool>> userLeafPools;
  for (int i = 0; i < numPools; ++i) {
    const std::string name(std::to_string(i));
    auto pool = i % 2 ? manager.addLeafPool(name) : manager.addRootPool(name);
    ASSERT_EQ(pool->name(), name);
    if (i % 2) {
      ASSERT_EQ(pool->kind(), MemoryPool::Kind::kLeaf);
      userLeafPools.push_back(pool);
      ASSERT_EQ(pool->parent()->name(), manager.testingDefaultRoot().name());
    } else {
      ASSERT_EQ(pool->kind(), MemoryPool::Kind::kAggregate);
      ASSERT_EQ(pool->parent(), nullptr);
      userRootPools.push_back(pool);
    }
  }
  auto leafUnamedPool = manager.addLeafPool();
  ASSERT_FALSE(leafUnamedPool->name().empty());
  ASSERT_EQ(leafUnamedPool->kind(), MemoryPool::Kind::kLeaf);
  auto rootUnamedPool = manager.addRootPool();
  ASSERT_FALSE(rootUnamedPool->name().empty());
  ASSERT_EQ(rootUnamedPool->kind(), MemoryPool::Kind::kAggregate);
  ASSERT_EQ(rootUnamedPool->parent(), nullptr);
  ASSERT_EQ(manager.numPools(), numPools + 2);
  userLeafPools.clear();
  leafUnamedPool.reset();
  ASSERT_EQ(manager.numPools(), numPools / 2 + 1);
  userRootPools.clear();
  ASSERT_EQ(manager.numPools(), 1);
  rootUnamedPool.reset();
  ASSERT_EQ(manager.numPools(), 0);
}

// TODO: when run sequentially, e.g. `buck run dwio/memory/...`, this has side
// effects for other tests using process singleton memory manager. Might need to
// use folly::Singleton for isolation by tag.
TEST(MemoryManagerTest, globalMemoryManager) {
  auto& manager = MemoryManager::getInstance();
  auto& managerII = MemoryManager::getInstance();
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools;
  {
    auto& rootI = manager.testingDefaultRoot();
    const std::string childIName("some_child");
    auto childI = rootI.addLeafChild(childIName);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);

    auto& rootII = managerII.testingDefaultRoot();
    ASSERT_EQ(kSharedPoolCount + 1, rootII.getChildCount());
    std::vector<MemoryPool*> pools{};
    rootII.visitChildren([&pools](MemoryPool* child) {
      pools.emplace_back(child);
      return true;
    });
    ASSERT_EQ(pools.size(), kSharedPoolCount + 1);
    int matchedCount = 0;
    for (const auto* pool : pools) {
      if (pool->name() == childIName) {
        ++matchedCount;
      }
    }
    ASSERT_EQ(matchedCount, 1);

    auto childII = manager.addLeafPool("another_child");
    ASSERT_EQ(childII->kind(), MemoryPool::Kind::kLeaf);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 2);
    ASSERT_EQ(childII->parent()->name(), kDefaultRootName.str());
    childII.reset();
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(rootII.getChildCount(), kSharedPoolCount + 1);
    auto userRootChild = manager.addRootPool("rootChild");
    ASSERT_EQ(userRootChild->kind(), MemoryPool::Kind::kAggregate);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(rootII.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(manager.numPools(), 2);
  }
  ASSERT_EQ(manager.numPools(), 0);
  {
    auto& manager = MemoryManager::getInstance();
    auto& defaultManager = defaultMemoryManager();
    ASSERT_EQ(&manager, &defaultManager);
    auto pool = addDefaultLeafMemoryPool();
    ASSERT_EQ(pool->kind(), MemoryPool::Kind::kLeaf);
    ASSERT_EQ(pool->parent()->name(), kDefaultRootName.str());
    ASSERT_EQ(manager.numPools(), 1);
    ASSERT_EQ(
        manager.testingDefaultRoot().getChildCount(), kSharedPoolCount + 1);
    pool.reset();
    ASSERT_EQ(manager.testingDefaultRoot().getChildCount(), kSharedPoolCount);
  }
  ASSERT_EQ(manager.numPools(), 0);
}

TEST(MemoryManagerTest, Reserve) {
  {
    MemoryManager manager{};
    ASSERT_TRUE(manager.reserve(0));
    ASSERT_EQ(0, manager.getTotalBytes());
    manager.release(0);
    ASSERT_TRUE(manager.reserve(42));
    ASSERT_EQ(42, manager.getTotalBytes());
    manager.release(42);
    ASSERT_TRUE(manager.reserve(std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), manager.getTotalBytes());
    manager.release(std::numeric_limits<int64_t>::max());
    ASSERT_EQ(0, manager.getTotalBytes());
  }
  {
    MemoryManager manager{{.capacity = 42}};
    ASSERT_TRUE(manager.reserve(1));
    ASSERT_TRUE(manager.reserve(1));
    ASSERT_TRUE(manager.reserve(2));
    ASSERT_TRUE(manager.reserve(3));
    ASSERT_TRUE(manager.reserve(5));
    ASSERT_TRUE(manager.reserve(8));
    ASSERT_TRUE(manager.reserve(13));
    ASSERT_FALSE(manager.reserve(21));
    ASSERT_FALSE(manager.reserve(1));
    ASSERT_FALSE(manager.reserve(2));
    ASSERT_FALSE(manager.reserve(3));
    manager.release(20);
    ASSERT_TRUE(manager.reserve(1));
    ASSERT_FALSE(manager.reserve(2));
    manager.release(manager.getTotalBytes());
    ASSERT_EQ(manager.getTotalBytes(), 0);
  }
}

TEST(MemoryManagerTest, GlobalMemoryManagerQuota) {
  auto& manager = MemoryManager::getInstance();
  ASSERT_THROW(
      MemoryManager::getInstance({.capacity = 42}, true),
      velox::VeloxUserError);

  auto& coercedManager = MemoryManager::getInstance({.capacity = 42});
  ASSERT_EQ(manager.capacity(), coercedManager.capacity());
}

TEST(MemoryManagerTest, alignmentOptionCheck) {
  struct {
    uint16_t alignment;
    bool expectedSuccess;

    std::string debugString() const {
      return fmt::format(
          "alignment:{}, expectedSuccess:{}", alignment, expectedSuccess);
    }
  } testSettings[] = {
      {0, true},
      {MemoryAllocator::kMinAlignment - 1, true},
      {MemoryAllocator::kMinAlignment, true},
      {MemoryAllocator::kMinAlignment * 2, true},
      {MemoryAllocator::kMinAlignment + 1, false},
      {MemoryAllocator::kMaxAlignment - 1, false},
      {MemoryAllocator::kMaxAlignment, true},
      {MemoryAllocator::kMaxAlignment + 1, false},
      {MemoryAllocator::kMaxAlignment * 2, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    IMemoryManager::Options options;
    options.alignment = testData.alignment;
    if (!testData.expectedSuccess) {
      ASSERT_THROW(MemoryManager{options}, VeloxRuntimeError);
      continue;
    }
    MemoryManager manager{options};
    ASSERT_EQ(
        manager.alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
    ASSERT_EQ(
        manager.testingDefaultRoot().alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
    auto leafPool = manager.addLeafPool("leafPool");
    ASSERT_EQ(
        leafPool->alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
    auto rootPool = manager.addRootPool("rootPool");
    ASSERT_EQ(
        rootPool->alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
  }
}

TEST(MemoryManagerTest, concurrentPoolAccess) {
  MemoryManager manager{};
  const int numAllocThreads = 40;
  std::vector<std::thread> allocThreads;
  std::mutex mu;
  std::vector<std::shared_ptr<MemoryPool>> pools;
  std::atomic<int64_t> poolId{0};
  for (int32_t i = 0; i < numAllocThreads; ++i) {
    allocThreads.push_back(std::thread([&]() {
      for (int i = 0; i < 1000; ++i) {
        if (folly::Random().oneIn(3)) {
          std::shared_ptr<MemoryPool> poolToDelete;
          {
            std::lock_guard<std::mutex> l(mu);
            if (pools.empty()) {
              continue;
            }
            const int idx = folly::Random().rand32() % pools.size();
            poolToDelete = pools[idx];
            pools.erase(pools.begin() + idx);
          }
        } else {
          const std::string name =
              fmt::format("concurrentPoolAccess{}", poolId++);
          std::shared_ptr<MemoryPool> poolToAdd;
          if (folly::Random().oneIn(2)) {
            poolToAdd = manager.addLeafPool(name);
          } else {
            poolToAdd = manager.addRootPool(name);
          }
          std::lock_guard<std::mutex> l(mu);
          pools.push_back(std::move(poolToAdd));
        }
      }
    }));
  }

  std::atomic<bool> stopCheck{false};
  std::thread checkThread([&]() {
    while (!stopCheck) {
      const int numPools = manager.numPools();
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  });

  for (int32_t i = 0; i < allocThreads.size(); ++i) {
    allocThreads[i].join();
  }
  stopCheck = true;
  checkThread.join();
  ASSERT_EQ(manager.numPools(), pools.size());
  pools.clear();
  ASSERT_EQ(manager.numPools(), 0);
}

TEST(MemoryManagerTest, quotaEnforcement) {
  struct {
    int64_t memoryQuotaBytes;
    int64_t smallAllocationBytes;
    int64_t largeAllocationPages;
    bool expectedMemoryExceedError;

    std::string debugString() const {
      return fmt::format(
          "memoryQuotaBytes:{} smallAllocationBytes:{} largeAllocationPages:{} expectedMemoryExceedError:{}",
          succinctBytes(memoryQuotaBytes),
          succinctBytes(smallAllocationBytes),
          largeAllocationPages,
          expectedMemoryExceedError);
    }
  } testSettings[] = {
      {2 << 20, 1 << 20, 256, false},
      {2 << 20, 1 << 20, 512, true},
      {2 << 20, 2 << 20, 256, true},
      {2 << 20, 3 << 20, 0, true},
      {2 << 20, 0, 768, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::vector<bool> contiguousAllocations = {false, true};
    for (const auto& contiguousAlloc : contiguousAllocations) {
      SCOPED_TRACE(fmt::format("contiguousAlloc {}", contiguousAlloc));
      const int alignment = 32;
      IMemoryManager::Options options;
      options.alignment = alignment;
      options.capacity = testData.memoryQuotaBytes;
      MemoryManager manager{options};
      auto pool = manager.addLeafPool("quotaEnforcement");
      void* smallBuffer{nullptr};
      if (testData.smallAllocationBytes != 0) {
        if ((testData.largeAllocationPages == 0) &&
            testData.expectedMemoryExceedError) {
          VELOX_ASSERT_THROW(pool->allocate(testData.smallAllocationBytes), "");
          continue;
        }
        smallBuffer = pool->allocate(testData.smallAllocationBytes);
      }
      if (contiguousAlloc) {
        ContiguousAllocation contiguousAllocation;
        if (testData.expectedMemoryExceedError) {
          VELOX_ASSERT_THROW(
              pool->allocateContiguous(
                  testData.largeAllocationPages, contiguousAllocation),
              "");
        } else {
          pool->allocateContiguous(
              testData.largeAllocationPages, contiguousAllocation);
        }
      } else {
        Allocation allocation;
        if (testData.expectedMemoryExceedError) {
          VELOX_ASSERT_THROW(
              pool->allocateNonContiguous(
                  testData.largeAllocationPages, allocation),
              "");
        } else {
          pool->allocateNonContiguous(
              testData.largeAllocationPages, allocation);
        }
      }
      pool->free(smallBuffer, testData.smallAllocationBytes);
    }
  }
}

TEST(MemoryManagerTest, testCheckUsageLeak) {
  FLAGS_velox_memory_leak_check_enabled = true;
  auto& manager = MemoryManager::getInstance(
      memory::MemoryManager::Options{.checkUsageLeak = false}, true);

  auto rootPool = manager.addRootPool("duplicateRootPool", kMaxMemory);
  auto leafPool = manager.addLeafPool("duplicateLeafPool", true);
  ASSERT_FALSE(rootPool->testingCheckUsageLeak());
  ASSERT_FALSE(leafPool->testingCheckUsageLeak());
}

} // namespace memory
} // namespace velox
} // namespace facebook
