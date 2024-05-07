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
#include <gtest/gtest.h>
#include <vector>

#include <gmock/gmock-matchers.h>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"

DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

using namespace ::testing;

namespace facebook::velox::memory {

namespace {
constexpr folly::StringPiece kSysRootName{"__sys_root__"};

MemoryManager& toMemoryManager(MemoryManager& manager) {
  return *static_cast<MemoryManager*>(&manager);
}
} // namespace

class MemoryManagerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    SharedArbitrator::registerFactory();
  }

  inline static const std::string arbitratorKind_{"SHARED"};
};

TEST_F(MemoryManagerTest, ctor) {
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools;
  {
    MemoryManager manager{};
    ASSERT_EQ(manager.numPools(), 1);
    ASSERT_EQ(manager.capacity(), kMaxMemory);
    ASSERT_EQ(0, manager.getTotalBytes());
    ASSERT_EQ(manager.alignment(), MemoryAllocator::kMaxAlignment);
    ASSERT_EQ(manager.testingDefaultRoot().alignment(), manager.alignment());
    ASSERT_EQ(manager.testingDefaultRoot().capacity(), kMaxMemory);
    ASSERT_EQ(manager.testingDefaultRoot().maxCapacity(), kMaxMemory);
    ASSERT_EQ(manager.arbitrator()->kind(), "NOOP");
  }
  {
    const auto kCapacity = 8L * 1024 * 1024;
    MemoryManager manager{
        {.allocatorCapacity = kCapacity,
         .arbitratorCapacity = kCapacity,
         .arbitratorReservedCapacity = 0}};
    ASSERT_EQ(kCapacity, manager.capacity());
    ASSERT_EQ(manager.numPools(), 1);
    ASSERT_EQ(manager.testingDefaultRoot().alignment(), manager.alignment());
  }
  {
    const auto kCapacity = 8L * 1024 * 1024;
    MemoryManager manager{
        {.alignment = 0,
         .allocatorCapacity = kCapacity,
         .arbitratorCapacity = kCapacity,
         .arbitratorReservedCapacity = 0}};

    ASSERT_EQ(manager.alignment(), MemoryAllocator::kMinAlignment);
    ASSERT_EQ(manager.testingDefaultRoot().alignment(), manager.alignment());
    // TODO: replace with root pool memory tracker quota check.
    ASSERT_EQ(
        kSharedPoolCount + 1, manager.testingDefaultRoot().getChildCount());
    ASSERT_EQ(kCapacity, manager.capacity());
    ASSERT_EQ(0, manager.getTotalBytes());
  }
  {
    MemoryManagerOptions options;
    const auto kCapacity = 4L << 30;
    options.allocatorCapacity = kCapacity;
    options.arbitratorCapacity = kCapacity;
    options.arbitratorReservedCapacity = 0;
    std::string arbitratorKind = "SHARED";
    options.arbitratorKind = arbitratorKind;
    MemoryManager manager{options};
    auto* arbitrator = manager.arbitrator();
    ASSERT_EQ(arbitrator->kind(), arbitratorKind);
    ASSERT_EQ(arbitrator->stats().maxCapacityBytes, kCapacity);
    ASSERT_EQ(
        manager.toString(),
        "Memory Manager[capacity 4.00GB alignment 64B usedBytes 0B number of "
        "pools 1\nList of root pools:\n\t__sys_root__\n"
        "Memory Allocator[MALLOC capacity 4.00GB allocated bytes 0 "
        "allocated pages 0 mapped pages 0]\n"
        "ARBITRATOR[SHARED CAPACITY[4.00GB] PENDING[0] "
        "STATS[numRequests 0 numAborted 0 numFailures 0 "
        "numNonReclaimableAttempts 0 numReserves 0 numReleases 0 queueTime 0us "
        "arbitrationTime 0us reclaimTime 0us shrunkMemory 0B "
        "reclaimedMemory 0B maxCapacity 4.00GB freeCapacity 4.00GB freeReservedCapacity 0B]]]");
  }
}

namespace {
class FakeTestArbitrator : public MemoryArbitrator {
 public:
  explicit FakeTestArbitrator(const Config& config)
      : MemoryArbitrator(
            {.kind = config.kind,
             .capacity = config.capacity,
             .memoryPoolTransferCapacity = config.memoryPoolTransferCapacity}) {
  }

  uint64_t growCapacity(MemoryPool* /*unused*/, uint64_t /*unused*/) override {
    VELOX_NYI();
  }

  bool growCapacity(
      MemoryPool* /*unused*/,
      const std::vector<std::shared_ptr<MemoryPool>>& /*unused*/,
      uint64_t /*unused*/) override {
    VELOX_NYI();
  }

  uint64_t shrinkCapacity(
      const std::vector<std::shared_ptr<MemoryPool>>& /*unused*/,
      uint64_t /*unused*/,
      bool /*unused*/,
      bool /*unused*/) override {
    VELOX_NYI();
  }

  uint64_t shrinkCapacity(MemoryPool* /*unused*/, uint64_t /*unused*/)
      override {
    VELOX_NYI();
  }

  Stats stats() const override {
    VELOX_NYI();
  }

  std::string toString() const override {
    VELOX_NYI();
  }

  std::string kind() const override {
    return "FAKE";
  }
};
} // namespace

TEST_F(MemoryManagerTest, createWithCustomArbitrator) {
  const std::string kindString = "FAKE";
  MemoryArbitrator::Factory factory =
      [](const MemoryArbitrator::Config& config) {
        return std::make_unique<FakeTestArbitrator>(config);
      };
  MemoryArbitrator::registerFactory(kindString, factory);
  auto guard = folly::makeGuard(
      [&] { MemoryArbitrator::unregisterFactory(kindString); });
  MemoryManagerOptions options;
  options.arbitratorKind = kindString;
  options.allocatorCapacity = 8L << 20;
  options.arbitratorCapacity = 256L << 20;
  MemoryManager manager{options};
  ASSERT_EQ(manager.arbitrator()->capacity(), options.allocatorCapacity);
  ASSERT_EQ(manager.allocator()->capacity(), options.allocatorCapacity);
}

TEST_F(MemoryManagerTest, addPool) {
  MemoryManager manager{};

  auto rootPool = manager.addRootPool("duplicateRootPool", kMaxMemory);
  ASSERT_EQ(rootPool->capacity(), kMaxMemory);
  ASSERT_EQ(rootPool->maxCapacity(), kMaxMemory);
  { ASSERT_ANY_THROW(manager.addRootPool("duplicateRootPool", kMaxMemory)); }
  auto threadSafeLeafPool = manager.addLeafPool("leafPool", true);
  ASSERT_EQ(threadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(threadSafeLeafPool->maxCapacity(), kMaxMemory);
  auto nonThreadSafeLeafPool = manager.addLeafPool("duplicateLeafPool", true);
  ASSERT_EQ(nonThreadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(nonThreadSafeLeafPool->maxCapacity(), kMaxMemory);
  { ASSERT_ANY_THROW(manager.addLeafPool("duplicateLeafPool")); }
  const int64_t poolCapacity = 1 << 20;
  auto rootPoolWithMaxCapacity =
      manager.addRootPool("rootPoolWithCapacity", poolCapacity);
  ASSERT_EQ(rootPoolWithMaxCapacity->maxCapacity(), poolCapacity);
  ASSERT_EQ(rootPoolWithMaxCapacity->capacity(), poolCapacity);
  auto leafPool = rootPoolWithMaxCapacity->addLeafChild("leaf");
  ASSERT_EQ(leafPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(leafPool->capacity(), poolCapacity);
  auto aggregationPool = rootPoolWithMaxCapacity->addLeafChild("aggregation");
  ASSERT_EQ(aggregationPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(aggregationPool->capacity(), poolCapacity);
}

TEST_F(MemoryManagerTest, addPoolWithArbitrator) {
  MemoryManagerOptions options;
  const auto kCapacity = 32L << 30;
  options.allocatorCapacity = kCapacity;
  options.arbitratorKind = arbitratorKind_;
  // The arbitrator capacity will be overridden by the memory manager's
  // capacity.
  const uint64_t initialPoolCapacity = options.allocatorCapacity / 32;
  options.memoryPoolInitCapacity = initialPoolCapacity;
  MemoryManager manager{options};

  auto rootPool = manager.addRootPool(
      "addPoolWithArbitrator", kMaxMemory, MemoryReclaimer::create());
  ASSERT_EQ(rootPool->capacity(), initialPoolCapacity);
  ASSERT_EQ(rootPool->maxCapacity(), kMaxMemory);
  {
    ASSERT_ANY_THROW(manager.addRootPool(
        "addPoolWithArbitrator", kMaxMemory, MemoryReclaimer::create()));
  }
  {
    ASSERT_NO_THROW(manager.addRootPool("addPoolWithArbitrator1", kMaxMemory));
  }
  auto threadSafeLeafPool = manager.addLeafPool("leafPool", true);
  ASSERT_EQ(threadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(threadSafeLeafPool->maxCapacity(), kMaxMemory);
  auto nonThreadSafeLeafPool = manager.addLeafPool("duplicateLeafPool", true);
  ASSERT_EQ(nonThreadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(nonThreadSafeLeafPool->maxCapacity(), kMaxMemory);
  { ASSERT_ANY_THROW(manager.addLeafPool("duplicateLeafPool")); }
  const int64_t poolCapacity = 1 << 30;
  auto rootPoolWithMaxCapacity = manager.addRootPool(
      "rootPoolWithCapacity", poolCapacity, MemoryReclaimer::create());
  ASSERT_EQ(rootPoolWithMaxCapacity->maxCapacity(), poolCapacity);
  ASSERT_EQ(rootPoolWithMaxCapacity->capacity(), initialPoolCapacity);
  auto leafPool = rootPoolWithMaxCapacity->addLeafChild("leaf");
  ASSERT_EQ(leafPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(leafPool->capacity(), initialPoolCapacity);
  auto aggregationPool = rootPoolWithMaxCapacity->addLeafChild("aggregation");
  ASSERT_EQ(aggregationPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(aggregationPool->capacity(), initialPoolCapacity);
}

// TODO: remove this test when remove deprecatedDefaultMemoryManager.
TEST_F(MemoryManagerTest, defaultMemoryManager) {
  auto& managerA = toMemoryManager(deprecatedDefaultMemoryManager());
  auto& managerB = toMemoryManager(deprecatedDefaultMemoryManager());
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools + 1;
  ASSERT_EQ(managerA.numPools(), 1);
  ASSERT_EQ(managerA.testingDefaultRoot().getChildCount(), kSharedPoolCount);
  ASSERT_EQ(managerB.numPools(), 1);
  ASSERT_EQ(managerB.testingDefaultRoot().getChildCount(), kSharedPoolCount);

  auto child1 = managerA.addLeafPool("child_1");
  ASSERT_EQ(child1->parent()->name(), managerA.testingDefaultRoot().name());
  auto child2 = managerB.addLeafPool("child_2");
  ASSERT_EQ(child2->parent()->name(), managerA.testingDefaultRoot().name());
  EXPECT_EQ(
      kSharedPoolCount + 2, managerA.testingDefaultRoot().getChildCount());
  EXPECT_EQ(
      kSharedPoolCount + 2, managerB.testingDefaultRoot().getChildCount());
  ASSERT_EQ(managerA.numPools(), 3);
  ASSERT_EQ(managerB.numPools(), 3);
  auto pool = managerB.addRootPool();
  ASSERT_EQ(managerA.numPools(), 4);
  ASSERT_EQ(managerB.numPools(), 4);
  ASSERT_EQ(
      managerA.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 4\nList of root pools:\n\t__sys_root__\n\tdefault_root_0\n\trefcount 2\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  ASSERT_EQ(
      managerB.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 4\nList of root pools:\n\t__sys_root__\n\tdefault_root_0\n\trefcount 2\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  child1.reset();
  EXPECT_EQ(
      kSharedPoolCount + 1, managerA.testingDefaultRoot().getChildCount());
  child2.reset();
  EXPECT_EQ(kSharedPoolCount, managerB.testingDefaultRoot().getChildCount());
  ASSERT_EQ(managerA.numPools(), 2);
  ASSERT_EQ(managerB.numPools(), 2);
  pool.reset();
  ASSERT_EQ(managerA.numPools(), 1);
  ASSERT_EQ(managerB.numPools(), 1);
  ASSERT_EQ(
      managerA.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 1\nList of root pools:\n\t__sys_root__\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  ASSERT_EQ(
      managerB.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 1\nList of root pools:\n\t__sys_root__\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  const std::string detailedManagerStr = managerA.toString(true);
  ASSERT_THAT(
      detailedManagerStr,
      testing::HasSubstr(
          "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 1\nList of root pools:\n__sys_root__ usage 0B reserved 0B peak 0B\n"));
  ASSERT_THAT(
      detailedManagerStr,
      testing::HasSubstr("__sys_spilling__ usage 0B reserved 0B peak 0B\n"));
  for (int i = 0; i < 32; ++i) {
    ASSERT_THAT(
        managerA.toString(true),
        testing::HasSubstr(fmt::format(
            "default_shared_leaf_pool_{} usage 0B reserved 0B peak 0B\n", i)));
  }
}

// TODO: remove this test when remove deprecatedAddDefaultLeafMemoryPool.
TEST(MemoryHeaderTest, addDefaultLeafMemoryPool) {
  auto& manager = toMemoryManager(deprecatedDefaultMemoryManager());
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools + 1;
  ASSERT_EQ(manager.testingDefaultRoot().getChildCount(), kSharedPoolCount);
  {
    auto poolA = deprecatedAddDefaultLeafMemoryPool();
    ASSERT_EQ(poolA->kind(), MemoryPool::Kind::kLeaf);
    auto poolB = deprecatedAddDefaultLeafMemoryPool();
    ASSERT_EQ(poolB->kind(), MemoryPool::Kind::kLeaf);
    EXPECT_EQ(
        kSharedPoolCount + 2, manager.testingDefaultRoot().getChildCount());
    {
      auto poolC = deprecatedAddDefaultLeafMemoryPool();
      ASSERT_EQ(poolC->kind(), MemoryPool::Kind::kLeaf);
      EXPECT_EQ(
          kSharedPoolCount + 3, manager.testingDefaultRoot().getChildCount());
      {
        auto poolD = deprecatedAddDefaultLeafMemoryPool();
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

  auto namedPool = deprecatedAddDefaultLeafMemoryPool("namedPool");
  ASSERT_EQ(namedPool->name(), "namedPool");
}

TEST_F(MemoryManagerTest, defaultMemoryUsageTracking) {
  for (bool trackDefaultMemoryUsage : {false, true}) {
    MemoryManagerOptions options;
    options.trackDefaultUsage = trackDefaultMemoryUsage;
    MemoryManager manager{options};
    auto defaultPool = manager.addLeafPool("defaultMemoryUsageTracking");
    ASSERT_EQ(defaultPool->trackUsage(), trackDefaultMemoryUsage);
  }

  for (bool trackDefaultMemoryUsage : {false, true}) {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool =
        trackDefaultMemoryUsage;
    MemoryManager manager{};
    auto defaultPool = manager.addLeafPool("defaultMemoryUsageTracking");
    ASSERT_EQ(defaultPool->trackUsage(), trackDefaultMemoryUsage);
  }
}

TEST_F(MemoryManagerTest, memoryPoolManagement) {
  const int alignment = 32;
  MemoryManagerOptions options;
  options.alignment = alignment;
  MemoryManager manager{options};
  ASSERT_EQ(manager.numPools(), 1);
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
  ASSERT_EQ(manager.numPools(), 1 + numPools + 2);
  userLeafPools.clear();
  leafUnamedPool.reset();
  ASSERT_EQ(manager.numPools(), 1 + numPools / 2 + 1);
  userRootPools.clear();
  ASSERT_EQ(manager.numPools(), 1 + 1);
  rootUnamedPool.reset();
  ASSERT_EQ(manager.numPools(), 1);
}

// TODO: when run sequentially, e.g. `buck run dwio/memory/...`, this has side
// effects for other tests using process singleton memory manager. Might need to
// use folly::Singleton for isolation by tag.
TEST_F(MemoryManagerTest, globalMemoryManager) {
  initializeMemoryManager({});
  auto* globalManager = memoryManager();
  ASSERT_TRUE(globalManager != nullptr);
  VELOX_ASSERT_THROW(initializeMemoryManager({}), "");
  ASSERT_EQ(memoryManager(), globalManager);
  MemoryManager::testingSetInstance({});
  auto* manager = memoryManager();
  ASSERT_NE(manager, globalManager);
  ASSERT_EQ(manager, memoryManager());
  auto* managerII = memoryManager();
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools + 1;
  {
    auto& rootI = manager->testingDefaultRoot();
    const std::string childIName("some_child");
    auto childI = rootI.addLeafChild(childIName);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);

    auto& rootII = managerII->testingDefaultRoot();
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

    auto childII = manager->addLeafPool("another_child");
    ASSERT_EQ(childII->kind(), MemoryPool::Kind::kLeaf);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 2);
    ASSERT_EQ(childII->parent()->name(), kSysRootName.str());
    childII.reset();
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(rootII.getChildCount(), kSharedPoolCount + 1);
    auto userRootChild = manager->addRootPool("rootChild");
    ASSERT_EQ(userRootChild->kind(), MemoryPool::Kind::kAggregate);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(rootII.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(manager->numPools(), 2 + 1);
  }
  ASSERT_EQ(manager->numPools(), 1);
}

TEST_F(MemoryManagerTest, alignmentOptionCheck) {
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
    MemoryManagerOptions options;
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

TEST_F(MemoryManagerTest, concurrentPoolAccess) {
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
  ASSERT_EQ(manager.numPools(), pools.size() + 1);
  pools.clear();
  ASSERT_EQ(manager.numPools(), 1);
}

TEST_F(MemoryManagerTest, quotaEnforcement) {
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
    const std::vector<bool> contiguousAllocations = {false, true};
    for (const auto contiguousAlloc : contiguousAllocations) {
      SCOPED_TRACE(fmt::format("contiguousAlloc {}", contiguousAlloc));
      const int alignment = 32;
      MemoryManagerOptions options;
      options.alignment = alignment;
      options.allocatorCapacity = testData.memoryQuotaBytes;
      options.arbitratorCapacity = testData.memoryQuotaBytes;
      options.arbitratorReservedCapacity = 0;
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

} // namespace facebook::velox::memory
