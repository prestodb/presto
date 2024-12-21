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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/tests/SharedArbitratorTestUtil.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::memory;

class MemoryReclaimerTest : public OperatorTestBase {
 protected:
  MemoryReclaimerTest() : pool_(memory::memoryManager()->addLeafPool()) {
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;

    rowType_ = ROW({"c0", "c1"}, {{INTEGER(), VARCHAR()}});
    VectorFuzzer fuzzer{{}, pool_.get()};

    std::vector<RowVectorPtr> values = {fuzzer.fuzzRow(rowType_)};
    core::PlanFragment fakePlanFragment;
    const core::PlanNodeId id{"0"};
    fakePlanFragment.planNode = std::make_shared<core::ValuesNode>(id, values);

    fakeTask_ = Task::create(
        "MemoryReclaimerTest",
        std::move(fakePlanFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        Task::ExecutionMode::kParallel);
  }

  void SetUp() override {}

  void TearDown() override {}

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(4)};
  const std::shared_ptr<memory::MemoryPool> pool_;
  RowTypePtr rowType_;
  std::shared_ptr<Task> fakeTask_;
  folly::Random::DefaultGenerator rng_;
};

TEST_F(MemoryReclaimerTest, enterArbitrationTest) {
  for (const auto& underDriverContext : {false, true}) {
    SCOPED_TRACE(fmt::format("underDriverContext: {}", underDriverContext));

    auto reclaimer = exec::MemoryReclaimer::create();
    auto driver = Driver::testingCreate(
        std::make_unique<DriverCtx>(fakeTask_, 0, 0, 0, 0));
    fakeTask_->testingIncrementThreads();
    if (underDriverContext) {
      driver->state().setThread();
      ScopedDriverThreadContext scopedDriverThreadCtx{driver->driverCtx()};
      reclaimer->enterArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_TRUE(driver->state().suspended());
      reclaimer->leaveArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().suspended());
    } else {
      reclaimer->enterArbitration();
      ASSERT_FALSE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().suspended());
      reclaimer->leaveArbitration();
    }
  }
}

TEST_F(MemoryReclaimerTest, abortTest) {
  for (const auto& leafPool : {false, true}) {
    const std::string testName = fmt::format("leafPool: {}", leafPool);
    SCOPED_TRACE(testName);
    auto rootPool = memory::memoryManager()->addRootPool(
        testName, kMaxMemory, exec::MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    if (leafPool) {
      auto leafPool = rootPool->addLeafChild(
          "leafAbortTest", true, exec::MemoryReclaimer::create());
      try {
        VELOX_FAIL("abortTest error");
      } catch (const VeloxRuntimeError&) {
        leafPool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(leafPool->aborted());
    } else {
      auto aggregatePool = rootPool->addAggregateChild(
          "nonLeafAbortTest", exec::MemoryReclaimer::create());
      try {
        VELOX_FAIL("abortTest error");
      } catch (const VeloxRuntimeError&) {
        aggregatePool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(aggregatePool->aborted());
    }
  }
}

TEST(ReclaimableSectionGuard, basic) {
  tsan_atomic<bool> nonReclaimableSection{false};
  {
    memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
    ASSERT_TRUE(nonReclaimableSection);
    {
      memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
      ASSERT_FALSE(nonReclaimableSection);
      {
        memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_TRUE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
      }
      ASSERT_FALSE(nonReclaimableSection);
    }
    ASSERT_TRUE(nonReclaimableSection);
  }
  ASSERT_FALSE(nonReclaimableSection);
  nonReclaimableSection = true;
  {
    memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
    ASSERT_FALSE(nonReclaimableSection);
    {
      memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
      ASSERT_TRUE(nonReclaimableSection);
      {
        memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_FALSE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_TRUE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
      }
      ASSERT_TRUE(nonReclaimableSection);
    }
    ASSERT_FALSE(nonReclaimableSection);
  }
  ASSERT_TRUE(nonReclaimableSection);
}

namespace {

using ReclaimCallback = const std::function<void(memory::MemoryPool*)>;

class MockMemoryReclaimer : public memory::MemoryReclaimer {
 public:
  static std::unique_ptr<MemoryReclaimer> create(
      bool reclaimable,
      uint64_t memoryBytes,
      ReclaimCallback& reclaimCallback = nullptr,
      int32_t priority = 0) {
    return std::unique_ptr<MemoryReclaimer>(new MockMemoryReclaimer(
        reclaimable, memoryBytes, reclaimCallback, priority));
  }

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const override {
    reclaimableBytes = 0;
    if (!reclaimable_) {
      return false;
    }
    reclaimableBytes = memoryBytes_;
    return true;
  }

  uint64_t reclaim(
      MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      Stats& stats) override {
    VELOX_CHECK(underMemoryArbitration());
    VELOX_CHECK(reclaimable_);
    if (reclaimCallback_) {
      reclaimCallback_(pool);
    }
    const uint64_t reclaimedBytes = memoryBytes_;
    memoryBytes_ = 0;
    return reclaimedBytes;
  }

  uint64_t memoryBytes() const {
    return memoryBytes_;
  }

  int32_t priority() const override {
    return priority_;
  }

 private:
  MockMemoryReclaimer(
      bool reclaimable,
      uint64_t memoryBytes,
      const std::function<void(memory::MemoryPool*)>& reclaimCallback,
      int32_t priority)
      : MemoryReclaimer(priority),
        reclaimCallback_(reclaimCallback),
        priority_(priority),
        reclaimable_(reclaimable),
        memoryBytes_(memoryBytes) {}

  const ReclaimCallback reclaimCallback_;
  const int32_t priority_;
  bool reclaimable_{false};
  uint64_t memoryBytes_{0};
};
} // namespace

TEST_F(MemoryReclaimerTest, parallelMemoryReclaimer) {
  struct TestReclaimer {
    bool reclaimable;
    uint64_t memoryBytes;
    uint64_t expectedMemoryBytesAfterReclaim;
  };

  struct {
    bool hasExecutor;
    uint64_t bytesToReclaim;
    std::vector<TestReclaimer> testReclaimers;
  } testSettings[] = {
      {false, 100, {{true, 100, 0}, {true, 90, 90}, {false, 200, 200}}},
      {true, 100, {{true, 100, 0}, {true, 90, 0}, {false, 200, 200}}},
      {false, 110, {{true, 100, 0}, {true, 90, 0}, {false, 200, 200}}},
      {true, 110, {{true, 100, 0}, {true, 90, 0}, {false, 200, 200}}},
      {false, 100, {{true, 100, 100}, {true, 90, 90}, {true, 200, 0}}},
      {true, 100, {{true, 100, 0}, {true, 90, 0}, {true, 200, 0}}},
      {false, 80, {{true, 100, 100}, {true, 90, 90}, {true, 200, 0}}},
      {true, 80, {{true, 100, 0}, {true, 90, 0}, {true, 200, 0}}}};

  for (const auto& testData : testSettings) {
    auto rootPool = memory::memoryManager()->addRootPool(
        "parallelMemoryReclaimer",
        kMaxMemory,
        exec::ParallelMemoryReclaimer::create(
            testData.hasExecutor ? executor_.get() : nullptr));
    std::vector<MockMemoryReclaimer*> memoryReclaimers;
    std::vector<std::shared_ptr<MemoryPool>> leafPools;
    int reclaimerIdx{0};
    for (const auto& testReclaimer : testData.testReclaimers) {
      auto reclaimer = MockMemoryReclaimer::create(
          testReclaimer.reclaimable, testReclaimer.memoryBytes);
      leafPools.push_back(rootPool->addLeafChild(
          std::to_string(reclaimerIdx++), true, std::move(reclaimer)));
      memoryReclaimers.push_back(
          static_cast<MockMemoryReclaimer*>(leafPools.back()->reclaimer()));
    }

    memory::ScopedMemoryArbitrationContext context(rootPool.get());
    memory::MemoryReclaimer::Stats stats;
    rootPool->reclaim(testData.bytesToReclaim, 0, stats);
    for (int i = 0; i < memoryReclaimers.size(); ++i) {
      auto* memoryReclaimer = memoryReclaimers[i];
      ASSERT_EQ(
          memoryReclaimer->memoryBytes(),
          testData.testReclaimers[i].expectedMemoryBytesAfterReclaim)
          << i;
    }
  }
}

// This test is to verify if the parallel memory reclaimer can prevent recursive
// arbitration.
TEST_F(MemoryReclaimerTest, recursiveArbitrationWithParallelReclaim) {
  std::atomic_bool reclaimExecuted{false};
  auto rootPool = memory::memoryManager()->addRootPool(
      "recursiveArbitrationWithParallelReclaim",
      32 << 20,
      exec::ParallelMemoryReclaimer::create(executor_.get()));
  const auto reclaimCallback = [&](memory::MemoryPool* pool) {
    void* buffer = pool->allocate(64 << 20);
    pool->free(buffer, 64 << 20);
    reclaimExecuted = true;
  };
  const int numLeafPools = 10;
  const int bufferSize = 1 << 20;
  std::vector<MockMemoryReclaimer*> memoryReclaimers;
  std::vector<std::shared_ptr<MemoryPool>> leafPools;
  std::vector<void*> buffers;
  for (int i = 0; i < numLeafPools; ++i) {
    auto reclaimer =
        MockMemoryReclaimer::create(true, bufferSize, reclaimCallback);
    leafPools.push_back(
        rootPool->addLeafChild(std::to_string(i), true, std::move(reclaimer)));
    buffers.push_back(leafPools.back()->allocate(bufferSize));
    memoryReclaimers.push_back(
        static_cast<MockMemoryReclaimer*>(leafPools.back()->reclaimer()));
  }

  memory::testingRunArbitration();

  for (int i = 0; i < numLeafPools; ++i) {
    leafPools[i]->free(buffers[i], bufferSize);
  }
  ASSERT_TRUE(reclaimExecuted);
}

TEST_F(MemoryReclaimerTest, reclaimerPriorities) {
  auto rootPool = memory::memoryManager()->addRootPool(
      "reclaimerPriorities", kMaxMemory, exec::MemoryReclaimer::create());
  std::vector<std::shared_ptr<MemoryPool>> leafPools;

  const uint32_t kNumChildren = 10;
  const uint64_t kPoolMemoryBytes = 1024;
  std::vector<int32_t> priorityOrder;
  priorityOrder.reserve(kNumChildren);
  ReclaimCallback reclaimerCb = [&](MemoryPool* pool) {
    auto* mockReclaimer = dynamic_cast<MockMemoryReclaimer*>(pool->reclaimer());
    ASSERT_TRUE(mockReclaimer != nullptr);
    priorityOrder.push_back(mockReclaimer->priority());
  };

  for (uint32_t i = 0; i < kNumChildren; ++i) {
    auto reclaimer = MockMemoryReclaimer::create(
        kPoolMemoryBytes,
        kPoolMemoryBytes,
        reclaimerCb,
        static_cast<int32_t>(folly::Random::rand32(10000)) - 5000);
    leafPools.push_back(rootPool->addLeafChild(
        fmt::format("leaf-{}", i), true, std::move(reclaimer)));
  }

  memory::ScopedMemoryArbitrationContext context(rootPool.get());
  memory::MemoryReclaimer::Stats stats;
  rootPool->reclaim(kNumChildren * kPoolMemoryBytes, 0, stats);

  ASSERT_EQ(priorityOrder.size(), kNumChildren);
  for (uint32_t i = 0; i < priorityOrder.size() - 1; i++) {
    ASSERT_LE(priorityOrder[i], priorityOrder[i + 1]);
  }
}

TEST_F(MemoryReclaimerTest, reclaimerPrioritiesWithZeroUsage) {
  // This test makes sure the reclaim continues to lower priority reclaimers
  // even if the higher priority ones have nothing to reclaim.
  auto rootPool = memory::memoryManager()->addRootPool(
      "reclaimerPriorities", kMaxMemory, exec::MemoryReclaimer::create());
  std::vector<std::shared_ptr<MemoryPool>> leafPools;

  const uint32_t kNumChildren = 10;
  const uint64_t kPoolMemoryBytes = 1024;
  std::vector<int32_t> priorityOrder;
  priorityOrder.reserve(kNumChildren);
  ReclaimCallback reclaimerCb = [&](MemoryPool* pool) {
    auto* mockReclaimer = dynamic_cast<MockMemoryReclaimer*>(pool->reclaimer());
    ASSERT_TRUE(mockReclaimer != nullptr);
    priorityOrder.push_back(mockReclaimer->priority());
  };

  for (uint32_t i = 0; i < kNumChildren; ++i) {
    auto reclaimer = MockMemoryReclaimer::create(
        kPoolMemoryBytes,
        i % 5 == 0 ? kPoolMemoryBytes : 0,
        reclaimerCb,
        i / 5);
    leafPools.push_back(rootPool->addLeafChild(
        fmt::format("leaf-{}", i), true, std::move(reclaimer)));
  }

  memory::ScopedMemoryArbitrationContext context(rootPool.get());
  memory::MemoryReclaimer::Stats stats;
  rootPool->reclaim(2 * kPoolMemoryBytes, 0, stats);

  ASSERT_EQ(priorityOrder.size(), 2);
}

TEST_F(MemoryReclaimerTest, multiLevelReclaimerPriorities) {
  // Following tree structure is built with priorities
  //  rootPool
  //  ├── serial-aggr-0 (priority: 4)
  //  │   ├── serial-aggr-0.leaf-0 (priority: 0)
  //  │   ├── serial-aggr-0.leaf-1 (priority: 0)
  //  │   ├── serial-aggr-0.leaf-2 (priority: 1)
  //  │   ├── serial-aggr-0.leaf-3 (priority: 1)
  //  │   ├── ...
  //  │   └── serial-aggr-0.leaf-9 (priority: 4)
  //  ├── parallel-aggr-1 (priority: 3)
  //  │   ├── parallel-aggr-1.leaf-0 (priority: 0)
  //  │   ├── parallel-aggr-1.leaf-1 (priority: 0)
  //  │   ├── parallel-aggr-1.leaf-2 (priority: 1)
  //  │   ├── parallel-aggr-1.leaf-3 (priority: 1)
  //  │   ├── ...
  //  │   └── parallel-aggr-1.leaf-9 (priority: 4)
  //  ├── serial-aggr-2 (priority: 2)
  //  │   ├── serial-aggr-2.leaf-0 (priority: 0)
  //  │   ├── serial-aggr-2.leaf-1 (priority: 0)
  //  │   ├── serial-aggr-2.leaf-2 (priority: 1)
  //  │   ├── serial-aggr-2.leaf-3 (priority: 1)
  //  │   ├── ...
  //  │   └── serial-aggr-2.leaf-9 (priority: 4)
  //  └── parallel-aggr-3 (priority: 1)
  //      ├── parallel-aggr-3.leaf-0 (priority: 0)
  //      ├── parallel-aggr-3.leaf-1 (priority: 0)
  //      ├── parallel-aggr-3.leaf-2 (priority: 1)
  //      ├── parallel-aggr-3.leaf-3 (priority: 1)
  //      ├── ...
  //      └── parallel-aggr-3.leaf-9 (priority: 4)
  auto rootPool = memory::memoryManager()->addRootPool(
      "multiLevelReclaimerPriorities",
      32 << 20,
      exec::MemoryReclaimer::create());
  const uint32_t kNumAggrPools = 4;
  const uint32_t kNumLeafPerAggr = 10;
  const uint64_t kPoolMemoryBytes = 1024;
  std::vector<std::shared_ptr<MemoryPool>> aggrPools;
  std::vector<std::shared_ptr<MemoryPool>> leafPools;
  std::mutex reclaimOrderMutex;
  std::vector<MemoryPool*> reclaimOrder;
  uint32_t poolIdx{0};
  for (uint32_t i = 0; i < kNumAggrPools; i++) {
    if (i % 2 == 0) {
      aggrPools.push_back(rootPool->addAggregateChild(
          fmt::format("serial-aggr-{}", poolIdx++),
          exec::MemoryReclaimer::create(kNumAggrPools - i)));
    } else {
      aggrPools.push_back(rootPool->addAggregateChild(
          fmt::format("parallel-aggr-{}", poolIdx++),
          exec::ParallelMemoryReclaimer::create(
              executor_.get(), kNumAggrPools - i)));
    }
    auto& aggrPool = aggrPools.back();
    const auto aggrPoolName = aggrPool->name();
    for (uint32_t j = 0; j < kNumLeafPerAggr; j++) {
      leafPools.push_back(aggrPools.back()->addLeafChild(
          fmt::format("{}.leaf-{}", aggrPoolName, j),
          true,
          MockMemoryReclaimer::create(
              kPoolMemoryBytes,
              kPoolMemoryBytes,
              [&](MemoryPool* pool) {
                std::lock_guard<std::mutex> l(reclaimOrderMutex);
                reclaimOrder.push_back(pool);
              },
              j / 2)));
    }
  }

  memory::ScopedMemoryArbitrationContext context(rootPool.get());
  memory::MemoryReclaimer::Stats stats;
  rootPool->reclaim(
      kNumAggrPools * kNumLeafPerAggr * kPoolMemoryBytes, 0, stats);

  ASSERT_EQ(reclaimOrder.size(), kNumAggrPools * kNumLeafPerAggr);
  for (uint32_t i = 0; i < kNumAggrPools; i++) {
    uint32_t start = i * kNumLeafPerAggr;
    const auto poolName = reclaimOrder[start]->name();
    bool isParallel = false;
    switch (i) {
      case 0:
        ASSERT_TRUE(poolName.find("parallel-aggr-3") != std::string::npos);
        isParallel = true;
        break;
      case 1:
        ASSERT_TRUE(poolName.find("serial-aggr-2") != std::string::npos);
        break;
      case 2:
        ASSERT_TRUE(poolName.find("parallel-aggr-1") != std::string::npos);
        isParallel = true;
        break;
      case 3:
        ASSERT_TRUE(poolName.find("serial-aggr-0") != std::string::npos);
        break;
      default:
        FAIL();
    }
    for (uint32_t j = start; j < start + kNumLeafPerAggr - 1; j++) {
      if (isParallel) {
        // Priority is not applicable to parallel reclaimer.
        continue;
      }
      const auto* firstReclaimer =
          dynamic_cast<MockMemoryReclaimer*>(reclaimOrder[j]->reclaimer());
      ASSERT_TRUE(firstReclaimer != nullptr);
      const auto* secondReclaimer =
          dynamic_cast<MockMemoryReclaimer*>(reclaimOrder[j + 1]->reclaimer());
      ASSERT_TRUE(secondReclaimer != nullptr);
      ASSERT_LE(firstReclaimer->priority(), secondReclaimer->priority());
    }
  }
}
