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

#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/MemoryPool.h"
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
        std::make_shared<core::QueryCtx>());
  }

  void SetUp() override {}

  void TearDown() override {}

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
    if (underDriverContext) {
      driver->state().setThread();
      ScopedDriverThreadContext scopedDriverThreadCtx{*driver->driverCtx()};
      reclaimer->enterArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_TRUE(driver->state().isSuspended);
      reclaimer->leaveArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().isSuspended);
    } else {
      reclaimer->enterArbitration();
      ASSERT_FALSE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().isSuspended);
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
      } catch (const VeloxRuntimeError& e) {
        leafPool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(leafPool->aborted());
    } else {
      auto aggregatePool = rootPool->addAggregateChild(
          "nonLeafAbortTest", exec::MemoryReclaimer::create());
      try {
        VELOX_FAIL("abortTest error");
      } catch (const VeloxRuntimeError& e) {
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
