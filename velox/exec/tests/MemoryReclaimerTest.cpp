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

#include "velox/exec/MemoryReclaimer.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::memory;

class MemoryReclaimerTest : public OperatorTestBase {
 protected:
  MemoryReclaimerTest() : pool_(memory::addDefaultLeafMemoryPool()) {
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
