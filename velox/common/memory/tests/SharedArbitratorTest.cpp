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

#include <re2/re2.h>
#include <deque>

#include <folly/init/Init.h>
#include <functional>
#include <optional>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/Driver.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_suppress_memory_capacity_exceeding_error_message);

using namespace ::testing;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::memory {
// Custom node for the custom factory.
class FakeMemoryNode : public core::PlanNode {
 public:
  FakeMemoryNode(const core::PlanNodeId& id, core::PlanNodePtr input)
      : PlanNode(id), sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "FakeMemoryNode";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

using AllocationCallback = std::function<TestAllocation(Operator* op)>;
// If return true, the caller will terminate execution and return early.
using ReclaimInjectionCallback = std::function<bool(
    memory::MemoryPool* pool,
    uint64_t targetByte,
    MemoryReclaimer::Stats& stats)>;

// Custom operator for the custom factory.
class FakeMemoryOperator : public Operator {
 public:
  FakeMemoryOperator(
      DriverCtx* ctx,
      int32_t id,
      core::PlanNodePtr node,
      bool canReclaim,
      AllocationCallback allocationCb,
      ReclaimInjectionCallback reclaimCb)
      : Operator(ctx, node->outputType(), id, node->id(), "FakeMemoryNode"),
        canReclaim_(canReclaim),
        allocationCb_(std::move(allocationCb)),
        reclaimCb_(std::move(reclaimCb)) {}

  ~FakeMemoryOperator() override {
    clear();
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
    if (allocationCb_ != nullptr) {
      TestAllocation allocation = allocationCb_(this);
      if (allocation.buffer != nullptr) {
        allocations_.push_back(allocation);
      }
      totalBytes_ += allocation.size;
    }
  }

  void noMoreInput() override {
    clear();
    Operator::noMoreInput();
  }

  RowVectorPtr getOutput() override {
    return std::move(input_);
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr && allocations_.empty();
  }

  void close() override {
    clear();
    Operator::close();
  }

  bool canReclaim() const override {
    return canReclaim_;
  }

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override {
    VELOX_CHECK(canReclaim());
    auto* driver = operatorCtx_->driver();
    VELOX_CHECK(!driver->state().isOnThread() || driver->state().isSuspended);
    VELOX_CHECK(driver->task()->pauseRequested());
    VELOX_CHECK_GT(targetBytes, 0);

    if (reclaimCb_ != nullptr && reclaimCb_(pool(), targetBytes, stats)) {
      return;
    }

    uint64_t bytesReclaimed{0};
    auto allocIt = allocations_.begin();
    while (allocIt != allocations_.end() &&
           ((targetBytes != 0) && (bytesReclaimed < targetBytes))) {
      bytesReclaimed += allocIt->size;
      totalBytes_ -= allocIt->size;
      pool()->free(allocIt->buffer, allocIt->size);
      allocIt = allocations_.erase(allocIt);
    }
    VELOX_CHECK_GE(totalBytes_, 0);
  }

 private:
  void clear() {
    for (auto& allocation : allocations_) {
      totalBytes_ -= allocation.free();
      VELOX_CHECK_GE(totalBytes_, 0);
    }
    allocations_.clear();
    VELOX_CHECK_EQ(totalBytes_.load(), 0);
  }

  const bool canReclaim_;
  const AllocationCallback allocationCb_;
  const ReclaimInjectionCallback reclaimCb_{nullptr};

  std::atomic<size_t> totalBytes_{0};
  std::vector<TestAllocation> allocations_;
};

// Custom factory that creates fake memory operator.
class FakeMemoryOperatorFactory : public Operator::PlanNodeTranslator {
 public:
  FakeMemoryOperatorFactory() = default;

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const FakeMemoryNode>(node)) {
      return std::make_unique<FakeMemoryOperator>(
          ctx, id, node, canReclaim_, allocationCallback_, reclaimCallback_);
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const FakeMemoryNode>(node)) {
      return maxDrivers_;
    }
    return std::nullopt;
  }

  void setMaxDrivers(uint32_t maxDrivers) {
    maxDrivers_ = maxDrivers;
  }

  void setCanReclaim(bool canReclaim) {
    canReclaim_ = canReclaim;
  }

  void setAllocationCallback(AllocationCallback allocCb) {
    allocationCallback_ = std::move(allocCb);
  }

  void setReclaimCallback(ReclaimInjectionCallback reclaimCb) {
    reclaimCallback_ = std::move(reclaimCb);
  }

 private:
  bool canReclaim_{true};
  AllocationCallback allocationCallback_{nullptr};
  ReclaimInjectionCallback reclaimCallback_{nullptr};
  uint32_t maxDrivers_{1};
};

class SharedArbitrationTest : public exec::test::HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    exec::test::HiveConnectorTestBase::SetUpTestCase();
    auto fakeOperatorFactory = std::make_unique<FakeMemoryOperatorFactory>();
    fakeOperatorFactory_ = fakeOperatorFactory.get();
    Operator::registerOperator(std::move(fakeOperatorFactory));
  }

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    fakeOperatorFactory_->setCanReclaim(true);

    setupMemory();

    rowType_ = ROW(
        {{"c0", INTEGER()},
         {"c1", INTEGER()},
         {"c2", VARCHAR()},
         {"c3", VARCHAR()}});
    fuzzerOpts_.vectorSize = 1024;
    fuzzerOpts_.nullRatio = 0;
    fuzzerOpts_.stringVariableLength = false;
    fuzzerOpts_.stringLength = 1024;
    fuzzerOpts_.allowLazyVector = false;
    vector_ = makeRowVector(rowType_, fuzzerOpts_);
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(32);
    numAddedPools_ = 0;
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
  }

  void setupMemory(
      int64_t memoryCapacity = 0,
      uint64_t memoryPoolInitCapacity = kMemoryPoolInitCapacity) {
    memoryCapacity = (memoryCapacity != 0) ? memoryCapacity : kMemoryCapacity;
    memoryManager_ =
        createMemoryManager(memoryCapacity, memoryPoolInitCapacity);
    ASSERT_EQ(memoryManager_->arbitrator()->kind(), "SHARED");
    arbitrator_ = static_cast<SharedArbitrator*>(memoryManager_->arbitrator());
    numAddedPools_ = 0;
  }

  static inline FakeMemoryOperatorFactory* fakeOperatorFactory_;
  std::unique_ptr<memory::MemoryManager> memoryManager_;
  SharedArbitrator* arbitrator_;
  RowTypePtr rowType_;
  VectorFuzzer::Options fuzzerOpts_;
  RowVectorPtr vector_;
  std::atomic_uint64_t numAddedPools_{0};
};

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimToOrderBy) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto oldStats = arbitrator_->stats();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
    ++numAddedPools_;
    std::shared_ptr<core::QueryCtx> orderByQueryCtx;
    if (sameQuery) {
      orderByQueryCtx = fakeMemoryQueryCtx;
    } else {
      orderByQueryCtx = newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
      ++numAddedPools_;
    }

    folly::EventCount orderByWait;
    auto orderByWaitKey = orderByWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity - (32L << 20);

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return TestAllocation{};
      }
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      orderByWait.notify();
      // Wait for pause to be triggered.
      taskPauseWait.wait(taskPauseWaitKey);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
    });

    std::atomic<bool> injectOrderByOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "OrderBy") {
            return;
          }
          if (!injectOrderByOnce.exchange(false)) {
            return;
          }
          orderByWait.wait(orderByWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            ([&](Task* /*unused*/) { taskPauseWait.notify(); })));

    std::thread orderByThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(orderByQueryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .orderBy({"c0 ASC NULLS LAST"}, false)
                        .planNode())
              .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
    });

    std::thread memThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(fakeMemoryQueryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .addNode([&](std::string id, core::PlanNodePtr input) {
                          return std::make_shared<FakeMemoryNode>(id, input);
                        })
                        .planNode())
              .assertResults("SELECT * FROM tmp");
    });

    orderByThread.join();
    memThread.join();
    waitForAllTasksToBeDeleted();
    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
    ASSERT_GT(newStats.reclaimTimeUs, oldStats.reclaimTimeUs);
    ASSERT_EQ(arbitrator_->stats().numReserves, numAddedPools_);
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimToAggregation) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto oldStats = arbitrator_->stats();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
    ++numAddedPools_;
    std::shared_ptr<core::QueryCtx> aggregationQueryCtx;
    if (sameQuery) {
      aggregationQueryCtx = fakeMemoryQueryCtx;
    } else {
      aggregationQueryCtx =
          newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
      ++numAddedPools_;
    }

    folly::EventCount aggregationWait;
    auto aggregationWaitKey = aggregationWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity - (32L << 20);

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return TestAllocation{};
      }
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      aggregationWait.notify();
      // Wait for pause to be triggered.
      taskPauseWait.wait(taskPauseWaitKey);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
    });

    std::atomic<bool> injectAggregationOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "Aggregation") {
            return;
          }
          if (!injectAggregationOnce.exchange(false)) {
            return;
          }
          aggregationWait.wait(aggregationWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            ([&](Task* /*unused*/) { taskPauseWait.notify(); })));

    std::thread aggregationThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(aggregationQueryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                        .planNode())
              .assertResults(
                  "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
    });

    std::thread memThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(fakeMemoryQueryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .addNode([&](std::string id, core::PlanNodePtr input) {
                          return std::make_shared<FakeMemoryNode>(id, input);
                        })
                        .planNode())
              .assertResults("SELECT * FROM tmp");
    });

    aggregationThread.join();
    memThread.join();
    waitForAllTasksToBeDeleted();

    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
    ASSERT_GT(newStats.reclaimTimeUs, oldStats.reclaimTimeUs);
    ASSERT_EQ(newStats.numReserves, numAddedPools_);
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimToJoinBuilder) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto oldStats = arbitrator_->stats();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
    ++numAddedPools_;
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
      ++numAddedPools_;
    }

    folly::EventCount joinWait;
    auto joinWaitKey = joinWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity - (32L << 20);

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return TestAllocation{};
      }
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      joinWait.notify();
      // Wait for pause to be triggered.
      taskPauseWait.wait(taskPauseWaitKey);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
    });

    std::atomic<bool> injectJoinOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          if (!injectJoinOnce.exchange(false)) {
            return;
          }
          joinWait.wait(joinWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            ([&](Task* /*unused*/) { taskPauseWait.notify(); })));

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(joinQueryCtx)
              .plan(PlanBuilder(planNodeIdGenerator)
                        .values(vectors)
                        .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                        .hashJoin(
                            {"t0"},
                            {"u0"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(vectors)
                                .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                                .planNode(),
                            "",
                            {"t1"},
                            core::JoinType::kAnti)
                        .planNode())
              .assertResults(
                  "SELECT c1 FROM tmp WHERE c0 NOT IN (SELECT c0 FROM tmp)");
    });

    std::thread memThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(fakeMemoryQueryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .addNode([&](std::string id, core::PlanNodePtr input) {
                          return std::make_shared<FakeMemoryNode>(id, input);
                        })
                        .planNode())
              .assertResults("SELECT * FROM tmp");
    });

    joinThread.join();
    memThread.join();
    waitForAllTasksToBeDeleted();

    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
    ASSERT_GT(newStats.reclaimTimeUs, oldStats.reclaimTimeUs);
    ASSERT_EQ(arbitrator_->stats().numReserves, numAddedPools_);
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, driverInitTriggeredArbitration) {
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  const int vectorSize = 100;
  fuzzerOpts_.vectorSize = vectorSize;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  const int expectedResultVectorSize = numVectors * vectorSize;
  const auto expectedVector = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int64_t>(
           expectedResultVectorSize, [&](auto /*unused*/) { return 6; }),
       makeFlatVector<int64_t>(
           expectedResultVectorSize, [&](auto /*unused*/) { return 7; })});

  createDuckDbTable(vectors);
  setupMemory(kMemoryCapacity, 0);
  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);
  ASSERT_EQ(queryCtx->pool()->maxCapacity(), kMemoryCapacity);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  AssertQueryBuilder(duckDbQueryRunner_)
      .config(core::QueryConfig::kSpillEnabled, "false")
      .queryCtx(queryCtx)
      .plan(PlanBuilder(planNodeIdGenerator, pool())
                .values(vectors)
                // Set filter projection to trigger memory allocation on
                // driver init.
                .project({"1+1+4 as t0", "1+3+3 as t1"})
                .planNode())
      .assertResults(expectedVector);
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    DISABLED_raceBetweenTaskTerminateAndReclaim) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 10;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);

  // Allocate a large chunk of memory to trigger memory reclaim during the query
  // execution.
  auto fakeLeafPool = queryCtx->pool()->addLeafChild("fakeLeaf");
  const size_t fakeAllocationSize = kMemoryCapacity / 2;
  TestAllocation fakeAllocation{
      fakeLeafPool.get(),
      fakeLeafPool->allocate(fakeAllocationSize),
      fakeAllocationSize};

  // Set test injection to enforce memory arbitration based on the fake
  // allocation size and the total available memory.
  std::shared_ptr<Task> task;
  std::atomic<bool> injectAllocationOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        if (!injectAllocationOnce.exchange(false)) {
          return;
        }
        task = values->testingOperatorCtx()->task();
        memory::MemoryPool* pool = values->pool();
        VELOX_ASSERT_THROW(
            pool->allocate(kMemoryCapacity * 2 / 3),
            "Exceeded memory pool cap");
      }));

  // Set test injection to wait until the reclaim on hash aggregation operator
  // triggers.
  folly::EventCount opReclaimStartWait;
  std::atomic<bool> opReclaimStarted{false};
  folly::EventCount taskAbortWait;
  std::atomic<bool> taskAborted{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Operator::MemoryReclaimer::reclaim",
      std::function<void(memory::MemoryPool*)>(([&](memory::MemoryPool* pool) {
        const std::string re(".*Aggregation");
        if (!RE2::FullMatch(pool->name(), re)) {
          return;
        }
        opReclaimStarted = true;
        opReclaimStartWait.notifyAll();
        // Wait for task abort to happen before the actual memory reclaim.
        taskAbortWait.await([&]() { return taskAborted.load(); });
      })));

  const int numDrivers = 1;
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::thread queryThread([&]() {
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(duckDbQueryRunner_)
            .queryCtx(queryCtx)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kJoinSpillEnabled, "true")
            .config(core::QueryConfig::kSpillNumPartitionBits, "2")
            .maxDrivers(numDrivers)
            .plan(PlanBuilder()
                      .values(vectors)
                      .localPartition({"c0", "c1"})
                      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                      .localPartition(std::vector<std::string>{})
                      .planNode())
            .assertResults(
                "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1"),
        "Aborted for external error");
  });

  // Wait for the reclaim on aggregation to be started before the task abort.
  opReclaimStartWait.await([&]() { return opReclaimStarted.load(); });
  ASSERT_TRUE(task != nullptr);
  task->requestAbort().wait();

  // Resume aggregation reclaim to execute.
  taskAborted = true;
  taskAbortWait.notifyAll();

  queryThread.join();
  fakeAllocation.free();
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, raceBetweenMaybeReserveAndTaskAbort) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 10;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);

  auto queryCtx = newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);

  // Create a fake query to hold some memory to trigger memory arbitration.
  auto fakeQueryCtx = newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
  auto fakeLeafPool = fakeQueryCtx->pool()->addLeafChild(
      "fakeLeaf", true, FakeMemoryReclaimer::create());
  TestAllocation fakeAllocation{
      fakeLeafPool.get(),
      fakeLeafPool->allocate(kMemoryCapacity / 3),
      kMemoryCapacity / 3};

  std::unique_ptr<TestAllocation> injectAllocation;
  std::atomic<bool> injectAllocationOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
        if (!injectAllocationOnce.exchange(false)) {
          return;
        }
        // The injection memory allocation (with the given size) makes sure that
        // maybeReserve fails and abort this query itself.
        const size_t injectAllocationSize =
            pool->freeBytes() + arbitrator_->stats().freeCapacityBytes;
        injectAllocation.reset(new TestAllocation{
            fakeLeafPool.get(),
            fakeLeafPool->allocate(injectAllocationSize),
            injectAllocationSize});
      }));

  const int numDrivers = 1;
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::thread queryThread([&]() {
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(duckDbQueryRunner_)
            .queryCtx(queryCtx)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kJoinSpillEnabled, "true")
            .config(core::QueryConfig::kSpillNumPartitionBits, "2")
            .maxDrivers(numDrivers)
            .plan(PlanBuilder()
                      .values(vectors)
                      .localPartition({"c0", "c1"})
                      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                      .localPartition(std::vector<std::string>{})
                      .planNode())
            .copyResults(pool()),
        "Exceeded memory pool cap");
  });

  queryThread.join();
  fakeAllocation.free();
  injectAllocation->free();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, asyncArbitratonFromNonDriverContext) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 10;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);
  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);

  folly::EventCount aggregationAllocationWait;
  std::atomic<bool> aggregationAllocationOnce{true};
  folly::EventCount aggregationAllocationUnblockWait;
  std::atomic<bool> aggregationAllocationUnblocked{false};
  std::atomic<memory::MemoryPool*> injectPool{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(memory::MemoryPool*)>(([&](memory::MemoryPool* pool) {
        const std::string re(".*Aggregation");
        if (!RE2::FullMatch(pool->name(), re)) {
          return;
        }

        if (!aggregationAllocationOnce.exchange(false)) {
          return;
        }
        injectPool = pool;
        aggregationAllocationWait.notifyAll();

        aggregationAllocationUnblockWait.await(
            [&]() { return aggregationAllocationUnblocked.load(); });
      })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::shared_ptr<Task> task;
  std::thread queryThread([&]() {
    task = AssertQueryBuilder(duckDbQueryRunner_)
               .queryCtx(queryCtx)
               .spillDirectory(spillDirectory->path)
               .config(core::QueryConfig::kSpillEnabled, "true")
               .config(core::QueryConfig::kJoinSpillEnabled, "true")
               .config(core::QueryConfig::kSpillNumPartitionBits, "2")
               .plan(PlanBuilder()
                         .values(vectors)
                         .localPartition({"c0", "c1"})
                         .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                         .localPartition(std::vector<std::string>{})
                         .planNode())
               .assertResults(
                   "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
  });

  aggregationAllocationWait.await(
      [&]() { return !aggregationAllocationOnce.load(); });
  ASSERT_TRUE(injectPool != nullptr);

  // Trigger the memory arbitration with memory pool whose associated driver is
  // running on driver thread.
  const size_t fakeAllocationSize = arbitrator_->stats().freeCapacityBytes / 2;
  TestAllocation fakeAllocation = {
      injectPool.load(),
      injectPool.load()->allocate(fakeAllocationSize),
      fakeAllocationSize};

  aggregationAllocationUnblocked = true;
  aggregationAllocationUnblockWait.notifyAll();

  queryThread.join();
  fakeAllocation.free();

  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, runtimeStats) {
  const uint64_t memoryCapacity = 128 * MB;
  setupMemory(memoryCapacity);
  fuzzerOpts_.vectorSize = 1000;
  fuzzerOpts_.stringLength = 1024;
  fuzzerOpts_.stringVariableLength = false;
  VectorFuzzer fuzzer(fuzzerOpts_, pool());
  std::vector<RowVectorPtr> vectors;
  int numRows{0};
  for (int i = 0; i < 10; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(rowType_));
    numRows += vectors.back()->size();
  }

  std::atomic<int> outputCount{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const facebook::velox::exec::Values*)>(
          ([&](const facebook::velox::exec::Values* values) {
            if (outputCount++ != 5) {
              return;
            }
            const auto fakeAllocationSize =
                arbitrator_->stats().maxCapacityBytes -
                values->pool()->capacity() + 1;
            void* buffer = values->pool()->allocate(fakeAllocationSize);
            values->pool()->free(buffer, fakeAllocationSize);
          })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  const auto outputDirectory = TempDirectoryPath::create();
  const auto queryCtx = newQueryCtx(memoryManager_, executor_, memoryCapacity);
  auto writerPlan =
      PlanBuilder()
          .values(vectors)
          .tableWrite(outputDirectory->path)
          .singleAggregation(
              {},
              {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())})
          .planNode();
  {
    const std::shared_ptr<Task> task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWriterSpillEnabled, "true")
            // Set 0 file writer flush threshold to always trigger flush in
            // test.
            .config(core::QueryConfig::kWriterFlushThresholdBytes, "0")
            // Set stripe size to extreme large to avoid writer internal
            // triggered flush.
            .connectorSessionProperty(
                kHiveConnectorId,
                connector::hive::HiveConfig::kOrcWriterMaxStripeSizeSession,
                "1GB")
            .connectorSessionProperty(
                kHiveConnectorId,
                connector::hive::HiveConfig::
                    kOrcWriterMaxDictionaryMemorySession,
                "1GB")
            .plan(std::move(writerPlan))
            .assertResults(fmt::format("SELECT {}", numRows));

    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    // TableWrite Operator's stripeSize runtime stats would be updated twice:
    // - Values Operator's memory allocation triggers TableWrite's memory
    // reclaim, which triggers data flush.
    // - TableWrite Operator's close would trigger flush.
    ASSERT_EQ(stats[1].runtimeStats["stripeSize"].count, 2);
    // Values Operator won't be set stripeSize in its runtimeStats.
    ASSERT_EQ(stats[0].runtimeStats["stripeSize"].count, 0);
  }
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, arbitrateMemoryFromOtherOperator) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 10;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  createDuckDbTable(vectors);

  for (bool sameDriver : {false, true}) {
    SCOPED_TRACE(fmt::format("sameDriver {}", sameDriver));
    std::shared_ptr<core::QueryCtx> queryCtx =
        newQueryCtx(memoryManager_, executor_, kMemoryCapacity);
    ASSERT_EQ(queryCtx->pool()->capacity(), 0);

    std::atomic<bool> injectAllocationOnce{true};
    const int initialBufferLen = 1 << 20;
    std::atomic<void*> buffer{nullptr};
    std::atomic<memory::MemoryPool*> bufferPool{nullptr};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Values::getOutput",
        std::function<void(const exec::Values*)>(
            [&](const exec::Values* values) {
              if (!injectAllocationOnce.exchange(false)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                return;
              }
              buffer = values->pool()->allocate(initialBufferLen);
              bufferPool = values->pool();
            }));
    std::atomic<bool> injectReallocateOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              const std::string re(".*Aggregation");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (pool->root()->currentBytes() == 0) {
                return;
              }
              if (!injectReallocateOnce.exchange(false)) {
                return;
              }
              ASSERT_TRUE(buffer != nullptr);
              ASSERT_TRUE(bufferPool != nullptr);
              const int newLength =
                  kMemoryCapacity - bufferPool.load()->capacity() + 1;
              VELOX_ASSERT_THROW(
                  bufferPool.load()->reallocate(
                      buffer, initialBufferLen, newLength),
                  "Exceeded memory pool cap");
            })));

    std::shared_ptr<Task> task;
    std::thread queryThread([&]() {
      if (sameDriver) {
        task = AssertQueryBuilder(duckDbQueryRunner_)
                   .queryCtx(queryCtx)
                   .plan(PlanBuilder()
                             .values(vectors)
                             .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                             .localPartition(std::vector<std::string>{})
                             .planNode())
                   .assertResults(
                       "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
      } else {
        task = AssertQueryBuilder(duckDbQueryRunner_)
                   .queryCtx(queryCtx)
                   .plan(PlanBuilder()
                             .values(vectors)
                             .localPartition({"c0", "c1"})
                             .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                             .planNode())
                   .assertResults(
                       "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
      }
    });

    queryThread.join();
    ASSERT_TRUE(buffer != nullptr);
    ASSERT_TRUE(bufferPool != nullptr);
    bufferPool.load()->free(buffer, initialBufferLen);

    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(SharedArbitrationTest, concurrentArbitration) {
  // Tries to replicate an actual workload by concurrently running multiple
  // query shapes that support spilling (and hence can be forced to abort or
  // spill by the arbitrator). Also adds an element of randomness by randomly
  // keeping completed tasks alive (zombie tasks) hence holding on to some
  // memory. Ensures that arbitration is engaged under memory contention and
  // failed queries only have errors related to memory or arbitration.
  FLAGS_velox_suppress_memory_capacity_exceeding_error_message = true;
  const int numVectors = 8;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 32;
  fuzzerOpts_.stringVariableLength = false;
  fuzzerOpts_.stringLength = 32;
  vectors.reserve(numVectors);
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(rowType_, fuzzerOpts_));
  }
  const int numDrivers = 4;
  const auto expectedWriteResult =
      runWriteTask(
          vectors, nullptr, numDrivers, pool(), kHiveConnectorId, false)
          .data;
  const auto expectedJoinResult =
      runHashJoinTask(vectors, nullptr, numDrivers, pool(), false).data;
  const auto expectedOrderResult =
      runOrderByTask(vectors, nullptr, numDrivers, pool(), false).data;
  const auto expectedRowNumberResult =
      runRowNumberTask(vectors, nullptr, numDrivers, pool(), false).data;
  const auto expectedTopNResult =
      runTopNTask(vectors, nullptr, numDrivers, pool(), false).data;

  struct {
    uint64_t totalCapacity;
    uint64_t queryCapacity;

    std::string debugString() const {
      return fmt::format(
          "totalCapacity = {}, queryCapacity = {}.",
          succinctBytes(totalCapacity),
          succinctBytes(queryCapacity));
    }
  } testSettings[] = {
      {16 * MB, 128 * MB}, {128 * MB, 16 * MB}, {128 * MB, 128 * MB}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto totalCapacity = testData.totalCapacity;
    const auto queryCapacity = testData.queryCapacity;
    setupMemory(totalCapacity);

    std::mutex mutex;
    std::vector<std::shared_ptr<core::QueryCtx>> queries;
    std::deque<std::shared_ptr<Task>> zombieTasks;

    const int numThreads = 32;
    const int maxNumZombieTasks = 8;
    std::vector<std::thread> queryThreads;
    queryThreads.reserve(numThreads);
    TestScopedAbortInjection testScopedAbortInjection(10, numThreads);
    for (int i = 0; i < numThreads; ++i) {
      queryThreads.emplace_back([&, i]() {
        std::shared_ptr<Task> task;
        try {
          auto queryCtx = newQueryCtx(memoryManager_, executor_, queryCapacity);
          if (i == 0) {
            // Write task contains aggregate node, which does not support
            // multithread aggregation type resolver, so make sure it is built
            // in a single thread.
            task = runWriteTask(
                       vectors,
                       queryCtx,
                       numDrivers,
                       pool(),
                       kHiveConnectorId,
                       true,
                       expectedWriteResult)
                       .task;
          } else if ((i % 4) == 0) {
            task = runHashJoinTask(
                       vectors,
                       queryCtx,
                       numDrivers,
                       pool(),
                       true,
                       expectedJoinResult)
                       .task;
          } else if ((i % 4) == 1) {
            task = runOrderByTask(
                       vectors,
                       queryCtx,
                       numDrivers,
                       pool(),
                       true,
                       expectedOrderResult)
                       .task;
          } else if ((i % 4) == 2) {
            task = runRowNumberTask(
                       vectors,
                       queryCtx,
                       numDrivers,
                       pool(),
                       true,
                       expectedRowNumberResult)
                       .task;
          } else {
            task = runTopNTask(
                       vectors,
                       queryCtx,
                       numDrivers,
                       pool(),
                       true,
                       expectedTopNResult)
                       .task;
          }
        } catch (const VeloxException& e) {
          if (e.errorCode() != error_code::kMemCapExceeded.c_str() &&
              e.errorCode() != error_code::kMemAborted.c_str() &&
              e.errorCode() != error_code::kMemAllocError.c_str() &&
              (e.message() != "Aborted for external error")) {
            std::rethrow_exception(std::current_exception());
          }
        }

        std::lock_guard<std::mutex> l(mutex);
        if (folly::Random().oneIn(3)) {
          zombieTasks.emplace_back(std::move(task));
        }
        while (zombieTasks.size() > maxNumZombieTasks) {
          zombieTasks.pop_front();
        }
      });
    }

    for (auto& queryThread : queryThreads) {
      queryThread.join();
    }
    zombieTasks.clear();
    waitForAllTasksToBeDeleted();
    ASSERT_GT(arbitrator_->stats().numRequests, 0);
  }
}

TEST_F(SharedArbitrationTest, reserveReleaseCounters) {
  for (int i = 0; i < 37; ++i) {
    folly::Random::DefaultGenerator rng(i);
    auto numRootPools = folly::Random::rand32(rng) % 11 + 3;
    std::vector<std::thread> threads;
    threads.reserve(numRootPools);
    std::mutex mutex;
    setupMemory(kMemoryCapacity, 0);
    {
      std::vector<std::shared_ptr<core::QueryCtx>> queries;
      queries.reserve(numRootPools);
      for (int j = 0; j < numRootPools; ++j) {
        threads.emplace_back([&]() {
          {
            std::lock_guard<std::mutex> l(mutex);
            auto oldNum = arbitrator_->stats().numReserves;
            queries.emplace_back(newQueryCtx(memoryManager_, executor_));
            ASSERT_EQ(arbitrator_->stats().numReserves, oldNum + 1);
          }
        });
      }

      for (auto& queryThread : threads) {
        queryThread.join();
      }
      ASSERT_EQ(arbitrator_->stats().numReserves, numRootPools);
      ASSERT_EQ(arbitrator_->stats().numReleases, 0);
    }
    ASSERT_EQ(arbitrator_->stats().numReserves, numRootPools);
    ASSERT_EQ(arbitrator_->stats().numReleases, numRootPools);
  }
}
} // namespace facebook::velox::memory

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
