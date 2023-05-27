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

#include <folly/Singleton.h>
#include <re2/re2.h>
#include <deque>

#include "folly/experimental/EventCount.h"
#include "folly/futures/Barrier.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_suppress_memory_capacity_exceeding_error_message);

using namespace ::testing;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::memory {
namespace {
constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;

constexpr uint64_t kMemoryCapacity = 512 * MB;
constexpr uint64_t kInitMemoryPoolCapacity = 16 * MB;
constexpr uint64_t kMinMemoryPoolCapacityTransferSize = 8 * MB;

struct Allocation {
  void* buffer{nullptr};
  size_t size{0};
};

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

using AllocationCallback = std::function<Allocation(Operator* op)>;
using ReclaimInjectionCallback =
    std::function<void(MemoryPool* pool, uint64_t targetByte)>;

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
      Allocation allocation = allocationCb_(this);
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

  void reclaim(uint64_t targetBytes) override {
    VELOX_CHECK(canReclaim());
    auto* driver = operatorCtx_->driver();
    VELOX_CHECK(!driver->state().isOnThread() || driver->state().isSuspended);
    VELOX_CHECK(driver->task()->pauseRequested());
    VELOX_CHECK_GT(targetBytes, 0);

    if (reclaimCb_ != nullptr) {
      reclaimCb_(pool(), targetBytes);
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
      totalBytes_ -= allocation.size;
      VELOX_CHECK_GE(totalBytes_, 0);
      pool()->free(allocation.buffer, allocation.size);
    }
    allocations_.clear();
    VELOX_CHECK_EQ(totalBytes_, 0);
  }

  const bool canReclaim_;
  const AllocationCallback allocationCb_;
  const ReclaimInjectionCallback reclaimCb_;

  std::atomic<size_t> totalBytes_{0};
  std::vector<Allocation> allocations_;
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
    OperatorTestBase::SetUp();

    setupMemory();
    auto fakeOperatorFactory = std::make_unique<FakeMemoryOperatorFactory>();

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
    VectorFuzzer fuzzer(fuzzerOpts_, pool());
    vector_ = newVector();
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(32);
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }

  void setupMemory(
      int64_t memoryCapacity = 0,
      uint64_t initMemoryPoolCapacity = 0,
      uint64_t minMemoryPoolCapacityTransferSize = 0) {
    if (initMemoryPoolCapacity == 0) {
      initMemoryPoolCapacity = kInitMemoryPoolCapacity;
    }
    if (minMemoryPoolCapacityTransferSize == 0) {
      minMemoryPoolCapacityTransferSize = kMinMemoryPoolCapacityTransferSize;
    }
    IMemoryManager::Options options;
    options.capacity = (memoryCapacity != 0) ? memoryCapacity : kMemoryCapacity;
    options.arbitratorConfig = {
        .kind = MemoryArbitrator::Kind::kShared,
        .capacity = options.capacity,
        .initMemoryPoolCapacity = initMemoryPoolCapacity,
        .minMemoryPoolCapacityTransferSize = minMemoryPoolCapacityTransferSize};
    options.checkUsageLeak = true;
    memoryManager_ = std::make_unique<MemoryManager>(options);
    ASSERT_EQ(
        memoryManager_->arbitrator()->kind(), MemoryArbitrator::Kind::kShared);
    arbitrator_ = static_cast<SharedArbitrator*>(memoryManager_->arbitrator());
  }

  RowVectorPtr newVector() {
    VectorFuzzer fuzzer(fuzzerOpts_, pool());
    return fuzzer.fuzzRow(rowType_);
  }

  std::shared_ptr<core::QueryCtx> newQueryCtx(
      int64_t memoryCapacity = kMaxMemory) {
    std::unordered_map<std::string, std::shared_ptr<Config>> configs;
    std::shared_ptr<MemoryPool> pool = memoryManager_->addRootPool(
        "", memoryCapacity, MemoryReclaimer::create());
    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(),
        std::unordered_map<std::string, std::string>{},
        configs,
        memory::MemoryAllocator::getInstance(),
        std::move(pool));
    return queryCtx;
  }

  static inline FakeMemoryOperatorFactory* fakeOperatorFactory_;
  std::unique_ptr<MemoryManager> memoryManager_;
  SharedArbitrator* arbitrator_;
  RowTypePtr rowType_;
  VectorFuzzer::Options fuzzerOpts_;
  RowVectorPtr vector_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimFromOrderBy) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> orderByQueryCtx;
    if (sameQuery) {
      orderByQueryCtx = fakeMemoryQueryCtx;
    } else {
      orderByQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto orderByMemoryUsage = 32L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - orderByMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

    std::atomic<bool> injectOrderByOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "OrderBy") {
            return;
          }
          if (op->pool()->capacity() < orderByMemoryUsage) {
            return;
          }
          if (!injectOrderByOnce.exchange(false)) {
            return;
          }
          fakeAllocationWait.notify();
          // Wait for pause to be triggered.
          taskPauseWait.wait(taskPauseWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            ([&](Task* /*unused*/) { taskPauseWait.notify(); })));

    std::thread orderByThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kOrderBySpillEnabled, "true")
              .queryCtx(orderByQueryCtx)
              .plan(
                  PlanBuilder()
                      .values(vectors)
                      .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                      .planNode())
              .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
      auto stats = task->taskStats().pipelineStats;
      ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimToOrderBy) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto oldStats = arbitrator_->stats();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> orderByQueryCtx;
    if (sameQuery) {
      orderByQueryCtx = fakeMemoryQueryCtx;
    } else {
      orderByQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount orderByWait;
    auto orderByWaitKey = orderByWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity - (32L << 20);

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      orderByWait.notify();
      // Wait for pause to be triggered.
      taskPauseWait.wait(taskPauseWaitKey);
      return Allocation{buffer, fakeAllocationSize};
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
              .plan(
                  PlanBuilder()
                      .values(vectors)
                      .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
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
    Task::testingWaitForAllTasksToBeDeleted();
    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
  }
}

TEST_F(SharedArbitrationTest, reclaimFromCompletedOrderBy) {
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> orderByQueryCtx;
    if (sameQuery) {
      orderByQueryCtx = fakeMemoryQueryCtx;
    } else {
      orderByQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

    std::thread orderByThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(orderByQueryCtx)
              .plan(
                  PlanBuilder()
                      .values(vectors)
                      .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                      .planNode())
              .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
      waitForTaskCompletion(task.get());
      fakeAllocationWait.notify();
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimFromAggregation) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> aggregationQueryCtx;
    if (sameQuery) {
      aggregationQueryCtx = fakeMemoryQueryCtx;
    } else {
      aggregationQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto aggregationMemoryUsage = 32L << 20;
    const auto fakeAllocationSize =
        kMemoryCapacity - aggregationMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

    std::atomic<bool> injectAggregationByOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "Aggregation") {
            return;
          }
          if (op->pool()->capacity() < aggregationMemoryUsage) {
            return;
          }
          if (!injectAggregationByOnce.exchange(false)) {
            return;
          }
          fakeAllocationWait.notify();
          // Wait for pause to be triggered.
          taskPauseWait.wait(taskPauseWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            ([&](Task* /*unused*/) { taskPauseWait.notify(); })));

    std::thread aggregationThread([&]() {
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kAggregationSpillEnabled, "true")
              .config(core::QueryConfig::kSpillPartitionBits, "2")
              .queryCtx(aggregationQueryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                        .planNode())
              .assertResults(
                  "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
      auto stats = task->taskStats().pipelineStats;
      ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimToAggregation) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto oldStats = arbitrator_->stats();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> aggregationQueryCtx;
    if (sameQuery) {
      aggregationQueryCtx = fakeMemoryQueryCtx;
    } else {
      aggregationQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount aggregationWait;
    auto aggregationWaitKey = aggregationWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity - (32L << 20);

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      aggregationWait.notify();
      // Wait for pause to be triggered.
      taskPauseWait.wait(taskPauseWaitKey);
      return Allocation{buffer, fakeAllocationSize};
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
    Task::testingWaitForAllTasksToBeDeleted();

    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
  }
}

TEST_F(SharedArbitrationTest, reclaimFromCompletedAggregation) {
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> aggregationQueryCtx;
    if (sameQuery) {
      aggregationQueryCtx = fakeMemoryQueryCtx;
    } else {
      aggregationQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

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
      waitForTaskCompletion(task.get());
      fakeAllocationWait.notify();
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimFromJoinBuilder) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto joinMemoryUsage = 32L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

    std::atomic<bool> injectAggregationByOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          if (op->pool()->currentBytes() < joinMemoryUsage) {
            return;
          }
          if (!injectAggregationByOnce.exchange(false)) {
            return;
          }
          fakeAllocationWait.notify();
          // Wait for pause to be triggered.
          taskPauseWait.wait(taskPauseWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            ([&](Task* /*unused*/) { taskPauseWait.notify(); })));

    std::thread aggregationThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kJoinSpillEnabled, "true")
              .config(core::QueryConfig::kSpillPartitionBits, "2")
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
      auto stats = task->taskStats().pipelineStats;
      ASSERT_GT(stats[1].operatorStats[2].spilledBytes, 0);
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimToJoinBuilder) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto oldStats = arbitrator_->stats();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount joinWait;
    auto joinWaitKey = joinWait.prepareWait();
    folly::EventCount taskPauseWait;
    auto taskPauseWaitKey = taskPauseWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity - (32L << 20);

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      joinWait.notify();
      // Wait for pause to be triggered.
      taskPauseWait.wait(taskPauseWaitKey);
      return Allocation{buffer, fakeAllocationSize};
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
    Task::testingWaitForAllTasksToBeDeleted();

    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
  }
}

TEST_F(SharedArbitrationTest, reclaimFromCompletedJoinBuilder) {
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();

    const auto fakeAllocationSize = kMemoryCapacity;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

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
      waitForTaskCompletion(task.get());
      fakeAllocationWait.notify();
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    reclaimFromJoinBuilderWithMultiDrivers) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 128;
  fuzzerOpts_.stringVariableLength = false;
  fuzzerOpts_.stringLength = 512;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  const int numDrivers = 4;
  createDuckDbTable(vectors);
  // std::vector<bool> sameQueries = {false, true};
  std::vector<bool> sameQueries = {false};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount fakeAllocationWait;
    auto fakeAllocationWaitKey = fakeAllocationWait.prepareWait();
    folly::EventCount taskPauseWait;

    const auto joinMemoryUsage = 8L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{nullptr, 0};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return Allocation{buffer, fakeAllocationSize};
    });

    std::atomic<int> injectCount{0};
    folly::futures::Barrier builderBarrier(numDrivers);
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          // Check all the hash build operators' memory usage instead of
          // individual operator.
          if (op->pool()->parent()->currentBytes() < joinMemoryUsage) {
            return;
          }
          if (++injectCount > numDrivers) {
            return;
          }
          auto future = builderBarrier.wait();
          if (future.wait().value()) {
            fakeAllocationWait.notify();
          }

          auto taskPauseWaitKey = taskPauseWait.prepareWait();
          // Wait for pause to be triggered.
          taskPauseWait.wait(taskPauseWaitKey);
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(
            [&](Task* /*unused*/) { taskPauseWait.notifyAll(); }));

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kJoinSpillEnabled, "true")
              .config(core::QueryConfig::kSpillPartitionBits, "2")
              // NOTE: set an extreme large value to avoid non-reclaimable
              // section in test.
              .config(core::QueryConfig::kSpillableReservationGrowthPct, "8000")
              .maxDrivers(numDrivers)
              .queryCtx(joinQueryCtx)
              .plan(PlanBuilder(planNodeIdGenerator)
                        .values(vectors, true)
                        .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                        .hashJoin(
                            {"t0"},
                            {"u1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(vectors, true)
                                .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                                .planNode(),
                            "",
                            {"t1"},
                            core::JoinType::kInner)
                        .planNode())
              .assertResults(
                  "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1");
      auto stats = task->taskStats().pipelineStats;
      ASSERT_GT(stats[1].operatorStats[2].spilledBytes, 0);
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
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    failedToReclaimFromHashJoinBuildersInNonReclaimableSection) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 128;
  fuzzerOpts_.stringVariableLength = false;
  fuzzerOpts_.stringLength = 512;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  const int numDrivers = 4;
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount allocationWait;
    auto allocationWaitKey = allocationWait.prepareWait();
    folly::EventCount allocationDoneWait;
    auto allocationDoneWaitKey = allocationDoneWait.prepareWait();

    const auto joinMemoryUsage = 8L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{};
      }
      allocationWait.wait(allocationWaitKey);
      EXPECT_ANY_THROW(op->pool()->allocate(fakeAllocationSize));
      allocationDoneWait.notify();
      return Allocation{};
    });

    std::atomic<int> injectCount{0};
    folly::futures::Barrier builderBarrier(numDrivers);
    folly::futures::Barrier pauseBarrier(numDrivers + 1);
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          // Check all the hash build operators' memory usage instead of
          // individual operator.
          if (op->pool()->parent()->currentBytes() < joinMemoryUsage) {
            return;
          }
          if (++injectCount > numDrivers - 1) {
            return;
          }
          if (builderBarrier.wait().get()) {
            allocationWait.notify();
          }
          pauseBarrier.wait();
        })));

    std::atomic<bool> injectNonReclaimableSectionOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              const std::string re(".*HashBuild");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (pool->parent()->currentBytes() < joinMemoryUsage) {
                return;
              }
              if (!injectNonReclaimableSectionOnce.exchange(false)) {
                return;
              }
              if (builderBarrier.wait().get()) {
                allocationWait.notify();
              }
              pauseBarrier.wait();
              // Suspend the driver to simulate the arbitration.
              pool->reclaimer()->enterArbitration();
              allocationDoneWait.wait(allocationDoneWaitKey);
              pool->reclaimer()->leaveArbitration();
            })));

    std::atomic<bool> injectPauseOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>([&](Task* /*unused*/) {
          if (!injectPauseOnce.exchange(false)) {
            return;
          }
          pauseBarrier.wait();
        }));

    // Verifies that we only trigger the hash build reclaim once.
    std::atomic<int> numHashBuildReclaims{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::HashBuild::reclaim",
        std::function<void(Operator*)>([&](Operator* /*unused*/) {
          ++numHashBuildReclaims;
          ASSERT_EQ(numHashBuildReclaims, 1);
        }));

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kJoinSpillEnabled, "true")
              .config(core::QueryConfig::kSpillPartitionBits, "2")
              // NOTE: set an extreme large value to avoid non-reclaimable
              // section in test.
              .config(core::QueryConfig::kSpillableReservationGrowthPct, "8000")
              .maxDrivers(numDrivers)
              .queryCtx(joinQueryCtx)
              .plan(PlanBuilder(planNodeIdGenerator)
                        .values(vectors, true)
                        .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                        .hashJoin(
                            {"t0"},
                            {"u1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(vectors, true)
                                .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                                .planNode(),
                            "",
                            {"t1"},
                            core::JoinType::kInner)
                        .planNode())
              .assertResults(
                  "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1");
      // We expect the spilling is not triggered because of non-reclaimable
      // section.
      auto stats = task->taskStats().pipelineStats;
      ASSERT_EQ(stats[1].operatorStats[2].spilledBytes, 0);
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
    // We only expect to reclaim from one hash build operator once.
    ASSERT_EQ(numHashBuildReclaims, 1);
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    failedToReclaimFromHashJoinBuildersInNotRunningState) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 128;
  fuzzerOpts_.stringVariableLength = false;
  fuzzerOpts_.stringLength = 512;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  const int numDrivers = 4;
  createDuckDbTable(vectors);
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
        newQueryCtx(kMemoryCapacity);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeMemoryQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(kMemoryCapacity);
    }

    folly::EventCount allocationWait;
    auto allocationWaitKey = allocationWait.prepareWait();

    const auto joinMemoryUsage = 8L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return Allocation{};
      }
      allocationWait.wait(allocationWaitKey);
      EXPECT_ANY_THROW(op->pool()->allocate(fakeAllocationSize));
      return Allocation{};
    });

    std::atomic<int> injectCount{0};
    folly::futures::Barrier builderBarrier(numDrivers);
    folly::futures::Barrier pauseBarrier(numDrivers + 1);
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          // Check all the hash build operators' memory usage instead of
          // individual operator.
          if (op->pool()->parent()->currentBytes() < joinMemoryUsage) {
            return;
          }
          if (++injectCount > numDrivers - 1) {
            return;
          }
          if (builderBarrier.wait().get()) {
            allocationWait.notify();
          }
          // Wait for pause to be triggered.
          pauseBarrier.wait();
        })));

    std::atomic<bool> injectNoMoreInputOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          if (!injectNoMoreInputOnce.exchange(false)) {
            return;
          }
          if (builderBarrier.wait().get()) {
            allocationWait.notify();
          }
          // Wait for pause to be triggered.
          pauseBarrier.wait();
        })));

    std::atomic<bool> injectPauseOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>([&](Task* /*unused*/) {
          if (!injectPauseOnce.exchange(false)) {
            return;
          }
          pauseBarrier.wait();
        }));

    // Verifies that we only trigger the hash build reclaim once.
    std::atomic<int> numHashBuildReclaims{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::HashBuild::reclaim",
        std::function<void(Operator*)>(([&](Operator* /*unused*/) {
          ++numHashBuildReclaims;
          ASSERT_EQ(numHashBuildReclaims, 1);
        })));

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kJoinSpillEnabled, "true")
              .config(core::QueryConfig::kSpillPartitionBits, "2")
              // NOTE: set an extreme large value to avoid non-reclaimable
              // section in test.
              .config(core::QueryConfig::kSpillableReservationGrowthPct, "8000")
              .maxDrivers(numDrivers)
              .queryCtx(joinQueryCtx)
              .plan(PlanBuilder(planNodeIdGenerator)
                        .values(vectors, true)
                        .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                        .hashJoin(
                            {"t0"},
                            {"u1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(vectors, true)
                                .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                                .planNode(),
                            "",
                            {"t1"},
                            core::JoinType::kInner)
                        .planNode())
              .assertResults(
                  "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1");
      // We expect the spilling is not triggered because of non-reclaimable
      // section.
      auto stats = task->taskStats().pipelineStats;
      ASSERT_EQ(stats[1].operatorStats[2].spilledBytes, 0);
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
    // We only expect to reclaim from one hash build operator once.
    ASSERT_EQ(numHashBuildReclaims, 1);
    Task::testingWaitForAllTasksToBeDeleted();
  }
}

TEST_F(SharedArbitrationTest, concurrentArbitration) {
  FLAGS_velox_suppress_memory_capacity_exceeding_error_message = true;
  const int numVectors = 8;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 32;
  fuzzerOpts_.stringVariableLength = false;
  fuzzerOpts_.stringLength = 32;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  const int numDrivers = 4;
  createDuckDbTable(vectors);

  const auto queryPlan =
      PlanBuilder()
          .values(vectors, true)
          .addNode([&](std::string id, core::PlanNodePtr input) {
            return std::make_shared<FakeMemoryNode>(id, input);
          })
          .planNode();
  const std::string referenceSQL = "SELECT * FROM tmp";

  std::atomic<bool> stopped{false};

  std::mutex mutex;
  std::vector<std::shared_ptr<core::QueryCtx>> queries;
  std::deque<std::shared_ptr<Task>> zombieTasks;

  fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
    if (folly::Random::oneIn(4)) {
      auto task = op->testingOperatorCtx()->driverCtx()->task;
      if (folly::Random::oneIn(3)) {
        task->requestAbort();
      } else {
        task->requestYield();
      }
    }
    const size_t allocationSize = std::max(
        kMemoryCapacity / 16, folly::Random::rand32() % kMemoryCapacity);
    auto buffer = op->pool()->allocate(allocationSize);
    return Allocation{buffer, allocationSize};
  });
  fakeOperatorFactory_->setMaxDrivers(numDrivers);
  const std::string injectReclaimErrorMessage("Inject reclaim failure");
  fakeOperatorFactory_->setReclaimCallback(
      [&](MemoryPool* /*unused*/, uint64_t /*unused*/) {
        if (folly::Random::oneIn(10)) {
          VELOX_FAIL(injectReclaimErrorMessage);
        }
      });

  const int numThreads = 30;
  const int maxNumZombieTasks = 128;
  std::vector<std::thread> queryThreads;
  for (int i = 0; i < numThreads; ++i) {
    queryThreads.emplace_back([&, i]() {
      folly::Random::DefaultGenerator rng;
      rng.seed(i);
      while (!stopped) {
        std::shared_ptr<core::QueryCtx> query;
        {
          std::lock_guard<std::mutex> l(mutex);
          if (queries.empty()) {
            queries.emplace_back(newQueryCtx());
          }
          const int index = folly::Random::rand32() % queries.size();
          query = queries[index];
        }
        std::shared_ptr<Task> task;
        try {
          task = AssertQueryBuilder(duckDbQueryRunner_)
                     .queryCtx(query)
                     .plan(PlanBuilder()
                               .values(vectors)
                               .addNode([&](std::string id,
                                            core::PlanNodePtr input) {
                                 return std::make_shared<FakeMemoryNode>(
                                     id, input);
                               })
                               .planNode())
                     .assertResults("SELECT * FROM tmp");
        } catch (const VeloxException& e) {
          continue;
        }
        std::lock_guard<std::mutex> l(mutex);
        zombieTasks.emplace_back(std::move(task));
        while (zombieTasks.size() > maxNumZombieTasks) {
          zombieTasks.pop_front();
        }
      }
    });
  }

  const int maxNumQueries = 64;
  std::thread controlThread([&]() {
    folly::Random::DefaultGenerator rng;
    rng.seed(1000);
    while (!stopped) {
      std::shared_ptr<core::QueryCtx> queryToDelete;
      {
        std::lock_guard<std::mutex> l(mutex);
        if (queries.empty() ||
            ((queries.size() < maxNumQueries) &&
             folly::Random::oneIn(4, rng))) {
          queries.emplace_back(newQueryCtx());
        } else {
          const int deleteIndex = folly::Random::rand32(rng) % queries.size();
          queryToDelete = queries[deleteIndex];
          queries.erase(queries.begin() + deleteIndex);
        }
      }
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(5));
  stopped = true;

  for (auto& queryThread : queryThreads) {
    queryThread.join();
  }
  controlThread.join();
}

// TODO: add more tests.

} // namespace
} // namespace facebook::velox::memory

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
