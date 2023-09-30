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
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Values.h"
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
constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;

constexpr uint64_t kMemoryCapacity = 512 * MB;
constexpr uint64_t kMemoryPoolInitCapacity = 16 * MB;
constexpr uint64_t kMemoryPoolTransferCapacity = 8 * MB;

struct TestAllocation {
  MemoryPool* pool{nullptr};
  void* buffer{nullptr};
  size_t size{0};

  size_t free() {
    const size_t freedBytes = size;
    if (pool == nullptr) {
      VELOX_CHECK_EQ(freedBytes, 0);
      return freedBytes;
    }
    VELOX_CHECK_GT(freedBytes, 0);
    pool->free(buffer, freedBytes);
    pool = nullptr;
    buffer = nullptr;
    size = 0;
    return freedBytes;
  }
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

using AllocationCallback = std::function<TestAllocation(Operator* op)>;
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
      totalBytes_ -= allocation.free();
      VELOX_CHECK_GE(totalBytes_, 0);
    }
    allocations_.clear();
    VELOX_CHECK_EQ(totalBytes_, 0);
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

class FakeMemoryReclaimer : public MemoryReclaimer {
 public:
  FakeMemoryReclaimer() = default;

  static std::unique_ptr<MemoryReclaimer> create() {
    return std::make_unique<FakeMemoryReclaimer>();
  }

  void enterArbitration() override {
    auto* driverThreadCtx = driverThreadContext();
    if (driverThreadCtx == nullptr) {
      return;
    }
    auto* driver = driverThreadCtx->driverCtx.driver;
    ASSERT_TRUE(driver != nullptr);
    if (driver->task()->enterSuspended(driver->state()) != StopReason::kNone) {
      VELOX_FAIL("Terminate detected when entering suspension");
    }
  }

  void leaveArbitration() noexcept override {
    auto* driverThreadCtx = driverThreadContext();
    if (driverThreadCtx == nullptr) {
      return;
    }
    auto* driver = driverThreadCtx->driverCtx.driver;
    ASSERT_TRUE(driver != nullptr);
    driver->task()->leaveSuspended(driver->state());
  }
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
    HiveConnectorTestBase::TearDown();
  }

  void setupMemory(
      int64_t memoryCapacity = 0,
      uint64_t memoryPoolInitCapacity = kMemoryPoolInitCapacity,
      uint64_t memoryPoolTransferCapacity = kMemoryPoolTransferCapacity) {
    memoryCapacity = (memoryCapacity != 0) ? memoryCapacity : kMemoryCapacity;
    allocator_ = std::make_shared<MallocAllocator>(memoryCapacity);
    MemoryManagerOptions options;
    options.allocator = allocator_.get();
    options.capacity = allocator_->capacity();
    options.arbitratorKind = "SHARED";
    options.capacity = options.capacity;
    options.memoryPoolInitCapacity = memoryPoolInitCapacity;
    options.memoryPoolTransferCapacity = memoryPoolTransferCapacity;
    options.checkUsageLeak = true;
    options.arbitrationStateCheckCb = memoryArbitrationStateCheck;
    memoryManager_ = std::make_unique<MemoryManager>(options);
    ASSERT_EQ(memoryManager_->arbitrator()->kind(), "SHARED");
    arbitrator_ = static_cast<SharedArbitrator*>(memoryManager_->arbitrator());
  }

  RowVectorPtr newVector() {
    VectorFuzzer fuzzer(fuzzerOpts_, pool());
    return fuzzer.fuzzRow(rowType_);
  }

  std::shared_ptr<core::QueryCtx> newQueryCtx(
      int64_t memoryCapacity = kMaxMemory,
      std::unique_ptr<MemoryReclaimer>&& reclaimer = nullptr) {
    std::unordered_map<std::string, std::shared_ptr<Config>> configs;
    std::shared_ptr<MemoryPool> pool = memoryManager_->addRootPool(
        "",
        memoryCapacity,
        reclaimer != nullptr ? std::move(reclaimer)
                             : MemoryReclaimer::create());
    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(),
        std::unordered_map<std::string, std::string>{},
        configs,
        cache::AsyncDataCache::getInstance(),
        std::move(pool));
    return queryCtx;
  }

  static inline FakeMemoryOperatorFactory* fakeOperatorFactory_;
  std::shared_ptr<MemoryAllocator> allocator_;
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
        return TestAllocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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
    waitForAllTasksToBeDeleted();
  }
}

class TestMemoryReclaimer : public MemoryReclaimer {
 public:
  TestMemoryReclaimer(std::function<void(MemoryPool*)> reclaimCb)
      : reclaimCb_(std::move(reclaimCb)) {}

  uint64_t reclaim(MemoryPool* pool, uint64_t targetBytes) override {
    if (pool->kind() == MemoryPool::Kind::kLeaf) {
      return 0;
    }
    std::vector<std::shared_ptr<MemoryPool>> children;
    {
      children.reserve(pool->children_.size());
      for (auto& entry : pool->children_) {
        auto child = entry.second.lock();
        if (child != nullptr) {
          children.push_back(std::move(child));
        }
      }
    }

    std::vector<ArbitrationCandidate> candidates(children.size());
    for (uint32_t i = 0; i < children.size(); ++i) {
      candidates[i].pool = children[i].get();
      children[i]->reclaimableBytes(candidates[i].reclaimableBytes);
    }
    sortCandidatesByReclaimableMemoryAsc(candidates);

    uint64_t reclaimedBytes{0};
    for (const auto& candidate : candidates) {
      const auto bytes = candidate.pool->reclaim(targetBytes);
      if (reclaimCb_ != nullptr) {
        reclaimCb_(candidate.pool);
      }
      reclaimedBytes += bytes;
      if (targetBytes != 0) {
        if (bytes >= targetBytes) {
          break;
        }
        targetBytes -= bytes;
      }
    }
    return reclaimedBytes;
  }

 private:
  struct ArbitrationCandidate {
    uint64_t reclaimableBytes{0};
    MemoryPool* pool{nullptr};
  };

  void sortCandidatesByReclaimableMemoryAsc(
      std::vector<ArbitrationCandidate>& candidates) {
    std::sort(
        candidates.begin(),
        candidates.end(),
        [](const ArbitrationCandidate& lhs, const ArbitrationCandidate& rhs) {
          return lhs.reclaimableBytes < rhs.reclaimableBytes;
        });
  }

  std::function<void(MemoryPool*)> reclaimCb_{nullptr};
};

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    filterProjectInitTriggeredArbitration) {
  setupMemory(1.5 * MB, 0);
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  std::atomic_bool filterProjectDriverInitHit{false};
  folly::EventCount fakeMemoryTaskCreationWait;
  std::atomic_bool filterProjectDriverInitUnblocked{false};
  folly::EventCount filterProjectDriverInitWait;
  std::atomic_bool filterProjectReclaimFinished{false};
  folly::EventCount fakeOperatorAllocCbWait;
  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(
      2 * MB, std::make_unique<TestMemoryReclaimer>([&](MemoryPool* pool) {
        if (pool->reservedBytes() == 0) {
          filterProjectReclaimFinished = true;
          fakeOperatorAllocCbWait.notifyAll();
        }
      }));

  std::atomic_bool createDriverBlockedOnce{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::createDriversLocked",
      std::function<void(void*)>([&](void* /*unused*/) {
        if (!createDriverBlockedOnce.exchange(true)) {
          filterProjectDriverInitHit = true;
          fakeMemoryTaskCreationWait.notifyAll();
          filterProjectDriverInitWait.await(
              [&]() { return filterProjectDriverInitUnblocked.load(); });
        }
      }));

  std::thread filterProjectThread([&]() {
    auto task1 =
        AssertQueryBuilder(duckDbQueryRunner_)
            .queryCtx(queryCtx)
            .plan(
                PlanBuilder()
                    .values(vectors)
                    .project(
                        {"c0 / c1 as x",
                         "c1 / c0 as y",
                         "'need 12 chars to be larger than inline size (StringView::kInlineSize) to trigger pool allocation from expr compiling' as z"})
                    .planNode())
            .assertResults(
                "SELECT c0 / c1 as x, c1 / c0 as y, 'need 12 chars to be larger than inline size (StringView::kInlineSize) to trigger pool allocation from expr compiling' as z FROM tmp");
  });

  fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
    auto buffer = op->pool()->allocate(1 * MB);
    filterProjectDriverInitUnblocked = true;
    filterProjectDriverInitWait.notifyAll();
    fakeOperatorAllocCbWait.await(
        [&]() { return filterProjectReclaimFinished.load(); });
    return TestAllocation{op->pool(), buffer, 1 * MB};
  });
  std::thread fakeMemoryThread([&]() {
    fakeMemoryTaskCreationWait.await(
        [&]() { return filterProjectDriverInitHit.load(); });
    try {
      auto task2 =
          AssertQueryBuilder(duckDbQueryRunner_)
              .queryCtx(queryCtx)
              .plan(PlanBuilder()
                        .values(vectors)
                        .addNode([&](std::string id, core::PlanNodePtr input) {
                          return std::make_shared<FakeMemoryNode>(id, input);
                        })
                        .planNode())
              .assertResults("SELECT * FROM tmp");
    } catch (const VeloxRuntimeError& e) {
      ASSERT_EQ(e.errorCode(), error_code::kMemCapExceeded);
    }
  });
  waitForAllTasksToBeDeleted(10'000'000);
  filterProjectThread.join();
  fakeMemoryThread.join();
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
    waitForAllTasksToBeDeleted();
    const auto newStats = arbitrator_->stats();
    ASSERT_GT(newStats.numReclaimedBytes, oldStats.numReclaimedBytes);
    ASSERT_GT(newStats.reclaimTimeUs, oldStats.reclaimTimeUs);
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
        return TestAllocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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
    waitForAllTasksToBeDeleted();
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
        kMemoryCapacity - aggregationMemoryUsage + 1;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return TestAllocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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
              .config(core::QueryConfig::kAggregationSpillPartitionBits, "2")
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
    waitForAllTasksToBeDeleted();
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
        return TestAllocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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
    waitForAllTasksToBeDeleted();
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

    std::atomic_bool fakeAllocationReady{false};
    folly::EventCount fakeAllocationWait;
    std::atomic_bool taskPauseDone{false};
    folly::EventCount taskPauseWait;

    const auto joinMemoryUsage = 32L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return TestAllocation{};
      }
      fakeAllocationWait.await([&]() { return fakeAllocationReady.load(); });
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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
          fakeAllocationReady.store(true);
          fakeAllocationWait.notifyAll();
          // Wait for pause to be triggered.
          taskPauseWait.await([&]() { return taskPauseDone.load(); });
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(([&](Task* /*unused*/) {
          taskPauseDone.store(true);
          taskPauseWait.notifyAll();
        })));

    // joinQueryCtx and fakeMemoryQueryCtx may be the same and thus share the
    // same underlying QueryConfig.  We apply the changes here instead of using
    // the AssertQueryBuilder to avoid a potential race condition caused by
    // writing the config in the join thread, and reading it in the memThread.
    std::unordered_map<std::string, std::string> config{
        {core::QueryConfig::kSpillEnabled, "true"},
        {core::QueryConfig::kJoinSpillEnabled, "true"},
        {core::QueryConfig::kJoinSpillPartitionBits, "2"},
    };
    joinQueryCtx->testingOverrideConfigUnsafe(std::move(config));

    std::thread aggregationThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
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
    waitForAllTasksToBeDeleted();
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
    const uint64_t numCreatedTasks = Task::numCreatedTasks();
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
        return TestAllocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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
      task.reset();
      // Make sure the join query task has been destroyed.
      waitForAllTasksToBeDeleted(numCreatedTasks + 1, 3'000'000);
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
    waitForAllTasksToBeDeleted();
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

    const auto joinMemoryUsage = 8L << 20;
    const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

    std::atomic<bool> injectAllocationOnce{true};
    fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
      if (!injectAllocationOnce.exchange(false)) {
        return TestAllocation{};
      }
      fakeAllocationWait.wait(fakeAllocationWaitKey);
      auto buffer = op->pool()->allocate(fakeAllocationSize);
      return TestAllocation{op->pool(), buffer, fakeAllocationSize};
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

    // joinQueryCtx and fakeMemoryQueryCtx may be the same and thus share the
    // same underlying QueryConfig.  We apply the changes here instead of using
    // the AssertQueryBuilder to avoid a potential race condition caused by
    // writing the config in the join thread, and reading it in the memThread.
    std::unordered_map<std::string, std::string> config{
        {core::QueryConfig::kSpillEnabled, "true"},
        {core::QueryConfig::kJoinSpillEnabled, "true"},
        {core::QueryConfig::kJoinSpillPartitionBits, "2"},
        // NOTE: set an extreme large value to avoid non-reclaimable
        // section in test.
        {core::QueryConfig::kSpillableReservationGrowthPct, "8000"},
    };
    joinQueryCtx->testingOverrideConfigUnsafe(std::move(config));

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
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
    waitForAllTasksToBeDeleted();
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
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
      newQueryCtx(kMemoryCapacity);
  std::shared_ptr<core::QueryCtx> joinQueryCtx = newQueryCtx(kMemoryCapacity);

  folly::EventCount allocationWait;
  auto allocationWaitKey = allocationWait.prepareWait();
  folly::EventCount allocationDoneWait;
  auto allocationDoneWaitKey = allocationDoneWait.prepareWait();

  const auto joinMemoryUsage = 8L << 20;
  const auto fakeAllocationSize = kMemoryCapacity - joinMemoryUsage / 2;

  std::atomic<bool> injectAllocationOnce{true};
  fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
    if (!injectAllocationOnce.exchange(false)) {
      return TestAllocation{};
    }
    allocationWait.wait(allocationWaitKey);
    EXPECT_ANY_THROW(op->pool()->allocate(fakeAllocationSize));
    allocationDoneWait.notify();
    return TestAllocation{};
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
            .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
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
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    reclaimFromJoinBuildWaitForTableBuild) {
  setupMemory(kMemoryCapacity, 0);
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

  std::shared_ptr<core::QueryCtx> fakeMemoryQueryCtx =
      newQueryCtx(kMemoryCapacity);
  std::shared_ptr<core::QueryCtx> joinQueryCtx = newQueryCtx(kMemoryCapacity);

  folly::EventCount fakeAllocationWait;
  std::atomic<bool> fakeAllocationUnblock{false};
  std::atomic<bool> injectFakeAllocationOnce{true};

  fakeOperatorFactory_->setAllocationCallback([&](Operator* op) {
    if (!injectFakeAllocationOnce.exchange(false)) {
      return TestAllocation{};
    }
    fakeAllocationWait.await([&]() { return fakeAllocationUnblock.load(); });
    // Set the fake allocation size to trigger memory reclaim.
    const auto fakeAllocationSize = arbitrator_->stats().freeCapacityBytes +
        joinQueryCtx->pool()->freeBytes() + 1;
    return TestAllocation{
        op->pool(),
        op->pool()->allocate(fakeAllocationSize),
        fakeAllocationSize};
  });

  folly::futures::Barrier builderBarrier(numDrivers);
  folly::futures::Barrier pauseBarrier(numDrivers + 1);
  std::atomic<int> addInputInjectCount{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal",
      std::function<void(Driver*)>(([&](Driver* driver) {
        // Check if the driver is from join query.
        if (driver->task()->queryCtx()->pool()->name() !=
            joinQueryCtx->pool()->name()) {
          return;
        }
        // Check if the driver is from the pipeline with hash build.
        if (driver->driverCtx()->pipelineId != 1) {
          return;
        }
        if (++addInputInjectCount > numDrivers - 1) {
          return;
        }
        if (builderBarrier.wait().get()) {
          fakeAllocationUnblock = true;
          fakeAllocationWait.notifyAll();
        }
        // Wait for pause to be triggered.
        pauseBarrier.wait().get();
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
          fakeAllocationUnblock = true;
          fakeAllocationWait.notifyAll();
        }
        // Wait for pause to be triggered.
        pauseBarrier.wait().get();
      })));

  std::atomic<bool> injectPauseOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::requestPauseLocked",
      std::function<void(Task*)>([&](Task* /*unused*/) {
        if (!injectPauseOnce.exchange(false)) {
          return;
        }
        pauseBarrier.wait().get();
      }));

  // Verifies that we only trigger the hash build reclaim once.
  std::atomic<int> numHashBuildReclaims{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::reclaim",
      std::function<void(Operator*)>(
          ([&](Operator* /*unused*/) { ++numHashBuildReclaims; })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::thread joinThread([&]() {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kJoinSpillEnabled, "true")
            .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
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
    // We expect the spilling triggered.
    auto stats = task->taskStats().pipelineStats;
    ASSERT_GT(stats[1].operatorStats[2].spilledBytes, 0);
  });

  std::thread memThread([&]() {
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
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    arbitrationTriggeredDuringParallelJoinBuild) {
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  // Build a large vector to trigger memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx = newQueryCtx(kMemoryCapacity);

  // Make sure the parallel build has been triggered.
  std::atomic<bool> parallelBuildTriggered{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(void*)>(
          [&](void*) { parallelBuildTriggered = true; }));

  // TODO: add driver context to test if the memory allocation is triggered in
  // driver context or not.

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  AssertQueryBuilder(duckDbQueryRunner_)
      // Set very low table size threshold to trigger parallel build.
      .config(
          core::QueryConfig::kMinTableRowsForParallelJoinBuild,
          std::to_string(0))
      // Set multiple hash build drivers to trigger parallel build.
      .maxDrivers(4)
      .queryCtx(joinQueryCtx)
      .plan(PlanBuilder(planNodeIdGenerator)
                .values(vectors, true)
                .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                .hashJoin(
                    {"t0", "t1"},
                    {"u1", "u0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values(vectors, true)
                        .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                        .planNode(),
                    "",
                    {"t1"},
                    core::JoinType::kInner)
                .planNode())
      .assertResults(
          "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1 AND t.c1 == u.c0");
  ASSERT_TRUE(parallelBuildTriggered);
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(
    SharedArbitrationTest,
    arbitrationTriggeredByEnsureJoinTableFit) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 10'000;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);
  std::shared_ptr<core::QueryCtx> fakeCtx = newQueryCtx(kMemoryCapacity);
  auto fakePool = fakeCtx->pool()->addLeafChild(
      "fakePool", true, FakeMemoryReclaimer::create());
  std::vector<std::unique_ptr<TestAllocation>> injectAllocations;
  std::atomic<bool> injectAllocationOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::ensureTableFits",
      std::function<void(HashBuild*)>([&](HashBuild* buildOp) {
        // Inject the allocation once to ensure the merged table allocation will
        // trigger memory arbitration.
        if (!injectAllocationOnce.exchange(false)) {
          return;
        }
        auto* buildPool = buildOp->pool();
        // Free up available reservation from the leaf build memory pool.
        uint64_t injectAllocationSize = buildPool->availableReservation();
        injectAllocations.emplace_back(new TestAllocation{
            buildPool,
            buildPool->allocate(injectAllocationSize),
            injectAllocationSize});
        // Free up available memory from the system.
        injectAllocationSize = arbitrator_->stats().freeCapacityBytes +
            queryCtx->pool()->freeBytes();
        injectAllocations.emplace_back(new TestAllocation{
            fakePool.get(),
            fakePool->allocate(injectAllocationSize),
            injectAllocationSize});
      }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::reclaim",
      std::function<void(Operator*)>([&](Operator* /*unused*/) {
        ASSERT_EQ(injectAllocations.size(), 2);
        for (auto& injectAllocation : injectAllocations) {
          injectAllocation->free();
        }
      }));

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kJoinSpillEnabled, "true")
          .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
          // Set multiple hash build drivers to trigger parallel build.
          .maxDrivers(4)
          .queryCtx(queryCtx)
          .plan(PlanBuilder(planNodeIdGenerator)
                    .values(vectors, true)
                    .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                    .hashJoin(
                        {"t0", "t1"},
                        {"u1", "u0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(vectors, true)
                            .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                            .planNode(),
                        "",
                        {"t1"},
                        core::JoinType::kInner)
                    .planNode())
          .assertResults(
              "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1 AND t.c1 == u.c0");
  task.reset();
  waitForAllTasksToBeDeleted();
  ASSERT_EQ(injectAllocations.size(), 2);
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, reclaimDuringJoinTableBuild) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  fuzzerOpts_.vectorSize = 10'000;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);

  std::atomic<bool> blockTableBuildOpOnce{true};
  std::atomic<bool> tableBuildBlocked{false};
  folly::EventCount tableBuildBlockWait;
  std::atomic<bool> unblockTableBuild{false};
  folly::EventCount unblockTableBuildWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(MemoryPool*)>(([&](MemoryPool* pool) {
        if (!blockTableBuildOpOnce.exchange(false)) {
          return;
        }
        tableBuildBlocked = true;
        tableBuildBlockWait.notifyAll();
        unblockTableBuildWait.await([&]() { return unblockTableBuild.load(); });
        void* buffer = pool->allocate(kMemoryCapacity / 4);
        pool->free(buffer, kMemoryCapacity / 4);
      })));

  std::thread queryThread([&]() {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kJoinSpillEnabled, "true")
            .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
            // Set multiple hash build drivers to trigger parallel build.
            .maxDrivers(4)
            .queryCtx(queryCtx)
            .plan(PlanBuilder(planNodeIdGenerator)
                      .values(vectors, true)
                      .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                      .hashJoin(
                          {"t0", "t1"},
                          {"u1", "u0"},
                          PlanBuilder(planNodeIdGenerator)
                              .values(vectors, true)
                              .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                              .planNode(),
                          "",
                          {"t1"},
                          core::JoinType::kInner)
                      .planNode())
            .assertResults(
                "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1 AND t.c1 == u.c0");
  });

  tableBuildBlockWait.await([&]() { return tableBuildBlocked.load(); });

  folly::EventCount taskPauseWait;
  std::atomic<bool> taskPaused{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::requestPauseLocked",
      std::function<void(Task*)>(([&](Task* /*unused*/) {
        taskPaused = true;
        taskPauseWait.notifyAll();
      })));

  std::unique_ptr<TestAllocation> fakeAllocation;
  std::thread memThread([&]() {
    std::shared_ptr<core::QueryCtx> fakeCtx = newQueryCtx(kMemoryCapacity);
    auto fakePool = fakeCtx->pool()->addLeafChild("fakePool");
    const auto fakeAllocationSize = arbitrator_->stats().freeCapacityBytes +
        queryCtx->pool()->freeBytes() + 1;
    VELOX_ASSERT_THROW(
        fakePool->allocate(fakeAllocationSize), "Exceeded memory pool cap");
  });

  taskPauseWait.await([&]() { return taskPaused.load(); });

  unblockTableBuild = true;
  unblockTableBuildWait.notifyAll();

  memThread.join();
  queryThread.join();

  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, driverInitTriggeredArbitration) {
  const int numVectors = 2;
  std::vector<RowVectorPtr> vectors;
  const int vectorSize = 100;
  fuzzerOpts_.vectorSize = vectorSize;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
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
  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);
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
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);
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
        MemoryPool* pool = values->pool();
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
      std::function<void(MemoryPool*)>(([&](MemoryPool* pool) {
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
            .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
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
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  auto queryCtx = newQueryCtx(kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);

  // Create a fake query to hold some memory to trigger memory arbitration.
  auto fakeQueryCtx = newQueryCtx(kMemoryCapacity);
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
            .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
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
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);
  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);

  folly::EventCount aggregationAllocationWait;
  std::atomic<bool> aggregationAllocationOnce{true};
  folly::EventCount aggregationAllocationUnblockWait;
  std::atomic<bool> aggregationAllocationUnblocked{false};
  std::atomic<MemoryPool*> injectPool{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(MemoryPool*)>(([&](MemoryPool* pool) {
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
               .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
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

DEBUG_ONLY_TEST_F(SharedArbitrationTest, arbitrationFromTableWriter) {
  setupMemory(kMemoryCapacity, 0);

  VectorFuzzer::Options options;
  const int batchSize = 1000;
  options.vectorSize = batchSize;
  VectorFuzzer fuzzer(options, pool());
  const int numBatches = 10;
  std::vector<RowVectorPtr> vectors;
  int numRows{0};
  for (int i = 0; i < numBatches; ++i) {
    numRows += batchSize;
    vectors.push_back(fuzzer.fuzzRow(rowType_));
  }

  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), 0);

  std::atomic<bool> injectArbitrationOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
        const std::string dictPoolRe(".*dictionary");
        const std::string generalPoolRe(".*general");
        const std::string compressionPoolRe(".*compression");
        if (!RE2::FullMatch(pool->name(), dictPoolRe) &&
            !RE2::FullMatch(pool->name(), generalPoolRe) &&
            !RE2::FullMatch(pool->name(), compressionPoolRe)) {
          return;
        }
        if (pool->currentBytes() == 0) {
          return;
        }
        if (!injectArbitrationOnce.exchange(false)) {
          return;
        }
        const auto fakeAllocationSize =
            arbitrator_->stats().maxCapacityBytes - pool->currentBytes();
        VELOX_ASSERT_THROW(
            pool->allocate(fakeAllocationSize), "Exceeded memory pool");
      }));

  auto outputDirectory = TempDirectoryPath::create();
  auto writerPlan =
      PlanBuilder()
          .values(vectors)
          .tableWrite(outputDirectory->path)
          .project({TableWriteTraits::rowCountColumnName()})
          .singleAggregation(
              {},
              {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())})
          .planNode();

  AssertQueryBuilder(duckDbQueryRunner_)
      .queryCtx(queryCtx)
      .spillDirectory(outputDirectory->path)
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kJoinSpillEnabled, "true")
      .config(core::QueryConfig::kJoinSpillPartitionBits, "2")
      .plan(std::move(writerPlan))
      .assertResults(fmt::format("SELECT {}", numRows));

  ASSERT_EQ(arbitrator_->stats().numFailures, 1);
}

DEBUG_ONLY_TEST_F(SharedArbitrationTest, arbitrateMemoryFromOtherOperator) {
  setupMemory(kMemoryCapacity, 0);
  const int numVectors = 10;
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(newVector());
  }
  createDuckDbTable(vectors);

  for (bool sameDriver : {false, true}) {
    SCOPED_TRACE(fmt::format("sameDriver {}", sameDriver));
    std::shared_ptr<core::QueryCtx> queryCtx = newQueryCtx(kMemoryCapacity);
    ASSERT_EQ(queryCtx->pool()->capacity(), 0);

    std::atomic<bool> injectAllocationOnce{true};
    const int initialBufferLen = 1 << 20;
    std::atomic<void*> buffer{nullptr};
    std::atomic<MemoryPool*> bufferPool{nullptr};
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
    return TestAllocation{op->pool(), buffer, allocationSize};
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
      DuckDbQueryRunner duckDbQueryRunner;
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
          task = AssertQueryBuilder(duckDbQueryRunner)
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

} // namespace facebook::velox::memory

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
