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

#include <fmt/format.h>
#include <re2/re2.h>
#include <deque>
#include <vector>
#include "folly/experimental/EventCount.h"
#include "folly/futures/Barrier.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

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
constexpr uint64_t kMemoryPoolInitCapacity = 16 * MB;
constexpr uint64_t kMemoryPoolTransferCapacity = 8 * MB;

class MemoryReclaimer;
class MockMemoryOperator;

using ReclaimInjectionCallback =
    std::function<void(MemoryPool* pool, uint64_t targetByte)>;
using ArbitrationInjectionCallback = std::function<void()>;

struct Allocation {
  void* buffer{nullptr};
  size_t size{0};
};

class MockTask : public std::enable_shared_from_this<MockTask> {
 public:
  MockTask() {}

  ~MockTask();

  class MemoryReclaimer : public memory::MemoryReclaimer {
   public:
    MemoryReclaimer(const std::shared_ptr<MockTask>& task) : task_(task) {}

    static std::unique_ptr<MemoryReclaimer> create(
        const std::shared_ptr<MockTask>& task) {
      return std::make_unique<MemoryReclaimer>(task);
    }

    void abort(MemoryPool* pool, const std::exception_ptr& error) override {
      auto task = task_.lock();
      if (task == nullptr) {
        return;
      }
      task->setError(error);
      memory::MemoryReclaimer::abort(pool, error);
    }

   private:
    std::weak_ptr<MockTask> task_;
  };

  void initTaskPool(MemoryManager* manager, uint64_t capacity) {
    root_ = manager->addRootPool(
        fmt::format("RootPool-{}", poolId_++),
        capacity,
        MemoryReclaimer::create(shared_from_this()));
  }

  MemoryPool* pool() const {
    return root_.get();
  }

  uint64_t capacity() const {
    return root_->capacity();
  }

  MockMemoryOperator* addMemoryOp(
      bool isReclaimable = true,
      ReclaimInjectionCallback reclaimInjectCb = nullptr,
      ArbitrationInjectionCallback arbitrationInjectCb = nullptr);

  MockMemoryOperator* memoryOp(int index = -1) {
    VELOX_CHECK(!ops_.empty());
    if (index == -1) {
      return ops_[nextOp_++ % ops_.size()].get();
    } else {
      VELOX_CHECK_LT(index, ops_.size());
      return ops_[index].get();
    }
  }

  std::exception_ptr error() const {
    return error_;
  }

  void setError(const std::exception_ptr& error) {
    error_ = error;
  }

 private:
  inline static std::atomic<int64_t> poolId_{0};
  std::shared_ptr<MemoryPool> root_;
  std::atomic<uint64_t> nextOp_{0};
  std::vector<std::shared_ptr<MemoryPool>> pools_;
  std::vector<std::shared_ptr<MockMemoryOperator>> ops_;
  std::exception_ptr error_{nullptr};
};

class MockMemoryOperator {
 public:
  MockMemoryOperator() = default;

  ~MockMemoryOperator() {
    freeAll();
  }

  class MemoryReclaimer : public memory::MemoryReclaimer {
   public:
    explicit MemoryReclaimer(
        std::shared_ptr<MockMemoryOperator> op,
        bool reclaimable,
        ReclaimInjectionCallback reclaimInjectCb = nullptr,
        ArbitrationInjectionCallback arbitrationInjectCb = nullptr)
        : op_(op),
          reclaimable_(reclaimable),
          reclaimInjectCb_(std::move(reclaimInjectCb)),
          arbitrationInjectCb_(std::move(arbitrationInjectCb)) {}

    bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
        const override {
      if (!reclaimable_) {
        return false;
      }
      return op_->reclaimableBytes(pool, reclaimableBytes);
    }

    uint64_t reclaim(
        MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t /*unused*/,
        Stats& stats) override {
      ++numReclaims_;
      if (!reclaimable_) {
        return 0;
      }
      if (reclaimInjectCb_ != nullptr) {
        reclaimInjectCb_(pool, targetBytes);
      }
      reclaimTargetBytes_.push_back(targetBytes);
      auto reclaimBytes = op_->reclaim(pool, targetBytes);
      stats.reclaimedBytes += reclaimBytes;
      return reclaimBytes;
    }

    void enterArbitration() override {
      if (arbitrationInjectCb_ != nullptr) {
        arbitrationInjectCb_();
      }
      ++numEnterArbitrations_;
    }

    void leaveArbitration() noexcept override {
      ++numLeaveArbitrations_;
    }

    void abort(MemoryPool* pool, const std::exception_ptr& error) override {
      ++numAborts_;
      error_ = error;
      op_->abort(pool);
    }

    struct Stats {
      uint64_t numEnterArbitrations;
      uint64_t numLeaveArbitrations;
      uint64_t numReclaims;
      uint64_t numAborts;
      std::vector<uint64_t> reclaimTargetBytes;
    };

    Stats stats() const {
      Stats stats;
      stats.numEnterArbitrations = numEnterArbitrations_;
      stats.numLeaveArbitrations = numLeaveArbitrations_;
      stats.numReclaims = numReclaims_;
      stats.reclaimTargetBytes = reclaimTargetBytes_;
      stats.numAborts = numAborts_;
      return stats;
    }

   private:
    const std::shared_ptr<MockMemoryOperator> op_;
    const bool reclaimable_;
    const ReclaimInjectionCallback reclaimInjectCb_;
    const ArbitrationInjectionCallback arbitrationInjectCb_;

    std::exception_ptr error_;

    std::atomic<uint64_t> numEnterArbitrations_{0};
    std::atomic<uint64_t> numLeaveArbitrations_{0};
    std::atomic<uint64_t> numReclaims_{0};
    std::atomic<uint64_t> numAborts_{0};
    std::vector<uint64_t> reclaimTargetBytes_;
  };

  void* allocate(uint64_t bytes) {
    VELOX_CHECK_EQ(bytes % pool_->alignment(), 0);
    void* buffer = pool_->allocate(bytes);
    std::lock_guard<std::mutex> l(mu_);
    totalBytes_ += bytes;
    allocations_.emplace(buffer, bytes);
    VELOX_CHECK_EQ(allocations_.count(buffer), 1);
    return buffer;
  }

  void free(void* buffer) {
    size_t size;
    {
      std::lock_guard<std::mutex> l(mu_);
      VELOX_CHECK_EQ(allocations_.count(buffer), 1);
      size = allocations_[buffer];
      totalBytes_ -= size;
      allocations_.erase(buffer);
    }
    pool_->free(buffer, size);
  }

  void freeAll() {
    std::unordered_map<void*, size_t> allocations;
    {
      std::lock_guard<std::mutex> l(mu_);
      for (auto entry : allocations_) {
        totalBytes_ -= entry.second;
      }
      allocations.swap(allocations_);
      VELOX_CHECK_EQ(totalBytes_, 0);
    }
    for (auto entry : allocations) {
      pool_->free(entry.first, entry.second);
    }
  }

  void free() {
    Allocation allocation;
    {
      std::lock_guard<std::mutex> l(mu_);
      if (allocations_.empty()) {
        return;
      }
      allocation.buffer = allocations_.begin()->first;
      allocation.size = allocations_.begin()->second;
      totalBytes_ -= allocation.size;
      allocations_.erase(allocations_.begin());
    }
    pool_->free(allocation.buffer, allocation.size);
  }

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const {
    reclaimableBytes = 0;
    std::lock_guard<std::mutex> l(mu_);
    if (pool_ == nullptr) {
      return false;
    }
    VELOX_CHECK_EQ(pool.name(), pool_->name());
    reclaimableBytes = totalBytes_;
    return true;
  }

  uint64_t reclaim(MemoryPool* pool, uint64_t targetBytes) {
    VELOX_CHECK_GT(targetBytes, 0);
    uint64_t bytesReclaimed{0};
    std::vector<Allocation> allocationsToFree;
    {
      std::lock_guard<std::mutex> l(mu_);
      VELOX_CHECK_NOT_NULL(pool_);
      VELOX_CHECK_EQ(pool->name(), pool_->name());
      auto allocIt = allocations_.begin();
      while (allocIt != allocations_.end() &&
             ((targetBytes != 0) && (bytesReclaimed < targetBytes))) {
        allocationsToFree.push_back({allocIt->first, allocIt->second});
        bytesReclaimed += allocIt->second;
        allocIt = allocations_.erase(allocIt);
      }
      totalBytes_ -= bytesReclaimed;
    }
    for (const auto& allocation : allocationsToFree) {
      pool_->free(allocation.buffer, allocation.size);
    }
    return pool_->shrink(targetBytes);
  }

  void abort(MemoryPool* pool) {
    std::vector<Allocation> allocationsToFree;
    {
      std::lock_guard<std::mutex> l(mu_);
      VELOX_CHECK_NOT_NULL(pool_);
      VELOX_CHECK_EQ(pool->name(), pool_->name());
      for (auto allocEntry : allocations_) {
        allocationsToFree.push_back(
            Allocation{allocEntry.first, allocEntry.second});
        totalBytes_ -= allocEntry.second;
      }
      allocations_.clear();
    }
    for (const auto& allocation : allocationsToFree) {
      pool_->free(allocation.buffer, allocation.size);
    }
  }

  void setPool(MemoryPool* pool) {
    std::lock_guard<std::mutex> l(mu_);
    VELOX_CHECK_NOT_NULL(pool);
    VELOX_CHECK_NULL(pool_);
    pool_ = pool;
  }

  MemoryPool* pool() const {
    return pool_;
  }

  uint64_t capacity() const {
    return pool_->capacity();
  }

  MemoryReclaimer* reclaimer() const;

 private:
  mutable std::mutex mu_;
  MemoryPool* pool_{nullptr};
  uint64_t totalBytes_{0};
  std::unordered_map<void*, size_t> allocations_;
};

MockMemoryOperator::MemoryReclaimer* MockMemoryOperator::reclaimer() const {
  return static_cast<MockMemoryOperator::MemoryReclaimer*>(pool_->reclaimer());
}

MockMemoryOperator* MockTask::addMemoryOp(
    bool isReclaimable,
    ReclaimInjectionCallback reclaimInjectCb,
    ArbitrationInjectionCallback arbitrationInjectCb) {
  ops_.push_back(std::make_shared<MockMemoryOperator>());
  pools_.push_back(root_->addLeafChild(
      fmt::format("MockTask{}", poolId_++),
      true,
      std::make_unique<MockMemoryOperator::MemoryReclaimer>(
          ops_.back(),
          isReclaimable,
          std::move(reclaimInjectCb),
          std::move(arbitrationInjectCb))));
  ops_.back()->setPool(pools_.back().get());
  return ops_.back().get();
}

MockTask::~MockTask() {
  for (auto op : ops_) {
    op->freeAll();
  }
}

class MockSharedArbitrationTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    SharedArbitrator::registerFactory();
    FLAGS_velox_memory_leak_check_enabled = true;
    TestValue::enable();
  }

  void SetUp() override {
    setupMemory();
  }

  void TearDown() override {
    clearTasks();
  }

  void setupMemory(
      int64_t memoryCapacity = 0,
      uint64_t memoryPoolInitCapacity = kMaxMemory,
      uint64_t memoryPoolTransferCapacity = 0,
      std::function<void(MemoryPool&)> arbitrationStateCheckCb = nullptr) {
    if (memoryPoolInitCapacity == kMaxMemory) {
      memoryPoolInitCapacity = kMemoryPoolInitCapacity;
    }
    if (memoryPoolTransferCapacity == 0) {
      memoryPoolTransferCapacity = kMemoryPoolTransferCapacity;
    }
    memoryCapacity = (memoryCapacity != 0) ? memoryCapacity : kMemoryCapacity;
    MemoryManagerOptions options;
    options.allocatorCapacity = memoryCapacity;
    std::string arbitratorKind = "SHARED";
    options.arbitratorKind = arbitratorKind;
    options.memoryPoolInitCapacity = memoryPoolInitCapacity;
    options.memoryPoolTransferCapacity = memoryPoolTransferCapacity;
    options.arbitrationStateCheckCb = std::move(arbitrationStateCheckCb);
    options.checkUsageLeak = true;
    manager_ = std::make_unique<MemoryManager>(options);
    ASSERT_EQ(manager_->arbitrator()->kind(), arbitratorKind);
    arbitrator_ = static_cast<SharedArbitrator*>(manager_->arbitrator());
  }

  std::shared_ptr<MockTask> addTask(int64_t capacity = kMaxMemory) {
    auto task = std::make_shared<MockTask>();
    task->initTaskPool(manager_.get(), capacity);
    return task;
  }

  MockMemoryOperator* addMemoryOp(
      std::shared_ptr<MockTask> task = nullptr,
      bool isReclaimable = true,
      ReclaimInjectionCallback reclaimInjectCb = nullptr,
      ArbitrationInjectionCallback arbitrationInjectCb = nullptr);

  const std::vector<std::shared_ptr<MockTask>>& tasks() const {
    return tasks_;
  }

  void clearTasks() {
    tasks_.clear();
  }

  std::unique_ptr<MemoryManager> manager_;
  SharedArbitrator* arbitrator_;
  std::vector<std::shared_ptr<MockTask>> tasks_;
};

MockMemoryOperator* MockSharedArbitrationTest::addMemoryOp(
    std::shared_ptr<MockTask> task,
    bool isReclaimable,
    ReclaimInjectionCallback reclaimInjectCb,
    ArbitrationInjectionCallback arbitrationInjectCb) {
  if (task == nullptr) {
    tasks_.push_back(addTask());
    task = tasks_.back();
  }
  return task->addMemoryOp(
      isReclaimable,
      std::move(reclaimInjectCb),
      std::move(arbitrationInjectCb));
}

void verifyArbitratorStats(
    const MemoryArbitrator::Stats& stats,
    uint64_t maxCapacityBytes,
    uint64_t freeCapacityBytes = 0,
    uint64_t numRequests = 0,
    uint64_t numSucceeded = 0,
    uint64_t numFailures = 0,
    uint64_t numReclaimedBytes = 0,
    uint64_t numShrunkBytes = 0,
    uint64_t arbitrationTimeUs = 0,
    uint64_t queueTimeUs = 0) {
  ASSERT_EQ(stats.numRequests, numRequests);
  ASSERT_EQ(stats.numSucceeded, numSucceeded);
  ASSERT_EQ(stats.numFailures, numFailures);
  ASSERT_EQ(stats.numReclaimedBytes, numReclaimedBytes);
  ASSERT_EQ(stats.numShrunkBytes, numShrunkBytes);
  ASSERT_GE(stats.arbitrationTimeUs, arbitrationTimeUs);
  ASSERT_GE(stats.queueTimeUs, queueTimeUs);
  ASSERT_EQ(stats.freeCapacityBytes, freeCapacityBytes);
  ASSERT_EQ(stats.maxCapacityBytes, maxCapacityBytes);
}

void verifyReclaimerStats(
    const MockMemoryOperator::MemoryReclaimer::Stats& stats,
    uint64_t numReclaims = 0,
    uint64_t numArbitrations = 0,
    uint64_t reclaimTargetBytes = 0) {
  ASSERT_EQ(stats.numReclaims, numReclaims);
  ASSERT_EQ(stats.numEnterArbitrations, numArbitrations);
  ASSERT_EQ(stats.numLeaveArbitrations, numArbitrations);
  for (const auto& reclaimTarget : stats.reclaimTargetBytes) {
    ASSERT_GE(reclaimTarget, reclaimTargetBytes);
  }
}

TEST_F(MockSharedArbitrationTest, constructor) {
  std::vector<std::shared_ptr<MockTask>> tasks;
  for (int i = 0; i <= kMemoryCapacity / kMemoryPoolInitCapacity; ++i) {
    auto task = addTask(kMemoryCapacity);
    ASSERT_NE(task->pool()->reclaimer(), nullptr);
    if (i < kMemoryCapacity / kMemoryPoolInitCapacity) {
      ASSERT_EQ(task->capacity(), kMemoryPoolInitCapacity);
    } else {
      ASSERT_EQ(task->capacity(), 0);
    }
    tasks.push_back(std::move(task));
  }
  auto stats = arbitrator_->stats();
  verifyArbitratorStats(stats, kMemoryCapacity);
  tasks.clear();
  stats = arbitrator_->stats();
  verifyArbitratorStats(stats, kMemoryCapacity, kMemoryCapacity);
}

TEST_F(MockSharedArbitrationTest, arbitrationStateCheck) {
  const int memCapacity = 256 * MB;
  const int minPoolCapacity = 32 * MB;
  std::atomic<int> checkCount{0};
  MemoryArbitrationStateCheckCB checkCountCb = [&](MemoryPool& pool) {
    const std::string re("MockTask.*");
    ASSERT_TRUE(RE2::FullMatch(pool.name(), re));
    ++checkCount;
  };
  setupMemory(memCapacity, 0, 0, checkCountCb);

  const int numTasks{5};
  std::vector<std::shared_ptr<MockTask>> tasks;
  for (int i = 0; i < numTasks; ++i) {
    auto task = addTask(kMemoryCapacity);
    ASSERT_EQ(task->capacity(), 0);
    tasks.push_back(std::move(task));
  }
  std::vector<void*> buffers;
  std::vector<MockMemoryOperator*> memOps;
  for (int i = 0; i < numTasks; ++i) {
    memOps.push_back(tasks[i]->addMemoryOp());
    buffers.push_back(memOps.back()->allocate(128));
  }
  ASSERT_EQ(numTasks, checkCount);
  for (int i = 0; i < numTasks; ++i) {
    memOps[i]->freeAll();
  }
  tasks.clear();

  // Check throw in arbitration state callback.
  MemoryArbitrationStateCheckCB badCheckCb = [&](MemoryPool& /*unused*/) {
    VELOX_FAIL("bad check");
  };
  setupMemory(memCapacity, 0, 0, badCheckCb);
  std::shared_ptr<MockTask> task = addTask(kMemoryCapacity);
  ASSERT_EQ(task->capacity(), 0);
  MockMemoryOperator* memOp = task->addMemoryOp();
  VELOX_ASSERT_THROW(memOp->allocate(128), "bad check");
}

TEST_F(MockSharedArbitrationTest, arbitrationFailsTask) {
  auto nonReclaimTask = addTask(384 * MB);
  auto nonReclaimOp = nonReclaimTask->addMemoryOp(false);
  auto buf = nonReclaimOp->allocate(384 * MB);

  // growTask is (192 + 128) = 320MB which is less than nonReclaimTask 384MB.
  // This makes sure nonReclaimTask gets picked as the victim during handleOOM()
  auto growTask = addTask(192 * MB);
  auto growOp = growTask->addMemoryOp(false);
  auto bufGrow = growOp->allocate(128 * MB);
  EXPECT_NO_THROW(manager_->testingGrowPool(growOp->pool(), 64 * MB));
  ASSERT_NE(nonReclaimTask->error(), nullptr);
  try {
    std::rethrow_exception(nonReclaimTask->error());
  } catch (const VeloxRuntimeError& e) {
    ASSERT_EQ(velox::error_code::kMemAborted, e.errorCode());
    ASSERT_TRUE(
        std::string(e.what()).find(
            "usage 384.00MB reserved 384.00MB peak 384.00MB") !=
        std::string::npos);
  } catch (...) {
    FAIL();
  }
  nonReclaimOp->freeAll();
  growOp->freeAll();
}

TEST_F(MockSharedArbitrationTest, shrinkPools) {
  struct TaskTestData {
    uint64_t capacity{0};
    bool reclaimable{false};

    uint64_t expectedCapacityAfterShrink{0};
    bool expectedAbortAfterShrink{false};

    std::string debugString() const {
      return fmt::format(
          "capacity: {}, reclaimable: {}, expectedCapacityAfterShrink: {}, expectedAbortAfterShrink: {}",
          succinctBytes(capacity),
          reclaimable,
          succinctBytes(expectedCapacityAfterShrink),
          expectedAbortAfterShrink);
    }
  };

  struct {
    std::vector<TaskTestData> taskTestDatas;
    uint64_t targetBytes;
    uint64_t expectedFreedBytes;
    bool allowSpill;
    bool allowAbort;

    std::string debugString() const {
      std::stringstream tasksOss;
      for (const auto& taskTestData : taskTestDatas) {
        tasksOss << taskTestData.debugString();
        tasksOss << ",";
      }
      return fmt::format(
          "taskTestDatas: [{}], targetBytes: {}, expectedFreedBytes: {}, allowSpill: {}, allowAbort: {}",
          tasksOss.str(),
          succinctBytes(targetBytes),
          succinctBytes(expectedFreedBytes),
          allowSpill,
          allowAbort);
    }
  } testSettings[] = {
      {{{kMemoryCapacity / 4, true, 0, false},
        {kMemoryCapacity / 2, true, 0, false},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       0,
       kMemoryCapacity / 4 * 3,
       true,
       false},
      {{{kMemoryCapacity / 4, true, 0, false},
        {kMemoryCapacity / 2, true, 0, false},
        {kMemoryCapacity / 4, false, 0, true}},
       0,
       kMemoryCapacity,
       true,
       true},
      {{{kMemoryCapacity / 4, true, kMemoryCapacity / 4, false},
        {kMemoryCapacity / 2, true, kMemoryCapacity / 2, false},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       0,
       0,
       false,
       false},
      {{{kMemoryCapacity / 4, true, 0, true},
        {kMemoryCapacity / 2, true, 0, true},
        {kMemoryCapacity / 4, false, 0, true}},
       0,
       kMemoryCapacity,
       false,
       true},
      {{{kMemoryCapacity / 4, true, 0, false},
        {kMemoryCapacity / 2, true, 0, false},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       1UL << 30,
       kMemoryCapacity / 4 * 3,
       true,
       false},
      {{{kMemoryCapacity / 4, true, 0, false},
        {kMemoryCapacity / 2, true, 0, false},
        {kMemoryCapacity / 4, false, 0, true}},
       1UL << 30,
       kMemoryCapacity,
       true,
       true},
      {{{kMemoryCapacity / 4, true, kMemoryCapacity / 4, false},
        {kMemoryCapacity / 2, true, kMemoryCapacity / 2, false},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       1UL << 30,
       0,
       false,
       false},
      {{{kMemoryCapacity / 4, true, 0, true},
        {kMemoryCapacity / 2, true, 0, true},
        {kMemoryCapacity / 4, false, 0, true}},
       1UL << 30,
       kMemoryCapacity,
       false,
       true},
      {{{kMemoryCapacity / 4, true, kMemoryCapacity / 4, false},
        {kMemoryCapacity / 2,
         true,
         kMemoryCapacity / 2 - kMemoryPoolTransferCapacity,
         false},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       1,
       kMemoryPoolTransferCapacity,
       true,
       false},
      {{{kMemoryCapacity / 4, true, kMemoryCapacity / 4, false},
        {kMemoryCapacity / 2,
         true,
         kMemoryCapacity / 2 - kMemoryPoolTransferCapacity,
         false},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       1,
       kMemoryPoolTransferCapacity,
       true,
       true},
      {{{kMemoryCapacity / 4, true, kMemoryCapacity / 4, false},
        {kMemoryCapacity / 2, true, kMemoryCapacity / 2, false},

        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       1,
       0,
       false,
       false},
      {{{kMemoryCapacity / 4, true, kMemoryCapacity / 4, false},
        {kMemoryCapacity / 2, true, 0, true},
        {kMemoryCapacity / 4, false, kMemoryCapacity / 4, false}},
       1,
       kMemoryCapacity / 2,
       false,
       true}};
  struct MockTaskContainer {
    std::shared_ptr<MockTask> task;
    MockMemoryOperator* op;
    void* buf;
    TaskTestData taskTestData;
  };

  std::function<void(MockTask*, bool)> checkTaskException =
      [](MockTask* task, bool expectedAbort) {
        if (!expectedAbort) {
          ASSERT_EQ(task->error(), nullptr);
          return;
        }
        ASSERT_NE(task->error(), nullptr);
        VELOX_ASSERT_THROW(
            std::rethrow_exception(task->error()),
            "Memory pool aborted to reclaim used memory, current usage");
      };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<MockTaskContainer> taskContainers;
    for (const auto& taskTestData : testData.taskTestDatas) {
      auto task = addTask(taskTestData.capacity);
      auto* op = addMemoryOp(task, taskTestData.reclaimable);
      auto* buf = op->allocate(taskTestData.capacity);
      ASSERT_EQ(op->capacity(), taskTestData.capacity);
      taskContainers.push_back({task, op, buf, taskTestData});
    }

    ASSERT_EQ(
        manager_->shrinkPools(
            testData.targetBytes, testData.allowSpill, testData.allowAbort),
        testData.expectedFreedBytes);

    for (const auto& taskContainer : taskContainers) {
      ASSERT_EQ(
          taskContainer.task->capacity(),
          taskContainer.taskTestData.expectedCapacityAfterShrink);
      checkTaskException(
          taskContainer.task.get(),
          taskContainer.taskTestData.expectedAbortAfterShrink);
    }

    uint64_t totalCapacity{0};
    for (const auto& taskContainer : taskContainers) {
      totalCapacity += taskContainer.task->capacity();
    }
    ASSERT_EQ(
        totalCapacity + arbitrator_->stats().freeCapacityBytes,
        arbitrator_->capacity());
  }
}

TEST_F(MockSharedArbitrationTest, singlePoolGrowWithoutArbitration) {
  auto* memOp = addMemoryOp();
  const int allocateSize = 1 * MB;
  while (memOp->capacity() < kMemoryCapacity) {
    memOp->allocate(allocateSize);
  }

  verifyArbitratorStats(
      arbitrator_->stats(),
      kMemoryCapacity,
      0,
      (kMemoryCapacity - kMemoryPoolInitCapacity) / kMemoryPoolTransferCapacity,
      (kMemoryCapacity - kMemoryPoolInitCapacity) /
          kMemoryPoolTransferCapacity);

  verifyReclaimerStats(
      memOp->reclaimer()->stats(),
      0,
      (kMemoryCapacity - kMemoryPoolInitCapacity) /
          kMemoryPoolTransferCapacity);

  clearTasks();
  verifyArbitratorStats(
      arbitrator_->stats(),
      kMemoryCapacity,
      kMemoryCapacity,
      (kMemoryCapacity - kMemoryPoolInitCapacity) / kMemoryPoolTransferCapacity,
      (kMemoryCapacity - kMemoryPoolInitCapacity) /
          kMemoryPoolTransferCapacity);
}

TEST_F(MockSharedArbitrationTest, maxCapacityReserve) {
  const int memCapacity = 256 * MB;
  const int minPoolCapacity = 32 * MB;
  setupMemory(memCapacity, minPoolCapacity);
  struct {
    uint64_t maxCapacity;
    uint64_t expectedInitialCapacity;

    std::string debugString() const {
      return fmt::format(
          "maxCapacity {}, expectedInitialCapacity {}",
          succinctBytes(maxCapacity),
          succinctBytes(expectedInitialCapacity));
    }
  } testSettings[] = {
      {minPoolCapacity, minPoolCapacity},
      {minPoolCapacity / 2, minPoolCapacity / 2},
      {minPoolCapacity * 2, minPoolCapacity}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto task = addTask(testData.maxCapacity);
    ASSERT_EQ(task->pool()->maxCapacity(), testData.maxCapacity);
    ASSERT_EQ(task->pool()->capacity(), testData.expectedInitialCapacity);
  }
}

TEST_F(MockSharedArbitrationTest, ensureMemoryPoolMaxCapacity) {
  const int memCapacity = 256 * MB;
  const int minPoolCapacity = 8 * MB;
  struct {
    uint64_t maxCapacity;
    bool isReclaimable;
    uint64_t allocatedBytes;
    uint64_t requestBytes;
    bool hasOtherTask;
    bool expectedSuccess;
    bool expectedReclaimFromOther;

    std::string debugString() const {
      return fmt::format(
          "maxCapacity {} isReclaimable {} allocatedBytes {} requestBytes {} hasOtherTask {} expectedSuccess {} expectedReclaimFromOther {}",
          succinctBytes(maxCapacity),
          isReclaimable,
          succinctBytes(allocatedBytes),
          succinctBytes(requestBytes),
          hasOtherTask,
          expectedSuccess,
          expectedReclaimFromOther);
    }
  } testSettings[] = {
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       false,
       true,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 8,
       false,
       true,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       false,
       true,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 2,
       memCapacity / 4,
       false,
       true,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 4,
       memCapacity / 2,
       false,
       false,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 2,
       memCapacity / 4,
       false,
       false,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       true,
       true,
       true},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 8,
       true,
       true,
       true},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       true,
       true,
       true},
      {memCapacity / 2,
       true,
       memCapacity / 2,
       memCapacity / 4,
       true,
       true,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 4,
       memCapacity / 2,
       true,
       false,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 2,
       memCapacity / 4,
       false,
       false,
       false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(memCapacity, minPoolCapacity);

    auto requestor = addTask(testData.maxCapacity);
    auto requestorOp = addMemoryOp(requestor, testData.isReclaimable);
    requestorOp->allocate(testData.allocatedBytes);
    std::shared_ptr<MockTask> other;
    MockMemoryOperator* otherOp;
    if (testData.hasOtherTask) {
      other = addTask();
      otherOp = addMemoryOp(other, true);
      otherOp->allocate(memCapacity - testData.allocatedBytes);
    }
    const auto numRequests = arbitrator_->stats().numRequests;
    if (testData.expectedSuccess) {
      requestorOp->allocate(testData.requestBytes);
    } else {
      VELOX_ASSERT_THROW(
          requestorOp->allocate(testData.requestBytes),
          "Exceeded memory pool cap of");
    }
    if (testData.expectedReclaimFromOther) {
      ASSERT_GT(otherOp->reclaimer()->stats().numReclaims, 0);
    } else if (testData.hasOtherTask) {
      ASSERT_EQ(otherOp->reclaimer()->stats().numReclaims, 0);
    }
    if (testData.expectedSuccess &&
        (((testData.allocatedBytes + testData.requestBytes) >
          testData.maxCapacity) ||
         testData.hasOtherTask)) {
      ASSERT_GT(arbitrator_->stats().numReclaimedBytes, 0);
    } else {
      ASSERT_EQ(arbitrator_->stats().numReclaimedBytes, 0);
    }
    ASSERT_EQ(arbitrator_->stats().numRequests, numRequests + 1);
  }
}

TEST_F(MockSharedArbitrationTest, ensureNodeMaxCapacity) {
  struct {
    uint64_t nodeCapacity;
    uint64_t poolMaxCapacity;
    bool isReclaimable;
    uint64_t allocatedBytes;
    uint64_t requestBytes;
    bool expectedSuccess;
    bool expectedReclaimedBytes;

    std::string debugString() const {
      return fmt::format(
          "nodeCapacity {} poolMaxCapacity {} isReclaimable {} allocatedBytes {} requestBytes {} expectedSuccess {} expectedReclaimedBytes {}",
          succinctBytes(nodeCapacity),
          succinctBytes(poolMaxCapacity),
          isReclaimable,
          succinctBytes(allocatedBytes),
          succinctBytes(requestBytes),
          expectedSuccess,
          expectedReclaimedBytes);
    }
  } testSettings[] = {
      {256 * MB, 256 * MB, true, 128 * MB, 256 * MB, true, true},
      {256 * MB, 256 * MB, false, 128 * MB, 256 * MB, false, false},
      {256 * MB, 512 * MB, true, 128 * MB, 256 * MB, true, true},
      {256 * MB, 512 * MB, false, 128 * MB, 256 * MB, false, false},
      {256 * MB, 128 * MB, false, 128 * MB, 256 * MB, false, false},
      {256 * MB, 128 * MB, true, 128 * MB, 256 * MB, false, false},
      {256 * MB, 128 * MB, true, 128 * MB, 512 * MB, false, false},
      {256 * MB, 128 * MB, false, 128 * MB, 512 * MB, false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(testData.nodeCapacity);

    auto requestor = addTask(testData.poolMaxCapacity);
    auto requestorOp = addMemoryOp(requestor, testData.isReclaimable);
    requestorOp->allocate(testData.allocatedBytes);
    const auto numRequests = arbitrator_->stats().numRequests;
    if (testData.expectedSuccess) {
      requestorOp->allocate(testData.requestBytes);
    } else {
      VELOX_ASSERT_THROW(
          requestorOp->allocate(testData.requestBytes),
          "Exceeded memory pool cap");
    }
    if (testData.expectedSuccess) {
      ASSERT_GT(arbitrator_->stats().numReclaimedBytes, 0);
    } else {
      ASSERT_EQ(arbitrator_->stats().numReclaimedBytes, 0);
    }
    ASSERT_EQ(arbitrator_->stats().numRequests, numRequests + 1);
  }
}

TEST_F(MockSharedArbitrationTest, failedArbitration) {
  const int memCapacity = 256 * MB;
  const int minPoolCapacity = 8 * MB;
  setupMemory(memCapacity, minPoolCapacity);
  auto reclaimableOp = addMemoryOp();
  ASSERT_EQ(reclaimableOp->capacity(), minPoolCapacity);
  auto nonReclaimableOp = addMemoryOp(nullptr, false);
  ASSERT_EQ(nonReclaimableOp->capacity(), minPoolCapacity);
  auto arbitrateOp = addMemoryOp();
  ASSERT_EQ(arbitrateOp->capacity(), minPoolCapacity);

  reclaimableOp->allocate(minPoolCapacity);
  ASSERT_EQ(reclaimableOp->capacity(), minPoolCapacity);
  nonReclaimableOp->allocate(minPoolCapacity);
  ASSERT_EQ(nonReclaimableOp->capacity(), minPoolCapacity);
  ASSERT_ANY_THROW(arbitrateOp->allocate(memCapacity));
  verifyReclaimerStats(nonReclaimableOp->reclaimer()->stats());
  verifyReclaimerStats(reclaimableOp->reclaimer()->stats(), 1);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 1, 1);
  verifyArbitratorStats(
      arbitrator_->stats(), memCapacity, 260046848, 1, 0, 1, 8388608, 8388608);
  ASSERT_EQ(arbitrator_->stats().queueTimeUs, 0);
}

TEST_F(MockSharedArbitrationTest, singlePoolGrowCapacityWithArbitration) {
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory();
    auto op = addMemoryOp(nullptr, isLeafReclaimable);
    const int allocateSize = MB;
    while (op->pool()->currentBytes() < kMemoryCapacity) {
      op->allocate(allocateSize);
    }
    verifyArbitratorStats(arbitrator_->stats(), kMemoryCapacity, 0, 62, 62);
    verifyReclaimerStats(op->reclaimer()->stats(), 0, 62);

    if (!isLeafReclaimable) {
      ASSERT_ANY_THROW(op->allocate(allocateSize));
      verifyArbitratorStats(
          arbitrator_->stats(), kMemoryCapacity, 0, 63, 62, 1);
      verifyReclaimerStats(op->reclaimer()->stats(), 1, 63);
      continue;
    }

    // Do more allocations to trigger arbitration.
    for (int i = 0; i < kMemoryPoolTransferCapacity / allocateSize; ++i) {
      op->allocate(allocateSize);
    }
    verifyArbitratorStats(
        arbitrator_->stats(), kMemoryCapacity, 0, 63, 63, 0, 8388608);
    verifyReclaimerStats(op->reclaimer()->stats(), 1, 63);

    clearTasks();
    verifyArbitratorStats(
        arbitrator_->stats(),
        kMemoryCapacity,
        kMemoryCapacity,
        63,
        63,
        0,
        8388608);
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateWithCapacityShrink) {
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory();
    auto* reclaimedOp = addMemoryOp(nullptr, isLeafReclaimable);
    const int reclaimedOpCapacity = kMemoryCapacity * 2 / 3;
    const int allocateSize = 32 * MB;
    while (reclaimedOp->pool()->capacity() < reclaimedOpCapacity) {
      reclaimedOp->allocate(allocateSize);
    }
    const auto freeCapacity = arbitrator_->stats().freeCapacityBytes;
    ASSERT_GT(freeCapacity, 0);
    reclaimedOp->freeAll();
    ASSERT_GT(reclaimedOp->pool()->freeBytes(), 0);
    ASSERT_EQ(reclaimedOp->pool()->currentBytes(), 0);
    ASSERT_EQ(arbitrator_->stats().freeCapacityBytes, freeCapacity);

    auto* arbitrateOp = addMemoryOp(nullptr, isLeafReclaimable);
    while (arbitrator_->stats().numShrunkBytes == 0) {
      arbitrateOp->allocate(allocateSize);
    }
    const auto arbitratorStats = arbitrator_->stats();
    ASSERT_GT(arbitratorStats.numShrunkBytes, 0);
    ASSERT_EQ(arbitratorStats.numReclaimedBytes, 0);

    verifyReclaimerStats(reclaimedOp->reclaimer()->stats(), 0, 11);
    verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 5);

    clearTasks();
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateWithMemoryReclaim) {
  const uint64_t memoryCapacity = 256 * MB;
  const uint64_t minPoolCapacity = 8 * MB;
  const std::vector<char> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory(memoryCapacity, minPoolCapacity);
    auto* reclaimedOp = addMemoryOp(nullptr, isLeafReclaimable);
    const int allocateSize = 8 * MB;
    while (reclaimedOp->pool()->currentBytes() < memoryCapacity) {
      reclaimedOp->allocate(allocateSize);
    }
    auto* arbitrateOp = addMemoryOp();
    if (!isLeafReclaimable) {
      auto leafTask = tasks().front();
      ASSERT_NO_THROW(arbitrateOp->allocate(allocateSize));
      ASSERT_NE(leafTask->error(), nullptr);
      ASSERT_EQ(arbitrator_->stats().numFailures, 0);
      continue;
    }
    arbitrateOp->allocate(allocateSize);

    verifyArbitratorStats(
        arbitrator_->stats(), memoryCapacity, 0, 32, 32, 0, 8388608);

    verifyReclaimerStats(
        arbitrateOp->reclaimer()->stats(), 0, 1, kMemoryPoolTransferCapacity);

    verifyReclaimerStats(
        reclaimedOp->reclaimer()->stats(), 1, 31, kMemoryPoolTransferCapacity);
    clearTasks();
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateBySelfMemoryReclaim) {
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    const uint64_t memCapacity = 128 * MB;
    setupMemory(memCapacity);
    std::shared_ptr<MockTask> task = addTask(kMemoryCapacity);
    auto* memOp = addMemoryOp(task, isLeafReclaimable);
    const int allocateSize = 8 * MB;
    while (memOp->pool()->currentBytes() < memCapacity / 2) {
      memOp->allocate(allocateSize);
    }
    ASSERT_EQ(memOp->pool()->freeBytes(), 0);
    const int oldNumRequests = arbitrator_->stats().numRequests;
    // Allocate a large chunk of memory to trigger arbitration.
    if (!isLeafReclaimable) {
      ASSERT_ANY_THROW(memOp->allocate(memCapacity));
      ASSERT_EQ(oldNumRequests + 1, arbitrator_->stats().numRequests);
      ASSERT_EQ(arbitrator_->stats().numFailures, 1);
      continue;
    } else {
      memOp->allocate(memCapacity);
      ASSERT_EQ(oldNumRequests + 1, arbitrator_->stats().numRequests);
      ASSERT_EQ(arbitrator_->stats().numFailures, 0);
      ASSERT_EQ(arbitrator_->stats().numShrunkBytes, 0);
      ASSERT_GT(arbitrator_->stats().numReclaimedBytes, 0);
    }
    ASSERT_EQ(arbitrator_->stats().queueTimeUs, 0);
  }
}

TEST_F(MockSharedArbitrationTest, noAbortOnRequestWhenArbitrationFails) {
  const uint64_t memCapacity = 128 * MB;
  struct {
    uint64_t initialAllocationSize;
    uint64_t failedAllocationSize;
    bool maybeReserve;

    std::string debugString() const {
      return fmt::format(
          "initialAllocationSize {}, failedAllocationSize {}, maybeReserve {}",
          initialAllocationSize,
          failedAllocationSize,
          maybeReserve);
    }
  } testSettings[] = {
      {memCapacity / 2, memCapacity / 2 + memCapacity / 4, true},
      {memCapacity / 2, memCapacity / 2 + memCapacity / 4, false},
      {0, memCapacity + memCapacity / 4, true},
      {0, memCapacity + memCapacity / 4, false},
      {memCapacity / 2, memCapacity, true},
      {memCapacity / 2, memCapacity, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(memCapacity, 0);
    std::shared_ptr<MockTask> task = addTask(kMemoryCapacity);
    auto* memOp = addMemoryOp(task, false);
    if (testData.initialAllocationSize != 0) {
      memOp->allocate(testData.initialAllocationSize);
    }
    if (testData.maybeReserve) {
      ASSERT_FALSE(memOp->pool()->maybeReserve(testData.failedAllocationSize));
    } else {
      VELOX_ASSERT_THROW(
          memOp->allocate(testData.failedAllocationSize),
          "Exceeded memory pool cap");
    }
    ASSERT_EQ(arbitrator_->stats().numFailures, 1);
    ASSERT_EQ(arbitrator_->stats().numAborted, 0);
    memOp->pool()->release();
  }
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, orderedArbitration) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByFreeCapacity",
      std::function<void(const std::vector<SharedArbitrator::Candidate>*)>(
          ([&](const std::vector<SharedArbitrator::Candidate>* candidates) {
            for (int i = 1; i < candidates->size(); ++i) {
              ASSERT_LE(
                  (*candidates)[i].freeBytes, (*candidates)[i - 1].freeBytes);
            }
          })));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableMemory",
      std::function<void(const std::vector<SharedArbitrator::Candidate>*)>(
          ([&](const std::vector<SharedArbitrator::Candidate>* candidates) {
            for (int i = 1; i < candidates->size(); ++i) {
              ASSERT_LE(
                  (*candidates)[i].reclaimableBytes,
                  (*candidates)[i - 1].reclaimableBytes);
            }
          })));
  folly::Random::DefaultGenerator rng;
  rng.seed(512);
  const uint64_t memCapacity = 512 * MB;
  const uint64_t minPoolCapacity = 32 * MB;
  const uint64_t minPoolCapacityTransferSize = 8 * MB;
  const int numTasks = 8;
  struct {
    bool freeCapacity;
    bool sameSize;

    std::string debugString() const {
      return fmt::format(
          "freeCapacity {}, sameSize {}", freeCapacity, sameSize);
    }
  } testSettings[] = {
      {true, false}, {true, true}, {false, false}, {false, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(memCapacity, minPoolCapacity, minPoolCapacityTransferSize);
    std::vector<MockMemoryOperator*> memOps;
    std::vector<uint64_t> memOpCapacities;
    for (int i = 0; i < numTasks; ++i) {
      auto* memOp = addMemoryOp();
      int allocationSize = testData.sameSize ? memCapacity / numTasks
                                             : minPoolCapacity +
              folly::Random::rand32(rng) %
                  ((memCapacity / numTasks) - minPoolCapacity);
      allocationSize = allocationSize / MB * MB;
      memOp->allocate(allocationSize);
      if (testData.freeCapacity) {
        memOp->freeAll();
        ASSERT_EQ(memOp->pool()->currentBytes(), 0);
      }
      memOps.push_back(memOp);
    }

    auto* arbitrateOp = addMemoryOp();
    arbitrateOp->allocate(memCapacity);
    for (auto* memOp : memOps) {
      ASSERT_EQ(memOp->capacity(), 0);
    }
    ASSERT_EQ(arbitrator_->stats().queueTimeUs, 0);
    clearTasks();
  }
}

TEST_F(MockSharedArbitrationTest, poolCapacityTransferWithFreeCapacity) {
  const uint64_t memCapacity = 512 * MB;
  const uint64_t minPoolCapacity = 32 * MB;
  const uint64_t minPoolCapacityTransferSize = 16 * MB;
  setupMemory(memCapacity, minPoolCapacity, minPoolCapacityTransferSize);
  auto* memOp = addMemoryOp();
  ASSERT_EQ(memOp->capacity(), minPoolCapacity);
  memOp->allocate(minPoolCapacity);
  ASSERT_EQ(memOp->pool()->freeBytes(), 0);
  const uint64_t allocationSize = 8 * MB;
  uint64_t capacity = memOp->pool()->capacity();
  while (capacity < memCapacity) {
    memOp->allocate(allocationSize);
    ASSERT_EQ(capacity + minPoolCapacityTransferSize, memOp->capacity());
    while (memOp->pool()->freeBytes() > 0) {
      memOp->allocate(allocationSize);
    }
    capacity = memOp->capacity();
  }
  const int expectedArbitrationRequests =
      (memCapacity - minPoolCapacity) / minPoolCapacityTransferSize;
  verifyReclaimerStats(
      memOp->reclaimer()->stats(), 0, expectedArbitrationRequests);
  verifyArbitratorStats(
      arbitrator_->stats(),
      memCapacity,
      0,
      expectedArbitrationRequests,
      expectedArbitrationRequests);
  ASSERT_EQ(arbitrator_->stats().queueTimeUs, 0);
}

TEST_F(MockSharedArbitrationTest, poolCapacityTransferSizeWithCapacityShrunk) {
  const int numCandidateOps = 8;
  const uint64_t minPoolCapacity = 64 * MB;
  const uint64_t minPoolCapacityTransferSize = 32 * MB;
  const uint64_t memCapacity = minPoolCapacity * numCandidateOps;
  setupMemory(memCapacity, minPoolCapacity, minPoolCapacityTransferSize);
  const int allocationSize = 8 * MB;
  std::vector<MockMemoryOperator*> candidateOps;
  for (int i = 0; i < numCandidateOps; ++i) {
    candidateOps.push_back(addMemoryOp());
    ASSERT_EQ(candidateOps.back()->capacity(), minPoolCapacity);
    candidateOps.back()->allocate(allocationSize);
    ASSERT_EQ(candidateOps.back()->capacity(), minPoolCapacity);
    ASSERT_GT(candidateOps.back()->pool()->freeBytes(), 0);
  }
  auto* arbitrateOp = addMemoryOp();
  ASSERT_EQ(arbitrateOp->capacity(), 0);
  arbitrateOp->allocate(allocationSize);
  ASSERT_EQ(arbitrateOp->capacity(), minPoolCapacityTransferSize);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  ASSERT_EQ(arbitrator_->stats().numShrunkBytes, minPoolCapacityTransferSize);
  ASSERT_EQ(arbitrator_->stats().numReclaimedBytes, 0);
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
}

TEST_F(MockSharedArbitrationTest, partialPoolCapacityTransferSize) {
  const int numCandidateOps = 8;
  const uint64_t minPoolCapacity = 64 * MB;
  const uint64_t minPoolCapacityTransferSize = 32 * MB;
  const uint64_t memCapacity = minPoolCapacity * numCandidateOps;
  setupMemory(memCapacity, minPoolCapacity, minPoolCapacityTransferSize);
  const int allocationSize = 8 * MB;
  std::vector<MockMemoryOperator*> candidateOps;
  for (int i = 0; i < numCandidateOps; ++i) {
    candidateOps.push_back(addMemoryOp());
    ASSERT_EQ(candidateOps.back()->capacity(), minPoolCapacity);
    candidateOps.back()->allocate(allocationSize);
    ASSERT_EQ(candidateOps.back()->capacity(), minPoolCapacity);
    ASSERT_GT(candidateOps.back()->pool()->freeBytes(), 0);
  }
  auto* arbitrateOp = addMemoryOp();
  ASSERT_EQ(arbitrateOp->capacity(), 0);
  arbitrateOp->allocate(allocationSize);
  ASSERT_EQ(arbitrateOp->capacity(), minPoolCapacityTransferSize);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  ASSERT_EQ(arbitrator_->stats().numShrunkBytes, minPoolCapacityTransferSize);
  ASSERT_EQ(arbitrator_->stats().numReclaimedBytes, 0);
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
}

TEST_F(MockSharedArbitrationTest, poolCapacityTransferSizeWithMemoryReclaim) {
  const uint64_t memCapacity = 128 * MB;
  const uint64_t minPoolCapacity = memCapacity;
  const uint64_t minPoolCapacityTransferSize = 64 * MB;
  setupMemory(memCapacity, minPoolCapacity, minPoolCapacityTransferSize);
  auto* reclaimedOp = addMemoryOp();
  ASSERT_EQ(reclaimedOp->capacity(), memCapacity);
  const int allocationSize = 8 * MB;
  std::vector<std::shared_ptr<MockMemoryOperator>> candidateOps;
  for (int i = 0; i < memCapacity / allocationSize; ++i) {
    reclaimedOp->allocate(allocationSize);
  }
  ASSERT_EQ(reclaimedOp->pool()->freeBytes(), 0);

  auto* arbitrateOp = addMemoryOp();
  ASSERT_EQ(arbitrateOp->capacity(), 0);
  arbitrateOp->allocate(allocationSize);
  ASSERT_EQ(arbitrateOp->capacity(), minPoolCapacityTransferSize);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  verifyReclaimerStats(reclaimedOp->reclaimer()->stats(), 1);
  ASSERT_EQ(arbitrator_->stats().numShrunkBytes, 0);
  ASSERT_EQ(
      arbitrator_->stats().numReclaimedBytes, minPoolCapacityTransferSize);
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
}

TEST_F(MockSharedArbitrationTest, enterArbitrationException) {
  const uint64_t memCapacity = 128 * MB;
  const uint64_t minPoolCapacity = memCapacity;
  const uint64_t minPoolCapacityTransferSize = 64 * MB;
  setupMemory(memCapacity, minPoolCapacity, minPoolCapacityTransferSize);
  auto* reclaimedOp = addMemoryOp();
  ASSERT_EQ(reclaimedOp->capacity(), memCapacity);
  const int allocationSize = 8 * MB;
  std::vector<std::shared_ptr<MockMemoryOperator>> candidateOps;
  for (int i = 0; i < memCapacity / allocationSize; ++i) {
    reclaimedOp->allocate(allocationSize);
  }
  ASSERT_EQ(reclaimedOp->pool()->freeBytes(), 0);

  auto failedArbitrateOp = addMemoryOp(nullptr, true, nullptr, []() {
    VELOX_FAIL("enterArbitrationException failed");
  });
  ASSERT_EQ(failedArbitrateOp->capacity(), 0);
  ASSERT_ANY_THROW(failedArbitrateOp->allocate(allocationSize));
  verifyReclaimerStats(failedArbitrateOp->reclaimer()->stats());
  ASSERT_EQ(failedArbitrateOp->capacity(), 0);
  auto* arbitrateOp = addMemoryOp();
  arbitrateOp->allocate(allocationSize);
  ASSERT_EQ(arbitrateOp->capacity(), minPoolCapacityTransferSize);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  verifyReclaimerStats(reclaimedOp->reclaimer()->stats(), 1);
  ASSERT_EQ(arbitrator_->stats().numShrunkBytes, 0);
  ASSERT_EQ(
      arbitrator_->stats().numReclaimedBytes, minPoolCapacityTransferSize);
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
  ASSERT_EQ(arbitrator_->stats().numFailures, 0);
}

TEST_F(MockSharedArbitrationTest, noArbitratiognFromAbortedPool) {
  auto* reclaimedOp = addMemoryOp();
  ASSERT_EQ(reclaimedOp->capacity(), kMemoryPoolInitCapacity);
  reclaimedOp->allocate(128);
  try {
    VELOX_MEM_POOL_ABORTED("Manual abort pool");
  } catch (VeloxException& e) {
    reclaimedOp->pool()->abort(std::current_exception());
  }
  ASSERT_TRUE(reclaimedOp->pool()->aborted());
  ASSERT_TRUE(reclaimedOp->pool()->aborted());
  const int largeAllocationSize = 2 * kMemoryPoolInitCapacity;
  VELOX_ASSERT_THROW(reclaimedOp->allocate(largeAllocationSize), "");
  ASSERT_EQ(arbitrator_->stats().numRequests, 0);
  ASSERT_EQ(arbitrator_->stats().numAborted, 0);
  ASSERT_EQ(arbitrator_->stats().numFailures, 0);
  // Check we don't allow memory reservation increase or trigger memory
  // arbitration at root memory pool.
  ASSERT_EQ(reclaimedOp->pool()->capacity(), kMemoryPoolInitCapacity);
  ASSERT_EQ(reclaimedOp->pool()->currentBytes(), 0);
  VELOX_ASSERT_THROW(reclaimedOp->allocate(128), "");
  ASSERT_EQ(reclaimedOp->pool()->currentBytes(), 0);
  ASSERT_EQ(reclaimedOp->pool()->capacity(), kMemoryPoolInitCapacity);
  VELOX_ASSERT_THROW(reclaimedOp->allocate(kMemoryPoolInitCapacity * 2), "");
  ASSERT_EQ(reclaimedOp->pool()->capacity(), kMemoryPoolInitCapacity);
  ASSERT_EQ(reclaimedOp->pool()->currentBytes(), 0);
  ASSERT_EQ(arbitrator_->stats().numRequests, 0);
  ASSERT_EQ(arbitrator_->stats().numAborted, 0);
  ASSERT_EQ(arbitrator_->stats().numFailures, 0);
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, failedToReclaimFromRequestor) {
  const int numOtherTasks = 4;
  const int otherTaskMemoryCapacity = kMemoryCapacity / 8;
  const int failedTaskMemoryCapacity = kMemoryCapacity / 2;
  struct {
    bool hasAllocationFromFailedTaskAfterAbort;
    bool hasAllocationFromOtherTaskAfterAbort;
    int64_t expectedFailedTaskMemoryCapacity;
    int64_t expectedFailedTaskMemoryUsage;
    int64_t expectedOtherTaskMemoryCapacity;
    int64_t expectedOtherTaskMemoryUsage;
    int64_t expectedFreeCapacity;

    std::string debugString() const {
      return fmt::format(
          "hasAllocationFromFailedTaskAfterAbort {}, hasAllocationFromOtherTaskAfterAbort {} expectedFailedTaskMemoryCapacity {} expectedFailedTaskMemoryUsage {} expectedOtherTaskMemoryCapacity {} expectedOtherTaskMemoryUsage {} expectedFreeCapacity{}",
          hasAllocationFromFailedTaskAfterAbort,
          hasAllocationFromOtherTaskAfterAbort,
          expectedFailedTaskMemoryCapacity,
          expectedFailedTaskMemoryUsage,
          expectedOtherTaskMemoryCapacity,
          expectedOtherTaskMemoryUsage,
          expectedFreeCapacity);
    }
  } testSettings[] = {
      {false,
       false,
       0,
       0,
       otherTaskMemoryCapacity,
       otherTaskMemoryCapacity,
       failedTaskMemoryCapacity},
      {true,
       false,
       0,
       0,
       otherTaskMemoryCapacity,
       otherTaskMemoryCapacity,
       failedTaskMemoryCapacity},
      {true,
       true,
       0,
       0,
       otherTaskMemoryCapacity * 2,
       otherTaskMemoryCapacity * 2,
       0},
      {false,
       true,
       0,
       0,
       otherTaskMemoryCapacity * 2,
       otherTaskMemoryCapacity * 2,
       0}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory();

    std::vector<std::shared_ptr<MockTask>> otherTasks;
    std::vector<MockMemoryOperator*> otherTaskOps;
    for (int i = 0; i < numOtherTasks; ++i) {
      otherTasks.push_back(addTask());
      otherTaskOps.push_back(addMemoryOp(otherTasks.back(), false));
      otherTaskOps.back()->allocate(otherTaskMemoryCapacity);
      ASSERT_EQ(
          otherTasks.back()->pool()->currentBytes(), otherTaskMemoryCapacity);
    }
    std::shared_ptr<MockTask> failedTask = addTask();
    MockMemoryOperator* failedTaskOp = addMemoryOp(
        failedTask, true, [&](MemoryPool* /*unsed*/, uint64_t /*unsed*/) {
          VELOX_FAIL("throw reclaim exception");
        });
    failedTaskOp->allocate(failedTaskMemoryCapacity);
    for (int i = 0; i < numOtherTasks; ++i) {
      ASSERT_EQ(otherTaskOps[0]->pool()->capacity(), otherTaskMemoryCapacity);
    }
    ASSERT_EQ(failedTaskOp->capacity(), failedTaskMemoryCapacity);

    const auto oldStats = arbitrator_->stats();
    ASSERT_EQ(oldStats.numFailures, 0);
    ASSERT_EQ(oldStats.numAborted, 0);

    const int numFailedTaskAllocationsAfterAbort =
        testData.hasAllocationFromFailedTaskAfterAbort ? 3 : 0;
    // If 'hasAllocationFromOtherTaskAfterAbort' is true, then one allocation
    // from each of the other tasks.
    const int numOtherAllocationsAfterAbort =
        testData.hasAllocationFromOtherTaskAfterAbort ? numOtherTasks : 0;

    // One barrier count is for the initial allocation from the failed task to
    // trigger memory arbitration.
    folly::futures::Barrier arbitrationStartBarrier(
        numFailedTaskAllocationsAfterAbort + numOtherAllocationsAfterAbort + 1);
    folly::futures::Barrier arbitrationBarrier(
        numFailedTaskAllocationsAfterAbort + numOtherAllocationsAfterAbort + 1);
    std::atomic<int> testInjectionCount{0};
    std::atomic<bool> arbitrationStarted{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::SharedArbitrator::startArbitration",
        std::function<void(const MemoryPool*)>(
            ([&](const MemoryPool* /*unsed*/) {
              if (!arbitrationStarted) {
                return;
              }
              if (++testInjectionCount <= numFailedTaskAllocationsAfterAbort +
                      numOtherAllocationsAfterAbort + 1) {
                arbitrationBarrier.wait().wait();
              }
            })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::SharedArbitrator::sortCandidatesByFreeCapacity",
        std::function<void(const std::vector<SharedArbitrator::Candidate>*)>(
            ([&](const std::vector<SharedArbitrator::Candidate>* /*unused*/) {
              if (!arbitrationStarted.exchange(true)) {
                arbitrationStartBarrier.wait().wait();
              }
              if (++testInjectionCount <= numFailedTaskAllocationsAfterAbort +
                      numOtherAllocationsAfterAbort + 1) {
                arbitrationBarrier.wait().wait();
              }
            })));

    std::vector<std::thread> allocationThreadsAfterAbort;
    for (int i = 0;
         i < numFailedTaskAllocationsAfterAbort + numOtherAllocationsAfterAbort;
         ++i) {
      allocationThreadsAfterAbort.emplace_back([&, i]() {
        arbitrationStartBarrier.wait().wait();
        if (i < numFailedTaskAllocationsAfterAbort) {
          VELOX_ASSERT_THROW(
              failedTaskOp->allocate(failedTaskMemoryCapacity), "");
        } else {
          otherTaskOps[i - numFailedTaskAllocationsAfterAbort]->allocate(
              otherTaskMemoryCapacity);
        }
      });
    }

    // Trigger memory arbitration to reclaim from itself which throws.
    VELOX_ASSERT_THROW(failedTaskOp->allocate(failedTaskMemoryCapacity), "");
    // Wait for all the allocation threads to complete.
    for (auto& allocationThread : allocationThreadsAfterAbort) {
      allocationThread.join();
    }
    ASSERT_TRUE(failedTaskOp->pool()->aborted());
    ASSERT_EQ(
        failedTaskOp->pool()->currentBytes(),
        testData.expectedFailedTaskMemoryCapacity);
    ASSERT_EQ(
        failedTaskOp->pool()->capacity(),
        testData.expectedFailedTaskMemoryUsage);
    ASSERT_EQ(failedTaskOp->reclaimer()->stats().numAborts, 1);
    ASSERT_EQ(failedTaskOp->reclaimer()->stats().numReclaims, 1);

    const auto newStats = arbitrator_->stats();
    ASSERT_EQ(
        newStats.numRequests,
        oldStats.numRequests + 1 + numFailedTaskAllocationsAfterAbort +
            numOtherAllocationsAfterAbort);
    ASSERT_EQ(newStats.numAborted, 1);
    ASSERT_EQ(newStats.freeCapacityBytes, testData.expectedFreeCapacity);
    ASSERT_EQ(newStats.numFailures, numFailedTaskAllocationsAfterAbort + 1);
    ASSERT_EQ(newStats.maxCapacityBytes, kMemoryCapacity);
    // Check if memory pools have been aborted or not as expected.
    for (const auto* taskOp : otherTaskOps) {
      ASSERT_FALSE(taskOp->pool()->aborted());
      ASSERT_EQ(taskOp->reclaimer()->stats().numAborts, 0);
      ASSERT_EQ(taskOp->reclaimer()->stats().numReclaims, 0);
      ASSERT_EQ(
          taskOp->pool()->capacity(), testData.expectedOtherTaskMemoryCapacity);
      ASSERT_EQ(
          taskOp->pool()->currentBytes(),
          testData.expectedOtherTaskMemoryUsage);
    }

    VELOX_ASSERT_THROW(failedTaskOp->allocate(failedTaskMemoryCapacity), "");
    ASSERT_EQ(arbitrator_->stats().numRequests, newStats.numRequests);
    ASSERT_EQ(arbitrator_->stats().numAborted, 1);
  }
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, failedToReclaimFromOtherTask) {
  const int numNonFailedTasks = 3;
  const int nonFailTaskMemoryCapacity = kMemoryCapacity / 8;
  const int failedTaskMemoryCapacity =
      kMemoryCapacity / 2 + nonFailTaskMemoryCapacity;
  struct {
    bool hasAllocationFromFailedTaskAfterAbort;
    bool hasAllocationFromNonFailedTaskAfterAbort;
    int64_t expectedFailedTaskMemoryCapacity;
    int64_t expectedFailedTaskMemoryUsage;
    int64_t expectedNonFailedTaskMemoryCapacity;
    int64_t expectedNonFailedTaskMemoryUsage;
    int64_t expectedFreeCapacity;

    std::string debugString() const {
      return fmt::format(
          "hasAllocationFromFailedTaskAfterAbort {}, hasAllocationFromNonFailedTaskAfterAbort {} expectedFailedTaskMemoryCapacity {} expectedFailedTaskMemoryUsage {} expectedNonFailedTaskMemoryCapacity {} expectedNonFailedTaskMemoryUsage {} expectedFreeCapacity {}",
          hasAllocationFromFailedTaskAfterAbort,
          hasAllocationFromNonFailedTaskAfterAbort,
          expectedFailedTaskMemoryCapacity,
          expectedFailedTaskMemoryUsage,
          expectedNonFailedTaskMemoryCapacity,
          expectedNonFailedTaskMemoryUsage,
          expectedFreeCapacity);
    }
  } testSettings[] = {
      {false,
       false,
       0,
       0,
       nonFailTaskMemoryCapacity,
       nonFailTaskMemoryCapacity,
       failedTaskMemoryCapacity - nonFailTaskMemoryCapacity},
      {true,
       false,
       0,
       0,
       nonFailTaskMemoryCapacity,
       nonFailTaskMemoryCapacity,
       failedTaskMemoryCapacity - nonFailTaskMemoryCapacity},
      {true,
       true,
       0,
       0,
       nonFailTaskMemoryCapacity * 2,
       nonFailTaskMemoryCapacity * 2,
       nonFailTaskMemoryCapacity},
      {false,
       true,
       0,
       0,
       nonFailTaskMemoryCapacity * 2,
       nonFailTaskMemoryCapacity * 2,
       nonFailTaskMemoryCapacity}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory();

    std::vector<std::shared_ptr<MockTask>> nonFailedTasks;
    std::vector<MockMemoryOperator*> nonFailedTaskOps;
    for (int i = 0; i < numNonFailedTasks; ++i) {
      nonFailedTasks.push_back(addTask());
      nonFailedTaskOps.push_back(addMemoryOp(nonFailedTasks.back(), false));
      nonFailedTaskOps.back()->allocate(nonFailTaskMemoryCapacity);
      ASSERT_EQ(
          nonFailedTasks.back()->pool()->currentBytes(),
          nonFailTaskMemoryCapacity);
    }
    std::shared_ptr<MockTask> failedTask = addTask();
    MockMemoryOperator* failedTaskOp = addMemoryOp(
        failedTask, true, [&](MemoryPool* /*unsed*/, uint64_t /*unsed*/) {
          VELOX_FAIL("throw reclaim exception");
        });
    failedTaskOp->allocate(failedTaskMemoryCapacity);
    for (int i = 0; i < numNonFailedTasks; ++i) {
      ASSERT_EQ(
          nonFailedTasks[0]->pool()->capacity(), nonFailTaskMemoryCapacity)
          << i;
    }
    ASSERT_EQ(failedTaskOp->capacity(), failedTaskMemoryCapacity);

    const auto oldStats = arbitrator_->stats();
    ASSERT_EQ(oldStats.numFailures, 0);
    ASSERT_EQ(oldStats.numAborted, 0);

    const int numFailedTaskAllocationsAfterAbort =
        testData.hasAllocationFromFailedTaskAfterAbort ? 3 : 0;
    // If 'hasAllocationFromOtherTaskAfterAbort' is true, then one allocation
    // from each of the other tasks.
    const int numNonFailedAllocationsAfterAbort =
        testData.hasAllocationFromNonFailedTaskAfterAbort ? numNonFailedTasks
                                                          : 0;
    // One barrier count is for the initial allocation from the failed task to
    // trigger memory arbitration.
    folly::futures::Barrier arbitrationStartBarrier(
        numFailedTaskAllocationsAfterAbort + numNonFailedAllocationsAfterAbort +
        1);
    folly::futures::Barrier arbitrationBarrier(
        numFailedTaskAllocationsAfterAbort + numNonFailedAllocationsAfterAbort +
        1);
    std::atomic<int> testInjectionCount{0};
    std::atomic<bool> arbitrationStarted{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::SharedArbitrator::startArbitration",
        std::function<void(const MemoryPool*)>(
            ([&](const MemoryPool* /*unsed*/) {
              if (!arbitrationStarted) {
                return;
              }
              if (++testInjectionCount <= numFailedTaskAllocationsAfterAbort +
                      numNonFailedAllocationsAfterAbort + 1) {
                arbitrationBarrier.wait().wait();
              }
            })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::SharedArbitrator::sortCandidatesByFreeCapacity",
        std::function<void(const std::vector<SharedArbitrator::Candidate>*)>(
            ([&](const std::vector<SharedArbitrator::Candidate>* /*unused*/) {
              if (!arbitrationStarted.exchange(true)) {
                arbitrationStartBarrier.wait().wait();
              }
              if (++testInjectionCount <= numFailedTaskAllocationsAfterAbort +
                      numNonFailedAllocationsAfterAbort + 1) {
                arbitrationBarrier.wait().wait();
              }
            })));

    std::vector<std::thread> allocationThreadsAfterAbort;
    for (int i = 0; i <
         numFailedTaskAllocationsAfterAbort + numNonFailedAllocationsAfterAbort;
         ++i) {
      allocationThreadsAfterAbort.emplace_back([&, i]() {
        arbitrationStartBarrier.wait().wait();
        if (i < numFailedTaskAllocationsAfterAbort) {
          VELOX_ASSERT_THROW(
              failedTaskOp->allocate(failedTaskMemoryCapacity), "");
        } else {
          nonFailedTaskOps[i - numFailedTaskAllocationsAfterAbort]->allocate(
              nonFailTaskMemoryCapacity);
        }
      });
    }

    // Trigger memory arbitration to reclaim from failedTask which throws.
    nonFailedTaskOps[0]->allocate(nonFailTaskMemoryCapacity);
    // Wait for all the allocation threads to complete.
    for (auto& allocationThread : allocationThreadsAfterAbort) {
      allocationThread.join();
    }
    ASSERT_TRUE(failedTaskOp->pool()->aborted());
    ASSERT_EQ(
        failedTaskOp->pool()->currentBytes(),
        testData.expectedFailedTaskMemoryCapacity);
    ASSERT_EQ(
        failedTaskOp->pool()->capacity(),
        testData.expectedFailedTaskMemoryUsage);
    ASSERT_EQ(failedTaskOp->reclaimer()->stats().numAborts, 1);
    ASSERT_EQ(failedTaskOp->reclaimer()->stats().numReclaims, 1);

    const auto newStats = arbitrator_->stats();
    ASSERT_EQ(
        newStats.numRequests,
        oldStats.numRequests + 1 + numFailedTaskAllocationsAfterAbort +
            numNonFailedAllocationsAfterAbort);
    ASSERT_EQ(newStats.numAborted, 1);
    ASSERT_EQ(newStats.freeCapacityBytes, testData.expectedFreeCapacity);
    ASSERT_EQ(newStats.numFailures, numFailedTaskAllocationsAfterAbort);
    ASSERT_EQ(newStats.maxCapacityBytes, kMemoryCapacity);
    // Check if memory pools have been aborted or not as expected.
    for (int i = 0; i < nonFailedTaskOps.size(); ++i) {
      auto* taskOp = nonFailedTaskOps[i];
      ASSERT_FALSE(taskOp->pool()->aborted());
      ASSERT_EQ(taskOp->reclaimer()->stats().numAborts, 0);
      ASSERT_EQ(taskOp->reclaimer()->stats().numReclaims, 0);
      if (i == 0) {
        ASSERT_EQ(
            taskOp->pool()->capacity(),
            testData.expectedNonFailedTaskMemoryCapacity +
                nonFailTaskMemoryCapacity);
        ASSERT_EQ(
            taskOp->pool()->currentBytes(),
            testData.expectedNonFailedTaskMemoryUsage +
                nonFailTaskMemoryCapacity);
      } else {
        ASSERT_EQ(
            taskOp->pool()->capacity(),
            testData.expectedNonFailedTaskMemoryCapacity);
        ASSERT_EQ(
            taskOp->pool()->currentBytes(),
            testData.expectedNonFailedTaskMemoryUsage);
      }
    }

    VELOX_ASSERT_THROW(failedTaskOp->allocate(failedTaskMemoryCapacity), "");
    ASSERT_EQ(arbitrator_->stats().numRequests, newStats.numRequests);
    ASSERT_EQ(arbitrator_->stats().numAborted, 1);
  }
}

TEST_F(MockSharedArbitrationTest, memoryPoolAbortThrow) {
  const int numTasks = 4;
  const int smallTaskMemoryCapacity = kMemoryCapacity / 8;
  const int largeTaskMemoryCapacity = kMemoryCapacity / 2;
  std::vector<std::shared_ptr<MockTask>> smallTasks;
  std::vector<MockMemoryOperator*> smallTaskOps;
  for (int i = 0; i < numTasks; ++i) {
    smallTasks.push_back(addTask());
    smallTaskOps.push_back(addMemoryOp(smallTasks.back(), false));
    smallTaskOps.back()->allocate(smallTaskMemoryCapacity);
  }
  std::shared_ptr<MockTask> largeTask = addTask();
  MockMemoryOperator* largeTaskOp = addMemoryOp(
      largeTask, true, [&](MemoryPool* /*unsed*/, uint64_t /*unsed*/) {
        VELOX_FAIL("throw reclaim exception");
      });
  largeTaskOp->allocate(largeTaskMemoryCapacity);
  const auto oldStats = arbitrator_->stats();
  ASSERT_EQ(oldStats.numFailures, 0);
  ASSERT_EQ(oldStats.numAborted, 0);

  // Trigger memory arbitration to reclaim from itself which throws.
  VELOX_ASSERT_THROW(largeTaskOp->allocate(largeTaskMemoryCapacity), "");
  const auto newStats = arbitrator_->stats();
  ASSERT_EQ(newStats.numRequests, oldStats.numRequests + 1);
  ASSERT_EQ(newStats.numAborted, 1);
  ASSERT_EQ(newStats.freeCapacityBytes, largeTaskMemoryCapacity);
  ASSERT_EQ(newStats.maxCapacityBytes, kMemoryCapacity);
  // Check if memory pools have been aborted or not as expected.
  for (const auto* taskOp : smallTaskOps) {
    ASSERT_FALSE(taskOp->pool()->aborted());
    ASSERT_EQ(taskOp->reclaimer()->stats().numAborts, 0);
    ASSERT_EQ(taskOp->reclaimer()->stats().numReclaims, 0);
  }
  ASSERT_TRUE(largeTaskOp->pool()->aborted());
  ASSERT_EQ(largeTaskOp->reclaimer()->stats().numAborts, 1);
  ASSERT_EQ(largeTaskOp->reclaimer()->stats().numReclaims, 1);
  VELOX_ASSERT_THROW(largeTaskOp->allocate(largeTaskMemoryCapacity), "");
  ASSERT_EQ(arbitrator_->stats().numRequests, newStats.numRequests);
  ASSERT_EQ(arbitrator_->stats().numAborted, 1);
}

DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    freeUnusedCapacityWhenReclaimMemoryPool) {
  setupMemory(kMemoryCapacity, 0);
  const int allocationSize = kMemoryCapacity / 4;
  std::shared_ptr<MockTask> reclaimedTask = addTask();
  MockMemoryOperator* reclaimedTaskOp = addMemoryOp(reclaimedTask);
  // The buffer to free later.
  void* bufferToFree = reclaimedTaskOp->allocate(allocationSize);
  reclaimedTaskOp->allocate(kMemoryCapacity - allocationSize);

  std::shared_ptr<MockTask> arbitrationTask = addTask();
  MockMemoryOperator* arbitrationTaskOp = addMemoryOp(arbitrationTask);
  folly::EventCount reclaimWait;
  auto reclaimWaitKey = reclaimWait.prepareWait();
  folly::EventCount reclaimBlock;
  auto reclaimBlockKey = reclaimBlock.prepareWait();
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableMemory",
      std::function<void(const MemoryPool*)>(([&](const MemoryPool* /*unsed*/) {
        reclaimWait.notify();
        reclaimBlock.wait(reclaimBlockKey);
      })));

  const auto oldStats = arbitrator_->stats();

  std::thread allocThread([&]() {
    // Allocate to trigger arbitration.
    arbitrationTaskOp->allocate(allocationSize);
  });

  reclaimWait.wait(reclaimWaitKey);
  reclaimedTaskOp->free(bufferToFree);
  reclaimBlock.notify();
  allocThread.join();
  const auto stats = arbitrator_->stats();
  ASSERT_EQ(stats.numFailures, 0);
  ASSERT_EQ(stats.numAborted, 0);
  ASSERT_EQ(stats.numRequests, oldStats.numRequests + 1);
  // We count the freed capacity in reclaimed bytes.
  ASSERT_EQ(stats.numShrunkBytes, oldStats.numShrunkBytes + allocationSize);
  ASSERT_EQ(stats.numReclaimedBytes, 0);
  ASSERT_EQ(reclaimedTaskOp->capacity(), kMemoryCapacity - allocationSize);
  ASSERT_EQ(arbitrationTaskOp->capacity(), allocationSize);
}

DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    raceBetweenInitialReservationAndArbitration) {
  std::shared_ptr<MockTask> arbitrationTask = addTask(kMemoryCapacity);
  MockMemoryOperator* arbitrationTaskOp = addMemoryOp(arbitrationTask);
  ASSERT_EQ(arbitrationTask->pool()->capacity(), kMemoryPoolInitCapacity);

  folly::EventCount arbitrationRun;
  auto arbitrationRunKey = arbitrationRun.prepareWait();
  folly::EventCount arbitrationBlock;
  auto arbitrationBlockKey = arbitrationBlock.prepareWait();

  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::startArbitration",
      std::function<void(const MemoryPool*)>(([&](const MemoryPool* /*unsed*/) {
        arbitrationRun.notify();
        arbitrationBlock.wait(arbitrationBlockKey);
      })));

  std::thread allocThread([&]() {
    // Allocate more than its capacity to trigger arbitration which is blocked
    // by the arbitration testvalue injection above.
    arbitrationTaskOp->allocate(2 * kMemoryPoolInitCapacity);
  });

  arbitrationRun.wait(arbitrationRunKey);

  // Allocate a new root memory pool and check its initial memory reservation is
  // zero.
  std::shared_ptr<MockTask> skipTask = addTask(kMemoryCapacity);
  MockMemoryOperator* skipTaskOp = addMemoryOp(skipTask);
  ASSERT_EQ(skipTaskOp->pool()->capacity(), 0);

  arbitrationBlock.notify();
  allocThread.join();
}

TEST_F(MockSharedArbitrationTest, arbitrationFailure) {
  int64_t maxCapacity = 128 * MB;
  int64_t initialCapacity = 0 * MB;
  int64_t minTransferCapacity = 1 * MB;
  struct {
    int64_t requestorCapacity;
    int64_t requestorRequestBytes;
    int64_t otherCapacity;
    bool expectedAllocationSuccess;
    bool expectedRequestorAborted;

    std::string debugString() const {
      return fmt::format(
          "requestorCapacity {} requestorRequestBytes {} otherCapacity {} expectedAllocationSuccess {} expectedRequestorAborted {}",
          succinctBytes(requestorCapacity),
          succinctBytes(requestorRequestBytes),
          succinctBytes(otherCapacity),
          expectedAllocationSuccess,
          expectedRequestorAborted);
    }
  } testSettings[] = {
      {64 * MB, 64 * MB, 32 * MB, false, false},
      {64 * MB, 48 * MB, 32 * MB, false, false},
      {32 * MB, 64 * MB, 64 * MB, false, false},
      {32 * MB, 32 * MB, 96 * MB, true, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    setupMemory(maxCapacity, initialCapacity, minTransferCapacity);
    std::shared_ptr<MockTask> requestorTask = addTask();
    MockMemoryOperator* requestorOp = addMemoryOp(requestorTask, false);
    requestorOp->allocate(testData.requestorCapacity);
    ASSERT_EQ(requestorOp->capacity(), testData.requestorCapacity);
    std::shared_ptr<MockTask> otherTask = addTask();
    MockMemoryOperator* otherOp = addMemoryOp(otherTask, false);
    otherOp->allocate(testData.otherCapacity);
    ASSERT_EQ(otherOp->capacity(), testData.otherCapacity);

    if (testData.expectedRequestorAborted) {
      VELOX_ASSERT_THROW(
          requestorOp->allocate(testData.requestorRequestBytes), "");
      ASSERT_TRUE(requestorOp->pool()->aborted());
      ASSERT_FALSE(otherOp->pool()->aborted());
    } else if (testData.expectedAllocationSuccess) {
      requestorOp->allocate(testData.requestorRequestBytes);
      ASSERT_FALSE(requestorOp->pool()->aborted());
      ASSERT_TRUE(otherOp->pool()->aborted());
    } else {
      VELOX_ASSERT_THROW(
          requestorOp->allocate(testData.requestorRequestBytes), "");
      ASSERT_FALSE(requestorOp->pool()->aborted());
      ASSERT_FALSE(otherOp->pool()->aborted());
    }
    ASSERT_EQ(
        arbitrator_->stats().numFailures,
        testData.expectedAllocationSuccess ? 0 : 1);
    ASSERT_EQ(
        arbitrator_->stats().numAborted,
        testData.expectedRequestorAborted
            ? 1
            : (testData.expectedAllocationSuccess ? 1 : 0));
  }
}

TEST_F(MockSharedArbitrationTest, concurrentArbitrations) {
  const int numTasks = 10;
  const int numOpsPerTask = 5;
  std::vector<std::shared_ptr<MockTask>> tasks;
  tasks.reserve(numTasks);
  std::vector<MockMemoryOperator*> memOps;
  memOps.reserve(numTasks * numOpsPerTask);
  const std::string injectReclaimErrorMessage("Inject reclaim failure");
  const std::string injectArbitrationErrorMessage(
      "Inject enter arbitration failure");
  for (int i = 0; i < numTasks; ++i) {
    tasks.push_back(addTask());
    for (int j = 0; j < numOpsPerTask; ++j) {
      memOps.push_back(addMemoryOp(
          tasks.back(),
          (j % 3) != 0,
          [&](MemoryPool* /*unused*/, uint64_t /*unused*/) {
            if (folly::Random::oneIn(10)) {
              VELOX_FAIL(injectReclaimErrorMessage);
            }
          },
          [&]() {
            if (folly::Random::oneIn(10)) {
              VELOX_FAIL(injectArbitrationErrorMessage);
            }
          }));
    }
  }

  std::atomic<bool> stopped{false};

  std::vector<std::thread> memThreads;
  for (int i = 0; i < numTasks * numOpsPerTask; ++i) {
    memThreads.emplace_back([&, i, memOp = memOps[i]]() {
      folly::Random::DefaultGenerator rng;
      rng.seed(i);
      while (!stopped) {
        if (folly::Random::oneIn(4, rng)) {
          if (folly::Random::oneIn(3, rng)) {
            memOp->freeAll();
          } else {
            memOp->free();
          }
        } else {
          const int allocationPages = AllocationTraits::numPages(
              folly::Random::rand32(rng) % (kMemoryCapacity / 8));
          try {
            memOp->allocate(AllocationTraits::pageBytes(allocationPages));
          } catch (VeloxException& e) {
            // Ignore memory limit exception and injected error exceptions.
            if ((e.message().find("Exceeded memory") == std::string::npos) &&
                (e.message().find(injectArbitrationErrorMessage) ==
                 std::string::npos) &&
                (e.message().find(injectReclaimErrorMessage) ==
                 std::string::npos) &&
                (e.message().find("aborted") == std::string::npos)) {
              ASSERT_FALSE(true) << "Unexpected exception " << e.message();
            }
          }
        }
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(5));
  stopped = true;

  for (auto& memThread : memThreads) {
    memThread.join();
  }
  tasks.clear();
}

TEST_F(MockSharedArbitrationTest, concurrentArbitrationWithTransientRoots) {
  std::mutex mutex;
  std::vector<std::shared_ptr<MockTask>> tasks;
  tasks.push_back(addTask());
  tasks.back()->addMemoryOp();

  std::atomic<bool> stopped{false};

  const int numMemThreads = 20;
  const std::string injectReclaimErrorMessage("Inject reclaim failure");
  const std::string injectArbitrationErrorMessage(
      "Inject enter arbitration failure");
  std::vector<std::thread> memThreads;
  for (int i = 0; i < numMemThreads; ++i) {
    memThreads.emplace_back([&, i]() {
      folly::Random::DefaultGenerator rng;
      rng.seed(i);
      while (!stopped) {
        std::shared_ptr<MockTask> task;
        {
          std::lock_guard<std::mutex> l(mutex);
          const int index = folly::Random::rand32() % tasks.size();
          task = tasks[index];
        }
        if (folly::Random::oneIn(4, rng)) {
          if (folly::Random::oneIn(3, rng)) {
            task->memoryOp()->freeAll();
          } else {
            task->memoryOp()->free();
          }
        } else {
          const int allocationPages = AllocationTraits::numPages(
              folly::Random::rand32(rng) % (kMemoryCapacity / 8));
          try {
            task->memoryOp()->allocate(
                AllocationTraits::pageBytes(allocationPages));
          } catch (VeloxException& e) {
            // Ignore memory limit exception and injected error exceptions.
            if ((e.message().find("Exceeded memory") == std::string::npos) &&
                (e.message().find(injectArbitrationErrorMessage) ==
                 std::string::npos) &&
                (e.message().find(injectReclaimErrorMessage) ==
                 std::string::npos) &&
                (e.message().find("aborted") == std::string::npos)) {
              ASSERT_FALSE(true) << "Unexpected exception " << e.message();
            }
          }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  const int maxNumTasks = 64;
  std::thread controlThread([&]() {
    folly::Random::DefaultGenerator rng;
    rng.seed(1000);
    while (!stopped) {
      {
        std::lock_guard<std::mutex> l(mutex);
        if ((tasks.size() == 1) ||
            (tasks.size() < maxNumTasks && folly::Random::oneIn(4, rng))) {
          tasks.push_back(addTask());
          tasks.back()->addMemoryOp(
              !folly::Random::oneIn(3, rng),
              [&](MemoryPool* /*unused*/, uint64_t /*unused*/) {
                if (folly::Random::oneIn(10)) {
                  VELOX_FAIL(injectReclaimErrorMessage);
                }
              },
              [&]() {
                if (folly::Random::oneIn(10)) {
                  VELOX_FAIL(injectArbitrationErrorMessage);
                }
              });
        } else {
          const int deleteIndex = folly::Random::rand32(rng) % tasks.size();
          tasks.erase(tasks.begin() + deleteIndex);
        }
      }
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(5));
  stopped = true;

  for (auto& memThread : memThreads) {
    memThread.join();
  }
  controlThread.join();
}

} // namespace
} // namespace facebook::velox::memory
