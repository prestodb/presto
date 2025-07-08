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
#include <gmock/gmock.h>
#include <re2/re2.h>
#include <deque>
#include <vector>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/memory/tests/SharedArbitratorTestUtil.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_suppress_memory_capacity_exceeding_error_message);

using namespace ::testing;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::memory {
// Class to write runtime stats in the tests to the stats container.
class TestRuntimeStatWriter : public BaseRuntimeStatWriter {
 public:
  explicit TestRuntimeStatWriter(
      std::unordered_map<std::string, RuntimeMetric>& stats)
      : stats_{stats} {}

  void addRuntimeStat(const std::string& name, const RuntimeCounter& value)
      override {
    addOperatorRuntimeStats(name, value, stats_);
  }

 private:
  std::unordered_map<std::string, RuntimeMetric>& stats_;
};

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;

constexpr uint64_t kMemoryCapacity = 512 * MB;
constexpr uint64_t kReservedMemoryCapacity = 128 * MB;
constexpr uint64_t kMemoryPoolInitCapacity = 16 * MB;
constexpr uint64_t kMemoryPoolReservedCapacity = 8 * MB;
constexpr uint64_t kFastExponentialGrowthCapacityLimit = 32 * MB;
constexpr double kSlowCapacityGrowPct = 0.25;
constexpr uint64_t kMemoryPoolMinFreeCapacity = 8 * MB;
constexpr double kMemoryPoolMinFreeCapacityPct = 0.25;
constexpr double kGlobalArbitrationReclaimPct = 10;
constexpr double kMemoryReclaimThreadsHwMultiplier = 0.5;

class MemoryReclaimer;
class MockMemoryOperator;

using ReclaimInjectionCallback =
    std::function<bool(MemoryPool* pool, uint64_t targetByte)>;
using ArbitrationInjectionCallback = std::function<void()>;

struct AllocatedBuffer {
  void* buffer{nullptr};
  size_t size{0};
};

class MockTask : public std::enable_shared_from_this<MockTask> {
 public:
  MockTask() {}

  ~MockTask();

  class MemoryReclaimer : public memory::MemoryReclaimer {
   public:
    MemoryReclaimer(const std::shared_ptr<MockTask>& task, int32_t priority)
        : memory::MemoryReclaimer(priority), task_(task) {}

    static std::unique_ptr<MemoryReclaimer> create(
        const std::shared_ptr<MockTask>& task,
        int32_t priority) {
      return std::make_unique<MemoryReclaimer>(task, priority);
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

  void
  initTaskPool(MemoryManager* manager, uint64_t capacity, int32_t priority) {
    root_ = manager->addRootPool(
        fmt::format("RootPool-{}", poolId_++),
        capacity,
        MemoryReclaimer::create(shared_from_this(), priority));
  }

  MemoryPool* pool() const {
    return root_.get();
  }

  uint64_t capacity() const {
    return root_->capacity();
  }

  uint64_t usedBytes() const {
    return root_->usedBytes();
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
        : memory::MemoryReclaimer(0),
          op_(op),
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
        if (!reclaimInjectCb_(pool, targetBytes)) {
          return 0;
        }
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
    std::lock_guard<std::mutex> l(mu_);
    VELOX_CHECK_EQ(allocations_.count(buffer), 1);
    size = allocations_[buffer];
    totalBytes_ -= size;
    allocations_.erase(buffer);
    pool_->free(buffer, size);
  }

  void freeAll() {
    std::unordered_map<void*, size_t> allocationsToFree;
    {
      std::lock_guard<std::mutex> l(mu_);
      for (auto entry : allocations_) {
        totalBytes_ -= entry.second;
      }
      VELOX_CHECK_EQ(totalBytes_, 0);
      allocationsToFree.swap(allocations_);
    }
    for (auto entry : allocationsToFree) {
      pool_->free(entry.first, entry.second);
    }
  }

  void free() {
    AllocatedBuffer allocation;
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
    std::vector<AllocatedBuffer> allocationsToFree;
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
    return bytesReclaimed;
  }

  void setAbortHook(const std::function<bool(MockMemoryOperator*)>& hook) {
    abortHook_ = hook;
  }

  void abort(MemoryPool* pool) {
    if (abortHook_ != nullptr && abortHook_(this)) {
      return;
    }

    std::unordered_map<void*, size_t> allocationsToFree;
    {
      std::lock_guard<std::mutex> l(mu_);
      VELOX_CHECK_NOT_NULL(pool_);
      VELOX_CHECK_EQ(pool->name(), pool_->name());
      for (const auto& allocation : allocations_) {
        totalBytes_ -= allocation.second;
      }
      allocationsToFree.swap(allocations_);
    }
    for (auto entry : allocationsToFree) {
      pool_->free(entry.first, entry.second);
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

  std::function<bool(MockMemoryOperator*)> abortHook_{nullptr};
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
    setupMemory({});
  }

  void TearDown() override {
    clearTasks();
  }

  struct ArbitratorOptions {
    int64_t memoryCapacity{kMemoryCapacity};
    int64_t reservedMemoryCapacity{0};
    uint64_t memoryPoolInitCapacity{0};
    uint64_t memoryPoolReserveCapacity{0};
    uint64_t fastExponentialGrowthCapacityLimit{0};

    double slowCapacityGrowPct{0};
    uint64_t memoryPoolMinFreeCapacity{0};
    double memoryPoolMinFreeCapacityPct{0};
    uint64_t memoryPoolMinReclaimBytes{0};
    double memoryPoolMinReclaimPct{0};

    uint64_t memoryPoolSpillCapacityLimit{0};
    uint64_t memoryPoolAbortCapacityLimit{0};
    double globalArbitrationReclaimPct{0};
    double memoryReclaimThreadsHwMultiplier{kMemoryReclaimThreadsHwMultiplier};
    std::function<void(MemoryPool&)> arbitrationStateCheckCb{nullptr};

    bool globalArtbitrationEnabled{true};
    uint64_t arbitrationTimeoutNs{5 * 60 * 1'000'000'000UL};
    bool globalArbitrationWithoutSpill{false};
    // Set the globalArbitrationAbortTimeRatio to be very small so that the
    // query can be aborted sooner and the test would not timeout.
    double globalArbitrationAbortTimeRatio{0.005};
  };

  void setupMemory(ArbitratorOptions arbitratorOptions) {
    MemoryManager::Options options;
    options.allocatorCapacity = arbitratorOptions.memoryCapacity;
    std::string arbitratorKind = "SHARED";
    options.arbitratorKind = arbitratorKind;

    using ExtraConfig = SharedArbitrator::ExtraConfig;
    options.extraArbitratorConfigs = {
        {std::string(ExtraConfig::kReservedCapacity),
         folly::to<std::string>(arbitratorOptions.reservedMemoryCapacity) +
             "B"},
        {std::string(ExtraConfig::kMemoryPoolInitialCapacity),
         folly::to<std::string>(arbitratorOptions.memoryPoolInitCapacity) +
             "B"},
        {std::string(ExtraConfig::kMemoryPoolReservedCapacity),
         folly::to<std::string>(arbitratorOptions.memoryPoolReserveCapacity) +
             "B"},
        {std::string(ExtraConfig::kFastExponentialGrowthCapacityLimit),
         folly::to<std::string>(
             arbitratorOptions.fastExponentialGrowthCapacityLimit) +
             "B"},
        {std::string(ExtraConfig::kSlowCapacityGrowPct),
         folly::to<std::string>(arbitratorOptions.slowCapacityGrowPct)},
        {std::string(ExtraConfig::kMemoryPoolMinFreeCapacity),
         folly::to<std::string>(arbitratorOptions.memoryPoolMinFreeCapacity) +
             "B"},
        {std::string(ExtraConfig::kMemoryPoolMinFreeCapacityPct),
         folly::to<std::string>(
             arbitratorOptions.memoryPoolMinFreeCapacityPct)},
        {std::string(ExtraConfig::kMemoryPoolMinReclaimBytes),
         folly::to<std::string>(arbitratorOptions.memoryPoolMinReclaimBytes) +
             "B"},
        {std::string(ExtraConfig::kMemoryPoolMinReclaimPct),
         folly::to<std::string>(arbitratorOptions.memoryPoolMinReclaimPct)},
        {std::string(ExtraConfig::kMemoryPoolSpillCapacityLimit),
         folly::to<std::string>(
             arbitratorOptions.memoryPoolSpillCapacityLimit) +
             "B"},
        {std::string(ExtraConfig::kMemoryPoolAbortCapacityLimit),
         folly::to<std::string>(
             arbitratorOptions.memoryPoolAbortCapacityLimit) +
             "B"},
        {std::string(ExtraConfig::kGlobalArbitrationMemoryReclaimPct),
         folly::to<std::string>(arbitratorOptions.globalArbitrationReclaimPct)},
        {std::string(ExtraConfig::kMemoryReclaimThreadsHwMultiplier),
         folly::to<std::string>(
             arbitratorOptions.memoryReclaimThreadsHwMultiplier)},
        {std::string(ExtraConfig::kMaxMemoryArbitrationTime),
         folly::to<std::string>(arbitratorOptions.arbitrationTimeoutNs) + "ns"},
        {std::string(ExtraConfig::kGlobalArbitrationEnabled),
         folly::to<std::string>(arbitratorOptions.globalArtbitrationEnabled)},
        {std::string(ExtraConfig::kGlobalArbitrationWithoutSpill),
         folly::to<std::string>(
             arbitratorOptions.globalArbitrationWithoutSpill)},
        {std::string(ExtraConfig::kGlobalArbitrationAbortTimeRatio),
         folly::to<std::string>(
             arbitratorOptions.globalArbitrationAbortTimeRatio)}};
    options.arbitrationStateCheckCb =
        std::move(arbitratorOptions.arbitrationStateCheckCb);
    options.checkUsageLeak = true;
    manager_ = std::make_unique<MemoryManager>(options);
    ASSERT_EQ(manager_->arbitrator()->kind(), arbitratorKind);
    arbitrator_ = static_cast<SharedArbitrator*>(manager_->arbitrator());
  }

  std::shared_ptr<MockTask> addTask(
      int64_t capacity = kMaxMemory,
      int32_t priority = 0) {
    auto task = std::make_shared<MockTask>();
    task->initTaskPool(manager_.get(), capacity, priority);
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
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(4);
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
    uint64_t freeReservedCapacityBytes = 0,
    uint64_t numRequests = 0,
    uint64_t numFailures = 0,
    uint64_t numReclaimedBytes = 0,
    uint64_t numShrunkBytes = 0) {
  ASSERT_EQ(stats.numRequests, numRequests);
  ASSERT_EQ(stats.numFailures, numFailures);
  ASSERT_EQ(stats.reclaimedUsedBytes, numReclaimedBytes);
  ASSERT_EQ(stats.reclaimedFreeBytes, numShrunkBytes);
  ASSERT_EQ(stats.freeReservedCapacityBytes, freeReservedCapacityBytes);
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

TEST_F(MockSharedArbitrationTest, configToString) {
  std::unordered_map<std::string, std::string> configs;
  configs[std::string(SharedArbitrator::ExtraConfig::kReservedCapacity)] =
      "100B";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolInitialCapacity)] = "512MB";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolReservedCapacity)] = "200B";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMaxMemoryArbitrationTime)] = "5000ms";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationEnabled)] = "true";
  configs[std::string(SharedArbitrator::ExtraConfig::kCheckUsageLeak)] =
      "false";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolMinReclaimBytes)] = "64mb";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolMinReclaimPct)] = "0.3";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolAbortCapacityLimit)] = "256mb";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationMemoryReclaimPct)] =
      "30";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryReclaimThreadsHwMultiplier)] =
      "1.0";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationWithoutSpill)] = "true";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationAbortTimeRatio)] = "0.8";

  MemoryArbitrator::Config arbitratorConfig{
      "SHARED", 1024, nullptr, std::move(configs)};
  ASSERT_EQ(
      arbitratorConfig.toString(),
      "kind=SHARED;capacity=1.00KB;"
      "arbitrationStateCheckCb=(unset);"
      "global-arbitration-without-spill=true;"
      "memory-reclaim-threads-hw-multiplier=1.0;"
      "memory-pool-min-reclaim-pct=0.3;"
      "check-usage-leak=false;"
      "global-arbitration-enabled=true;"
      "max-memory-arbitration-time=5000ms;"
      "global-arbitration-memory-reclaim-pct=30;"
      "memory-pool-abort-capacity-limit=256mb;"
      "memory-pool-min-reclaim-bytes=64mb;"
      "memory-pool-reserved-capacity=200B;"
      "memory-pool-initial-capacity=512MB;"
      "global-arbitration-abort-time-ratio=0.8;"
      "reserved-capacity=100B;");
}

TEST_F(MockSharedArbitrationTest, extraConfigs) {
  // Testing default values
  std::unordered_map<std::string, std::string> emptyConfigs;
  ASSERT_EQ(SharedArbitrator::ExtraConfig::reservedCapacity(emptyConfigs), 0);
  ASSERT_EQ(SharedArbitrator::ExtraConfig::reservedCapacity(emptyConfigs), 0);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolInitialCapacity(emptyConfigs),
      256 << 20);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::maxMemoryArbitrationTimeNs(emptyConfigs),
      300'000'000'000UL);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationEnabled(emptyConfigs),
      SharedArbitrator::ExtraConfig::kDefaultGlobalArbitrationEnabled);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::checkUsageLeak(emptyConfigs),
      SharedArbitrator::ExtraConfig::kDefaultCheckUsageLeak);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolMinReclaimBytes(emptyConfigs),
      128 << 20);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolMinReclaimPct(emptyConfigs),
      0.25);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolAbortCapacityLimit(emptyConfigs),
      1LL << 30);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationMemoryReclaimPct(
          emptyConfigs),
      SharedArbitrator::ExtraConfig::kDefaultGlobalMemoryArbitrationReclaimPct);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryReclaimThreadsHwMultiplier(
          emptyConfigs),
      SharedArbitrator::ExtraConfig::kDefaultMemoryReclaimThreadsHwMultiplier);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationWithoutSpill(
          emptyConfigs),
      SharedArbitrator::ExtraConfig::kDefaultGlobalArbitrationWithoutSpill);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationAbortTimeRatio(
          emptyConfigs),
      SharedArbitrator::ExtraConfig::kDefaultGlobalArbitrationAbortTimeRatio);

  // Testing custom values
  std::unordered_map<std::string, std::string> configs;
  configs[std::string(SharedArbitrator::ExtraConfig::kReservedCapacity)] =
      "100B";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolInitialCapacity)] = "512MB";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolReservedCapacity)] = "200B";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMaxMemoryArbitrationTime)] = "5000ms";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationEnabled)] = "true";
  configs[std::string(SharedArbitrator::ExtraConfig::kCheckUsageLeak)] =
      "false";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolMinReclaimBytes)] = "64mb";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolMinReclaimPct)] = "0.35";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolAbortCapacityLimit)] = "256mb";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationMemoryReclaimPct)] =
      "30";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryReclaimThreadsHwMultiplier)] =
      "1.0";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationWithoutSpill)] = "true";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationAbortTimeRatio)] = "0.8";

  ASSERT_EQ(SharedArbitrator::ExtraConfig::reservedCapacity(configs), 100);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolInitialCapacity(configs),
      512 << 20);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolReservedCapacity(configs), 200);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::maxMemoryArbitrationTimeNs(configs),
      5'000'000'000UL);
  ASSERT_TRUE(SharedArbitrator::ExtraConfig::globalArbitrationEnabled(configs));
  ASSERT_FALSE(SharedArbitrator::ExtraConfig::checkUsageLeak(configs));
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolMinReclaimBytes(configs),
      64 << 20);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolMinReclaimPct(configs), 0.35);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryPoolAbortCapacityLimit(configs),
      256 << 20);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationMemoryReclaimPct(configs),
      30);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::memoryReclaimThreadsHwMultiplier(configs),
      1.0);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationWithoutSpill(configs),
      true);
  ASSERT_EQ(
      SharedArbitrator::ExtraConfig::globalArbitrationAbortTimeRatio(configs),
      0.8);

  // Testing invalid values
  configs[std::string(SharedArbitrator::ExtraConfig::kReservedCapacity)] =
      "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolInitialCapacity)] = "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolReservedCapacity)] = "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMaxMemoryArbitrationTime)] = "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationEnabled)] = "invalid";
  configs[std::string(SharedArbitrator::ExtraConfig::kCheckUsageLeak)] =
      "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolMinReclaimBytes)] = "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolMinReclaimPct)] = "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryPoolAbortCapacityLimit)] =
      "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationMemoryReclaimPct)] =
      "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kMemoryReclaimThreadsHwMultiplier)] =
      "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationWithoutSpill)] =
      "invalid";
  configs[std::string(
      SharedArbitrator::ExtraConfig::kGlobalArbitrationAbortTimeRatio)] =
      "invalid";

  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::reservedCapacity(configs),
      "Invalid capacity string 'invalid'");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::memoryPoolInitialCapacity(configs),
      "Invalid capacity string 'invalid'");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::memoryPoolReservedCapacity(configs),
      "Invalid capacity string 'invalid'");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::maxMemoryArbitrationTimeNs(configs),
      "Invalid duration 'invalid'");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::globalArbitrationEnabled(configs),
      "Failed while parsing SharedArbitrator configs");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::checkUsageLeak(configs),
      "Failed while parsing SharedArbitrator configs");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::memoryPoolMinReclaimBytes(configs),
      "Invalid capacity string 'invalid'");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::memoryPoolMinReclaimPct(configs),
      "Failed while parsing SharedArbitrator configs");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::memoryPoolAbortCapacityLimit(configs),
      "Invalid capacity string 'invalid'");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::globalArbitrationMemoryReclaimPct(configs),
      "Failed while parsing SharedArbitrator configs");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::memoryReclaimThreadsHwMultiplier(configs),
      "Failed while parsing SharedArbitrator configs");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::globalArbitrationWithoutSpill(configs),
      "Failed while parsing SharedArbitrator configs");
  VELOX_ASSERT_THROW(
      SharedArbitrator::ExtraConfig::globalArbitrationAbortTimeRatio(configs),
      "Failed while parsing SharedArbitrator configs");
  // Invalid memory reclaim executor hw multiplier.
  VELOX_ASSERT_THROW(
      setupMemory({.memoryReclaimThreadsHwMultiplier = -1}),
      "memoryReclaimThreadsHwMultiplier_ needs to be positive");
  // Invalid global arbitration reclaim pct.
  VELOX_ASSERT_THROW(
      setupMemory({.globalArbitrationReclaimPct = 200}),
      "(200 vs. 100) Invalid globalArbitrationMemoryReclaimPct");
  // Invalid max memory arbitration time.
  VELOX_ASSERT_THROW(
      setupMemory(
          {.memoryReclaimThreadsHwMultiplier = 0,
           .globalArtbitrationEnabled = false,
           .arbitrationTimeoutNs = 0}),
      "(0 vs. 0) maxArbitrationTimeNs can't be zero");
}

TEST_F(MockSharedArbitrationTest, constructor) {
  setupMemory(
      {.reservedMemoryCapacity = kReservedMemoryCapacity,
       .memoryPoolInitCapacity = kMemoryPoolInitCapacity,
       .memoryPoolReserveCapacity = kMemoryPoolReservedCapacity});
  const int reservedCapacity = arbitrator_->stats().freeReservedCapacityBytes;
  const int nonReservedCapacity =
      arbitrator_->stats().freeCapacityBytes - reservedCapacity;
  std::vector<std::shared_ptr<MockTask>> tasks;
  int remainingFreeCapacity = arbitrator_->stats().freeCapacityBytes;
  for (int i = 0; i <= kMemoryCapacity / kMemoryPoolInitCapacity; ++i) {
    auto task = addTask(kMemoryCapacity);
    ASSERT_NE(task->pool()->reclaimer(), nullptr);
    if (i < nonReservedCapacity / kMemoryPoolInitCapacity) {
      ASSERT_EQ(task->capacity(), kMemoryPoolInitCapacity);
    } else {
      ASSERT_EQ(task->capacity(), kMemoryPoolReservedCapacity) << i;
    }
    remainingFreeCapacity -= task->capacity();
    tasks.push_back(std::move(task));
  }
  auto stats = arbitrator_->stats();
  ASSERT_EQ(remainingFreeCapacity, stats.freeCapacityBytes);
  ASSERT_EQ(remainingFreeCapacity, stats.freeReservedCapacityBytes);
  verifyArbitratorStats(
      stats, kMemoryCapacity, remainingFreeCapacity, remainingFreeCapacity);
  tasks.clear();
  stats = arbitrator_->stats();
  verifyArbitratorStats(
      stats, kMemoryCapacity, kMemoryCapacity, reservedCapacity);
}

TEST_F(MockSharedArbitrationTest, arbitrationStateCheck) {
  const int memCapacity = 256 * MB;
  std::atomic<int> checkCount{0};
  MemoryArbitrationStateCheckCB checkCountCb = [&](MemoryPool& pool) {
    const std::string re("RootPool.*");
    ASSERT_TRUE(RE2::FullMatch(pool.name(), re)) << pool.name();
    ++checkCount;
  };
  setupMemory(
      {.memoryCapacity = memCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationStateCheckCb = checkCountCb});

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
  setupMemory(
      {.memoryCapacity = memCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationStateCheckCb = badCheckCb});
  std::shared_ptr<MockTask> task = addTask(kMemoryCapacity);
  ASSERT_EQ(task->capacity(), 0);
  MockMemoryOperator* memOp = task->addMemoryOp();
  VELOX_ASSERT_THROW(memOp->allocate(128), "bad check");
}

TEST_F(MockSharedArbitrationTest, asyncArbitrationWork) {
  const int memoryCapacity = 512 * MB;
  const int poolCapacity = 256 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryPoolInitCapacity = poolCapacity});

  std::atomic_int reclaimedCount{0};
  std::shared_ptr<MockTask> task = addTask(poolCapacity);
  MockMemoryOperator* memoryOp = addMemoryOp(
      task, true, [&](MemoryPool* pool, uint64_t /*unsed*/) -> bool {
        struct Result {
          bool succeeded{true};

          explicit Result(bool _succeeded) : succeeded(_succeeded) {}
        };
        auto asyncReclaimTask = createAsyncMemoryReclaimTask<Result>([&]() {
          memoryOp->allocate(poolCapacity);
          return std::make_unique<Result>(true);
        });
        executor_->add([&]() { asyncReclaimTask->prepare(); });
        std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
        const auto result = asyncReclaimTask->move();
        VELOX_CHECK(result->succeeded);
        memoryOp->freeAll();
        ++reclaimedCount;
        return true;
      });
  memoryOp->allocate(poolCapacity);
  memoryOp->allocate(poolCapacity);
  ASSERT_EQ(reclaimedCount, 1);
}

// Test different kinds of arbitraton failures.
TEST_F(MockSharedArbitrationTest, arbitrationFailures) {
  // Local arbitration failure with exceeded capacity limit.
  {
    auto task = addTask(64 * MB);
    auto* op = task->addMemoryOp(false);
    op->allocate(32 * MB);
    VELOX_ASSERT_THROW(op->allocate(64 * MB), "Exceeded memory pool capacity");
  }

  // Global arbitration failure.
  {
    auto task1 = addTask(kMemoryCapacity / 2);
    auto* op1 = task1->addMemoryOp(false);
    op1->allocate(kMemoryCapacity / 2);

    auto task2 = addTask(kMemoryCapacity / 2);
    auto* op2 = task2->addMemoryOp(false);
    op2->allocate(kMemoryCapacity / 4);

    auto task3 = addTask(kMemoryCapacity / 2);
    auto* op3 = task3->addMemoryOp(false);
    op3->allocate(kMemoryCapacity / 4);
    VELOX_ASSERT_THROW(op3->allocate(kMemoryCapacity / 4), "aborted");
    try {
      std::rethrow_exception(task3->error());
    } catch (const VeloxRuntimeError& e) {
      ASSERT_EQ(velox::error_code::kMemAborted, e.errorCode());
      ASSERT_TRUE(
          std::string(e.what()).find(
              "Memory pool aborted to reclaim used memory") !=
          std::string::npos)
          << e.what();
    } catch (...) {
      FAIL();
    }
  }
}

TEST_F(MockSharedArbitrationTest, shrinkPools) {
  const int64_t memoryCapacity = 256 << 20;
  const int64_t memoryPoolCapacity = 64 << 20;

  struct TestTask {
    uint64_t capacity{0};
    bool reclaimable{false};
    uint64_t allocateBytes{0};

    uint64_t expectedCapacityAfterShrink;
    uint64_t expectedUsagedAfterShrink;
    bool expectedAbortAfterShrink{false};

    std::string debugString() const {
      return fmt::format(
          "capacity: {}, reclaimable: {}, allocateBytes: {}, expectedCapacityAfterShrink: {}, expectedUsagedAfterShrink: {}, expectedAbortAfterShrink: {}",
          succinctBytes(capacity),
          reclaimable,
          succinctBytes(allocateBytes),
          succinctBytes(expectedCapacityAfterShrink),
          succinctBytes(expectedUsagedAfterShrink),
          expectedAbortAfterShrink);
    }
  };

  struct {
    std::string testName;
    std::vector<TestTask> testTasks;
    uint64_t memoryPoolInitCapacity;
    uint64_t targetBytes;
    uint64_t expectedReclaimedCapacity;
    uint64_t expectedReclaimedUsedBytes;
    bool allowSpill;
    bool allowAbort;

    std::string debugString() const {
      std::stringstream tasksOss;
      for (const auto& testTask : testTasks) {
        tasksOss << "[";
        tasksOss << testTask.debugString();
        tasksOss << "], \n";
      }
      return fmt::format(
          "\ntestName: {}\n testTasks: \n[{}], \ntargetBytes: {}, expectedReclaimedCapacity: {}, expectedReclaimedUsedBytes: {}, "
          "allowSpill: {}, allowAbort: {}",
          testName,
          tasksOss.str(),
          succinctBytes(targetBytes),
          succinctBytes(expectedReclaimedCapacity),
          succinctBytes(expectedReclaimedUsedBytes),
          allowSpill,
          allowAbort);
    }
  } testSettings[] = {
      {"test-0",
       {{memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false}},
       memoryPoolCapacity,
       0,
       0,
       0,
       true,
       false},

      {"test-1",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false}},
       memoryPoolCapacity,
       0,
       memoryCapacity,
       memoryCapacity,
       true,
       false},

      {"test-2",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false}},
       memoryPoolCapacity,
       0,
       memoryCapacity / 2,
       memoryCapacity / 2,
       true,
       false},

      {"test-3",
       {{memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false}},
       memoryPoolCapacity,
       0,
       memoryCapacity / 2,
       memoryCapacity / 2,
       true,
       false},

      {"test-4",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false}},
       memoryPoolCapacity,
       memoryCapacity,
       memoryCapacity,
       memoryCapacity,
       true,
       false},

      {"test-5",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity,
         true,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         false},
        {memoryPoolCapacity,
         true,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         false}},
       memoryPoolCapacity,
       memoryCapacity / 2,
       memoryCapacity / 4 * 3,
       memoryCapacity / 2,
       true,
       false},

      {"test-6",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity,
         true,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         false},
        {memoryPoolCapacity,
         true,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         false}},
       memoryPoolCapacity,
       memoryCapacity / 2,
       memoryCapacity / 4 * 3,
       memoryCapacity / 2,
       true,
       true},

      {"test-7",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false}},
       memoryPoolCapacity,
       0,
       memoryCapacity,
       memoryCapacity,
       true,
       true},

      {"test-8",
       {{memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false}},
       memoryPoolCapacity,
       memoryCapacity / 2,
       memoryCapacity / 2,
       memoryCapacity / 2,
       true,
       true},

      {"test-9",
       {{memoryPoolCapacity,
         false,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        // Global arbitration choose to abort the younger participant with
        // same capacity bucket.
        {memoryPoolCapacity, false, memoryPoolCapacity, 0, 0, true},
        {memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity / 2, 0, 0, false}},
       memoryPoolCapacity,
       memoryCapacity / 2 + memoryPoolCapacity,
       memoryCapacity / 2 + memoryPoolCapacity,
       memoryCapacity / 2 + memoryPoolCapacity,
       true,
       true},

      {"test-10",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         false},
        {memoryPoolCapacity,
         false,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         memoryPoolCapacity / 2,
         false}},
       memoryPoolCapacity,
       memoryCapacity / 2 + memoryPoolCapacity / 2,
       memoryCapacity / 2 + memoryPoolCapacity,
       memoryCapacity / 2,
       true,
       true},

      {"test-11",
       {{memoryPoolCapacity,
         true,
         memoryPoolCapacity,
         memoryPoolCapacity,
         memoryPoolCapacity,
         false},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, true},
        {memoryPoolCapacity, false, memoryPoolCapacity / 2, 0, 0, true},
        // Global arbitration choose to abort the younger participant with
        // same capacity bucket.
        {memoryPoolCapacity, false, memoryPoolCapacity / 2, 0, 0, true}},
       memoryPoolCapacity,
       memoryCapacity / 2 + memoryPoolCapacity / 2,
       memoryCapacity / 2 + memoryPoolCapacity,
       memoryCapacity / 2,
       false,
       true},

      {"test-12",
       {{memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, true},
        {memoryPoolCapacity, true, memoryPoolCapacity, 0, 0, true},
        {memoryPoolCapacity, false, memoryPoolCapacity / 2, 0, 0, true},
        // Global arbitration choose to abort the younger participant with
        // same capacity bucket.
        {memoryPoolCapacity, false, memoryPoolCapacity / 2, 0, 0, true}},
       memoryPoolCapacity,
       0,
       memoryCapacity,
       memoryCapacity - memoryPoolCapacity,
       false,
       true}};

  struct TestTaskContainer {
    std::shared_ptr<MockTask> task;
    MockMemoryOperator* op;
    TestTask testTask;
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
            "Memory pool aborted to reclaim used memory, current capacity");
      };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    // Make simple settings to focus shrink capacity logic testing.
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolInitCapacity = testData.memoryPoolInitCapacity});
    std::vector<TestTaskContainer> taskContainers;
    for (const auto& testTask : testData.testTasks) {
      auto task = addTask(testTask.capacity);
      auto* op = addMemoryOp(task, testTask.reclaimable);
      ASSERT_EQ(op->capacity(), testTask.capacity);
      if (testTask.allocateBytes != 0) {
        op->allocate(testTask.allocateBytes);
      }
      ASSERT_EQ(task->capacity(), testTask.capacity);
      ASSERT_LE(task->usedBytes(), testTask.capacity);
      taskContainers.push_back({task, op, testTask});
    }

    ASSERT_EQ(
        manager_->shrinkPools(
            testData.targetBytes, testData.allowSpill, testData.allowAbort),
        testData.expectedReclaimedCapacity);
    ASSERT_EQ(
        arbitrator_->stats().reclaimedUsedBytes,
        testData.expectedReclaimedUsedBytes);

    for (const auto& taskContainer : taskContainers) {
      checkTaskException(
          taskContainer.task.get(),
          taskContainer.testTask.expectedAbortAfterShrink);
    }

    for (const auto& taskContainer : taskContainers) {
      ASSERT_EQ(
          taskContainer.task->pool()->capacity(),
          taskContainer.testTask.expectedCapacityAfterShrink);
      ASSERT_EQ(
          taskContainer.task->pool()->usedBytes(),
          taskContainer.testTask.expectedUsagedAfterShrink);
    }
  }
}

TEST_F(MockSharedArbitrationTest, shrinkPoolsDelayedAbort) {
  const int64_t memoryCapacity = 256 * MB;
  setupMemory({.memoryCapacity = memoryCapacity});

  // Create first task using half the memory
  auto task1 = addTask(128 * MB);
  auto* op1 = task1->addMemoryOp(false); // non-reclaimable
  op1->setAbortHook([&](MockMemoryOperator* /*unused*/) { return true; });
  auto* buf = op1->allocate(128 * MB);
  op1->free(buf);
  op1->allocate(64 * MB);
  ASSERT_EQ(task1->capacity(), 128 * MB);

  // Create second task using the other half of memory
  auto task2 = addTask(128 * MB);
  auto* op2 = task2->addMemoryOp(false); // non-reclaimable
  op2->setAbortHook([&](MockMemoryOperator* /*unused*/) { return true; });
  buf = op2->allocate(128 * MB);
  op2->free(buf);
  op2->allocate(64 * MB);
  ASSERT_EQ(task2->capacity(), 128 * MB);

  // Now try to shrink pools to reclaim half the memory
  // This should abort one of the tasks (the younger one)
  uint64_t reclaimedBytes = manager_->shrinkPools(192 * MB, false, true);

  // Verify the amount reclaimed matches what we expected
  ASSERT_EQ(reclaimedBytes, 192 * MB);

  // Verify that one task was aborted (should be task2 as it's younger)
  ASSERT_FALSE(task1->pool()->aborted());
  ASSERT_TRUE(task2->pool()->aborted());
  ASSERT_NE(task2->error(), nullptr);

  // Verify the arbitrator stats
  auto stats = arbitrator_->stats();
  ASSERT_EQ(stats.reclaimedUsedBytes, 0);
  ASSERT_EQ(stats.reclaimedFreeBytes, 128 * MB);
  ASSERT_EQ(stats.numAborted, 1);
}

// This test verifies arbitration operations from the same query has to wait for
// serial execution mode.
DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, localArbitrationsFromSameQuery) {
  const int64_t memoryCapacity = 256 << 20;
  setupMemory({.memoryCapacity = memoryCapacity});
  auto runTask = addTask(memoryCapacity);
  auto* runPool = runTask->addMemoryOp(true);
  auto* waitPool = runTask->addMemoryOp(true);

  std::atomic_bool allocationWaitFlag{true};
  folly::EventCount allocationWait;
  std::atomic_bool localArbitrationWaitFlag{true};
  folly::EventCount localArbitrationWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::growCapacity",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            if (!allocationWaitFlag.exchange(false)) {
              // Let the first allocation go through from 'runPool'.
              std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
              return;
            }
            allocationWait.notifyAll();
            localArbitrationWait.await(
                [&]() { return !localArbitrationWaitFlag.load(); });
          })));

  std::atomic_int allocationCount{0};
  auto runThread = std::thread([&]() {
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    runPool->allocate(memoryCapacity / 2);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);
    ++allocationCount;
  });

  auto waitThread = std::thread([&]() {
    allocationWait.await([&]() { return !allocationWaitFlag.load(); });
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    waitPool->allocate(memoryCapacity / 2 + MB);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 1);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].sum, 1);
    ++allocationCount;
  });

  allocationWait.await([&]() { return !allocationWaitFlag.load(); });
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
  ASSERT_EQ(allocationCount, 0);
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  test::ArbitrationParticipantTestHelper participantHelper(
      arbitratorHelper.getParticipant(runTask->pool()->name()).get());
  ASSERT_TRUE(participantHelper.runningOp() != nullptr);
  ASSERT_EQ(participantHelper.waitingOps().size(), 1);

  localArbitrationWaitFlag = false;
  localArbitrationWait.notifyAll();

  runThread.join();
  waitThread.join();
  ASSERT_EQ(allocationCount, 2);
}

// This test verifies arbitration operations from different queris can run in
// parallel.
DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    localArbitrationsFromDifferentQueries) {
  const int64_t memoryCapacity = 512 << 20;
  const uint64_t memoryPoolCapacity = memoryCapacity / 2;
  setupMemory({.memoryCapacity = memoryCapacity});

  auto task1 = addTask(memoryPoolCapacity);
  auto* op1 = task1->addMemoryOp(true);
  op1->allocate(memoryPoolCapacity);
  ASSERT_EQ(task1->capacity(), memoryPoolCapacity);

  auto task2 = addTask(memoryPoolCapacity);
  auto* op2 = task2->addMemoryOp(true);
  op2->allocate(memoryPoolCapacity);
  ASSERT_EQ(task2->capacity(), memoryPoolCapacity);

  ASSERT_EQ(arbitrator_->stats().freeCapacityBytes, 0);

  std::atomic_bool reclaimWaitFlag{true};
  folly::EventCount reclaimWait;
  std::atomic_int reclaimWaitCount{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::ArbitrationParticipant::reclaim",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            ++reclaimWaitCount;
            reclaimWait.await([&]() { return !reclaimWaitFlag.load(); });
          })));

  std::atomic_int allocationCount{0};
  auto taskThread1 = std::thread([&]() {
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    op1->allocate(MB);
    ASSERT_EQ(task1->capacity(), 8 * MB);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 1);
    ++allocationCount;
  });

  auto taskThread2 = std::thread([&]() {
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    op2->allocate(MB);
    ASSERT_EQ(task2->capacity(), 8 * MB);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 1);
    ++allocationCount;
  });

  while (reclaimWaitCount != 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200)); // NOLINT
  }
  ASSERT_EQ(allocationCount, 0);

  reclaimWaitFlag = false;
  reclaimWait.notifyAll();

  taskThread1.join();
  taskThread2.join();
  ASSERT_EQ(allocationCount, 2);
}

// This test verifies the global arbitration can switch to reclaim the other
// query or abort when one query claims to be reclaimable but can't actually
// reclaim.
TEST_F(MockSharedArbitrationTest, badNonReclaimableQuery) {
  const int64_t memoryCapacity = 256 << 20;
  const ReclaimInjectionCallback badReclaimInjectCallback =
      [&](MemoryPool* pool, uint64_t /*unsed*/) -> bool { return false; };

  struct TestTask {
    bool reclaimable;
    bool badQuery;
    uint64_t allocateBytes{0};

    uint64_t expectedCapacityAfterArbitration;
    uint64_t expectedUsagedAfterArbitration;
    bool expectedAbortAfterArbitration;

    std::string debugString() const {
      return fmt::format(
          "reclaimable: {}, badQuery: {}, allocateBytes: {}, expectedCapacityAfterArbitration: {}, expectedUsagedAfterArbitration: {}, expectedAbortAfterArbitration: {}",
          reclaimable,
          badQuery,
          succinctBytes(allocateBytes),
          succinctBytes(expectedCapacityAfterArbitration),
          succinctBytes(expectedUsagedAfterArbitration),
          expectedAbortAfterArbitration);
    }
  };

  struct TestTaskContainer {
    std::shared_ptr<MockTask> task;
    MockMemoryOperator* op;
    TestTask testTask;
  };

  struct {
    std::vector<TestTask> testTasks;

    std::string debugString() const {
      std::stringstream tasksOss;
      for (const auto& testTask : testTasks) {
        tasksOss << "[";
        tasksOss << testTask.debugString();
        tasksOss << "], \n";
      }
      return fmt::format("testTasks: \n{}", tasksOss.str());
    }
  } testSettings[] = {
      {{{true,
         true,
         memoryCapacity / 2,
         memoryCapacity / 2,
         memoryCapacity / 2,
         false},
        {true,
         false,
         memoryCapacity / 4,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {true, false, memoryCapacity / 4, 0, 0, false}}},

      {{{true,
         true,
         memoryCapacity / 2,
         memoryCapacity / 2,
         memoryCapacity / 2,
         false},
        {true,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {true,
         true,
         memoryCapacity / 4 - memoryCapacity / 8,
         memoryCapacity / 4 - memoryCapacity / 8,
         memoryCapacity / 4 - memoryCapacity / 8,
         false},
        {true, false, memoryCapacity / 8, 0, 0, false}}},

      {{
          {true,
           true,
           memoryCapacity / 2,
           memoryCapacity / 2,
           memoryCapacity / 2,
           false},
          {false,
           true,
           memoryCapacity / 4,
           memoryCapacity / 4,
           memoryCapacity / 4,
           false},
          // The newest participant is chosen to abort.
          {false, true, memoryCapacity / 4, 0, 0, true},
      }},

      {{
          {false,
           true,
           memoryCapacity / 4,
           memoryCapacity / 4,
           memoryCapacity / 4,
           false},
          {false,
           true,
           memoryCapacity / 4,
           memoryCapacity / 4,
           memoryCapacity / 4,
           false},
          // The newest participant is chosen to abort.
          {true, true, memoryCapacity / 2, 0, 0, true},
      }},

      {{
          {true,
           true,
           memoryCapacity / 2,
           memoryCapacity / 2,
           memoryCapacity / 2,
           false},
          {true,
           true,
           memoryCapacity / 4,
           memoryCapacity / 4,
           memoryCapacity / 4,
           false},
          // The newest participant is chosen to abort.
          {true, true, memoryCapacity / 4, 0, 0, true},
      }},
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    // Make simple settings to focus shrink capacity logic testing.
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolAbortCapacityLimit = memoryCapacity / 8});
    std::vector<TestTaskContainer> taskContainers;
    for (const auto& testTask : testData.testTasks) {
      auto task = addTask(memoryCapacity);
      auto* op = addMemoryOp(
          task,
          testTask.reclaimable,
          testTask.badQuery ? badReclaimInjectCallback : nullptr);
      ASSERT_EQ(op->capacity(), 0);
      if (testTask.allocateBytes != 0) {
        op->allocate(testTask.allocateBytes);
      }
      ASSERT_EQ(task->capacity(), testTask.allocateBytes);
      ASSERT_LE(task->usedBytes(), testTask.allocateBytes);
      taskContainers.push_back({task, op, testTask});
    }
    auto arbitrationTriggerTask = addTask(memoryCapacity);
    auto* arbitrationTriggerOp = addMemoryOp(arbitrationTriggerTask, false);
    ASSERT_EQ(arbitrationTriggerTask->capacity(), 0);
    arbitrationTriggerOp->allocate(MB);
    ASSERT_EQ(arbitrationTriggerTask->capacity(), MB);
    ASSERT_EQ(arbitrationTriggerTask->usedBytes(), MB);

    for (const auto& taskContainer : taskContainers) {
      ASSERT_EQ(
          taskContainer.task->pool()->capacity(),
          taskContainer.testTask.expectedCapacityAfterArbitration);
      ASSERT_EQ(
          taskContainer.task->pool()->usedBytes(),
          taskContainer.testTask.expectedUsagedAfterArbitration);
      ASSERT_EQ(
          taskContainer.task->pool()->aborted(),
          taskContainer.testTask.expectedAbortAfterArbitration);
    }
  }
} // namespace facebook::velox::memory

// This test verifies memory pool can allocate reserve memory during global
// arbitration.
DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    allocationFromFreeReservedMemoryDuringGlobalArbitration) {
  const int64_t memoryCapacity = 256 << 20;
  const uint64_t memoryPoolCapacity = 64 << 20;
  const uint64_t memoryPoolReservedCapacity = 8 << 20;
  const uint64_t reservedMemoryCapacity = 64 << 20;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .reservedMemoryCapacity = reservedMemoryCapacity,
       .memoryPoolReserveCapacity = memoryPoolReservedCapacity});

  auto globalArbitrationTriggerThread = std::thread([&]() {
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());

    std::vector<std::shared_ptr<MockTask>> tasks;
    std::vector<MockMemoryOperator*> ops;
    ops.reserve(4);
    tasks.reserve(4);
    for (int i = 0; i < 4; ++i) {
      tasks.push_back(addTask(memoryPoolCapacity));
      ops.push_back(tasks.back()->addMemoryOp(true));
    }
    for (int i = 0; i < 4; ++i) {
      ops[i]->allocate(memoryPoolCapacity);
    }
    // We expect global arbitration has been triggered.
    ASSERT_GE(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 0);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].sum, 0);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitWallNanos].count,
        0);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitWallNanos].sum,
        1'000'000'000);
  });

  std::atomic_bool globalArbitrationStarted{false};
  folly::EventCount globalArbitrationStartWait;
  std::atomic_bool globalArbitrationWaitFlag{true};
  folly::EventCount globalArbitrationWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            if (globalArbitrationStarted.exchange(true)) {
              return;
            }
            globalArbitrationStartWait.notifyAll();

            globalArbitrationWait.await(
                [&]() { return !globalArbitrationWaitFlag.load(); });
          })));

  globalArbitrationStartWait.await(
      [&]() { return globalArbitrationStarted.load(); });

  auto nonBlockingTask = addTask(memoryPoolCapacity);
  auto* nonBlockingOp = nonBlockingTask->addMemoryOp(true);
  nonBlockingOp->allocate(memoryPoolReservedCapacity);
  // Inject some delay for global arbitration.
  std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
  globalArbitrationWaitFlag = false;
  globalArbitrationWait.notifyAll();

  globalArbitrationTriggerThread.join();
  ASSERT_EQ(nonBlockingTask->capacity(), memoryPoolReservedCapacity);
}

DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    localArbitrationRunInParallelWithGlobalArbitration) {
  const int64_t memoryCapacity = 256 << 20;
  const uint64_t reservedMemoryCapacity = 64 << 20;
  const uint64_t memoryPoolCapacity = 64 << 20;
  const uint64_t memoryPoolReservedCapacity = 8 << 20;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .reservedMemoryCapacity = reservedMemoryCapacity,
       .memoryPoolReserveCapacity = memoryPoolReservedCapacity});

  auto localArbitrationTask = addTask(memoryPoolCapacity);
  auto* localArbitrationOp = localArbitrationTask->addMemoryOp(true);
  localArbitrationOp->allocate(memoryPoolCapacity);

  auto globalArbitrationTriggerThread = std::thread([&]() {
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());

    std::vector<std::shared_ptr<MockTask>> tasks;
    std::vector<MockMemoryOperator*> ops;
    ops.reserve(3);
    tasks.reserve(3);
    for (int i = 0; i < 3; ++i) {
      tasks.push_back(addTask(memoryPoolCapacity));
      ops.push_back(tasks.back()->addMemoryOp(true));
    }
    for (int i = 0; i < 3; ++i) {
      ops[i]->allocate(memoryPoolCapacity);
    }
    // We expect global arbitration has been triggered.
    ASSERT_GE(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_GE(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 1);
    ASSERT_GE(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 1);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].sum, 0);
    ASSERT_GE(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitWallNanos].count,
        1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitWallNanos].sum, 1);
  });

  std::atomic_bool globalArbitrationStarted{false};
  folly::EventCount globalArbitrationStartWait;
  std::atomic_bool globalArbitrationWaitFlag{true};
  folly::EventCount globalArbitrationWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            if (globalArbitrationStarted.exchange(true)) {
              return;
            }
            globalArbitrationStartWait.notifyAll();

            globalArbitrationWait.await(
                [&]() { return !globalArbitrationWaitFlag.load(); });
          })));

  globalArbitrationStartWait.await(
      [&]() { return globalArbitrationStarted.load(); });

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;
  auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
  setThreadLocalRunTimeStatWriter(statsWriter.get());

  localArbitrationOp->allocate(memoryPoolReservedCapacity);
  // Inject some delay for global arbitration.
  std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
  globalArbitrationWaitFlag = false;
  globalArbitrationWait.notifyAll();

  globalArbitrationTriggerThread.join();
  ASSERT_EQ(localArbitrationOp->capacity(), memoryPoolReservedCapacity);
  ASSERT_EQ(
      runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 0);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 0);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 1);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].sum, 1);

  // Global arbitration thread may still be running in the background,
  // triggerring ASAN failure. Wait until it exits.
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  arbitratorHelper.waitForGlobalArbitrationToFinish();
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, globalArbitrationAbortTimeRatio) {
  const int64_t memoryCapacity = 512 << 20;
  const uint64_t memoryPoolInitCapacity = memoryCapacity / 2;
  const uint64_t maxArbitrationTimeNs = 2'000'000'000UL;
  const double globalArbitrationAbortTimeRatio = 0.5;
  const uint64_t abortTimeThresholdNs =
      maxArbitrationTimeNs * globalArbitrationAbortTimeRatio;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryPoolInitCapacity = memoryPoolInitCapacity,
       .memoryReclaimThreadsHwMultiplier = kMemoryReclaimThreadsHwMultiplier,
       .arbitrationTimeoutNs = maxArbitrationTimeNs,
       .globalArbitrationAbortTimeRatio = globalArbitrationAbortTimeRatio});

  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);

  for (auto pauseTimeNs :
       {abortTimeThresholdNs / 2,
        (maxArbitrationTimeNs + abortTimeThresholdNs) / 2}) {
    auto task1 = addTask(memoryCapacity);
    auto* op1 = task1->addMemoryOp(false);
    op1->allocate(memoryCapacity / 2);

    auto task2 = addTask(memoryCapacity / 2);
    auto* op2 = task2->addMemoryOp(false);
    op2->allocate(memoryCapacity / 2);

    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
        std::function<void(const SharedArbitrator*)>(
            ([&](const SharedArbitrator* /*unused*/) {
              std::this_thread::sleep_for(
                  std::chrono::nanoseconds(pauseTimeNs));
            })));

    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    const auto prevGlobalArbitrationRuns =
        arbitratorHelper.globalArbitrationRuns();
    op1->allocate(memoryCapacity / 2);

    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 1);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 1);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);
    ASSERT_TRUE(task1->error() == nullptr);
    ASSERT_EQ(task1->capacity(), memoryCapacity);
    ASSERT_TRUE(task2->error() != nullptr);
    VELOX_ASSERT_THROW(
        std::rethrow_exception(task2->error()),
        "Memory pool aborted to reclaim used memory");

    const auto deltaGlobalArbitrationRuns =
        arbitratorHelper.globalArbitrationRuns() - prevGlobalArbitrationRuns;
    if (pauseTimeNs < abortTimeThresholdNs) {
      ASSERT_GT(deltaGlobalArbitrationRuns, 2);
    } else {
      // In SharedArbitrator::runGlobalArbitration()
      // First loop attempting spill, global run update.
      // Second loop abort, resume waiter. Global run update and the assert
      // below is a race condition, hence ASSERT_LE
      ASSERT_LE(deltaGlobalArbitrationRuns, 2);
    }
  }
}

TEST_F(MockSharedArbitrationTest, globalArbitrationWithoutSpill) {
  const int64_t memoryCapacity = 512 << 20;
  const uint64_t memoryPoolInitCapacity = memoryCapacity / 2;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryPoolInitCapacity = memoryPoolInitCapacity,
       .memoryReclaimThreadsHwMultiplier = kMemoryReclaimThreadsHwMultiplier,
       .arbitrationTimeoutNs = 5 * 60 * 1'000'000'000UL,
       .globalArbitrationWithoutSpill = true});
  auto triggerTask = addTask(memoryCapacity);
  auto* triggerOp = triggerTask->addMemoryOp(false);
  triggerOp->allocate(memoryCapacity / 2);

  auto abortTask = addTask(memoryCapacity / 2);
  auto* abortOp = abortTask->addMemoryOp(true);
  abortOp->allocate(memoryCapacity / 2);
  ASSERT_EQ(triggerTask->capacity(), memoryCapacity / 2);

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;
  auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
  setThreadLocalRunTimeStatWriter(statsWriter.get());
  triggerOp->allocate(memoryCapacity / 2);

  ASSERT_EQ(
      runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
  ASSERT_GT(runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
  ASSERT_EQ(
      runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 1);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 1);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);

  ASSERT_TRUE(triggerTask->error() == nullptr);
  ASSERT_EQ(triggerTask->capacity(), memoryCapacity);
  ASSERT_TRUE(abortTask->error() != nullptr);
  VELOX_ASSERT_THROW(
      std::rethrow_exception(abortTask->error()),
      "Memory pool aborted to reclaim used memory");
}

TEST_F(MockSharedArbitrationTest, globalArbitrationSmallParticipantLargeGrow) {
  // This test tests global arbitration takes into consideration the
  // additional attempting grow capacity when selecting abort partitipants.
  const int64_t kMemoryCapacity = 512 << 20;
  const uint64_t kMemoryPoolInitCapacity = kMemoryCapacity / 2;
  setupMemory(
      {.memoryCapacity = kMemoryCapacity,
       .memoryPoolInitCapacity = kMemoryPoolInitCapacity,
       // Set abort capacity limit to differenciate capacity.
       .memoryPoolAbortCapacityLimit = kMemoryCapacity,
       .globalArbitrationWithoutSpill = true});

  auto task0 = addTask(kMemoryCapacity);
  auto* op0 = task0->addMemoryOp(false);
  op0->allocate(kMemoryCapacity / 2);

  // task1 has 256MB in second abort capacity limit bucket.
  auto task1 = addTask(kMemoryCapacity / 2);
  auto* op1 = task1->addMemoryOp(true);
  op1->allocate(kMemoryCapacity / 2);
  ASSERT_EQ(task0->capacity(), kMemoryCapacity / 2);

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;
  auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
  setThreadLocalRunTimeStatWriter(statsWriter.get());

  // task0 has 256MB + 256MB (attempt) = 512MB in top abort capacity limit
  // bucket, which shall be evaluated first, and hence killed by global
  // arbitration.
  VELOX_ASSERT_THROW(op0->allocate(kMemoryCapacity / 2), "aborted");

  ASSERT_EQ(
      runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
  ASSERT_GT(runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
  ASSERT_EQ(
      runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 1);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 1);
  ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);

  ASSERT_TRUE(task1->error() == nullptr);
  ASSERT_EQ(task1->capacity(), kMemoryCapacity / 2);
  ASSERT_TRUE(task0->error() != nullptr);
  VELOX_ASSERT_THROW(
      std::rethrow_exception(task0->error()),
      "Memory pool aborted to reclaim used memory");
}

TEST_F(MockSharedArbitrationTest, globalArbitrationBySpillWithPriority) {
  const int64_t memoryCapacity = 512 << 20;

  struct TaskData {
    uint64_t capacity;
    int32_t priority;
    bool expectSpill;
    std::string debugString() const {
      return fmt::format(
          "capacity {}, priority {}, expectSpill {}",
          succinctBytes(capacity),
          priority,
          expectSpill);
    }
  };

  struct TestData {
    std::string testName;
    uint64_t shrinkBytes;
    uint64_t spillCapacityLimit;
    uint64_t spillCapacityLowerBound;
    std::vector<TaskData> tasks;
    std::string debugString() const {
      std::stringstream ss;
      for (const auto& task : tasks) {
        ss << task.debugString() << ", ";
      }
      return fmt::format(
          "testName {}, shrinkBytes {}, tasks [{}]",
          testName,
          shrinkBytes,
          ss.str());
    }
  };

  std::vector<TestData> testSettings = {
      {"test-0", 64 << 20, 512 << 20, 0, {}},

      {"test-1",
       64 << 20,
       512 << 20,
       0,
       {{256 << 20, 0, true}, {192 << 20, 1, false}, {64 << 20, 2, false}}},

      {"test-2",
       64 << 20,
       512 << 20,
       0,
       {{192 << 20, 2, true}, {192 << 20, 1, false}, {128 << 20, 0, false}}},

      {"test-3",
       256 << 20,
       512 << 20,
       0,
       {{144 << 20, 1, true},
        {136 << 20, 1, true},
        {128 << 20, 1, false},
        {104 << 20, 0, false}}},

      {"test-4",
       256 << 20,
       512 << 20,
       128 << 20,
       {{144 << 20, 1, true},
        {120 << 20, 1, false},
        {120 << 20, 1, false},
        {104 << 20, 0, false}}},

      {"test-5",
       230 << 20,
       512 << 20,
       0,
       {
           {48 << 20, 3, true},
           {44 << 20, 3, true},
           {40 << 20, 3, true},
           {52 << 20, 2, true},
           {48 << 20, 2, true},
           {44 << 20, 2, false},
           {56 << 20, 1, false},
           {52 << 20, 1, false},
           {48 << 20, 1, false},
           {28 << 20, 4, false},
           {28 << 20, 4, false},
           {24 << 20, 4, false},
       }},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolMinReclaimBytes = testData.spillCapacityLowerBound,
         .memoryPoolSpillCapacityLimit = testData.spillCapacityLimit,
         .globalArbitrationWithoutSpill = true});

    std::vector<std::shared_ptr<MockTask>> tasks;
    for (const auto& taskData : testData.tasks) {
      tasks.push_back(addTask(taskData.capacity, taskData.priority));
      auto* op = tasks.back()->addMemoryOp(true);
      op->allocate(taskData.capacity);
    }

    arbitrator_->shrinkCapacity(testData.shrinkBytes, true, false);
    for (auto i = 0; i < tasks.size(); ++i) {
      ASSERT_EQ(
          tasks[i]->capacity() < testData.tasks[i].capacity,
          testData.tasks[i].expectSpill);
    }
  }
}

TEST_F(MockSharedArbitrationTest, globalArbitrationByAbortWithPriority) {
  // This test tests global arbitration takes into consideration query
  // priority attempting to grow capacity when selecting abort partitipants.
  const int64_t memoryCapacity = 512 << 20;
  const uint64_t memoryPoolInitCapacity = memoryCapacity / 2;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryPoolInitCapacity = memoryPoolInitCapacity,
       // Set abort capacity limit to differenciate capacity.
       .memoryPoolAbortCapacityLimit = memoryCapacity,
       .globalArbitrationWithoutSpill = true});

  auto task0 = addTask(384 << 20, 1);
  auto* op0 = task0->addMemoryOp(false);
  op0->allocate(384 << 20);

  auto task1 = addTask(64 << 20, 1);
  auto* op1 = task1->addMemoryOp(false);
  op1->allocate(64 << 20);

  auto task2 = addTask(64 << 20, 2);
  auto* op2 = task2->addMemoryOp(false);
  op2->allocate(64 << 20);

  // At this point, memory pool is full
  ASSERT_EQ(manager_->capacity(), manager_->getTotalBytes());

  arbitrator_->shrinkCapacity(64 << 20, false, true);
  ASSERT_TRUE(task0->error() == nullptr);
  ASSERT_TRUE(task1->error() == nullptr);
  VELOX_ASSERT_THROW(
      std::rethrow_exception(task2->error()),
      "Memory pool aborted to reclaim used memory");

  arbitrator_->shrinkCapacity(64 << 20, false, true);
  VELOX_ASSERT_THROW(
      std::rethrow_exception(task0->error()),
      "Memory pool aborted to reclaim used memory");
  ASSERT_TRUE(task1->error() == nullptr);

  arbitrator_->shrinkCapacity(64 << 20, false, true);
  VELOX_ASSERT_THROW(
      std::rethrow_exception(task1->error()),
      "Memory pool aborted to reclaim used memory");
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, multipleGlobalRuns) {
  const int64_t memoryCapacity = 512 << 20;
  const uint64_t memoryPoolInitCapacity = memoryCapacity / 2;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryPoolInitCapacity = memoryPoolInitCapacity});
  auto runTask = addTask(memoryCapacity);
  auto* runPool = runTask->addMemoryOp(true);
  runPool->allocate(memoryCapacity / 2);
  auto waitTask = addTask(memoryCapacity);
  auto* waitPool = waitTask->addMemoryOp(true);
  waitPool->allocate(memoryCapacity / 2);

  std::atomic_bool allocationWaitFlag{true};
  folly::EventCount allocationWait;

  std::atomic_bool globalArbitrationWaitFlag{true};
  folly::EventCount globalArbitrationWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            if (!allocationWaitFlag.exchange(false)) {
              return;
            }
            allocationWait.notifyAll();
            globalArbitrationWait.await(
                [&]() { return !globalArbitrationWaitFlag.load(); });
          })));

  std::atomic_int allocations{0};
  auto waitThread = std::thread([&]() {
    allocationWait.await([&]() { return !allocationWaitFlag.load(); });
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    waitPool->allocate(memoryCapacity / 2);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 1);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 1);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);
    ++allocations;
  });

  auto runThread = std::thread([&]() {
    std::unordered_map<std::string, RuntimeMetric> runtimeStats;
    auto statsWriter = std::make_unique<TestRuntimeStatWriter>(runtimeStats);
    setThreadLocalRunTimeStatWriter(statsWriter.get());
    runPool->allocate(memoryCapacity / 2);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].count, 1);
    ASSERT_GT(
        runtimeStats[SharedArbitrator::kMemoryArbitrationWallNanos].sum, 0);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].count, 1);
    ASSERT_EQ(
        runtimeStats[SharedArbitrator::kGlobalArbitrationWaitCount].sum, 1);
    ASSERT_EQ(runtimeStats[SharedArbitrator::kLocalArbitrationCount].count, 0);
    ++allocations;
  });

  allocationWait.await([&]() { return !allocationWaitFlag.load(); });
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
  ASSERT_EQ(allocations, 0);
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  ASSERT_EQ(arbitratorHelper.numGlobalArbitrationWaiters(), 2);
  ASSERT_EQ(arbitrator_->stats().numRunning, 2);

  globalArbitrationWaitFlag = false;
  globalArbitrationWait.notifyAll();

  runThread.join();
  waitThread.join();
  ASSERT_EQ(allocations, 2);
  ASSERT_EQ(runTask->capacity(), memoryCapacity / 2);
  ASSERT_EQ(waitTask->capacity(), memoryCapacity / 2);
}

TEST_F(MockSharedArbitrationTest, globalArbitrationEnableCheck) {
  for (bool globalArbitrationEnabled : {false, true}) {
    SCOPED_TRACE(
        fmt::format("globalArbitrationEnabled: {}", globalArbitrationEnabled));
    const int64_t memoryCapacity = 512 << 20;
    const uint64_t memoryPoolInitCapacity = memoryCapacity / 2;
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolInitCapacity = memoryPoolInitCapacity,
         .fastExponentialGrowthCapacityLimit =
             kFastExponentialGrowthCapacityLimit,
         .slowCapacityGrowPct = kSlowCapacityGrowPct,
         .memoryPoolMinFreeCapacity = kMemoryPoolMinFreeCapacity,
         .memoryPoolMinFreeCapacityPct = kMemoryPoolMinFreeCapacityPct,
         .globalArbitrationReclaimPct = kGlobalArbitrationReclaimPct,
         .globalArtbitrationEnabled = globalArbitrationEnabled});

    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    ASSERT_EQ(
        arbitratorHelper.globalArbitrationController() != nullptr,
        globalArbitrationEnabled);
    ASSERT_TRUE(arbitratorHelper.memoryReclaimExecutor() != nullptr);

    auto reclaimedTask = addTask(memoryCapacity);
    auto* reclaimedPool = reclaimedTask->addMemoryOp(true);
    reclaimedPool->allocate(memoryCapacity / 2);
    auto requestTask = addTask(memoryCapacity);
    auto* requestPool = requestTask->addMemoryOp(false);
    requestPool->allocate(memoryCapacity / 2);
    if (globalArbitrationEnabled) {
      requestPool->allocate(memoryCapacity / 2);
    } else {
      VELOX_ASSERT_THROW(
          requestPool->allocate(memoryCapacity / 2),
          "Local arbitration failure.");
    }
  }
}

TEST_F(MockSharedArbitrationTest, singlePoolShrinkWithoutArbitration) {
  const int64_t memoryCapacity = 512 * MB;
  struct TestParam {
    uint64_t memoryPoolReservedBytes;
    uint64_t memoryPoolMinFreeCapacity;
    double memoryPoolMinFreeCapacityPct;
    uint64_t requestBytes;
    bool expectThrow;
    uint64_t expectedCapacity;
    std::string debugString() const {
      return fmt::format(
          "memoryPoolReservedBytes {}, "
          "memoryPoolMinFreeCapacity {}, "
          "memoryPoolMinFreeCapacityPct {}, "
          "requestBytes {}, expectThrow {}, expectedCapacity, {}",
          succinctBytes(memoryPoolReservedBytes),
          succinctBytes(memoryPoolMinFreeCapacity),
          memoryPoolMinFreeCapacityPct,
          succinctBytes(requestBytes),
          expectThrow,
          succinctBytes(expectedCapacity));
    }
  } testParams[] = {
      {0, 128 * MB, 0, 256 * MB, true, 0},
      {0, 0, 0.1, 256 * MB, true, 0},
      {256 * MB, 128 * MB, 0.5, 256 * MB, false, 256 * MB},
      {256 * MB, 128 * MB, 0.125, 256 * MB, false, 256 * MB},
      {0, 128 * MB, 0.25, 0 * MB, false, 0},
      {256 * MB, 128 * MB, 0.125, 0 * MB, false, 256 * MB},
      {256 * MB, 128 * MB, 0.125, 512 * MB, false, 256 * MB}};

  for (const auto& testParam : testParams) {
    SCOPED_TRACE(testParam.debugString());
    if (testParam.expectThrow) {
      VELOX_ASSERT_THROW(
          setupMemory(
              {memoryCapacity,
               0,
               memoryCapacity,
               0,
               0,
               0,
               testParam.memoryPoolMinFreeCapacity,
               testParam.memoryPoolMinFreeCapacityPct}),
          "both need to be set (non-zero) at the same time to enable shrink "
          "capacity adjustment.");
      continue;
    } else {
      setupMemory(
          {.memoryCapacity = memoryCapacity,
           .memoryPoolInitCapacity = memoryCapacity,
           .memoryPoolMinFreeCapacity = testParam.memoryPoolMinFreeCapacity,
           .memoryPoolMinFreeCapacityPct =
               testParam.memoryPoolMinFreeCapacityPct});
    }

    auto task = addTask();
    auto* memOp = task->addMemoryOp();
    memOp->allocate(testParam.memoryPoolReservedBytes);

    ASSERT_EQ(task->pool()->reservedBytes(), testParam.memoryPoolReservedBytes);
    arbitrator_->shrinkCapacity(task->pool(), testParam.requestBytes);
    ASSERT_EQ(task->capacity(), testParam.expectedCapacity);
    clearTasks();
  }
}

TEST_F(MockSharedArbitrationTest, singlePoolGrowWithoutArbitration) {
  const int64_t memoryCapacity = 512 << 20;
  const uint64_t memoryPoolInitCapacity = 32 << 20;
  struct TestParam {
    uint64_t fastExponentialGrowthCapacityLimit;
    double slowCapacityGrowPct;
    std::string debugString() const {
      return fmt::format(
          "fastExponentialGrowthCapacityLimit {}, "
          "slowCapacityGrowPct {}",
          succinctBytes(fastExponentialGrowthCapacityLimit),
          slowCapacityGrowPct);
    }
  };

  // Try to make each test allocation larger than the largest memory pool
  // quantization(8MB) to not have noise.
  std::vector<TestParam> testParams{
      {128 << 20, 0.1},
      {128 << 20, 0.1},
      {128 << 20, 0.5},
  };

  for (const auto& testParam : testParams) {
    SCOPED_TRACE(testParam.debugString());
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolInitCapacity = memoryPoolInitCapacity,
         .fastExponentialGrowthCapacityLimit =
             testParam.fastExponentialGrowthCapacityLimit,
         .slowCapacityGrowPct = testParam.slowCapacityGrowPct});

    auto* memOp = addMemoryOp();
    const int allocateSize = 1 * MB;
    while (memOp->capacity() < memoryCapacity) {
      memOp->allocate(allocateSize);
    }

    // Computations of expected number of requests depending on capacity grow
    // strategy (fast path or not).
    uint64_t expectedNumRequests{0};

    uint64_t simulateCapacity = memoryPoolInitCapacity;
    while (simulateCapacity * 2 <=
           testParam.fastExponentialGrowthCapacityLimit) {
      simulateCapacity += simulateCapacity;
      expectedNumRequests++;
    }
    while (simulateCapacity < memoryCapacity) {
      auto growth = static_cast<uint64_t>(
          simulateCapacity * testParam.slowCapacityGrowPct);
      simulateCapacity += growth;
      expectedNumRequests++;
    }

    verifyArbitratorStats(
        arbitrator_->stats(), memoryCapacity, 0, 0, expectedNumRequests);

    verifyReclaimerStats(memOp->reclaimer()->stats(), 0, expectedNumRequests);

    clearTasks();
    verifyArbitratorStats(
        arbitrator_->stats(),
        memoryCapacity,
        memoryCapacity,
        0,
        expectedNumRequests);
  }
}

TEST_F(MockSharedArbitrationTest, maxCapacityReserve) {
  struct {
    int64_t memCapacity;
    int64_t reservedCapacity;
    uint64_t poolInitCapacity;
    uint64_t poolReservedCapacity;
    uint64_t poolMaxCapacity;
    uint64_t expectedPoolInitCapacity;
    bool expectedError;

    std::string debugString() const {
      return fmt::format(
          "memCapacity {}, reservedCapacity {}, poolInitCapacity {}, poolReservedCapacity {}, poolMaxCapacity {}, expectedPoolInitCapacity {}, expectedError {}",
          succinctBytes(memCapacity),
          succinctBytes(reservedCapacity),
          succinctBytes(poolInitCapacity),
          succinctBytes(poolReservedCapacity),
          succinctBytes(poolMaxCapacity),
          succinctBytes(expectedPoolInitCapacity),
          expectedError);
    }
  } testSettings[] = {
      {256 << 20, 256 << 20, 128 << 20, 64 << 20, 256 << 20, 64 << 20, false},
      {256 << 20, 0, 128 << 20, 64 << 20, 256 << 20, 128 << 20, false},
      {256 << 20, 0, 512 << 20, 64 << 20, 256 << 20, 256 << 20, false},
      {256 << 20, 0, 128 << 20, 64 << 20, 256 << 20, 128 << 20, false},
      {256 << 20, 128 << 20, 128 << 20, 64 << 20, 256 << 20, 128 << 20, false},
      {256 << 20, 128 << 20, 256 << 20, 64 << 20, 256 << 20, 128 << 20, false},
      {256 << 20, 128 << 20, 256 << 20, 256 << 20, 256 << 20, 256 << 20, false},
      {256 << 20, 128 << 20, 256 << 20, 256 << 20, 128 << 20, 128 << 20, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(
        {.memoryCapacity = testData.memCapacity,
         .reservedMemoryCapacity = testData.reservedCapacity,
         .memoryPoolInitCapacity = testData.poolInitCapacity,
         .memoryPoolReserveCapacity = testData.poolReservedCapacity});
    if (testData.expectedError) {
      VELOX_ASSERT_THROW(addTask(testData.poolMaxCapacity), "");
      continue;
    }

    auto task = addTask(testData.poolMaxCapacity);
    ASSERT_EQ(task->pool()->maxCapacity(), testData.poolMaxCapacity);
    ASSERT_EQ(task->pool()->capacity(), testData.expectedPoolInitCapacity);
  }
}

TEST_F(MockSharedArbitrationTest, ensureMemoryPoolMaxCapacity) {
  const int memCapacity = 256 * MB;
  const int poolInitCapacity = 8 * MB;
  struct {
    uint64_t poolMaxCapacity;
    bool isReclaimable;
    uint64_t allocatedBytes;
    uint64_t requestBytes;
    bool hasOtherTask;
    uint64_t otherAllocatedBytes;
    bool expectedSuccess;
    bool expectedReclaimFromOther;

    std::string debugString() const {
      return fmt::format(
          "poolMaxCapacity {} isReclaimable {} allocatedBytes {} requestBytes {} hasOtherTask {} otherAllocatedBytes {} expectedSuccess {} expectedReclaimFromOther {}",
          succinctBytes(poolMaxCapacity),
          isReclaimable,
          succinctBytes(allocatedBytes),
          succinctBytes(requestBytes),
          hasOtherTask,
          succinctBytes(otherAllocatedBytes),
          expectedSuccess,
          expectedReclaimFromOther);
    }
  } testSettings[] = {
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       false,
       0,
       true,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 8,
       false,
       0,
       true,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       false,
       0,
       true,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 2,
       memCapacity / 4,
       false,
       0,
       true,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 4,
       memCapacity / 2,
       false,
       0,
       false,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 2,
       memCapacity / 4,
       false,
       0,
       false,
       false},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       true,
       memCapacity - memCapacity / 4,
       true,
       true},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 8,
       true,
       memCapacity - memCapacity / 4,
       true,
       true},
      {memCapacity / 2,
       true,
       memCapacity / 4,
       memCapacity / 2,
       true,
       memCapacity - memCapacity / 4,
       true,
       true},
      {memCapacity / 2,
       true,
       memCapacity / 2,
       memCapacity / 4,
       true,
       memCapacity - memCapacity / 2,
       true,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 4,
       memCapacity / 2,
       true,
       memCapacity - memCapacity / 4,
       false,
       false},
      {memCapacity / 2,
       false,
       memCapacity / 2,
       memCapacity / 4,
       false,
       memCapacity - memCapacity / 2,
       false,
       false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(
        {.memoryCapacity = memCapacity,
         .memoryPoolInitCapacity = poolInitCapacity,
         .fastExponentialGrowthCapacityLimit =
             kFastExponentialGrowthCapacityLimit,
         .slowCapacityGrowPct = kSlowCapacityGrowPct});

    auto requestor = addTask(testData.poolMaxCapacity);
    auto* requestorOp = addMemoryOp(requestor, testData.isReclaimable);
    requestorOp->allocate(testData.allocatedBytes);
    std::shared_ptr<MockTask> other;
    MockMemoryOperator* otherOp;
    if (testData.hasOtherTask) {
      other = addTask();
      otherOp = addMemoryOp(other, true);
      otherOp->allocate(testData.otherAllocatedBytes);
    }
    const auto numRequests = arbitrator_->stats().numRequests;
    if (testData.expectedSuccess) {
      requestorOp->allocate(testData.requestBytes);
    } else {
      VELOX_ASSERT_THROW(
          requestorOp->allocate(testData.requestBytes),
          "Exceeded memory pool capacity");
    }
    if (testData.expectedReclaimFromOther) {
      ASSERT_GT(otherOp->reclaimer()->stats().numReclaims, 0);
    } else if (testData.hasOtherTask) {
      ASSERT_EQ(otherOp->reclaimer()->stats().numReclaims, 0);
    }
    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    arbitratorHelper.waitForGlobalArbitrationToFinish();
    if (testData.expectedSuccess &&
        (((testData.allocatedBytes + testData.requestBytes) >
          testData.poolMaxCapacity) ||
         testData.hasOtherTask)) {
      ASSERT_GT(arbitrator_->stats().reclaimedUsedBytes, 0);
    } else {
      ASSERT_EQ(arbitrator_->stats().reclaimedUsedBytes, 0);
    }
    ASSERT_EQ(arbitrator_->stats().numRequests, numRequests + 1);
  }
}

TEST_F(MockSharedArbitrationTest, ensureNodeMaxCapacity) {
  struct {
    int64_t nodeCapacity;
    uint64_t poolMaxCapacity;
    bool isReclaimable;
    uint64_t allocatedBytes;
    uint64_t requestBytes;
    bool expectedSuccess;
    bool expectedReclaimedBytes;

    std::string debugString() const {
      return fmt::format(
          "nodeCapacity {} poolMaxCapacity {} isReclaimable {} "
          "allocatedBytes {} requestBytes {} expectedSuccess {} "
          "expectedReclaimedBytes {}",
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
    setupMemory({.memoryCapacity = testData.nodeCapacity});

    auto requestor = addTask(testData.poolMaxCapacity);
    auto* requestorOp = addMemoryOp(requestor, testData.isReclaimable);
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
      ASSERT_GT(arbitrator_->stats().reclaimedUsedBytes, 0);
    } else {
      ASSERT_EQ(arbitrator_->stats().reclaimedUsedBytes, 0);
    }
    ASSERT_EQ(arbitrator_->stats().numRequests, numRequests + 1);
  }
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, arbitrationAbort) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory({.memoryCapacity = memoryCapacity});
  std::shared_ptr<MockTask> task1 = addTask(memoryCapacity);
  auto* op1 =
      task1->addMemoryOp(true, [&](MemoryPool* /*unsed*/, uint64_t /*unsed*/) {
        VELOX_FAIL("throw reclaim exception");
        return false;
      });
  op1->allocate(memoryCapacity / 2);
  ASSERT_EQ(task1->capacity(), memoryCapacity / 2);

  std::shared_ptr<MockTask> task2 = addTask(memoryCapacity);
  auto* op2 = task2->addMemoryOp(true);
  op2->allocate(memoryCapacity / 4);
  ASSERT_EQ(task2->capacity(), memoryCapacity / 4);

  std::shared_ptr<MockTask> task3 = addTask(memoryCapacity);
  auto* op3 = task3->addMemoryOp(true);
  op3->allocate(memoryCapacity / 4);
  ASSERT_EQ(task3->capacity(), memoryCapacity / 4);

  folly::EventCount globalArbitrationWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* arbitrator) {
            test::SharedArbitratorTestHelper arbitratorHelper(
                const_cast<SharedArbitrator*>(arbitrator));
            ASSERT_EQ(arbitratorHelper.numGlobalArbitrationWaiters(), 1);
          })));
  try {
    op1->allocate(memoryCapacity / 4);
  } catch (const VeloxException& ex) {
    ASSERT_EQ(ex.errorCode(), error_code::kMemAborted);
    ASSERT_THAT(ex.what(), testing::HasSubstr("aborted"));
  }

  // Task1 has been aborted,
  ASSERT_EQ(task1->capacity(), 0);
  ASSERT_TRUE(task1->pool()->aborted());
  auto arbitratorHelper = test::SharedArbitratorTestHelper(arbitrator_);
  ASSERT_TRUE(
      arbitratorHelper.getParticipant(task1->pool()->name())->aborted());
  ASSERT_EQ(task2->capacity(), memoryCapacity / 4);
  ASSERT_EQ(task3->capacity(), memoryCapacity / 4);
}

TEST_F(MockSharedArbitrationTest, shutdown) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory({.memoryCapacity = memoryCapacity});
  arbitrator_->shutdown();
  // double shutdown.
  arbitrator_->shutdown();
  // Check APIs.
  // NOTE: the arbitrator running is first check for external APIs.
  VELOX_ASSERT_THROW(
      arbitrator_->addPool(nullptr), "SharedArbitrator is not running");
  VELOX_ASSERT_THROW(
      arbitrator_->growCapacity(nullptr, 0), "SharedArbitrator is not running");
  VELOX_ASSERT_THROW(
      arbitrator_->shrinkCapacity(nullptr, 0),
      "SharedArbitrator is not running");

  auto arbitratorHelper = test::SharedArbitratorTestHelper(arbitrator_);
  ASSERT_TRUE(arbitratorHelper.hasShutdown());
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, shutdownWait) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationTimeoutNs = 2'000'000'000UL});
  std::shared_ptr<MockTask> task1 = addTask(memoryCapacity);
  auto* op1 = task1->addMemoryOp(true);
  op1->allocate(memoryCapacity / 2);
  ASSERT_EQ(task1->capacity(), memoryCapacity / 2);

  std::shared_ptr<MockTask> task2 = addTask(memoryCapacity);
  auto* op2 = task2->addMemoryOp(true);
  op2->allocate(memoryCapacity / 2);
  ASSERT_EQ(task2->capacity(), memoryCapacity / 2);

  folly::EventCount globalArbitrationStarted;
  std::atomic_bool globalArbitrationStartedFlag{false};
  folly::EventCount globalArbitrationWait;
  std::atomic_bool globalArbitrationWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* arbitrator) {
            test::SharedArbitratorTestHelper arbitratorHelper(
                const_cast<SharedArbitrator*>(arbitrator));
            ASSERT_EQ(arbitratorHelper.numGlobalArbitrationWaiters(), 1);
            globalArbitrationStartedFlag = true;
            globalArbitrationStarted.notifyAll();
            globalArbitrationWait.await(
                [&]() { return !globalArbitrationWaitFlag.load(); });
          })));
  VELOX_ASSERT_THROW(
      op1->allocate(memoryCapacity / 4),
      "Memory arbitration timed out on memory pool");
  globalArbitrationStarted.await(
      [&]() { return globalArbitrationStartedFlag.load(); });

  op2->freeAll();
  task2.reset();
  op1->freeAll();
  task1.reset();

  test::SharedArbitratorTestHelper arbitratorHelper(
      const_cast<SharedArbitrator*>(arbitrator_));
  ASSERT_FALSE(arbitratorHelper.hasShutdown());

  std::atomic_bool shutdownCompleted{false};
  std::thread shutdownThread([&]() {
    arbitrator_->shutdown();
    shutdownCompleted = true;
  });

  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
  ASSERT_FALSE(shutdownCompleted);
  ASSERT_TRUE(arbitratorHelper.globalArbitrationRunning());
  ASSERT_TRUE(arbitratorHelper.hasShutdown());

  globalArbitrationWaitFlag = false;
  globalArbitrationWait.notifyAll();

  arbitratorHelper.waitForGlobalArbitrationToFinish();
  shutdownThread.join();
  ASSERT_TRUE(shutdownCompleted);
  ASSERT_TRUE(arbitratorHelper.hasShutdown());
}

TEST_F(MockSharedArbitrationTest, memoryPoolAbortCapacityLimit) {
  const int64_t memoryCapacity = 256 << 20;

  struct TestTask {
    uint64_t capacity;
    bool expectedAbort{false};

    std::string debugString() const {
      return fmt::format(
          "capacity: {}, expectedAbort: {}",
          succinctBytes(capacity),
          expectedAbort);
    }
  };

  struct {
    std::vector<TestTask> testTasks;
    uint64_t memoryPoolAbortCapacityLimit;
    uint64_t targetBytes;
    uint64_t expectedReclaimedUsedBytes;

    std::string debugString() const {
      std::stringstream tasksOss;
      for (const auto& testTask : testTasks) {
        tasksOss << "[";
        tasksOss << testTask.debugString();
        tasksOss << "], \n";
      }
      return fmt::format(
          "testTasks: \n[{}]\nmemoryPoolAbortCapacityLimit: {}, targetBytes: {}, expectedReclaimedUsedBytes: {}",
          tasksOss.str(),
          succinctBytes(memoryPoolAbortCapacityLimit),
          succinctBytes(targetBytes),
          succinctBytes(expectedReclaimedUsedBytes));
    }
  } testSettings[] = {
      {{{64 << 20, false},
        {128 << 20, false},
        // Young participant is chosen to abort first with the same bucket.
        {64 << 20, true}},
       64 << 20,
       32 << 20,
       64 << 20},
      {{{64 << 20, false}, {128 << 20, true}, {32 << 20, false}},
       64 << 20,
       32 << 20,
       128 << 20},
      {{{128 << 20, false}, {64 << 20, true}, {32 << 20, false}},
       64 << 20,
       32 << 20,
       64 << 20},
      {{{128 << 20, true}, {64 << 20, true}, {32 << 20, false}},
       64 << 20,
       128 << 20,
       192 << 20},
      {{{32 << 20, true}, {0, false}}, 64 << 20, 128 << 20, 32 << 20},
      {{{0, false}, {0, false}}, 64 << 20, 128 << 20, 0},
      {{{128 << 20, false}, {64 << 20, false}, {32 << 20, true}},
       32 << 20,
       16 << 20,
       32 << 20},
      {{{64 << 20, true},
        {16 << 20, false},
        {32 << 20, true},
        {32 << 20, true}},
       64 << 20,
       128 << 20,
       128 << 20},
      {{{8 << 20, true},
        {16 << 20, true},
        {7 << 20, true},
        {32 << 20, true},
        {128 << 20, true}},
       64 << 20,
       0,
       191 << 20}};

  struct TestTaskContainer {
    std::shared_ptr<MockTask> task;
    MockMemoryOperator* op;
    TestTask testTask;
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
            "Memory pool aborted to reclaim used memory, current capacity");
      };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolAbortCapacityLimit =
             testData.memoryPoolAbortCapacityLimit});

    std::vector<TestTaskContainer> taskContainers;
    for (const auto& testTask : testData.testTasks) {
      auto task = addTask();
      auto* op = addMemoryOp(task, true);
      ASSERT_EQ(op->capacity(), 0);
      if (testTask.capacity != 0) {
        op->allocate(testTask.capacity);
      }
      ASSERT_EQ(task->capacity(), testTask.capacity);
      ASSERT_LE(task->usedBytes(), testTask.capacity);
      taskContainers.push_back({task, op, testTask});
    }

    ASSERT_EQ(
        manager_->shrinkPools(testData.targetBytes, false, true),
        testData.expectedReclaimedUsedBytes);
    ASSERT_EQ(
        arbitrator_->stats().reclaimedUsedBytes,
        testData.expectedReclaimedUsedBytes);

    for (const auto& taskContainer : taskContainers) {
      checkTaskException(
          taskContainer.task.get(), taskContainer.testTask.expectedAbort);
    }
  }
}

DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    globalArbitrationWaitReturnEarlyWithFreeCapacity) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory({.memoryCapacity = memoryCapacity});
  std::shared_ptr<MockTask> task1 = addTask(memoryCapacity);
  auto* op1 = task1->addMemoryOp(true);
  op1->allocate(memoryCapacity / 2);
  ASSERT_EQ(task1->capacity(), memoryCapacity / 2);

  std::shared_ptr<MockTask> task2 = addTask(memoryCapacity);
  auto* op2 = task2->addMemoryOp(true);
  op2->allocate(memoryCapacity / 2);
  ASSERT_EQ(task2->capacity(), memoryCapacity / 2);

  folly::EventCount globalArbitrationStarted;
  std::atomic_bool globalArbitrationStartedFlag{false};
  folly::EventCount globalArbitrationWait;
  std::atomic_bool globalArbitrationWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* arbitrator) {
            test::SharedArbitratorTestHelper arbitratorHelper(
                const_cast<SharedArbitrator*>(arbitrator));
            ASSERT_EQ(arbitratorHelper.numGlobalArbitrationWaiters(), 1);
            globalArbitrationStartedFlag = true;
            globalArbitrationStarted.notifyAll();
            globalArbitrationWait.await(
                [&]() { return !globalArbitrationWaitFlag.load(); });
          })));
  std::thread allocationThread([&]() { op1->allocate(memoryCapacity / 4); });
  globalArbitrationStarted.await(
      [&]() { return globalArbitrationStartedFlag.load(); });

  op2->freeAll();
  task2.reset();
  allocationThread.join();

  ASSERT_EQ(task1->capacity(), memoryCapacity / 2 + memoryCapacity / 4);
  test::SharedArbitratorTestHelper arbitratorHelper(
      const_cast<SharedArbitrator*>(arbitrator_));
  ASSERT_TRUE(arbitratorHelper.globalArbitrationRunning());

  globalArbitrationWaitFlag = false;
  globalArbitrationWait.notifyAll();

  ASSERT_EQ(
      arbitratorHelper.getParticipant(task1->pool()->name())
          ->stats()
          .numReclaims,
      0);
  arbitratorHelper.waitForGlobalArbitrationToFinish();
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, globalArbitrationTimeout) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationTimeoutNs = 1'000'000'000UL});
  std::shared_ptr<MockTask> task1 = addTask(memoryCapacity);
  auto* op1 = task1->addMemoryOp(true);
  op1->allocate(memoryCapacity / 2);
  ASSERT_EQ(task1->capacity(), memoryCapacity / 2);

  std::shared_ptr<MockTask> task2 = addTask(memoryCapacity);
  auto* op2 = task2->addMemoryOp(true);
  ASSERT_EQ(task2->capacity(), 0);

  folly::EventCount globalArbitrationWait;
  std::atomic_bool globalArbitrationWaitFlag{true};
  std::atomic_bool globalArbitrationExecuted{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            globalArbitrationWait.await(
                [&]() { return !globalArbitrationWaitFlag.load(); });
            globalArbitrationExecuted = true;
          })));
  try {
    op2->allocate(memoryCapacity / 2 + memoryCapacity / 4);
  } catch (const VeloxException& ex) {
    ASSERT_EQ(ex.errorCode(), error_code::kMemArbitrationTimeout);
    ASSERT_THAT(
        ex.what(),
        testing::HasSubstr("Memory arbitration timed out on memory pool"));
  }
  globalArbitrationWaitFlag = false;
  globalArbitrationWait.notifyAll();

  // Nothing needs to reclaim as the arbitration has timed out.
  ASSERT_EQ(task1->capacity(), memoryCapacity / 2);
  ASSERT_EQ(task2->capacity(), 0);
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  arbitratorHelper.waitForGlobalArbitrationToFinish();
  ASSERT_TRUE(globalArbitrationExecuted);
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, localArbitrationTimeout) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationTimeoutNs = 1'000'000'000UL});
  std::shared_ptr<MockTask> task = addTask(memoryCapacity);
  ASSERT_EQ(task->capacity(), 0);
  auto* op = task->addMemoryOp(true);
  op->allocate(memoryCapacity / 2);

  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::growCapacity",
      std::function<void(const ArbitrationParticipant*)>(
          ([&](const ArbitrationParticipant* /*unused*/) {
            std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
          })));
  try {
    op->allocate(memoryCapacity);
  } catch (const VeloxException& ex) {
    ASSERT_EQ(ex.errorCode(), error_code::kMemArbitrationTimeout);
    ASSERT_THAT(
        ex.what(),
        testing::HasSubstr("Memory arbitration timed out on memory pool"));
  }

  // Timeout check happened before reclaim.
  ASSERT_EQ(task->capacity(), memoryCapacity / 2);
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, reclaimLockTimeout) {
  const uint64_t memoryCapacity = 256 * MB;
  const uint64_t arbitrationTimeoutMs = 1'000;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .globalArtbitrationEnabled = false,
       .arbitrationTimeoutNs = arbitrationTimeoutMs});
  std::shared_ptr<MockTask> task = addTask(memoryCapacity);
  ASSERT_EQ(task->capacity(), 0);
  auto* op = task->addMemoryOp(true);

  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::ArbitrationParticipant::abort",
      std::function<void(const ArbitrationParticipant*)>(
          ([&](const ArbitrationParticipant* /*unused*/) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(2 * arbitrationTimeoutMs)); // NOLINT
          })));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::ArbitrationParticipant::reclaim",
      std::function<void(const ArbitrationParticipant*)>(
          ([&](const ArbitrationParticipant* /*unused*/) {
            // Timeout shall be enforced at lock level. We don't expect code
            // to execute pass the lock in reclaim method.
            FAIL();
          })));

  auto abortThread = std::thread(
      [&]() { arbitrator_->shrinkCapacity(memoryCapacity, false, true); });
  try {
    op->allocate(memoryCapacity / 2);
  } catch (const VeloxException& ex) {
    ASSERT_EQ(ex.errorCode(), error_code::kMemArbitrationTimeout);
    ASSERT_THAT(
        ex.what(),
        testing::HasSubstr("Memory arbitration timed out on memory pool"));
  }

  abortThread.join();
}

DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, localArbitrationQueueTimeout) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationTimeoutNs = 1'000'000'000UL});
  std::shared_ptr<MockTask> task = addTask(memoryCapacity);
  ASSERT_EQ(task->capacity(), 0);
  auto* op = task->addMemoryOp(true);

  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::growCapacity",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* arbitrator) {
            test::SharedArbitratorTestHelper arbitratorHelper(
                const_cast<SharedArbitrator*>(arbitrator));
            ASSERT_EQ(arbitratorHelper.maxArbitrationTimeNs(), 1'000'000'000UL);
            std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
          })));
  try {
    op->allocate(memoryCapacity);
  } catch (const VeloxException& ex) {
    ASSERT_EQ(ex.errorCode(), error_code::kMemArbitrationTimeout);
    ASSERT_THAT(
        ex.what(),
        testing::HasSubstr("Memory arbitration timed out on memory pool"));
  }

  // Nothing needs to reclaim as the arbitration has timed out.
  ASSERT_EQ(task->capacity(), 0);
}

TEST_F(MockSharedArbitrationTest, minReclaimBytes) {
  const int64_t memoryCapacity = 256 << 20;

  struct TestTask {
    uint64_t capacity{0};
    bool reclaimable{false};

    uint64_t expectedCapacityAfterReclaim;
    uint64_t expectedUsagedAfterReclaim;
    bool expectedAbortAfterReclaim{false};

    std::string debugString() const {
      return fmt::format(
          "capacity: {}, expectedCapacityAfterReclaim: {}, expectedUsagedAfterReclaim: {}, expectedAbortAfterReclaim: {}",
          succinctBytes(capacity),
          succinctBytes(expectedCapacityAfterReclaim),
          succinctBytes(expectedUsagedAfterReclaim),
          expectedAbortAfterReclaim);
    }
  };

  struct {
    std::vector<TestTask> testTasks;
    uint64_t minReclaimBytes;
    uint64_t targetBytes;
    bool expectedAbortAfterReclaim;

    std::string debugString() const {
      std::stringstream tasksOss;
      for (const auto& testTask : testTasks) {
        tasksOss << "[";
        tasksOss << testTask.debugString();
        tasksOss << "], \n";
      }
      return fmt::format(
          "testTasks: \n[{}]\nminReclaimBytes: {}\ntargetBytes: {}\nexpectedAbortAfterReclaim: {}",
          tasksOss.str(),
          succinctBytes(minReclaimBytes),
          succinctBytes(targetBytes),
          expectedAbortAfterReclaim);
    }
  } testSettings[] = {
      {{{memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 2, true, 0, 0, false},
        {memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false}},
       memoryCapacity / 4,
       MB,
       false},

      {{{memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 2, true, 0, 0, false},
        {memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false}},
       memoryCapacity / 2,
       MB,
       false},

      {{{memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4, true, 0, 0, true}},
       memoryCapacity / 2,
       MB,
       false},

      {{{memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4,
         true,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4, true, 0, 0, true},
        {memoryCapacity / 4, true, 0, 0, true}},
       memoryCapacity / 2,
       memoryCapacity / 2,
       true},

      {{{memoryCapacity / 4,
         false,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4,
         false,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4, false, 0, 0, true},
        {memoryCapacity / 4, false, 0, 0, true}},
       memoryCapacity / 8,
       memoryCapacity / 2,
       true},

      {{{memoryCapacity / 4,
         false,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4,
         false,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4,
         false,
         memoryCapacity / 4,
         memoryCapacity / 4,
         false},
        {memoryCapacity / 4, false, 0, 0, true}},
       memoryCapacity / 8,
       MB,
       false}};

  struct TestTaskContainer {
    std::shared_ptr<MockTask> task;
    MockMemoryOperator* op;
    TestTask testTask;
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
            "Memory pool aborted to reclaim used memory, current capacity");
      };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    // Make simple settings to focus shrink capacity logic testing.
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolMinReclaimBytes = testData.minReclaimBytes,
         .memoryPoolSpillCapacityLimit = memoryCapacity,
         .memoryPoolAbortCapacityLimit = memoryCapacity});
    std::vector<TestTaskContainer> taskContainers;
    for (const auto& testTask : testData.testTasks) {
      auto task = addTask();
      auto* op = addMemoryOp(task, testTask.reclaimable);
      ASSERT_EQ(op->capacity(), 0);
      if (testTask.capacity != 0) {
        op->allocate(testTask.capacity);
      }
      ASSERT_EQ(task->capacity(), testTask.capacity);
      ASSERT_LE(task->usedBytes(), testTask.capacity);
      taskContainers.push_back({task, op, testTask});
    }

    auto arbitrationTask = addTask();
    auto* arbitrationOp = arbitrationTask->addMemoryOp(true);
    if (testData.expectedAbortAfterReclaim) {
      VELOX_ASSERT_THROW(
          arbitrationOp->allocate(testData.targetBytes), "aborted");
      continue;
    } else {
      arbitrationOp->allocate(testData.targetBytes);
    }

    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    arbitratorHelper.waitForGlobalArbitrationToFinish();

    for (const auto& taskContainer : taskContainers) {
      checkTaskException(
          taskContainer.task.get(),
          taskContainer.testTask.expectedAbortAfterReclaim);
    }

    for (const auto& taskContainer : taskContainers) {
      ASSERT_EQ(
          taskContainer.task->pool()->capacity(),
          taskContainer.testTask.expectedCapacityAfterReclaim);
      ASSERT_EQ(
          taskContainer.task->pool()->usedBytes(),
          taskContainer.testTask.expectedCapacityAfterReclaim);
    }
  }
}

TEST_F(MockSharedArbitrationTest, globalArbitrationReclaimPct) {
  const int64_t memoryCapacity = 256 << 20;

  struct TestTask {
    uint64_t capacity{0};

    uint64_t expectedCapacityAfterReclaim;
    uint64_t expectedUsagedAfterReclaim;

    std::string debugString() const {
      return fmt::format(
          "capacity: {}, expectedCapacityAfterReclaim: {}, expectedUsagedAfterReclaim: {}",
          succinctBytes(capacity),
          succinctBytes(expectedCapacityAfterReclaim),
          succinctBytes(expectedUsagedAfterReclaim));
    }
  };

  struct {
    std::vector<TestTask> testTasks;
    double reclaimPct;
    uint64_t targetBytes;

    std::string debugString() const {
      std::stringstream tasksOss;
      for (const auto& testTask : testTasks) {
        tasksOss << "[";
        tasksOss << testTask.debugString();
        tasksOss << "], \n";
      }
      return fmt::format(
          "testTasks: \n[{}], \reclaimPct: {}, targetBytes: {}",
          tasksOss.str(),
          reclaimPct,
          succinctBytes(targetBytes));
    }
  } testSettings[] = {
      {{{memoryCapacity / 2, 0, 0},
        {memoryCapacity / 4, memoryCapacity / 4, memoryCapacity / 4},
        {memoryCapacity / 4, memoryCapacity / 4, memoryCapacity / 4}},
       1,
       MB},
      {{{memoryCapacity / 4, 0, 0},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8}},
       1,
       MB},
      {{{memoryCapacity / 2, 0, 0},
        {memoryCapacity / 4, memoryCapacity / 4, memoryCapacity / 4},
        {memoryCapacity / 4, memoryCapacity / 4, memoryCapacity / 4}},
       0,
       MB},
      {{{memoryCapacity / 4, 0, 0},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8},
        {memoryCapacity / 8, memoryCapacity / 8, memoryCapacity / 8}},
       0,
       MB},
      {{{memoryCapacity / 2, 0, 0},
        {memoryCapacity / 4, 0, 0},
        {memoryCapacity / 4, 0, 0}},
       100,
       MB},
      {{{memoryCapacity / 2, 0, 0}, {memoryCapacity / 2, 0, 0}}, 60, MB},
      {{{memoryCapacity / 2, 0, 0},
        {memoryCapacity / 4, memoryCapacity / 4, memoryCapacity / 4},
        {memoryCapacity / 4, memoryCapacity / 4, memoryCapacity / 4}},
       50,
       MB},
  };

  struct TestTaskContainer {
    std::shared_ptr<MockTask> task;
    MockMemoryOperator* op;
    TestTask testTask;
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .globalArbitrationReclaimPct = testData.reclaimPct});
    std::vector<TestTaskContainer> taskContainers;
    for (const auto& testTask : testData.testTasks) {
      auto task = addTask();
      auto* op = addMemoryOp(task, true);
      ASSERT_EQ(op->capacity(), 0);
      if (testTask.capacity != 0) {
        op->allocate(testTask.capacity);
      }
      ASSERT_EQ(task->capacity(), testTask.capacity);
      ASSERT_LE(task->usedBytes(), testTask.capacity);
      taskContainers.push_back({task, op, testTask});
    }

    auto arbitrationTask = addTask();
    auto* arbitrationOp = arbitrationTask->addMemoryOp(true);
    arbitrationOp->allocate(testData.targetBytes);
    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    arbitratorHelper.waitForGlobalArbitrationToFinish();

    for (const auto& taskContainer : taskContainers) {
      ASSERT_EQ(
          taskContainer.task->pool()->capacity(),
          taskContainer.testTask.expectedCapacityAfterReclaim);
      ASSERT_EQ(
          taskContainer.task->pool()->usedBytes(),
          taskContainer.testTask.expectedCapacityAfterReclaim);
    }
  }
}

TEST_F(MockSharedArbitrationTest, noEligibleAbortCandidate) {
  int64_t memoryCapacity = 256 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .reservedMemoryCapacity = memoryCapacity / 2,
       .memoryPoolReserveCapacity = static_cast<uint64_t>(memoryCapacity / 4)});
  std::shared_ptr<MockTask> task = addTask(memoryCapacity);
  ASSERT_EQ(task->capacity(), memoryCapacity / 4);
  auto* op = task->addMemoryOp(true);
  VELOX_ASSERT_THROW(op->allocate(memoryCapacity), "aborted");
  ASSERT_TRUE(task->pool()->aborted());
}

TEST_F(MockSharedArbitrationTest, growWithArbitrationAbort) {
  const int memCapacity = 256 * MB;
  const int initPoolCapacity = 8 * MB;
  setupMemory(
      {.memoryCapacity = memCapacity,
       .memoryPoolInitCapacity = initPoolCapacity,
       .memoryPoolAbortCapacityLimit = memCapacity});

  auto* reclaimableOp = addMemoryOp(nullptr, true);
  ASSERT_EQ(reclaimableOp->capacity(), initPoolCapacity);

  auto* nonReclaimableOp = addMemoryOp(nullptr, false);
  ASSERT_EQ(nonReclaimableOp->capacity(), initPoolCapacity);

  auto* arbitrateOp = addMemoryOp();
  ASSERT_EQ(arbitrateOp->capacity(), initPoolCapacity);

  reclaimableOp->allocate(memCapacity / 8);
  ASSERT_EQ(reclaimableOp->capacity(), memCapacity / 8 + initPoolCapacity);

  nonReclaimableOp->allocate(memCapacity / 8 * 7);
  ASSERT_EQ(nonReclaimableOp->capacity(), memCapacity / 8 * 7);

  arbitrateOp->allocate(memCapacity / 4);
  ASSERT_TRUE(nonReclaimableOp->pool()->aborted());

  verifyReclaimerStats(nonReclaimableOp->reclaimer()->stats(), 0, 1);
  verifyReclaimerStats(reclaimableOp->reclaimer()->stats(), 1, 1);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  verifyArbitratorStats(
      arbitrator_->stats(),
      memCapacity,
      memCapacity - memCapacity / 4,
      0,
      3,
      0,
      memCapacity,
      24 * MB);
}

TEST_F(MockSharedArbitrationTest, singlePoolGrowCapacityWithArbitration) {
  const std::vector<bool> isLeafReclaimables = {false, true};
  const int64_t memoryCapacity = 128 * MB;
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory({.memoryCapacity = memoryCapacity});
    auto* op = addMemoryOp(nullptr, isLeafReclaimable);
    op->allocate(memoryCapacity);
    verifyArbitratorStats(arbitrator_->stats(), memoryCapacity, 0, 0, 1);
    verifyReclaimerStats(op->reclaimer()->stats(), 0, 1);

    if (!isLeafReclaimable) {
      VELOX_ASSERT_THROW(
          op->allocate(memoryCapacity), "Exceeded memory pool cap");
      verifyArbitratorStats(arbitrator_->stats(), memoryCapacity, 0, 0, 2, 1);
      verifyReclaimerStats(op->reclaimer()->stats(), 0, 2);
      clearTasks();
      continue;
    }

    // Do more allocations to trigger arbitration.
    op->allocate(memoryCapacity);
    verifyArbitratorStats(
        arbitrator_->stats(), memoryCapacity, 0, 0, 2, 0, memoryCapacity);
    verifyReclaimerStats(op->reclaimer()->stats(), 1, 2);

    clearTasks();
    verifyArbitratorStats(
        arbitrator_->stats(),
        memoryCapacity,
        memoryCapacity,
        0,
        2,
        0,
        memoryCapacity);
  }
}

// This test verifies if a single memory pool fails to grow capacity because
// of reserved capacity.
// TODO: add reserved capacity check in ensure capacity.
TEST_F(MockSharedArbitrationTest, singlePoolGrowCapacityFailedWithAbort) {
  const uint64_t memoryCapacity = 128 * MB;
  const uint64_t reservedMemoryCapacity = 64 * MB;
  const uint64_t memoryPoolReservedCapacity = 64 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .reservedMemoryCapacity = reservedMemoryCapacity,
       .memoryPoolReserveCapacity = memoryPoolReservedCapacity});
  auto* op = addMemoryOp(nullptr, true);
  op->allocate(memoryCapacity - reservedMemoryCapacity);
  verifyArbitratorStats(
      arbitrator_->stats(),
      memoryCapacity,
      reservedMemoryCapacity,
      reservedMemoryCapacity,
      0);
  verifyReclaimerStats(op->reclaimer()->stats(), 0, 0);

  // Do more allocations to trigger arbitration.
  try {
    op->allocate(memoryCapacity);
  } catch (const VeloxRuntimeError& ex) {
    ASSERT_EQ(ex.errorCode(), error_code::kMemAborted.c_str());
  }
  verifyArbitratorStats(
      arbitrator_->stats(),
      memoryCapacity,
      memoryCapacity,
      reservedMemoryCapacity,
      1,
      1,
      64 * MB);
  verifyReclaimerStats(op->reclaimer()->stats(), 1, 1);
}

TEST_F(MockSharedArbitrationTest, arbitrateWithCapacityShrink) {
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory({});
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
    ASSERT_EQ(reclaimedOp->pool()->usedBytes(), 0);
    ASSERT_EQ(arbitrator_->stats().freeCapacityBytes, freeCapacity);

    auto* arbitrateOp = addMemoryOp(nullptr, isLeafReclaimable);
    while (arbitrator_->stats().reclaimedFreeBytes == 0) {
      arbitrateOp->allocate(allocateSize);
    }
    const auto arbitratorStats = arbitrator_->stats();
    ASSERT_GT(arbitratorStats.reclaimedFreeBytes, 0);
    ASSERT_EQ(arbitratorStats.reclaimedUsedBytes, 0);

    verifyReclaimerStats(reclaimedOp->reclaimer()->stats(), 0, 11);
    verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 6);

    clearTasks();
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateWithMemoryReclaim) {
  const uint64_t memoryCapacity = 256 * MB;
  const uint64_t reservedMemoryCapacity = 128 * MB;
  const uint64_t reservedPoolCapacity = 8 * MB;
  const uint64_t memoryPoolAbortCapacityLimit = 256 * MB;
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .reservedMemoryCapacity = reservedMemoryCapacity,
         .memoryPoolReserveCapacity = reservedPoolCapacity,
         .memoryPoolAbortCapacityLimit = memoryPoolAbortCapacityLimit});
    auto* reclaimedOp = addMemoryOp(nullptr, isLeafReclaimable);
    reclaimedOp->allocate(
        memoryCapacity - reservedMemoryCapacity - reservedPoolCapacity);

    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    auto* arbitrateOp = addMemoryOp();
    if (!isLeafReclaimable) {
      auto leafTask = tasks().front();
      ASSERT_NO_THROW(arbitrateOp->allocate(reservedPoolCapacity * 2));

      ASSERT_NE(leafTask->error(), nullptr);
      ASSERT_EQ(arbitrator_->stats().numFailures, 0);
      arbitratorHelper.waitForGlobalArbitrationToFinish();
      clearTasks();
      continue;
    }
    arbitrateOp->allocate(reservedMemoryCapacity - reservedPoolCapacity);
    verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1, 0);
    verifyReclaimerStats(reclaimedOp->reclaimer()->stats(), 1, 1, 0);
    arbitratorHelper.waitForGlobalArbitrationToFinish();
    clearTasks();
  }
}

// This test verifies the global arbitration can handle the case that there is
// no candidates when reclaim memory by abort such as all the candidates have
// gone.
DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, abortWithNoCandidate) {
  const uint64_t memoryCapacity = 256 * MB;
  const uint64_t maxArbitrationTimeNs = 1'000'000'000UL;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationTimeoutNs = maxArbitrationTimeNs});
  auto* reclaimedOp1 = addMemoryOp(nullptr, false);
  reclaimedOp1->allocate(memoryCapacity / 2);
  auto* reclaimedOp2 = addMemoryOp(nullptr, false);
  reclaimedOp2->allocate(memoryCapacity / 2);

  auto* arbitrateOp = addMemoryOp(nullptr, false);

  folly::EventCount abortStart;
  std::atomic_bool abortStartFlag{false};
  folly::EventCount abortWait;
  std::atomic_bool abortWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::reclaimUsedMemoryByAbort",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            if (abortStartFlag.exchange(true)) {
              return;
            }
            abortStart.notifyAll();

            abortWait.await([&]() { return !abortWaitFlag.load(); });
          })));

  std::thread allocationThread([&]() {
    VELOX_ASSERT_THROW(
        arbitrateOp->allocate(memoryCapacity / 2),
        "Memory arbitration timed out on memory pool");
  });

  abortStart.await([&]() { return abortStartFlag.load(); });
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  ASSERT_EQ(arbitratorHelper.numGlobalArbitrationWaiters(), 0);

  clearTasks();
  ASSERT_EQ(arbitratorHelper.numParticipants(), 0);

  abortWaitFlag = false;
  abortWait.notifyAll();

  allocationThread.join();
  arbitratorHelper.waitForGlobalArbitrationToFinish();
}

// This test verifies the global arbitration can handle the case that there is
// no candidates when reclaim memory by spill such as all the candidates have
// gone.
DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, reclaimWithNoCandidate) {
  const uint64_t memoryCapacity = 256 * MB;
  const uint64_t maxArbitrationTimeNs = 1'000'000'000UL;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .arbitrationTimeoutNs = maxArbitrationTimeNs});
  auto* reclaimedOp1 = addMemoryOp(nullptr, true);
  reclaimedOp1->allocate(memoryCapacity / 2);
  auto* reclaimedOp2 = addMemoryOp(nullptr, true);
  reclaimedOp2->allocate(memoryCapacity / 2);

  auto* arbitrateOp = addMemoryOp(nullptr, true);

  folly::EventCount reclaimStart;
  std::atomic_bool reclaimStartFlag{false};
  folly::EventCount reclaimWait;
  std::atomic_bool reclaimWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::reclaimUsedMemoryBySpill",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* /*unused*/) {
            if (reclaimStartFlag.exchange(true)) {
              return;
            }
            reclaimStart.notifyAll();

            reclaimWait.await([&]() { return !reclaimWaitFlag.load(); });
          })));

  std::thread allocationThread([&]() {
    VELOX_ASSERT_THROW(
        arbitrateOp->allocate(memoryCapacity / 2),
        "Memory arbitration timed out on memory pool");
  });

  reclaimStart.await([&]() { return reclaimStartFlag.load(); });
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  ASSERT_EQ(arbitratorHelper.numGlobalArbitrationWaiters(), 0);

  clearTasks();
  ASSERT_EQ(arbitratorHelper.numParticipants(), 0);

  reclaimWaitFlag = false;
  reclaimWait.notifyAll();

  allocationThread.join();
  arbitratorHelper.waitForGlobalArbitrationToFinish();
}

TEST_F(MockSharedArbitrationTest, arbitrateBySelfMemoryReclaim) {
  for (const auto isLeafReclaimable : {true, false}) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    const uint64_t memCapacity = 128 * MB;
    const uint64_t reservedCapacity = 8 * MB;
    const uint64_t poolReservedCapacity = 4 * MB;
    setupMemory(
        {.memoryCapacity = memCapacity,
         .reservedMemoryCapacity = reservedCapacity,
         .memoryPoolInitCapacity = reservedCapacity,
         .memoryPoolReserveCapacity = poolReservedCapacity,
         .globalArtbitrationEnabled = false});
    std::shared_ptr<MockTask> task = addTask(kMemoryCapacity);
    auto* memOp = addMemoryOp(task, isLeafReclaimable);
    const int allocateSize = 8 * MB;
    while (memOp->pool()->usedBytes() < memCapacity / 2) {
      memOp->allocate(allocateSize);
    }
    ASSERT_EQ(memOp->pool()->freeBytes(), 0);
    const int oldNumRequests = arbitrator_->stats().numRequests;
    // Allocate a large chunk of memory to trigger arbitration.
    if (!isLeafReclaimable) {
      VELOX_ASSERT_THROW(
          memOp->allocate(memCapacity), "Exceeded memory pool cap");
      ASSERT_EQ(oldNumRequests + 1, arbitrator_->stats().numRequests);
      ASSERT_EQ(arbitrator_->stats().numFailures, 1);
    } else {
      memOp->allocate(memCapacity / 2);
      ASSERT_EQ(oldNumRequests + 1, arbitrator_->stats().numRequests);
      ASSERT_EQ(arbitrator_->stats().numFailures, 0);
      ASSERT_GT(arbitrator_->stats().reclaimedUsedBytes, 0);
    }
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
    setupMemory({.memoryCapacity = memCapacity});
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
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableFreeCapacity",
      std::function<void(const std::vector<ArbitrationCandidate>*)>(
          ([&](const std::vector<ArbitrationCandidate>* candidates) {
            for (int i = 1; i < candidates->size(); ++i) {
              ASSERT_LE(
                  (*candidates)[i].reclaimableFreeCapacity,
                  (*candidates)[i - 1].reclaimableFreeCapacity);
            }
          })));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::sortSpillCandidates",
      std::function<void(const std::vector<ArbitrationCandidate>*)>(
          ([&](const std::vector<ArbitrationCandidate>* candidates) {
            for (int i = 1; i < candidates->size(); ++i) {
              ASSERT_LE(
                  (*candidates)[i].reclaimableUsedCapacity,
                  (*candidates)[i - 1].reclaimableUsedCapacity);
            }
          })));

  folly::Random::DefaultGenerator rng;
  rng.seed(512);
  const int64_t memCapacity = 512 * MB;
  const uint64_t reservedMemCapacity = 128 * MB;
  const uint64_t initPoolCapacity = 32 * MB;
  const uint64_t reservedPoolCapacity = 8 * MB;
  const uint64_t baseAllocationSize = 8 * MB;
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

    setupMemory(
        {.memoryCapacity = memCapacity,
         .reservedMemoryCapacity = reservedMemCapacity,
         .memoryPoolInitCapacity = initPoolCapacity,
         .memoryPoolReserveCapacity = reservedPoolCapacity});
    std::vector<MockMemoryOperator*> memOps;
    for (int i = 0; i < numTasks; ++i) {
      auto* memOp = addMemoryOp();
      ASSERT_GE(memOp->capacity(), reservedPoolCapacity);
      int allocationSize = testData.sameSize ? memCapacity / numTasks
                                             : baseAllocationSize +
              folly::Random::rand32(rng) %
                  ((memCapacity / numTasks) - baseAllocationSize);
      allocationSize = allocationSize / MB * MB;
      memOp->allocate(allocationSize);
      if (testData.freeCapacity) {
        memOp->freeAll();
        ASSERT_EQ(memOp->pool()->usedBytes(), 0);
      }
      memOps.push_back(memOp);
    }

    auto* arbitrateOp = addMemoryOp();
    arbitrateOp->allocate(memCapacity / 2);
    for (auto* memOp : memOps) {
      ASSERT_GE(memOp->capacity(), 0) << memOp->pool()->name();
    }

    clearTasks();
    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    arbitratorHelper.waitForGlobalArbitrationToFinish();
  }
}

TEST_F(MockSharedArbitrationTest, enterArbitrationException) {
  const uint64_t memCapacity = 128 * MB;
  const uint64_t initPoolCapacity = memCapacity;
  setupMemory(
      {.memoryCapacity = memCapacity,
       .memoryPoolInitCapacity = initPoolCapacity});
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
  VELOX_ASSERT_THROW(
      failedArbitrateOp->allocate(allocationSize),
      "enterArbitrationException failed");
  ASSERT_FALSE(failedArbitrateOp->pool()->aborted());
  verifyReclaimerStats(failedArbitrateOp->reclaimer()->stats());
  ASSERT_EQ(failedArbitrateOp->capacity(), 0);
  auto* arbitrateOp = addMemoryOp();
  arbitrateOp->allocate(allocationSize);

  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  arbitratorHelper.waitForGlobalArbitrationToFinish();
  ASSERT_EQ(arbitrateOp->capacity(), allocationSize);
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  verifyReclaimerStats(reclaimedOp->reclaimer()->stats(), 1);
  ASSERT_EQ(arbitrator_->stats().reclaimedUsedBytes, memCapacity);
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
  ASSERT_EQ(arbitrator_->stats().numFailures, 0);
}

TEST_F(MockSharedArbitrationTest, noArbitratiognFromAbortedPool) {
  auto* reclaimedOp = addMemoryOp();
  ASSERT_EQ(reclaimedOp->capacity(), 0);
  reclaimedOp->allocate(128);
  try {
    VELOX_MEM_POOL_ABORTED("Manual abort pool");
  } catch (VeloxException&) {
    reclaimedOp->pool()->abort(std::current_exception());
  }
  ASSERT_TRUE(reclaimedOp->pool()->aborted());
  ASSERT_TRUE(reclaimedOp->pool()->aborted());
  const int largeAllocationSize = 2 * kMemoryPoolInitCapacity;
  VELOX_ASSERT_THROW(reclaimedOp->allocate(largeAllocationSize), "");
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
  ASSERT_EQ(arbitrator_->stats().numAborted, 0);
  ASSERT_EQ(arbitrator_->stats().numFailures, 0);
  // Check we don't allow memory reservation increase or trigger memory
  // arbitration at root memory pool.
  ASSERT_EQ(reclaimedOp->pool()->capacity(), MB);
  ASSERT_EQ(reclaimedOp->pool()->usedBytes(), 0);
  VELOX_ASSERT_THROW(reclaimedOp->allocate(128), "");
  ASSERT_EQ(reclaimedOp->pool()->usedBytes(), 0);
  ASSERT_EQ(reclaimedOp->pool()->capacity(), MB);
  VELOX_ASSERT_THROW(reclaimedOp->allocate(MB), "Manual abort pool");
  ASSERT_EQ(reclaimedOp->pool()->capacity(), MB);
  ASSERT_EQ(reclaimedOp->pool()->usedBytes(), 0);
  ASSERT_EQ(arbitrator_->stats().numRequests, 1);
  ASSERT_EQ(arbitrator_->stats().numAborted, 0);
  ASSERT_EQ(arbitrator_->stats().numFailures, 0);
}

TEST_F(MockSharedArbitrationTest, memoryReclaimeFailureTriggeredAbort) {
  setupMemory(
      {.memoryCapacity = kMemoryCapacity,
       .memoryPoolInitCapacity = kMemoryPoolInitCapacity,
       .fastExponentialGrowthCapacityLimit =
           kFastExponentialGrowthCapacityLimit,
       .slowCapacityGrowPct = kSlowCapacityGrowPct});
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
        return false;
      });
  largeTaskOp->allocate(largeTaskMemoryCapacity);
  const auto oldStats = arbitrator_->stats();
  ASSERT_EQ(oldStats.numFailures, 0);
  ASSERT_EQ(oldStats.numAborted, 0);

  // Trigger memory arbitration to reclaim from itself which throws.
  VELOX_ASSERT_THROW(largeTaskOp->allocate(largeTaskMemoryCapacity), "aborted");
  test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
  arbitratorHelper.waitForGlobalArbitrationToFinish();
  const auto newStats = arbitrator_->stats();
  ASSERT_EQ(newStats.numRequests, oldStats.numRequests + 1);
  ASSERT_EQ(newStats.numAborted, 0);
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
  ASSERT_EQ(arbitrator_->stats().numAborted, 0);
}

// This test makes sure the memory capacity grows as expected.
DEBUG_ONLY_TEST_F(MockSharedArbitrationTest, concurrentArbitrationRequests) {
  setupMemory({.memoryCapacity = kMemoryCapacity});
  std::shared_ptr<MockTask> task = addTask();
  MockMemoryOperator* op1 = addMemoryOp(task);
  MockMemoryOperator* op2 = addMemoryOp(task);

  std::atomic_bool injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::growCapacity",
      std::function<void(const SharedArbitrator*)>(
          ([&](const SharedArbitrator* arbitrator) {
            if (!injectOnce.exchange(false)) {
              return;
            }
            test::SharedArbitratorTestHelper arbitratorHelper(
                const_cast<SharedArbitrator*>(arbitrator));
            auto participant =
                arbitratorHelper.getParticipant(task->pool()->name());
            test::ArbitrationParticipantTestHelper participantHelper(
                participant.get());
            while (participantHelper.numOps() != 2) {
              std::this_thread::sleep_for(std::chrono::seconds(5)); // NOLINT
            }
          })));

  std::thread firstArbitrationThread([&]() { op1->allocate(64 << 20); });
  std::thread secondArbitrationThread([&]() { op2->allocate(64 << 20); });

  firstArbitrationThread.join();
  secondArbitrationThread.join();

  ASSERT_EQ(task->capacity(), 128 << 20);
}

DEBUG_ONLY_TEST_F(
    MockSharedArbitrationTest,
    freeUnusedCapacityWhenReclaimMemoryPool) {
  setupMemory({.memoryCapacity = kMemoryCapacity});
  const int allocationSize = kMemoryCapacity / 4;
  std::shared_ptr<MockTask> reclaimedTask = addTask();
  MockMemoryOperator* reclaimedTaskOp = addMemoryOp(reclaimedTask);
  // The buffer to free later.
  void* bufferToFree = reclaimedTaskOp->allocate(allocationSize);
  reclaimedTaskOp->allocate(kMemoryCapacity - allocationSize);

  std::shared_ptr<MockTask> arbitrationTask = addTask();
  MockMemoryOperator* arbitrationTaskOp = addMemoryOp(arbitrationTask);
  folly::EventCount reclaimWait;
  std::atomic_bool reclaimWaitFlag{true};
  folly::EventCount reclaimBlock;
  std::atomic_bool reclaimBlockFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::SharedArbitrator::sortAndGroupSpillCandidates",
      std::function<void(const MemoryPool*)>(([&](const MemoryPool* /*unsed*/) {
        reclaimWaitFlag = false;
        reclaimWait.notifyAll();
        reclaimBlock.await([&]() { return !reclaimBlockFlag.load(); });
      })));

  const auto oldStats = arbitrator_->stats();

  std::thread allocThread([&]() {
    // Allocate to trigger arbitration.
    arbitrationTaskOp->allocate(allocationSize);
  });

  reclaimWait.await([&]() { return !reclaimWaitFlag.load(); });
  reclaimedTaskOp->free(bufferToFree);
  reclaimBlockFlag = false;
  reclaimBlock.notifyAll();

  allocThread.join();
  const auto stats = arbitrator_->stats();
  ASSERT_EQ(stats.numFailures, 0);
  ASSERT_EQ(stats.numAborted, 0);
  ASSERT_EQ(stats.numRequests, oldStats.numRequests + 1);
  ASSERT_EQ(stats.reclaimedUsedBytes, kMemoryCapacity);
  ASSERT_EQ(reclaimedTaskOp->capacity(), 0);
  ASSERT_EQ(arbitrationTaskOp->capacity(), allocationSize);
}

TEST_F(MockSharedArbitrationTest, arbitrationFailure) {
  int64_t maxCapacity = 128 * MB;
  uint64_t initialCapacity = 0 * MB;
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
      {64 * MB, 64 * MB, 32 * MB, true, false},
      {64 * MB, 48 * MB, 32 * MB, true, false},
      {32 * MB, 64 * MB, 64 * MB, true, false},
      {32 * MB, 32 * MB, 96 * MB, true, false},
      {64 * MB, 96 * MB, 32 * MB, false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemory(
        {.memoryCapacity = maxCapacity,
         .memoryPoolInitCapacity = initialCapacity});
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
          requestorOp->allocate(testData.requestorRequestBytes),
          "Exceeded memory pool capacity");
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
    test::SharedArbitratorTestHelper arbitratorHelper(arbitrator_);
    arbitratorHelper.waitForGlobalArbitrationToFinish();
  }
}

// This test is to verify if a non-reclaimable query fails properly if global
// arbitration is disabled.
TEST_F(
    MockSharedArbitrationTest,
    arbitrationFailureOnNonReclaimableQueryWithGlobalArbitrationDisabled) {
  const int64_t memoryCapacity = 128 * MB;
  for (bool hasMinReclaimBytes : {false, true}) {
    SCOPED_TRACE(fmt::format("hasMinReclaimBytes {}", hasMinReclaimBytes));
    // Set min reclaim bytes to avoid reclaim from itself before fail the
    // arbitration.
    setupMemory(
        {.memoryCapacity = memoryCapacity,
         .memoryPoolMinReclaimBytes =
             (hasMinReclaimBytes ? static_cast<uint64_t>(MB) : 0UL),
         .memoryReclaimThreadsHwMultiplier = 1.0,
         .globalArtbitrationEnabled = false});
    std::shared_ptr<MockTask> task1 = addTask();
    MockMemoryOperator* op1 = task1->addMemoryOp(false);
    op1->allocate(memoryCapacity / 4 * 3);
    ASSERT_EQ(task1->capacity(), memoryCapacity / 4 * 3);

    std::shared_ptr<MockTask> task2 = addTask();
    MockMemoryOperator* op2 = task2->addMemoryOp(false);
    VELOX_ASSERT_THROW(
        op2->allocate(memoryCapacity / 2), "Local arbitration failure.");
  }
}

// This test is to verify if a reclaimable query reclaim from itself before
// reaching the capacity limit if global arbitration is disabled.
TEST_F(
    MockSharedArbitrationTest,
    reclaimBeforeReachCapacityLimitWhenGlobalArbitrationDisabled) {
  const int64_t memoryCapacity = 128 * MB;
  setupMemory(
      {.memoryCapacity = memoryCapacity,
       .memoryReclaimThreadsHwMultiplier = 1.0,
       .globalArtbitrationEnabled = false});
  std::shared_ptr<MockTask> task1 = addTask();
  MockMemoryOperator* op1 = task1->addMemoryOp(true);
  op1->allocate(memoryCapacity / 2);
  ASSERT_EQ(task1->capacity(), memoryCapacity / 2);

  std::shared_ptr<MockTask> task2 = addTask();
  MockMemoryOperator* op2 = task2->addMemoryOp(true);
  op2->allocate(memoryCapacity / 2);
  ASSERT_EQ(task2->capacity(), memoryCapacity / 2);

  op2->allocate(memoryCapacity / 4);
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
              return false;
            }
            return true;
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
                  return false;
                }
                return true;
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
} // namespace facebook::velox::memory
