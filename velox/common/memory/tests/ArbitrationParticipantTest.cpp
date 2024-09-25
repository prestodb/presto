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

#include "gmock/gmock-matchers.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/ArbitrationOperation.h"
#include "velox/common/memory/ArbitrationParticipant.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
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
static const std::string arbitratorKind("TEST");

class TestArbitrator : public MemoryArbitrator {
 public:
  explicit TestArbitrator(const Config& config)
      : MemoryArbitrator(
            {.kind = config.kind,
             .capacity = config.capacity,
             .extraConfigs = config.extraConfigs}) {}

  void addPool(const std::shared_ptr<MemoryPool>& /*unused*/) override {}

  void removePool(MemoryPool* /*unused*/) override {}

  bool growCapacity(MemoryPool* memoryPool, uint64_t requestBytes) override {
    VELOX_CHECK_LE(
        memoryPool->capacity() + requestBytes, memoryPool->maxCapacity());
    memoryPool->grow(requestBytes, requestBytes);
    return true;
  }

  uint64_t shrinkCapacity(uint64_t /*unused*/, bool /*unused*/, bool /*unused*/)
      override {
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
    return arbitratorKind;
  }
};

namespace {
constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;

constexpr uint64_t kMemoryCapacity = 512 * MB;
constexpr uint64_t kMemoryPoolReservedCapacity = 64 * MB;
constexpr uint64_t kMemoryPoolMinFreeCapacity = 32 * MB;
constexpr double kMemoryPoolMinFreeCapacityRatio = 0.25;
constexpr uint64_t kFastExponentialGrowthCapacityLimit = 256 * MB;
constexpr double kSlowCapacityGrowRatio = 0.25;

class MemoryReclaimer;

using ReclaimInjectionCallback =
    std::function<void(MemoryPool* pool, uint64_t targetByte)>;
using ArbitrationInjectionCallback = std::function<void()>;

struct Allocation {
  void* buffer{nullptr};
  size_t size{0};
};

class MockTask : public std::enable_shared_from_this<MockTask> {
 public:
  MockTask(MemoryManager* manager, uint64_t capacity)
      : root_(manager->addRootPool(
            fmt::format("TaskPool-{}", taskId_++),
            capacity)),
        pool_(root_->addLeafChild("MockOperator")) {}

  ~MockTask() {
    free();
  }

  class RootMemoryReclaimer : public memory::MemoryReclaimer {
   public:
    RootMemoryReclaimer(const std::shared_ptr<MockTask>& task) : task_(task) {}

    static std::unique_ptr<MemoryReclaimer> create(
        const std::shared_ptr<MockTask>& task) {
      return std::make_unique<RootMemoryReclaimer>(task);
    }

    bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
        const override {
      auto task = task_.lock();
      if (task == nullptr) {
        return false;
      }
      return memory::MemoryReclaimer::reclaimableBytes(pool, reclaimableBytes);
    }

    uint64_t reclaim(
        MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        Stats& stats) override {
      auto task = task_.lock();
      if (task == nullptr) {
        return 0;
      }
      return memory::MemoryReclaimer::reclaim(
          pool, targetBytes, maxWaitMs, stats);
    }

    void abort(MemoryPool* pool, const std::exception_ptr& error) override {
      auto task = task_.lock();
      if (task == nullptr) {
        return;
      }
      memory::MemoryReclaimer::abort(pool, error);
    }

   private:
    std::weak_ptr<MockTask> task_;
  };

  class LeafMemoryReclaimer : public memory::MemoryReclaimer {
   public:
    LeafMemoryReclaimer(
        std::shared_ptr<MockTask> task,
        bool reclaimable,
        ReclaimInjectionCallback reclaimInjectCb = nullptr,
        ArbitrationInjectionCallback arbitrationInjectCb = nullptr)
        : task_(task),
          reclaimable_(reclaimable),
          reclaimInjectCb_(std::move(reclaimInjectCb)),
          arbitrationInjectCb_(std::move(arbitrationInjectCb)) {}

    bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
        const override {
      if (!reclaimable_) {
        return false;
      }
      std::shared_ptr<MockTask> task = task_.lock();
      VELOX_CHECK_NOT_NULL(task);
      return task->reclaimableBytes(pool, reclaimableBytes);
    }

    uint64_t reclaim(
        MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t /*unused*/,
        Stats& stats) override {
      if (!reclaimable_) {
        return 0;
      }
      if (reclaimInjectCb_ != nullptr) {
        reclaimInjectCb_(pool, targetBytes);
      }
      std::shared_ptr<MockTask> task = task_.lock();
      VELOX_CHECK_NOT_NULL(task);
      const auto reclaimBytes = task->reclaim(pool, targetBytes);
      stats.reclaimedBytes += reclaimBytes;
      return reclaimBytes;
    }

    void abort(MemoryPool* pool, const std::exception_ptr& error) override {
      std::shared_ptr<MockTask> task = task_.lock();
      VELOX_CHECK_NOT_NULL(task);
      task->abort(pool, error);
    }

   private:
    std::weak_ptr<MockTask> task_;
    const bool reclaimable_;
    const ReclaimInjectionCallback reclaimInjectCb_;
    const ArbitrationInjectionCallback arbitrationInjectCb_;

    std::exception_ptr abortError_;
  };

  void setMemoryReclaimers(
      bool reclaimable,
      ReclaimInjectionCallback reclaimInjectCb,
      ArbitrationInjectionCallback arbitrationInjectCb) {
    root_->setReclaimer(RootMemoryReclaimer::create(shared_from_this()));
    pool_->setReclaimer(std::make_unique<LeafMemoryReclaimer>(
        shared_from_this(),
        reclaimable,
        std::move(reclaimInjectCb),
        std::move(arbitrationInjectCb)));
  }

  const std::shared_ptr<MemoryPool>& pool() const {
    return root_;
  }

  std::exception_ptr abortError() const {
    return abortError_;
  }

  uint64_t capacity() const {
    return root_->capacity();
  }

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
    size_t size{0};
    {
      std::lock_guard<std::mutex> l(mu_);
      VELOX_CHECK_EQ(allocations_.count(buffer), 1);
      size = allocations_[buffer];
      totalBytes_ -= size;
      allocations_.erase(buffer);
    }
    pool_->free(buffer, size);
  }

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const {
    std::lock_guard<std::mutex> l(mu_);
    VELOX_CHECK_EQ(pool.name(), pool_->name());
    reclaimableBytes = totalBytes_;
    return true;
  }

  uint64_t reclaim(MemoryPool* pool, uint64_t targetBytes) {
    VELOX_CHECK_GT(targetBytes, 0);
    ++numReclaims_;
    reclaimTargetBytes_.push_back(targetBytes);
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
    return bytesReclaimed;
  }

  void abort(MemoryPool* pool, const std::exception_ptr& error) {
    ++numAborts_;
    abortError_ = error;
    free();
  }

  struct Stats {
    uint64_t numReclaims;
    uint64_t numAborts;
    std::vector<uint64_t> reclaimTargetBytes;
  };

  Stats stats() const {
    Stats stats;
    stats.numReclaims = numReclaims_;
    stats.reclaimTargetBytes = reclaimTargetBytes_;
    stats.numAborts = numAborts_;
    return stats;
  }

 private:
  void free() {
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

  inline static std::atomic_int taskId_{0};

  const std::shared_ptr<MemoryPool> root_;
  const std::shared_ptr<MemoryPool> pool_;

  mutable std::mutex mu_;
  uint64_t totalBytes_{0};

  std::unordered_map<void*, size_t> allocations_;
  std::atomic_uint64_t numReclaims_{0};
  std::atomic_uint64_t numAborts_{0};
  std::vector<uint64_t> reclaimTargetBytes_;
  std::exception_ptr abortError_{nullptr};
};

class ArbitrationParticipantTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    SharedArbitrator::registerFactory();
    FLAGS_velox_memory_leak_check_enabled = true;
    TestValue::enable();
    MemoryArbitrator::Factory factory =
        [](const MemoryArbitrator::Config& config) {
          return std::make_unique<TestArbitrator>(config);
        };
    MemoryArbitrator::registerFactory(arbitratorKind, factory);
  }

  void SetUp() override {
    setupMemory();
  }

  void TearDown() override {}

  void setupMemory(int64_t memoryCapacity = kMemoryCapacity) {
    MemoryManagerOptions options;
    options.allocatorCapacity = memoryCapacity;
    options.arbitratorKind = arbitratorKind;
    options.checkUsageLeak = true;
    manager_ = std::make_unique<MemoryManager>(options);
  }

  std::shared_ptr<MockTask> createTask(
      int64_t capacity = 0,
      bool reclaimable = true,
      ReclaimInjectionCallback reclaimInjectCb = nullptr,
      ArbitrationInjectionCallback arbitrationInjectCb = nullptr) {
    if (capacity == 0) {
      capacity = manager_->capacity();
    }
    auto task = std::make_shared<MockTask>(manager_.get(), capacity);
    task->setMemoryReclaimers(
        reclaimable, reclaimInjectCb, arbitrationInjectCb);
    return task;
  }

  std::unique_ptr<MemoryManager> manager_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(4);
};

static ArbitrationParticipant::Config arbitrationConfig(
    uint64_t minCapacity = kMemoryPoolReservedCapacity,
    uint64_t fastExponentialGrowthCapacityLimit =
        kFastExponentialGrowthCapacityLimit,
    double slowCapacityGrowRatio = kSlowCapacityGrowRatio,
    uint64_t minFreeCapacity = kMemoryPoolMinFreeCapacity,
    double minFreeCapacityRatio = kMemoryPoolMinFreeCapacityRatio) {
  return ArbitrationParticipant::Config{
      minCapacity,
      fastExponentialGrowthCapacityLimit,
      slowCapacityGrowRatio,
      minFreeCapacity,
      minFreeCapacityRatio};
}

TEST_F(ArbitrationParticipantTest, config) {
  struct {
    uint64_t minCapacity;
    uint64_t fastExponentialGrowthCapacityLimit;
    double slowCapacityGrowRatio;
    uint64_t minFreeCapacity;
    double minFreeCapacityRatio;
    bool expectedError;
    std::string expectedToString;

    std::string debugString() const {
      return fmt::format(
          "minCapacity {}, fastExponentialGrowthCapacityLimit: {}, slowCapacityGrowRatio: {}, minFreeCapacity: {}, minFreeCapacityRatio: {}, expectedError: {}, expectedToString: {}",
          succinctBytes(minCapacity),
          succinctBytes(fastExponentialGrowthCapacityLimit),
          slowCapacityGrowRatio,
          succinctBytes(minFreeCapacity),
          minFreeCapacityRatio,
          expectedError,
          expectedToString);
    }
  } testSettings[] = {
      {1,
       1,
       0.1,
       1,
       0.1,
       false,
       "minCapacity 1, fastExponentialGrowthCapacityLimit 1, slowCapacityGrowRatio 0.1, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {1,
       0,
       0,
       1,
       0.1,
       false,
       "minCapacity 1, fastExponentialGrowthCapacityLimit 0, slowCapacityGrowRatio 0, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {1,
       0,
       0,
       0,
       0,
       false,
       "minCapacity 1, fastExponentialGrowthCapacityLimit 0, slowCapacityGrowRatio 0, minFreeCapacity 0, minFreeCapacityRatio 0"},
      {1,
       0,
       0,
       1,
       0.1,
       false,
       "minCapacity 1, fastExponentialGrowthCapacityLimit 0, slowCapacityGrowRatio 0, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {0,
       1,
       0.1,
       1,
       0.1,
       false,
       "minCapacity 0, fastExponentialGrowthCapacityLimit 1, slowCapacityGrowRatio 0.1, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {0,
       0,
       0,
       1,
       0.1,
       false,
       "minCapacity 0, fastExponentialGrowthCapacityLimit 0, slowCapacityGrowRatio 0, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {0,
       0,
       0,
       0,
       0,
       false,
       "minCapacity 0, fastExponentialGrowthCapacityLimit 0, slowCapacityGrowRatio 0, minFreeCapacity 0, minFreeCapacityRatio 0"},
      {0,
       0,
       0,
       1,
       0.1,
       false,
       "minCapacity 0, fastExponentialGrowthCapacityLimit 0, slowCapacityGrowRatio 0, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {1, 0, 0.1, 1, 0.1, true, ""},
      {1, 1, 0.1, 0, 0.1, true, ""},
      {1, 1, 0.1, 1, 0, true, ""},
      {1,
       1,
       2,
       1,
       0.1,
       false,
       "minCapacity 1, fastExponentialGrowthCapacityLimit 1, slowCapacityGrowRatio 2, minFreeCapacity 1, minFreeCapacityRatio 0.1"},
      {1, 1, -1, 1, 0.1, true, ""},
      {1, 1, 0.1, 1, 2, true, ""},
      {1, 1, 0.1, 1, -1, true, ""}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.expectedError) {
      VELOX_ASSERT_THROW(
          ArbitrationParticipant::Config(
              testData.minCapacity,
              testData.fastExponentialGrowthCapacityLimit,
              testData.slowCapacityGrowRatio,
              testData.minFreeCapacity,
              testData.minFreeCapacityRatio),
          "");
      continue;
    }
    const auto config = ArbitrationParticipant::Config(
        testData.minCapacity,
        testData.fastExponentialGrowthCapacityLimit,
        testData.slowCapacityGrowRatio,
        testData.minFreeCapacity,
        testData.minFreeCapacityRatio);
    ASSERT_EQ(testData.minCapacity, config.minCapacity);
    ASSERT_EQ(
        testData.fastExponentialGrowthCapacityLimit,
        config.fastExponentialGrowthCapacityLimit);
    ASSERT_EQ(testData.slowCapacityGrowRatio, config.slowCapacityGrowRatio);
    ASSERT_EQ(testData.minFreeCapacity, config.minFreeCapacity);
    ASSERT_EQ(testData.minFreeCapacityRatio, config.minFreeCapacityRatio);
    ASSERT_EQ(config.toString(), testData.expectedToString);
  }
}

TEST_F(ArbitrationParticipantTest, constructor) {
  auto task = createTask();
  const auto config = arbitrationConfig();
  auto participant = ArbitrationParticipant::create(10, task->pool(), &config);
  ASSERT_EQ(participant->id(), 10);
  ASSERT_EQ(participant->name(), task->pool()->name());
  ASSERT_EQ(participant->pool(), task->pool().get());
  ASSERT_EQ(participant->maxCapacity(), kMemoryCapacity);
  ASSERT_EQ(participant->minCapacity(), kMemoryPoolReservedCapacity);
  ASSERT_EQ(participant->capacity(), 0);
  ASSERT_FALSE(participant->hasRunningOp());
  ASSERT_EQ(participant->numWaitingOps(), 0);
  ASSERT_THAT(
      participant->stats().toString(),
      ::testing::StartsWith(
          "numRequests: 0, numReclaims: 0, numShrinks: 0, numGrows: 0, reclaimedBytes: 0B, growBytes: 0B, aborted: false"));

  {
    auto scopedParticipant = participant->lock().value();
    ASSERT_EQ(scopedParticipant->id(), 10);
    ASSERT_EQ(scopedParticipant->name(), task->pool()->name());
    ASSERT_EQ(scopedParticipant->pool(), task->pool().get());
    ASSERT_EQ(scopedParticipant->maxCapacity(), kMemoryCapacity);
    ASSERT_EQ(scopedParticipant->minCapacity(), kMemoryPoolReservedCapacity);
    ASSERT_EQ(scopedParticipant->capacity(), 0);

    task.reset();

    ASSERT_EQ(scopedParticipant->capacity(), 0);
    ASSERT_EQ(participant->capacity(), 0);
  }

  ASSERT_FALSE(participant->lock().has_value());
}

TEST_F(ArbitrationParticipantTest, getGrowTargets) {
  struct {
    uint64_t minCapacity;
    uint64_t fastExponentialGrowthCapacityLimit;
    double slowCapacityGrowRatio;
    uint64_t capacity;
    uint64_t requestBytes;
    uint64_t expectedMaxGrowTarget;
    uint64_t expectedMinGrowTarget;

    std::string debugString() const {
      return fmt::format(
          "minCapacity {}, fastExponentialGrowthCapacityLimit {}, slowCapacityGrowRatio {}, capacity {}, requestBytes {}, expectedMaxGrowTarget {}, expectedMinGrowTarget {}",
          succinctBytes(minCapacity),
          succinctBytes(fastExponentialGrowthCapacityLimit),
          slowCapacityGrowRatio,
          succinctBytes(capacity),
          succinctBytes(requestBytes),
          succinctBytes(expectedMaxGrowTarget),
          succinctBytes(expectedMinGrowTarget));
    }
  } testSettings[] = {
      // Without exponential growth.
      {0, 0, 0.0, 0, 1 << 20, 1 << 20, 0},
      {0, 0, 0.0, 32 << 20, 1 << 20, 1 << 20, 0},
      {0, 0, 0.0, 32 << 20, 32 << 20, 32 << 20, 0},
      // Fast growth.
      {0, 16 << 20, 1.0, 0, 1 << 20, 1 << 20, 0},
      {0, 16 << 20, 1.0, 1 << 20, 1 << 20, 1 << 20, 0},
      {0, 16 << 20, 1.0, 2 << 20, 1 << 20, 2 << 20, 0},
      {0, 16 << 20, 1.0, 4 << 20, 1 << 20, 4 << 20, 0},
      {0, 16 << 20, 1.0, 8 << 20, 1 << 20, 8 << 20, 0},
      {0, 16 << 20, 1.0, 12 << 20, 1 << 20, 12 << 20, 0},
      {0, 16 << 20, 1.0, 16 << 20, 1 << 20, 16 << 20, 0},
      {0, 16 << 20, 1.0, 12 << 20, 23 << 20, 23 << 20, 0},
      // Slow growth.
      {0, 16 << 20, 1.0, 24 << 20, 1 << 20, 24 << 20, 0},
      {0, 16 << 20, 2.0, 24 << 20, 1 << 20, 48 << 20, 0},
      {0, 16 << 20, 100.0, 24 << 20, 1 << 20, kMemoryCapacity - (24 << 20), 0},
      {0, 16 << 20, 0.1, 24 << 20, 1 << 20, uint64_t((24 << 20) * 0.1), 0},
      // With min capacity.
      // Without exponential growth.
      {4 << 20, 0, 0.0, 0, 1 << 20, 4 << 20, 4 << 20},
      {4 << 20, 0, 0.0, 32 << 20, 1 << 20, 1 << 20, 0},
      {4 << 20, 0, 0.0, 32 << 20, 32 << 20, 32 << 20, 0},
      {64 << 20, 0, 0.0, 32 << 20, 1 << 20, 32 << 20, 32 << 20},
      {64 << 20, 0, 0.0, 32 << 20, 32 << 20, 32 << 20, 32 << 20},
      {48 << 20, 0, 0.0, 32 << 20, 32 << 20, 32 << 20, 16 << 20},
      // Fast growth.
      {1 << 20, 16 << 20, 1.0, 0, 1 << 20, 1 << 20, 1 << 20},
      {1 << 20, 16 << 20, 1.0, 0, 2 << 20, 2 << 20, 1 << 20},
      {4 << 20, 16 << 20, 1.0, 0, 1 << 20, 4 << 20, 4 << 20},
      {1 << 20, 16 << 20, 1.0, 1 << 20, 1 << 20, 1 << 20, 0},
      {1 << 20, 16 << 20, 1.0, 2 << 20, 1 << 20, 2 << 20, 0},
      {2 << 20, 16 << 20, 1.0, 2 << 20, 1 << 20, 2 << 20, 0},
      {4 << 20, 16 << 20, 1.0, 2 << 20, 1 << 20, 2 << 20, 2 << 20},
      {8 << 20, 16 << 20, 1.0, 2 << 20, 1 << 20, 6 << 20, 6 << 20},
      {3 << 20, 16 << 20, 1.0, 4 << 20, 1 << 20, 4 << 20, 0},
      {4 << 20, 16 << 20, 1.0, 4 << 20, 1 << 20, 4 << 20, 0},
      {5 << 20, 16 << 20, 1.0, 4 << 20, 1 << 20, 4 << 20, 1 << 20},
      {1 << 20, 16 << 20, 1.0, 12 << 20, 1 << 20, 12 << 20, 0},
      {12 << 20, 16 << 20, 1.0, 12 << 20, 1 << 20, 12 << 20, 0},
      {13 << 20, 16 << 20, 1.0, 12 << 20, 1 << 20, 12 << 20, 1 << 20},
      {24 << 20, 16 << 20, 1.0, 12 << 20, 1 << 20, 12 << 20, 12 << 20},
      {25 << 20, 16 << 20, 1.0, 12 << 20, 1 << 20, 13 << 20, 13 << 20},
      {1 << 20, 16 << 20, 1.0, 16 << 20, 1 << 20, 16 << 20, 0},
      {16 << 20, 16 << 20, 1.0, 16 << 20, 1 << 20, 16 << 20, 0},
      {17 << 20, 16 << 20, 1.0, 16 << 20, 1 << 20, 16 << 20, 1 << 20},
      {32 << 20, 16 << 20, 1.0, 16 << 20, 1 << 20, 16 << 20, 16 << 20},
      {64 << 20, 16 << 20, 1.0, 16 << 20, 1 << 20, 48 << 20, 48 << 20},
      {1 << 20, 16 << 20, 1.0, 12 << 20, 23 << 20, 23 << 20, 0},
      {12 << 20, 16 << 20, 1.0, 12 << 20, 23 << 20, 23 << 20, 0},
      {13 << 20, 16 << 20, 1.0, 12 << 20, 23 << 20, 23 << 20, 1 << 20},
      {23 << 20, 16 << 20, 1.0, 12 << 20, 23 << 20, 23 << 20, 11 << 20},
      {35 << 20, 16 << 20, 1.0, 12 << 20, 23 << 20, 23 << 20, 23 << 20},
      {48 << 20, 16 << 20, 1.0, 12 << 20, 23 << 20, 36 << 20, 36 << 20},
      // Slow growth.
      {1 << 20, 16 << 20, 1.0, 24 << 20, 1 << 20, 24 << 20, 0},
      {24 << 20, 16 << 20, 1.0, 24 << 20, 1 << 20, 24 << 20, 0},
      {25 << 20, 16 << 20, 1.0, 24 << 20, 1 << 20, 24 << 20, 1 << 20},
      {48 << 20, 16 << 20, 1.0, 24 << 20, 1 << 20, 24 << 20, 24 << 20},
      {47 << 20, 16 << 20, 1.0, 24 << 20, 1 << 20, 24 << 20, 23 << 20},
      {64 << 20, 16 << 20, 1.0, 24 << 20, 1 << 20, 40 << 20, 40 << 20},
      {1 << 20, 16 << 20, 2.0, 24 << 20, 1 << 20, 48 << 20, 0},
      {36 << 20, 16 << 20, 2.0, 24 << 20, 1 << 20, 48 << 20, 12 << 20},
      {72 << 20, 16 << 20, 2.0, 24 << 20, 1 << 20, 48 << 20, 48 << 20},
      {96 << 20, 16 << 20, 2.0, 24 << 20, 1 << 20, 72 << 20, 72 << 20},
      {1 << 20,
       16 << 20,
       100.0,
       24 << 20,
       1 << 20,
       kMemoryCapacity - (24 << 20),
       0},
      {36 << 20,
       16 << 20,
       100.0,
       24 << 20,
       1 << 20,
       kMemoryCapacity - (24 << 20),
       12 << 20},
      {1 << 20,
       16 << 20,
       0.1,
       24 << 20,
       1 << 20,
       uint64_t((24 << 20) * 0.1),
       0},
      {24 << 20,
       16 << 20,
       0.1,
       24 << 20,
       1 << 20,
       uint64_t((24 << 20) * 0.1),
       0}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto task = createTask(kMemoryCapacity);
    const auto config = arbitrationConfig(
        testData.minCapacity,
        testData.fastExponentialGrowthCapacityLimit,
        testData.slowCapacityGrowRatio);
    auto participant =
        ArbitrationParticipant::create(10, task->pool(), &config);
    auto scopedParticipant = participant->lock().value();
    scopedParticipant->shrink(/*reclaimFromAll=*/true);
    ASSERT_EQ(scopedParticipant->capacity(), 0);
    void* buffer = task->allocate(testData.capacity);
    SCOPE_EXIT {
      task->free(buffer);
    };
    ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);
    uint64_t maxGrowBytes{0};
    uint64_t minGrowBytes{0};
    scopedParticipant->getGrowTargets(
        testData.requestBytes, maxGrowBytes, minGrowBytes);
    ASSERT_EQ(maxGrowBytes, testData.expectedMaxGrowTarget);
    ASSERT_EQ(minGrowBytes, testData.expectedMinGrowTarget);

    // Test operation corresponding API.
    ArbitrationOperation op(
        std::move(scopedParticipant), testData.requestBytes, 1 << 30);
    op.start();
    ASSERT_EQ(op.maxGrowBytes(), 0);
    ASSERT_EQ(op.minGrowBytes(), 0);
    ASSERT_EQ(op.requestBytes(), testData.requestBytes);
    op.setGrowTargets();
    ASSERT_EQ(op.requestBytes(), testData.requestBytes);
    ASSERT_EQ(op.maxGrowBytes(), testData.expectedMaxGrowTarget);
    ASSERT_EQ(op.minGrowBytes(), testData.expectedMinGrowTarget);
    // Can't set grow targets twice.
    VELOX_ASSERT_THROW(
        op.setGrowTargets(),
        "Arbitration operation grow targets have already been set");
    op.finish();
  }
}

TEST_F(ArbitrationParticipantTest, reclaimableFreeCapacityAndShrink) {
  struct {
    uint64_t minCapacity;
    uint64_t minFreeCapacity;
    double minFreeCapacityRatio;
    uint64_t capacity;
    uint64_t usedBytes;
    uint64_t peakBytes;
    uint64_t expectedFreeCapacity;

    std::string debugString() const {
      return fmt::format(
          "minCapacity {}, minFreeCapacity {}, minFreeCapacityRatio {}, capacity {}, usedBytes {}, peakBytes {}, expectedFreeCapacity {}",
          succinctBytes(minCapacity),
          succinctBytes(minFreeCapacity),
          minFreeCapacityRatio,
          succinctBytes(capacity),
          succinctBytes(usedBytes),
          succinctBytes(peakBytes),
          succinctBytes(expectedFreeCapacity));
    }
  } testSettings[] = {
      {128 << 20, 0, 0.0, 128 << 20, 0, 0, 0},
      {128 << 20, 0, 0.0, 128 << 20, 0, 32 << 20, 128 << 20},
      {128 << 20, 0, 0.0, 128 << 20, 32 << 20, 0, 0},
      {128 << 20, 0, 0.0, 128 << 20, 128 << 20, 0, 0},
      {128 << 20, 0, 0.0, 256 << 20, 256 << 20, 0, 0},
      {128 << 20, 0, 0.0, 256 << 20, 200 << 20, 0, 56 << 20},
      {128 << 20, 0, 0.0, 256 << 20, 32 << 20, 0, 128 << 20},
      {0, 32 << 20, 0.25, 128 << 20, 0, 0, 96 << 20},
      {0, 64 << 20, 0.25, 128 << 20, 0, 0, 96 << 20},
      {0, 32 << 20, 0.25, 256 << 20, 0, 0, 224 << 20},
      {0, 32 << 20, 0.25, 256 << 20, 0, 64 << 20, 256 << 20},
      {0, 32 << 20, 0.25, 128 << 20, 64 << 20, 0, 32 << 20},
      {0, 32 << 20, 0.25, 128 << 20, 96 << 20, 0, 0},
      {0, 32 << 20, 0.25, 128 << 20, 72 << 20, 0, 24 << 20},
      {0, 64 << 20, 0.25, 128 << 20, 64 << 20, 0, 32 << 20},
      {0, 64 << 20, 0.25, 128 << 20, 96 << 20, 0, 0},
      {0, 64 << 20, 0.25, 128 << 20, 72 << 20, 0, 24 << 20},
      {0, 32 << 20, 0.25, 256 << 20, 64 << 20, 0, 160 << 20},
      {0, 32 << 20, 0.25, 256 << 20, 96 << 20, 0, 128 << 20},
      {0, 32 << 20, 0.25, 256 << 20, 224 << 20, 0, 0},
      {128 << 20, 32 << 20, 0.25, 128 << 20, 0, 0, 0},
      {64 << 20, 32 << 20, 0.25, 128 << 20, 0, 0, 64 << 20},
      {96 << 20, 32 << 20, 0.25, 128 << 20, 0, 0, 32 << 20},
      {64 << 20, 64 << 20, 0.25, 128 << 20, 0, 0, 64 << 20},
      {64 << 20, 32 << 20, 0.5, 128 << 20, 0, 0, 64 << 20},
      {128 << 20, 32 << 20, 0.25, 128 << 20, 0, 32 << 20, 128 << 20},
      {128 << 20, 32 << 20, 0.25, 128 << 20, 64 << 20, 64 << 20, 0},
      {64 << 20, 32 << 20, 0.25, 128 << 20, 64 << 20, 64 << 20, 32 << 20},
      {96 << 20, 32 << 20, 0.25, 128 << 20, 64 << 20, 64 << 20, 32 << 20},
      {96 << 20, 32 << 20, 0.25, 256 << 20, 64 << 20, 64 << 20, 160 << 20}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    for (bool reclaimAll : {false, true}) {
      SCOPED_TRACE(fmt::format("reclaimAll {}", reclaimAll));
      auto task = createTask(kMemoryCapacity);
      const auto config = arbitrationConfig(
          testData.minCapacity,
          0,
          0.0,
          testData.minFreeCapacity,
          testData.minFreeCapacityRatio);
      auto participant =
          ArbitrationParticipant::create(10, task->pool(), &config);
      auto scopedParticipant = participant->lock().value();
      ASSERT_EQ(scopedParticipant->stats().numShrinks, 0);
      if (testData.peakBytes > 0) {
        void* buffer = task->allocate(testData.peakBytes);
        task->free(buffer);
        ASSERT_EQ(scopedParticipant->pool()->peakBytes(), testData.peakBytes);
      }

      scopedParticipant->shrink(/*reclaimFromAll=*/true);
      scopedParticipant->grow(testData.capacity, 0);
      ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);

      void* buffer{nullptr};
      if (testData.usedBytes > 0) {
        buffer = task->allocate(testData.usedBytes);
      }
      ASSERT_EQ(scopedParticipant->pool()->usedBytes(), testData.usedBytes);
      ASSERT_EQ(scopedParticipant->pool()->reservedBytes(), testData.usedBytes);

      ASSERT_EQ(
          scopedParticipant->reclaimableFreeCapacity(),
          testData.expectedFreeCapacity);

      const uint64_t prevReclaimedBytes =
          scopedParticipant->stats().reclaimedBytes;
      if (reclaimAll) {
        ASSERT_EQ(
            scopedParticipant->shrink(reclaimAll),
            testData.capacity - testData.usedBytes);
        ASSERT_EQ(
            scopedParticipant->stats().reclaimedBytes,
            prevReclaimedBytes + testData.capacity - testData.usedBytes);
      } else {
        ASSERT_EQ(
            scopedParticipant->shrink(reclaimAll),
            testData.expectedFreeCapacity);
        ASSERT_EQ(
            scopedParticipant->stats().reclaimedBytes,
            prevReclaimedBytes + testData.expectedFreeCapacity);
      }
      ASSERT_EQ(scopedParticipant->stats().numShrinks, 2);
      ASSERT_EQ(scopedParticipant->stats().numReclaims, 0);
      ASSERT_EQ(scopedParticipant->stats().numGrows, 1);
      ASSERT_GE(scopedParticipant->stats().durationUs, 0);
      ASSERT_FALSE(scopedParticipant->stats().aborted);

      if (buffer != nullptr) {
        task->free(buffer);
      }
    }
  }
}

TEST_F(ArbitrationParticipantTest, reclaimableUsedCapacityAndReclaim) {
  struct {
    uint64_t minCapacity;
    uint64_t minFreeCapacity;
    double minFreeCapacityRatio;
    uint64_t capacity;
    uint64_t usedBytes;
    uint64_t peakBytes;
    uint64_t expectedReclaimableUsedBytes;
    uint64_t expectedActualReclaimedBytes;
    uint64_t expectedUsedBytes;

    std::string debugString() const {
      return fmt::format(
          "minCapacity {}, minFreeCapacity {}, minFreeCapacityRatio {}, capacity {}, usedBytes {}, peakBytes {}, expectedReclaimableUsedBytes {}, expectedActualReclaimedBytes {}, expectedUsedBytes {}",
          succinctBytes(minCapacity),
          succinctBytes(minFreeCapacity),
          minFreeCapacityRatio,
          succinctBytes(capacity),
          succinctBytes(usedBytes),
          succinctBytes(peakBytes),
          succinctBytes(expectedReclaimableUsedBytes),
          succinctBytes(expectedActualReclaimedBytes),
          succinctBytes(expectedUsedBytes));
    }
  } testSettings[] = {
      {128 << 20, 0, 0.0, 128 << 20, 0, 0, 0, 0, 0},
      {128 << 20, 0, 0.0, 128 << 20, 0, 32 << 20, 0, 0, 0},
      {128 << 20, 0, 0.0, 128 << 20, 32 << 20, 0, 0, 0, 32 << 20},
      {64 << 20, 0, 0.0, 128 << 20, 96 << 20, 0, 64 << 20, 64 << 20, 32 << 20},
      {64 << 20, 0, 0.0, 128 << 20, 128 << 20, 0, 64 << 20, 64 << 20, 64 << 20},
      {0, 32 << 20, 0.25, 128 << 20, 0, 0, 0, 0},
      {0, 64 << 20, 0.25, 128 << 20, 0, 0, 0, 0},
      {0, 32 << 20, 0.25, 256 << 20, 0, 0, 0, 0},
      {0, 32 << 20, 0.25, 256 << 20, 0, 64 << 20, 0, 0},
      {0, 32 << 20, 0.25, 128 << 20, 96 << 20, 0, 96 << 20, 128 << 20, 0},
      {128 << 20, 32 << 20, 0.25, 128 << 20, 0, 0, 0, 0, 0},
      {128 << 20, 32 << 20, 0.25, 128 << 20, 64 << 20, 0, 0, 0, 64 << 20},
      {128 << 20, 32 << 20, 0.25, 128 << 20, 128 << 20, 0, 0, 0, 128 << 20},
      {64 << 20,
       32 << 20,
       0.25,
       128 << 20,
       64 << 20,
       0,
       64 << 20,
       128 << 20,
       0},
      {128 << 20,
       32 << 20,
       0.25,
       128 << 20,
       64 << 20,
       64 << 20,
       0,
       0,
       64 << 20},
      {64 << 20,
       32 << 20,
       0.25,
       128 << 20,
       64 << 20,
       64 << 20,
       64 << 20,
       128 << 20,
       0},
      {96 << 20,
       32 << 20,
       0.25,
       128 << 20,
       64 << 20,
       64 << 20,
       32 << 20,
       32 << 20,
       32 << 20},
      {32 << 20,
       32 << 20,
       0.5,
       256 << 20,
       256 << 20,
       0,
       224 << 20,
       192 << 20,
       32 << 20},
      {32 << 20,
       64 << 20,
       0.125,
       256 << 20,
       256 << 20,
       0,
       224 << 20,
       192 << 20,
       32 << 20}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto task = createTask(kMemoryCapacity);
    const auto config = arbitrationConfig(
        testData.minCapacity,
        0,
        0.0,
        testData.minFreeCapacity,
        testData.minFreeCapacityRatio);
    auto participant =
        ArbitrationParticipant::create(10, task->pool(), &config);
    auto scopedParticipant = participant->lock().value();
    if (testData.peakBytes > 0) {
      void* buffer = task->allocate(testData.peakBytes);
      task->free(buffer);
      ASSERT_EQ(scopedParticipant->pool()->peakBytes(), testData.peakBytes);
    }

    scopedParticipant->shrink(/*reclaimFromAll=*/true);
    scopedParticipant->grow(testData.capacity, 0);
    ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);

    if (testData.usedBytes > 0) {
      for (int i = 0; i < testData.usedBytes / MB; ++i) {
        task->allocate(MB);
      }
    }
    ASSERT_EQ(scopedParticipant->pool()->usedBytes(), testData.usedBytes);
    ASSERT_EQ(scopedParticipant->pool()->reservedBytes(), testData.usedBytes);

    ASSERT_EQ(
        scopedParticipant->reclaimableUsedCapacity(),
        testData.expectedReclaimableUsedBytes);

    const auto targetBytes = scopedParticipant->reclaimableUsedCapacity();
    const uint64_t prevReclaimedBytes =
        scopedParticipant->stats().reclaimedBytes;
    ASSERT_EQ(
        scopedParticipant->reclaim(targetBytes, 1'000'000),
        testData.expectedActualReclaimedBytes);
    ASSERT_EQ(
        scopedParticipant->pool()->usedBytes(), testData.expectedUsedBytes);

    if (targetBytes != 0) {
      ASSERT_EQ(scopedParticipant->stats().numShrinks, 2);
      ASSERT_EQ(scopedParticipant->stats().numReclaims, 1);
      ASSERT_EQ(scopedParticipant->stats().numGrows, 1);
      ASSERT_FALSE(scopedParticipant->stats().aborted);
    } else {
      ASSERT_EQ(scopedParticipant->stats().numShrinks, 1);
      ASSERT_EQ(scopedParticipant->stats().numReclaims, 0);
      ASSERT_EQ(scopedParticipant->stats().numGrows, 1);
      ASSERT_FALSE(scopedParticipant->stats().aborted);
    }
    ASSERT_EQ(
        scopedParticipant->stats().reclaimedBytes,
        prevReclaimedBytes + testData.expectedActualReclaimedBytes);
  }
}

TEST_F(ArbitrationParticipantTest, checkCapacityGrowth) {
  struct {
    uint64_t maxCapacity;
    uint64_t capacity;
    uint64_t requestBytes;
    bool expectedGrowth;

    std::string debugString() const {
      return fmt::format(
          "maxCapacity {}, capacity {}, requestBytes {}, expectedGrowth {}",
          succinctBytes(maxCapacity),
          succinctBytes(capacity),
          succinctBytes(requestBytes),
          expectedGrowth);
    }
  } testSettings[] = {
      {128 << 20, 32 << 20, 1 << 20, true},
      {128 << 20, 128 << 20, 1 << 20, false},
      {128 << 20, 64 << 20, 64 << 20, true},
      {128 << 20, 128 << 20, 0, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto task = createTask(testData.maxCapacity);
    const auto config = arbitrationConfig(0);
    auto participant =
        ArbitrationParticipant::create(10, task->pool(), &config);
    auto scopedParticipant = participant->lock().value();
    task->allocate(testData.capacity);
    ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);
    ASSERT_EQ(
        scopedParticipant->checkCapacityGrowth(testData.requestBytes),
        testData.expectedGrowth);
  }
}

TEST_F(ArbitrationParticipantTest, grow) {
  struct {
    uint64_t maxCapacity;
    uint64_t capacity;
    uint64_t usedBytes;
    uint64_t growthBytes;
    uint64_t reservationBytes;
    bool expectedFailure;
    uint64_t expectedReservationBytes;
    uint64_t expectedCapacityAfterGrowth;

    std::string debugString() const {
      return fmt::format(
          "maxCapacity {}, capacity {}, usedBytes {}, growthBytes {}, reservationBytes {}, expectedFailure {}, expectedReservationBytes {}, expectedCapacityAfterGrowth {}",
          succinctBytes(maxCapacity),
          succinctBytes(capacity),
          succinctBytes(usedBytes),
          succinctBytes(growthBytes),
          succinctBytes(reservationBytes),
          expectedFailure,
          succinctBytes(expectedReservationBytes),
          succinctBytes(expectedCapacityAfterGrowth));
    }
  } testSettings[] = {
      {256 << 20, 128 << 20, 0, 1 << 20, 0, false, 0, 129 << 20},
      {256 << 20, 128 << 20, 0, 256 << 20, 0, true, 0, 128 << 20},
      {256 << 20, 128 << 20, 0, 0 << 20, 192 << 20, true, 0, 128 << 20},
      {256 << 20, 128 << 20, 0, 32 << 20, 256 << 20, true, 0, 128 << 20},
      {256 << 20, 128 << 20, 0, 32 << 20, 32 << 20, false, 32 << 20, 160 << 20},
      {256 << 20, 128 << 20, 0, 32 << 20, 16 << 20, false, 16 << 20, 160 << 20},
      {256 << 20, 128 << 20, 0, 0, 16 << 20, false, 16 << 20, 128 << 20},
      {256 << 20, 128 << 20, 0, 0, 128 << 20, false, 128 << 20, 128 << 20},
      {256 << 20,
       128 << 20,
       96 << 20,
       0,
       16 << 20,
       false,
       112 << 20,
       128 << 20},
      {256 << 20, 128 << 20, 96 << 20, 0, 64 << 20, true, 96 << 20, 128 << 20},
      {256 << 20,
       128 << 20,
       96 << 20,
       8 << 20,
       64 << 20,
       true,
       96 << 20,
       128 << 20},
      {256 << 20,
       128 << 20,
       96 << 20,
       128 << 20,
       64 << 20,
       false,
       160 << 20,
       256 << 20},
      {256 << 20,
       128 << 20,
       96 << 20,
       256 << 20,
       64 << 20,
       true,
       96 << 20,
       128 << 20}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto task = createTask(testData.maxCapacity);
    const auto config = arbitrationConfig(0);
    auto participant =
        ArbitrationParticipant::create(10, task->pool(), &config);
    auto scopedParticipant = participant->lock().value();
    scopedParticipant->shrink(/*reclaimAll=*/true);
    scopedParticipant->grow(testData.capacity, 0);
    ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);

    if (testData.usedBytes > 0) {
      task->allocate(testData.usedBytes);
    }
    ASSERT_EQ(scopedParticipant->pool()->usedBytes(), testData.usedBytes);
    ASSERT_EQ(scopedParticipant->pool()->reservedBytes(), testData.usedBytes);

    const uint64_t prevGrowBytes = scopedParticipant->stats().growBytes;
    ASSERT_EQ(
        !testData.expectedFailure,
        scopedParticipant->grow(
            testData.growthBytes, testData.reservationBytes));
    ASSERT_EQ(
        testData.expectedReservationBytes,
        scopedParticipant->pool()->reservedBytes());
    ASSERT_EQ(
        scopedParticipant->capacity(), testData.expectedCapacityAfterGrowth);
    if (!testData.expectedFailure && testData.reservationBytes > 0) {
      static_cast<MemoryPoolImpl*>(scopedParticipant->pool())
          ->testingSetReservation(
              testData.expectedReservationBytes - testData.reservationBytes);
    }
    if (testData.expectedFailure) {
      ASSERT_EQ(scopedParticipant->stats().growBytes, prevGrowBytes);
    } else {
      ASSERT_EQ(
          scopedParticipant->stats().growBytes,
          prevGrowBytes + testData.growthBytes);
    }
  }
}

TEST_F(ArbitrationParticipantTest, shrink) {
  struct {
    uint64_t maxCapacity;
    uint64_t minCapacity;
    uint64_t capacity;
    uint64_t usedBytes;
    uint64_t expectedFreeCapacity;

    std::string debugString() const {
      return fmt::format(
          "maxCapacity {}, minCapacity {}, capacity {}, usedBytes {}, expectedFreeCapacity {}",
          succinctBytes(maxCapacity),
          succinctBytes(minCapacity),
          succinctBytes(capacity),
          succinctBytes(usedBytes),
          succinctBytes(expectedFreeCapacity));
    }
  } testSettings[] = {
      {256 << 20, 128 << 20, 0, 0, 0},
      {256 << 20, 128 << 20, 64 << 20, 0, 0},
      {256 << 20, 128 << 20, 64 << 20, 32 << 20, 0},
      {256 << 20, 128 << 20, 64 << 20, 64 << 20, 0},
      {256 << 20, 128 << 20, 128 << 20, 64 << 20, 0},
      {256 << 20, 128 << 20, 192 << 20, 64 << 20, 64 << 20},
      {256 << 20, 128 << 20, 256 << 20, 128 << 20, 128 << 20},
      {256 << 20, 128 << 20, 256 << 20, 0, 128 << 20},
      {256 << 20, 128 << 20, 192 << 20, 0, 64 << 20},
      {256 << 20, 128 << 20, 128 << 20, 0, 0}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    for (bool reclaimAll : {false, true}) {
      SCOPED_TRACE(fmt::format("reclaimAll {}", reclaimAll));

      auto task = createTask(testData.maxCapacity);
      const auto config =
          arbitrationConfig(testData.minCapacity, 0, 0.0, 0, 0.0);
      auto participant =
          ArbitrationParticipant::create(10, task->pool(), &config);
      auto scopedParticipant = participant->lock().value();
      scopedParticipant->shrink(/*reclaimAll=*/true);
      scopedParticipant->grow(testData.capacity, 0);
      ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);

      if (testData.usedBytes > 0) {
        task->allocate(testData.usedBytes);
      }
      ASSERT_EQ(scopedParticipant->pool()->usedBytes(), testData.usedBytes);
      ASSERT_EQ(scopedParticipant->pool()->reservedBytes(), testData.usedBytes);

      const uint64_t prevFreedBytes = scopedParticipant->stats().reclaimedBytes;
      const uint32_t prevNumShrunks = scopedParticipant->stats().numShrinks;
      if (reclaimAll) {
        ASSERT_EQ(
            scopedParticipant->shrink(reclaimAll),
            testData.capacity - testData.usedBytes);
        ASSERT_EQ(
            prevFreedBytes + testData.capacity - testData.usedBytes,
            scopedParticipant->stats().reclaimedBytes);
      } else {
        ASSERT_EQ(
            scopedParticipant->shrink(reclaimAll),
            testData.expectedFreeCapacity);
        ASSERT_EQ(
            prevFreedBytes + testData.expectedFreeCapacity,
            scopedParticipant->stats().reclaimedBytes);
      }
      ASSERT_EQ(prevNumShrunks + 1, scopedParticipant->stats().numShrinks);
    }
  }
}

TEST_F(ArbitrationParticipantTest, abort) {
  struct {
    uint64_t maxCapacity;
    uint64_t minCapacity;
    uint64_t capacity;
    uint64_t usedBytes;
    uint64_t expectedReclaimCapacity;

    std::string debugString() const {
      return fmt::format(
          "maxCapacity {}, minCapacity {}, capacity {}, usedBytes {}, expectedReclaimCapacity {}",
          succinctBytes(maxCapacity),
          succinctBytes(minCapacity),
          succinctBytes(capacity),
          succinctBytes(usedBytes),
          succinctBytes(expectedReclaimCapacity));
    }
  } testSettings[] = {
      {256 << 20, 128 << 20, 0, 0, 0},
      {256 << 20, 128 << 20, 128 << 20, 0, 128 << 20},
      {256 << 20, 128 << 20, 256 << 20, 0, 256 << 20},
      {256 << 20, 128 << 20, 64 << 20, 0, 64 << 20},
      {256 << 20, 128 << 20, 128 << 20, 64 << 20, 128 << 20},
      {256 << 20, 128 << 20, 128 << 20, 128 << 20, 128 << 20},
      {256 << 20, 128 << 20, 256 << 20, 128 << 20, 256 << 20},
      {256 << 20, 128 << 20, 256 << 20, 256 << 20, 256 << 20},
      {256 << 20, 128 << 20, 64 << 20, 32 << 20, 64 << 20},
      {256 << 20, 128 << 20, 64 << 20, 64 << 20, 64 << 20}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto task = createTask(testData.maxCapacity);
    const auto config = arbitrationConfig(testData.minCapacity, 0, 0.0, 0, 0.0);
    auto participant =
        ArbitrationParticipant::create(10, task->pool(), &config);
    auto scopedParticipant = participant->lock().value();
    scopedParticipant->shrink(/*reclaimAll=*/true);
    scopedParticipant->grow(testData.capacity, 0);
    ASSERT_EQ(scopedParticipant->capacity(), testData.capacity);

    if (testData.usedBytes > 0) {
      task->allocate(testData.usedBytes);
    }
    ASSERT_EQ(scopedParticipant->pool()->usedBytes(), testData.usedBytes);
    ASSERT_EQ(scopedParticipant->pool()->reservedBytes(), testData.usedBytes);

    ASSERT_FALSE(scopedParticipant->stats().aborted);
    ASSERT_FALSE(scopedParticipant->aborted());
    const uint64_t prevFreedBytes = scopedParticipant->stats().reclaimedBytes;
    const uint32_t prevNumShrunks = scopedParticipant->stats().numShrinks;
    const uint32_t prevNumReclaims = scopedParticipant->stats().numReclaims;
    const std::string abortReason = "test abort";
    try {
      VELOX_FAIL(abortReason);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_EQ(
          scopedParticipant->abort(std::current_exception()),
          testData.expectedReclaimCapacity);
    }
    ASSERT_TRUE(task->pool()->aborted());
    ASSERT_TRUE(scopedParticipant->stats().aborted);
    ASSERT_TRUE(scopedParticipant->aborted());
    ASSERT_EQ(
        scopedParticipant->stats().reclaimedBytes,
        prevFreedBytes + testData.expectedReclaimCapacity);
    ASSERT_EQ(scopedParticipant->stats().numShrinks, prevNumShrunks + 1);
    ASSERT_EQ(scopedParticipant->stats().numReclaims, prevNumReclaims);

    try {
      VELOX_FAIL(abortReason);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_EQ(scopedParticipant->abort(std::current_exception()), 0);
    }
    ASSERT_EQ(scopedParticipant->stats().numShrinks, prevNumShrunks + 1);
    ASSERT_EQ(scopedParticipant->stats().numReclaims, prevNumReclaims);
    ASSERT_TRUE(scopedParticipant->aborted());
    ASSERT_EQ(scopedParticipant->capacity(), 0);

    ASSERT_EQ(scopedParticipant->reclaim(MB, 1'000'000), 0);
    ASSERT_EQ(scopedParticipant->stats().numReclaims, prevNumReclaims + 1);
    ASSERT_EQ(scopedParticipant->stats().numShrinks, prevNumShrunks + 2);
  }
}

DEBUG_ONLY_TEST_F(ArbitrationParticipantTest, reclaimLock) {
  auto task = createTask(kMemoryCapacity);
  const auto config = arbitrationConfig();
  auto participant = ArbitrationParticipant::create(10, task->pool(), &config);
  const uint64_t allocatedBytes = 32 * MB;
  for (int i = 0; i < 32; ++i) {
    task->allocate(MB);
  }
  auto scopedParticipant = participant->lock().value();

  std::atomic_bool reclaim1WaitFlag{false};
  folly::EventCount reclaim1Wait;
  std::atomic_bool reclaim1ResumeFlag{false};
  folly::EventCount reclaim1Resume;
  std::atomic_bool reclaim2WaitFlag{false};
  folly::EventCount reclaim2Wait;
  std::atomic_bool reclaim2ResumeFlag{false};
  folly::EventCount reclaim2Resume;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::ArbitrationParticipant::reclaim",
      std::function<void(ArbitrationParticipant*)>(
          ([&](ArbitrationParticipant* /*unused*/) {
            if (!reclaim1WaitFlag.exchange(true)) {
              reclaim1Wait.notifyAll();
              reclaim1Resume.await([&]() { return reclaim1ResumeFlag.load(); });
              return;
            }
            if (!reclaim2WaitFlag.exchange(true)) {
              reclaim2Wait.notifyAll();
              reclaim1Resume.await([&]() { return reclaim1ResumeFlag.load(); });
              return;
            }
          })));

  std::atomic_bool abortWaitFlag{false};
  folly::EventCount abortWait;
  std::atomic_bool abortResumeFlag{false};
  folly::EventCount abortResume;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::ArbitrationParticipant::abortLocked",
      std::function<void(ArbitrationParticipant*)>(
          ([&](ArbitrationParticipant* /*unused*/) {
            if (!abortWaitFlag.exchange(true)) {
              abortWait.notifyAll();
              abortResume.await([&]() { return abortResumeFlag.load(); });
              return;
            }
          })));

  std::atomic_bool reclaim1CompletedFlag{false};
  folly::EventCount reclaim1CompletedWait;
  std::thread reclaimThread1([&]() {
    ASSERT_EQ(scopedParticipant->reclaim(MB, 1'000'000), 0);
    reclaim1CompletedFlag = true;
    reclaim1CompletedWait.notifyAll();
  });
  reclaim1Wait.await([&]() { return reclaim1WaitFlag.load(); });

  std::atomic_bool abortCompletedFlag{false};
  folly::EventCount abortCompletedWait;
  std::thread abortThread([&]() {
    const std::string abortReason = "test abort";
    try {
      VELOX_FAIL(abortReason);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_EQ(scopedParticipant->abort(std::current_exception()), 32 * MB);
    }
    abortCompletedFlag = true;
    abortCompletedWait.notifyAll();
  });

  std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
  ASSERT_FALSE(reclaim1CompletedFlag);
  ASSERT_FALSE(abortWaitFlag);

  reclaim1ResumeFlag = true;
  reclaim1Resume.notifyAll();
  reclaim1CompletedWait.await([&]() { return reclaim1CompletedFlag.load(); });
  reclaimThread1.join();

  abortWait.await([&]() { return abortWaitFlag.load(); });

  std::atomic_bool reclaim2CompletedFlag{false};
  folly::EventCount reclaim2CompletedWait;
  std::thread reclaimThread2([&]() {
    ASSERT_EQ(scopedParticipant->reclaim(MB, 1'000'000), 0);
    reclaim2CompletedFlag = true;
    reclaim2CompletedWait.notifyAll();
  });

  std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
  ASSERT_FALSE(abortCompletedFlag);
  ASSERT_FALSE(reclaim2WaitFlag);

  abortResumeFlag = true;
  abortResume.notifyAll();
  abortCompletedWait.await([&]() { return abortCompletedFlag.load(); });
  abortThread.join();

  reclaim2ResumeFlag = true;
  reclaim2Resume.notifyAll();
  reclaim2CompletedWait.await([&]() { return reclaim2CompletedFlag.load(); });
  reclaimThread2.join();

  ASSERT_TRUE(task->pool()->aborted());
  ASSERT_TRUE(task->abortError() != nullptr);
  ASSERT_TRUE(scopedParticipant->aborted());
  ASSERT_EQ(scopedParticipant->capacity(), 0);
  ASSERT_EQ(scopedParticipant->pool()->usedBytes(), 0);
  ASSERT_EQ(scopedParticipant->stats().numReclaims, 2);
  ASSERT_EQ(scopedParticipant->stats().numShrinks, 3);
  ASSERT_EQ(scopedParticipant->stats().reclaimedBytes, 32 << 20);
}

DEBUG_ONLY_TEST_F(ArbitrationParticipantTest, waitForReclaimOrAbort) {
  struct {
    uint64_t waitTimeUs;
    bool pendingReclaim;
    uint64_t reclaimWaitMs{0};
    bool expectedTimeout;

    std::string debugString() const {
      return fmt::format(
          "waitTime {}, pendingReclaim {}, reclaimWait {}, expectedTimeout {}",
          succinctMicros(waitTimeUs),
          pendingReclaim,
          succinctMillis(reclaimWaitMs),
          expectedTimeout);
    }
  } testSettings[] = {
      {0, true, 1'000, true},
      {0, false, 1'000, true},
      {1'000'000, true, 1'000, false},
      {1'000'000, true, 1'000, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::atomic_bool reclaimWaitFlag{false};
    folly::EventCount reclaimWait;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::ArbitrationParticipant::reclaim",
        std::function<void(ArbitrationParticipant*)>(
            ([&](ArbitrationParticipant* /*unused*/) {
              reclaimWaitFlag = true;
              reclaimWait.notifyAll();
              std::this_thread::sleep_for(
                  std::chrono::milliseconds(testData.reclaimWaitMs)); // NOLINT
            })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::ArbitrationParticipant::abortLocked",
        std::function<void(ArbitrationParticipant*)>(
            ([&](ArbitrationParticipant* /*unused*/) {
              reclaimWaitFlag = true;
              reclaimWait.notifyAll();
              std::this_thread::sleep_for(
                  std::chrono::milliseconds(testData.reclaimWaitMs)); // NOLINT
            })));

    auto task = createTask(kMemoryCapacity);
    const auto config = arbitrationConfig();
    auto participant =
        ArbitrationParticipant::create(10, task->pool(), &config);
    task->allocate(MB);
    auto scopedParticipant = participant->lock().value();

    std::thread reclaimThread([&]() {
      if (testData.pendingReclaim) {
        ASSERT_EQ(scopedParticipant->reclaim(MB, 1'000'000), MB);
      } else {
        const std::string abortReason = "test abort";
        try {
          VELOX_FAIL(abortReason);
        } catch (const VeloxRuntimeError& e) {
          ASSERT_EQ(scopedParticipant->abort(std::current_exception()), MB);
        }
      }
    });
    reclaimWait.await([&]() { return reclaimWaitFlag.load(); });
    ASSERT_EQ(
        scopedParticipant->waitForReclaimOrAbort(testData.waitTimeUs),
        !testData.expectedTimeout);
    reclaimThread.join();
  }
}

TEST_F(ArbitrationParticipantTest, capacityCheck) {
  auto task = createTask(256 << 20);
  const auto config = arbitrationConfig(512 << 20);
  VELOX_ASSERT_THROW(
      ArbitrationParticipant::create(0, task->pool(), &config),
      "The min capacity is larger than the max capacity for memory pool");
}

TEST_F(ArbitrationParticipantTest, arbitrationCandidate) {
  auto task = createTask(kMemoryCapacity);
  const auto config = arbitrationConfig(0, 0, 0.0, 0, 0.0);
  auto participant = ArbitrationParticipant::create(10, task->pool(), &config);

  auto scopedParticipant = participant->lock().value();
  scopedParticipant->shrink(/*reclaimAll=*/true);
  scopedParticipant->grow(32 << 20, 0);
  ASSERT_EQ(scopedParticipant->capacity(), 32 << 20);
  task->allocate(MB);
  ASSERT_EQ(scopedParticipant->pool()->reservedBytes(), MB);

  ArbitrationCandidate candidateWithFreeCapacityOnly(
      participant->lock().value(), /*freeCapacityOnly=*/true);
  ASSERT_EQ(
      candidateWithFreeCapacityOnly.participant->name(),
      scopedParticipant->name());
  ASSERT_EQ(candidateWithFreeCapacityOnly.reclaimableUsedCapacity, 0);
  ASSERT_EQ(candidateWithFreeCapacityOnly.reclaimableFreeCapacity, 31 << 20);
  ASSERT_EQ(
      candidateWithFreeCapacityOnly.toString(),
      "TaskPool-0 RECLAIMABLE_USED_CAPACITY 0B RECLAIMABLE_FREE_CAPACITY 31.00MB");

  ArbitrationCandidate candidate(
      participant->lock().value(), /*freeCapacityOnly=*/false);
  ASSERT_EQ(candidate.participant->name(), scopedParticipant->name());
  ASSERT_EQ(candidate.reclaimableUsedCapacity, MB);
  ASSERT_EQ(candidate.reclaimableFreeCapacity, 31 << 20);
  ASSERT_EQ(
      candidate.toString(),
      "TaskPool-0 RECLAIMABLE_USED_CAPACITY 1.00MB RECLAIMABLE_FREE_CAPACITY 31.00MB");
}

TEST_F(ArbitrationParticipantTest, arbitrationOperation) {
  auto task = createTask(kMemoryCapacity);
  const auto config = arbitrationConfig(0, 0, 0.0, 0, 0.0);
  const int participantId{10};
  auto participant =
      ArbitrationParticipant::create(participantId, task->pool(), &config);
  auto scopedParticipant = participant->lock().value();
  const int requestBytes = 1 << 20;
  const int opTimeoutMs = 1'000'000;
  ArbitrationOperation op(
      participant->lock().value(), requestBytes, opTimeoutMs);
  VELOX_ASSERT_THROW(
      ArbitrationOperation(participant->lock().value(), 0, opTimeoutMs), "");
  ASSERT_EQ(op.requestBytes(), requestBytes);
  ASSERT_FALSE(op.aborted());
  ASSERT_FALSE(op.hasTimeout());
  ASSERT_EQ(op.allocatedBytes(), 0);
  ASSERT_LE(op.timeoutMs(), opTimeoutMs);

  std::this_thread::sleep_for(std::chrono::milliseconds(1'000)); // NOLINT
  ASSERT_GE(op.executionTimeMs(), 1'000);
  ASSERT_LE(op.timeoutMs(), opTimeoutMs - 1'000);
  ASSERT_EQ(op.maxGrowBytes(), 0);
  ASSERT_EQ(op.minGrowBytes(), 0);
  ASSERT_EQ(op.localArbitrationWaitTimeUs(), 0);
  ASSERT_EQ(op.globalArbitrationWaitTimeUs(), 0);
  ASSERT_FALSE(op.hasTimeout());
  VELOX_ASSERT_THROW(op.setGrowTargets(), "");
  ASSERT_EQ(op.requestBytes(), requestBytes);
  ASSERT_EQ(op.maxGrowBytes(), 0);
  ASSERT_EQ(op.minGrowBytes(), 0);

  ASSERT_EQ(op.localArbitrationWaitTimeUs(), 0);
  ASSERT_EQ(op.globalArbitrationWaitTimeUs(), 0);

  ASSERT_EQ(op.state(), ArbitrationOperation::State::kInit);
  ASSERT_FALSE(scopedParticipant->hasRunningOp());
  ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);
  VELOX_ASSERT_THROW(op.setLocalArbitrationWaitTimeUs(2'000), "");
  VELOX_ASSERT_THROW(op.setGlobalArbitrationWaitTimeUs(2'000), "");
  op.start();
  op.setGrowTargets();
  ASSERT_EQ(op.requestBytes(), requestBytes);
  ASSERT_EQ(op.maxGrowBytes(), requestBytes);
  ASSERT_EQ(op.minGrowBytes(), 0);
  VELOX_ASSERT_THROW(op.setGrowTargets(), "");
  ASSERT_EQ(op.requestBytes(), requestBytes);
  ASSERT_EQ(op.maxGrowBytes(), requestBytes);
  ASSERT_EQ(op.minGrowBytes(), 0);

  ASSERT_TRUE(scopedParticipant->hasRunningOp());
  ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);
  ASSERT_EQ(op.state(), ArbitrationOperation::State::kRunning);

  VELOX_ASSERT_THROW(op.setLocalArbitrationWaitTimeUs(2'000), "");
  ASSERT_EQ(op.localArbitrationWaitTimeUs(), 0);
  op.setGlobalArbitrationWaitTimeUs(2'000);
  ASSERT_EQ(op.globalArbitrationWaitTimeUs(), 2'000);
  VELOX_ASSERT_THROW(op.setGlobalArbitrationWaitTimeUs(2'000), "");
  op.allocatedBytes() = op.maxGrowBytes();

  op.finish();
  ASSERT_EQ(op.state(), ArbitrationOperation::State::kFinished);
  ASSERT_FALSE(scopedParticipant->hasRunningOp());
  ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);
  VELOX_ASSERT_THROW(op.setLocalArbitrationWaitTimeUs(2'000), "");
  VELOX_ASSERT_THROW(op.setGlobalArbitrationWaitTimeUs(2'000), "");
  ASSERT_FALSE(op.hasTimeout());
  const auto execTimeMs = op.executionTimeMs();
  std::this_thread::sleep_for(std::chrono::milliseconds(1'000)); // NOLINT
  ASSERT_EQ(op.executionTimeMs(), execTimeMs);
  ASSERT_FALSE(op.hasTimeout());

  // Operation timeout.
  {
    ArbitrationOperation timedOutOp(participant->lock().value(), 1 << 20, 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(200)); // NOLINT
    ASSERT_TRUE(timedOutOp.hasTimeout());

    ArbitrationOperation noTimedoutOp(
        participant->lock().value(), 1 << 20, 100);
    noTimedoutOp.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200)); // NOLINT
    noTimedoutOp.finish();
    ASSERT_FALSE(noTimedoutOp.hasTimeout());
  }

  // Operation abort.
  {
    ArbitrationOperation abortOp(participant->lock().value(), 1 << 20, 100);
    ASSERT_FALSE(abortOp.aborted());
    try {
      VELOX_FAIL("abort op");
    } catch (const VeloxRuntimeError& e) {
      ASSERT_EQ(scopedParticipant->abort(std::current_exception()), 0);
    }
    ASSERT_TRUE(abortOp.aborted());

    ArbitrationOperation abortCheckOp(
        participant->lock().value(), 1 << 20, 100);
    ASSERT_TRUE(abortCheckOp.aborted());
  }
}

TEST_F(ArbitrationParticipantTest, arbitrationOperationWait) {
  auto task = createTask(kMemoryCapacity);
  const auto config = arbitrationConfig(0, 0, 0.0, 0, 0.0);
  auto participant = ArbitrationParticipant::create(10, task->pool(), &config);
  auto scopedParticipant = participant->lock().value();
  const int requestBytes = 1 << 20;
  const int opTimeoutMs = 1'000'000;
  ArbitrationOperation op1(
      participant->lock().value(), requestBytes, opTimeoutMs);
  ArbitrationOperation op2(
      participant->lock().value(), requestBytes, opTimeoutMs);
  ArbitrationOperation op3(
      participant->lock().value(), requestBytes, opTimeoutMs);
  ArbitrationOperation op4(participant->lock().value(), requestBytes, 1'000);
  ASSERT_FALSE(scopedParticipant->hasRunningOp());
  ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);

  op1.start();
  ASSERT_TRUE(scopedParticipant->hasRunningOp());
  ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);
  ASSERT_EQ(op1.state(), ArbitrationOperation::State::kRunning);

  std::thread op2Thread([&]() {
    ASSERT_TRUE(scopedParticipant->hasRunningOp());
    op2.start();
    ASSERT_TRUE(scopedParticipant->hasRunningOp());
    ASSERT_FALSE(op2.hasTimeout());
    ASSERT_EQ(scopedParticipant->numWaitingOps(), 2);
    ASSERT_EQ(op3.state(), ArbitrationOperation::State::kWaiting);
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // NOLINT
    ASSERT_EQ(scopedParticipant->numWaitingOps(), 2);
    ASSERT_EQ(op3.state(), ArbitrationOperation::State::kWaiting);
    op2.finish();
  });

  while (scopedParticipant->numWaitingOps() != 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }

  std::thread op3Thread([&]() {
    ASSERT_TRUE(scopedParticipant->hasRunningOp());
    op3.start();
    ASSERT_TRUE(scopedParticipant->hasRunningOp());
    ASSERT_FALSE(op3.hasTimeout());
    ASSERT_EQ(scopedParticipant->numWaitingOps(), 1);
    ASSERT_EQ(op4.state(), ArbitrationOperation::State::kWaiting);
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // NOLINT
    ASSERT_EQ(scopedParticipant->numWaitingOps(), 1);
    ASSERT_EQ(op4.state(), ArbitrationOperation::State::kWaiting);
    op3.finish();
  });

  while (scopedParticipant->numWaitingOps() != 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }

  std::thread op4Thread([&]() {
    ASSERT_TRUE(scopedParticipant->hasRunningOp());
    op4.start();
    ASSERT_TRUE(scopedParticipant->hasRunningOp());
    ASSERT_TRUE(op4.hasTimeout());
    ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // NOLINT
    op4.finish();
  });

  while (scopedParticipant->numWaitingOps() != 3) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }

  ASSERT_EQ(op2.state(), ArbitrationOperation::State::kWaiting);
  ASSERT_EQ(op2.state(), ArbitrationOperation::State::kWaiting);
  ASSERT_EQ(op2.state(), ArbitrationOperation::State::kWaiting);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  op1.finish();
  ASSERT_EQ(op1.state(), ArbitrationOperation::State::kFinished);
  ASSERT_FALSE(op1.hasTimeout());
  ASSERT_GE(op1.executionTimeMs(), 1'000);

  op2Thread.join();
  ASSERT_EQ(op2.state(), ArbitrationOperation::State::kFinished);
  ASSERT_GE(op2.executionTimeMs(), 1'000 + 500);

  op3Thread.join();
  ASSERT_EQ(op3.state(), ArbitrationOperation::State::kFinished);
  ASSERT_GE(op3.executionTimeMs(), 1'000 + 500 + 500);

  op4Thread.join();
  ASSERT_EQ(op4.state(), ArbitrationOperation::State::kFinished);
  ASSERT_GE(op4.executionTimeMs(), 1'000 + 500 + 500);

  ASSERT_FALSE(scopedParticipant->hasRunningOp());
  ASSERT_EQ(scopedParticipant->numWaitingOps(), 0);

  ASSERT_EQ(scopedParticipant->stats().numRequests, 4);
}

TEST_F(ArbitrationParticipantTest, arbitrationOperationFuzzerTest) {
  const int numThreads = 10;
  const int numOpsPerThread = 100;
  auto task = createTask(kMemoryCapacity);
  const auto config = arbitrationConfig(0, 0, 0.0, 0, 0.0);
  auto participant = ArbitrationParticipant::create(10, task->pool(), &config);

  std::vector<std::thread> arbitrationThreads;
  for (int i = 0; i < numThreads; ++i) {
    arbitrationThreads.emplace_back([&, i]() {
      folly::Random::DefaultGenerator rng;
      rng.seed(i);
      for (int j = 0; j < numOpsPerThread; ++j) {
        const int numExecutionTimeUs = folly::Random::rand32(0, 1'000, rng);
        ArbitrationOperation op(participant->lock().value(), 1 << 20, 1'000);
        op.start();
        std::this_thread::sleep_for(
            std::chrono::microseconds(numExecutionTimeUs)); // NOLINT
        op.finish();
      }
    });
  }
  for (auto& thread : arbitrationThreads) {
    thread.join();
  }

  ASSERT_EQ(participant->stats().numRequests, numThreads * numOpsPerThread);
}

TEST_F(ArbitrationParticipantTest, arbitrationOperationState) {
  ASSERT_EQ(
      ArbitrationOperation::stateName(ArbitrationOperation::State::kInit),
      "init");
  ASSERT_EQ(
      ArbitrationOperation::stateName(ArbitrationOperation::State::kWaiting),
      "waiting");
  ASSERT_EQ(
      ArbitrationOperation::stateName(ArbitrationOperation::State::kRunning),
      "running");
  ASSERT_EQ(
      ArbitrationOperation::stateName(ArbitrationOperation::State::kFinished),
      "finished");
  ASSERT_EQ(
      ArbitrationOperation::stateName(
          static_cast<ArbitrationOperation::State>(10)),
      "unknown state: 10");
}
} // namespace
} // namespace facebook::velox::memory
