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

#include <deque>

#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"

DECLARE_bool(velox_memory_leak_check_enabled);

using namespace ::testing;
using namespace facebook::velox::common::testutil;

namespace facebook::velox::memory {
namespace {
class MockLeafMemoryReclaimer;

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;

constexpr uint64_t kMemoryCapacity = 512 * MB;
constexpr uint64_t kInitMemoryPoolCapacity = 16 * MB;
constexpr uint64_t kMinMemoryPoolCapacityTransferSize = 8 * MB;

class MockMemoryReclaimer;
class MockMemoryOperator;

using ReclaimInjectionCallback =
    std::function<void(MemoryPool* pool, uint64_t targetByte)>;
using ArbitrationInjectionCallback = std::function<void()>;

struct Allocation {
  void* buffer;
  size_t size;
};

class MockQuery {
 public:
  MockQuery(MemoryManager* manager, uint64_t capacity)
      : root_(manager->addRootPool(
            fmt::format("RootPool-{}", poolId_++),
            capacity,
            true,
            MemoryReclaimer::create())) {
    rng_.seed(poolId_);
  }

  ~MockQuery();

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
      return ops_[folly::Random::rand32(rng_) % ops_.size()].get();
    } else {
      VELOX_CHECK_LT(index, ops_.size());
      return ops_[index].get();
    }
  }

 private:
  inline static std::atomic<int64_t> poolId_{0};
  const std::shared_ptr<MemoryPool> root_;
  folly::Random::DefaultGenerator rng_;
  std::vector<std::shared_ptr<MemoryPool>> pools_;
  std::vector<std::shared_ptr<MockMemoryOperator>> ops_;
};

class MockMemoryOperator {
 public:
  MockMemoryOperator() = default;

  ~MockMemoryOperator() {
    freeAll();
  }

  void* allocate(uint64_t bytes) {
    VELOX_CHECK_EQ(bytes % pool_->getAlignment(), 0);
    void* buffer = pool_->allocate(bytes);
    std::lock_guard<std::mutex> l(mu_);
    totalBytes_ += bytes;
    allocations_.emplace(buffer, bytes);
    VELOX_CHECK_EQ(allocations_.count(buffer), 1);
    ++numAllocs_;
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
      ++numFrees_;
    }
    pool_->free(buffer, size);
  }

  void freeAll() {
    std::unordered_map<void*, size_t> allocations;
    {
      std::lock_guard<std::mutex> l(mu_);
      for (auto entry : allocations_) {
        ++numFrees_;
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
      ++numFrees_;
    }
    pool_->free(allocation.buffer, allocation.size);
  }

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const {
    VELOX_CHECK_EQ(pool.name(), pool_->name());
    std::lock_guard<std::mutex> l(mu_);
    reclaimableBytes = totalBytes_;
    return true;
  }

  uint64_t reclaim(MemoryPool* pool, uint64_t targetBytes) {
    VELOX_CHECK_EQ(pool->name(), pool_->name());
    VELOX_CHECK_GT(targetBytes, 0);
    uint64_t bytesReclaimed{0};
    std::vector<Allocation> allocationsToFree;
    {
      std::lock_guard<std::mutex> l(mu_);
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
      ++numFrees_;
      pool_->free(allocation.buffer, allocation.size);
    }
    return pool_->shrink(targetBytes);
  }

  void setPool(MemoryPool* pool) {
    VELOX_CHECK_NOT_NULL(pool);
    VELOX_CHECK_NULL(pool_);
    pool_ = pool;
  }

  MemoryPool* pool() {
    return pool_;
  }

  uint64_t capacity() const {
    return pool_->capacity();
  }

  MockMemoryReclaimer* reclaimer();

 private:
  MemoryPool* pool_;
  mutable std::mutex mu_;
  uint64_t numAllocs_{0};
  uint64_t numFrees_{0};
  uint64_t totalBytes_{0};
  std::unordered_map<void*, size_t> allocations_;
};

class MockMemoryReclaimer : public MemoryReclaimer {
 public:
  explicit MockMemoryReclaimer(
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

  uint64_t reclaim(MemoryPool* pool, uint64_t targetBytes) noexcept override {
    ++numReclaims_;
    if (reclaimInjectCb_ != nullptr) {
      reclaimInjectCb_(pool, targetBytes);
    }
    if (!reclaimable_) {
      return 0;
    }
    reclaimTargetBytes_.push_back(targetBytes);
    return op_->reclaim(pool, targetBytes);
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

  struct Stats {
    uint64_t numEnterArbitrations;
    uint64_t numLeaveArbitrations;
    uint64_t numReclaims;
    std::vector<uint64_t> reclaimTargetBytes;
  };

  Stats stats() const {
    Stats stats;
    stats.numEnterArbitrations = numEnterArbitrations_;
    stats.numLeaveArbitrations = numLeaveArbitrations_;
    stats.numReclaims = numReclaims_;
    stats.reclaimTargetBytes = reclaimTargetBytes_;
    return stats;
  }

 private:
  const std::shared_ptr<MockMemoryOperator> op_;
  const bool reclaimable_;
  const ReclaimInjectionCallback reclaimInjectCb_;
  const ArbitrationInjectionCallback arbitrationInjectCb_;

  uint64_t numEnterArbitrations_{0};
  uint64_t numLeaveArbitrations_{0};
  uint64_t numReclaims_{0};
  std::vector<uint64_t> reclaimTargetBytes_;
};

MockMemoryReclaimer* MockMemoryOperator::reclaimer() {
  return static_cast<MockMemoryReclaimer*>(pool_->reclaimer());
}

MockMemoryOperator* MockQuery::addMemoryOp(
    bool isReclaimable,
    ReclaimInjectionCallback reclaimInjectCb,
    ArbitrationInjectionCallback arbitrationInjectCb) {
  ops_.push_back(std::make_shared<MockMemoryOperator>());
  pools_.push_back(root_->addLeafChild(
      std::to_string(poolId_++),
      true,
      std::make_shared<MockMemoryReclaimer>(
          ops_.back(),
          isReclaimable,
          std::move(reclaimInjectCb),
          std::move(arbitrationInjectCb))));
  ops_.back()->setPool(pools_.back().get());
  return ops_.back().get();
}

MockQuery::~MockQuery() {
  for (auto op : ops_) {
    op->freeAll();
  }
}

class MockSharedArbitrationTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    FLAGS_velox_memory_leak_check_enabled = true;
    TestValue::enable();
  }

  void SetUp() override {
    setupMemory();
  }

  void TearDown() override {
    clearQueries();
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
    manager_ = std::make_unique<MemoryManager>(options);
    ASSERT_EQ(manager_->arbitrator()->kind(), MemoryArbitrator::Kind::kShared);
    arbitrator_ = static_cast<SharedArbitrator*>(manager_->arbitrator());
  }

  std::shared_ptr<MockQuery> addQuery(int64_t capacity = 0) {
    return std::make_shared<MockQuery>(manager_.get(), capacity);
  }

  MockMemoryOperator* addMemoryOp(
      std::shared_ptr<MockQuery> query = nullptr,
      bool isReclaimable = true,
      ReclaimInjectionCallback reclaimInjectCb = nullptr,
      ArbitrationInjectionCallback arbitrationInjectCb = nullptr);

  void clearQueries() {
    queries_.clear();
  }

  std::unique_ptr<MemoryManager> manager_;
  SharedArbitrator* arbitrator_;
  std::vector<std::shared_ptr<MockQuery>> queries_;
};

MockMemoryOperator* MockSharedArbitrationTest::addMemoryOp(
    std::shared_ptr<MockQuery> query,
    bool isReclaimable,
    ReclaimInjectionCallback reclaimInjectCb,
    ArbitrationInjectionCallback arbitrationInjectCb) {
  if (query == nullptr) {
    queries_.push_back(addQuery());
    query = queries_.back();
  }
  return query->addMemoryOp(
      isReclaimable,
      std::move(reclaimInjectCb),
      std::move(arbitrationInjectCb));
}

void verifyArbitratorStats(
    const MemoryArbitrator::Stats& stats,
    uint64_t maxCapacityBytes,
    uint64_t freeCapacityBytes = 0,
    uint64_t numRequests = 0,
    uint64_t numFailures = 0,
    uint64_t numReclaimedBytes = 0,
    uint64_t numShrunkBytes = 0,
    uint64_t arbitrationTimeUs = 0,
    uint64_t queueTimeUs = 0) {
  ASSERT_EQ(stats.numRequests, numRequests);
  ASSERT_EQ(stats.numFailures, numFailures);
  ASSERT_EQ(stats.numReclaimedBytes, numReclaimedBytes);
  ASSERT_EQ(stats.numShrunkBytes, numShrunkBytes);
  ASSERT_GE(stats.arbitrationTimeUs, arbitrationTimeUs);
  ASSERT_GE(stats.queueTimeUs, queueTimeUs);
  ASSERT_EQ(stats.freeCapacityBytes, freeCapacityBytes);
  ASSERT_EQ(stats.maxCapacityBytes, maxCapacityBytes);
}

void verifyReclaimerStats(
    const MockMemoryReclaimer::Stats& stats,
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
  std::vector<std::shared_ptr<MockQuery>> queries;
  for (int i = 0; i <= kMemoryCapacity / kInitMemoryPoolCapacity; ++i) {
    uint64_t poolCapacity = kMemoryCapacity;
    if (i % 3 == 0) {
      poolCapacity = 0;
    } else {
      poolCapacity = kMaxMemory;
    }
    auto query = addQuery(i % 2 ? 0 : kMemoryCapacity);
    ASSERT_NE(query->pool()->reclaimer(), nullptr);
    if (i < kMemoryCapacity / kInitMemoryPoolCapacity) {
      ASSERT_EQ(query->capacity(), kInitMemoryPoolCapacity);
    } else {
      ASSERT_EQ(query->capacity(), 0);
    }
    queries.push_back(std::move(query));
  }
  auto stats = arbitrator_->stats();
  verifyArbitratorStats(stats, kMemoryCapacity);
  queries.clear();
  stats = arbitrator_->stats();
  verifyArbitratorStats(stats, kMemoryCapacity, kMemoryCapacity);
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
      (kMemoryCapacity - kInitMemoryPoolCapacity) /
          kMinMemoryPoolCapacityTransferSize);

  verifyReclaimerStats(
      memOp->reclaimer()->stats(),
      0,
      (kMemoryCapacity - kInitMemoryPoolCapacity) /
          kMinMemoryPoolCapacityTransferSize);

  clearQueries();
  verifyArbitratorStats(
      arbitrator_->stats(),
      kMemoryCapacity,
      kMemoryCapacity,
      (kMemoryCapacity - kInitMemoryPoolCapacity) /
          kMinMemoryPoolCapacityTransferSize);
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
  verifyReclaimerStats(arbitrateOp->reclaimer()->stats(), 0, 1);
  verifyArbitratorStats(
      arbitrator_->stats(), memCapacity, 260046848, 1, 1, 8388608, 8388608);
  ASSERT_EQ(arbitrator_->stats().queueTimeUs, 0);
}

TEST_F(MockSharedArbitrationTest, singlePoolGrowCapacityWithArbitration) {
  std::vector<bool> isLeafReclaimables = {true}; //{true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory();
    auto op = addMemoryOp(nullptr, isLeafReclaimable);
    const int allocateSize = MB;
    while (op->pool()->getCurrentBytes() < kMemoryCapacity) {
      op->allocate(allocateSize);
    }
    verifyArbitratorStats(arbitrator_->stats(), kMemoryCapacity, 0, 62);
    verifyReclaimerStats(op->reclaimer()->stats(), 0, 62);

    if (!isLeafReclaimable) {
      ASSERT_ANY_THROW(op->allocate(allocateSize));
      verifyArbitratorStats(arbitrator_->stats(), kMemoryCapacity, 0, 75, 1);
      verifyReclaimerStats(op->reclaimer()->stats(), 1, 75);
      continue;
    }

    // Do more allocations to trigger arbitration.
    for (int i = 0; i < kMinMemoryPoolCapacityTransferSize / allocateSize;
         ++i) {
      op->allocate(allocateSize);
    }
    verifyArbitratorStats(
        arbitrator_->stats(), kMemoryCapacity, 0, 63, 0, 8388608);
    verifyReclaimerStats(op->reclaimer()->stats(), 1, 63);

    clearQueries();
    verifyArbitratorStats(
        arbitrator_->stats(), kMemoryCapacity, kMemoryCapacity, 63, 0, 8388608);
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateWithCapacityShrink) {
  std::vector<bool> isLeafReclaimables = {true, false};
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
    ASSERT_EQ(reclaimedOp->pool()->getCurrentBytes(), 0);
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

    clearQueries();
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateWithMemoryReclaim) {
  const uint64_t memoryCapacity = 256 * MB;
  const uint64_t minPoolCapacity = 8 * MB;
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    setupMemory(memoryCapacity, minPoolCapacity);
    auto* reclaimedOp = addMemoryOp(nullptr, isLeafReclaimable);
    const int allocateSize = 8 * MB;
    while (reclaimedOp->pool()->getCurrentBytes() < memoryCapacity) {
      reclaimedOp->allocate(allocateSize);
    }
    auto* arbitrateOp = addMemoryOp();
    if (!isLeafReclaimable) {
      ASSERT_ANY_THROW(arbitrateOp->allocate(allocateSize));
      ASSERT_EQ(arbitrator_->stats().numFailures, 1);
      continue;
    }
    arbitrateOp->allocate(allocateSize);

    verifyArbitratorStats(
        arbitrator_->stats(), memoryCapacity, 0, 32, 0, 8388608);

    verifyReclaimerStats(
        arbitrateOp->reclaimer()->stats(),
        0,
        1,
        kMinMemoryPoolCapacityTransferSize);

    verifyReclaimerStats(
        reclaimedOp->reclaimer()->stats(),
        1,
        31,
        kMinMemoryPoolCapacityTransferSize);
    clearQueries();
  }
}

TEST_F(MockSharedArbitrationTest, arbitrateBySelfMemoryReclaim) {
  const std::vector<bool> isLeafReclaimables = {true, false};
  for (const auto isLeafReclaimable : isLeafReclaimables) {
    SCOPED_TRACE(fmt::format("isLeafReclaimable {}", isLeafReclaimable));
    const uint64_t memCapacity = 128 * MB;
    setupMemory(memCapacity);
    std::shared_ptr<MockQuery> query = addQuery(kMemoryCapacity);
    auto* memOp = addMemoryOp(query, isLeafReclaimable);
    const int allocateSize = 8 * MB;
    while (memOp->pool()->getCurrentBytes() < memCapacity / 2) {
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
  const int numQueries = 8;
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
    for (int i = 0; i < numQueries; ++i) {
      auto* memOp = addMemoryOp();
      int allocationSize = testData.sameSize ? memCapacity / numQueries
                                             : minPoolCapacity +
              folly::Random::rand32(rng) %
                  ((memCapacity / numQueries) - minPoolCapacity);
      allocationSize = allocationSize / MB * MB;
      memOp->allocate(allocationSize);
      if (testData.freeCapacity) {
        memOp->freeAll();
        ASSERT_EQ(memOp->pool()->getCurrentBytes(), 0);
      }
      memOps.push_back(memOp);
    }

    auto* arbitrateOp = addMemoryOp();
    arbitrateOp->allocate(memCapacity);
    for (auto* memOp : memOps) {
      ASSERT_EQ(memOp->capacity(), 0);
    }
    ASSERT_EQ(arbitrator_->stats().queueTimeUs, 0);
    clearQueries();
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
      arbitrator_->stats(), memCapacity, 0, expectedArbitrationRequests);
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

TEST_F(MockSharedArbitrationTest, DISABLED_concurrentArbitrations) {
  const int numQueries = 10;
  const int numOpsPerQuery = 5;
  std::vector<std::shared_ptr<MockQuery>> queries;
  queries.reserve(numQueries);
  std::vector<MockMemoryOperator*> memOps;
  memOps.reserve(numQueries * numOpsPerQuery);
  for (int i = 0; i < numQueries; ++i) {
    queries.push_back(addQuery());
    for (int j = 0; j < numOpsPerQuery; ++j) {
      memOps.push_back(addMemoryOp(queries.back(), (j % 3) != 0));
    }
  }
  const int numAllocationsPerOp = 1000;
  std::vector<std::thread> memThreads;
  for (int i = 0; i < numQueries * numOpsPerQuery; ++i) {
    memThreads.emplace_back([&, i, memOp = memOps[i]]() {
      folly::Random::DefaultGenerator rng;
      rng.seed(i);
      for (int j = 0; j < numAllocationsPerOp; ++j) {
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
            // Ignore memory limit exception.
            ASSERT_TRUE(
                e.message().find("Exceeded memory cap") != std::string::npos);
          }
        }
      }
    });
  }
  for (auto& memThread : memThreads) {
    memThread.join();
  }
  queries.clear();
}

TEST_F(
    MockSharedArbitrationTest,
    DISABLED_concurrentArbitrationWithTransientRoots) {
  std::mutex mutex;
  std::vector<std::shared_ptr<MockQuery>> queries;
  queries.push_back(addQuery());
  queries.back()->addMemoryOp();

  const int numMemThreads = 20;
  const int numAllocationsPerQuery = 1000;
  std::vector<std::thread> memThreads;
  for (int i = 0; i < numMemThreads; ++i) {
    memThreads.emplace_back([&, i]() {
      folly::Random::DefaultGenerator rng;
      rng.seed(i);
      for (int j = 0; j < numAllocationsPerQuery; ++j) {
        std::shared_ptr<MockQuery> query;
        {
          std::lock_guard<std::mutex> l(mutex);
          const int index = folly::Random::rand32() % queries.size();
          query = queries[index];
        }
        if (folly::Random::oneIn(4, rng)) {
          if (folly::Random::oneIn(3, rng)) {
            query->memoryOp()->freeAll();
          } else {
            query->memoryOp()->free();
          }
        } else {
          const int allocationPages = AllocationTraits::numPages(
              folly::Random::rand32(rng) % (kMemoryCapacity / 8));
          try {
            query->memoryOp()->allocate(
                AllocationTraits::pageBytes(allocationPages));
          } catch (VeloxException& e) {
            // Ignore the memory capacity limit exception.
            ASSERT_TRUE(
                e.message().find("Exceeded memory cap") != std::string::npos);
          }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  const int numControlOps = 500;
  const int maxNumQueries = 64;
  std::thread controlThread([&]() {
    folly::Random::DefaultGenerator rng;
    rng.seed(1000);
    for (int i = 0; i < numControlOps; ++i) {
      {
        std::lock_guard<std::mutex> l(mutex);
        if ((queries.size() == 1) ||
            (queries.size() < maxNumQueries && folly::Random::oneIn(4, rng))) {
          queries.push_back(addQuery());
          queries.back()->addMemoryOp(!folly::Random::oneIn(3, rng));
        } else {
          const int deleteIndex = folly::Random::rand32(rng) % queries.size();
          queries.erase(queries.begin() + deleteIndex);
        }
      }
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }
  });

  for (auto& memThread : memThreads) {
    memThread.join();
  }
  controlThread.join();
}
} // namespace
} // namespace facebook::velox::memory
