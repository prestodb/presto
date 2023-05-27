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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <deque>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"

using namespace ::testing;

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;
constexpr int64_t GB = 1024L * MB;

namespace facebook::velox::memory {

class MemoryArbitrationTest : public testing::Test {};

TEST_F(MemoryArbitrationTest, stats) {
  MemoryArbitrator::Stats stats;
  stats.numRequests = 2;
  stats.numAborted = 3;
  stats.numFailures = 100;
  stats.queueTimeUs = 230'000;
  stats.arbitrationTimeUs = 1020;
  stats.numShrunkBytes = 100'000'000;
  stats.numReclaimedBytes = 10'000;
  ASSERT_EQ(
      stats.toString(),
      "STATS[numRequests 2 numAborted 3 numFailures 100 queueTime 230.00ms arbitrationTime 1.02ms shrunkMemory 95.37MB reclaimedMemory 9.77KB maxCapacity 0B freeCapacity 0B]");
}

TEST_F(MemoryArbitrationTest, kind) {
  struct {
    MemoryArbitrator::Kind kind;
    std::string expectedKindStr;

    std::string debugString() const {
      return fmt::format("kind:{}, expectedStr:{}", kind, expectedKindStr);
    }
  } testSettings[] = {
      {MemoryArbitrator::Kind::kNoOp, "NOOP"},
      {MemoryArbitrator::Kind::kShared, "SHARED"},
      {static_cast<MemoryArbitrator::Kind>(100), "UNKNOWN: 100"}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_EQ(
        MemoryArbitrator::kindString(testData.kind), testData.expectedKindStr);
  }
}

TEST_F(MemoryArbitrationTest, create) {
  const std::vector<MemoryArbitrator::Kind> kinds = {
      MemoryArbitrator::Kind::kNoOp,
      MemoryArbitrator::Kind::kShared,
      static_cast<MemoryArbitrator::Kind>(100)};
  for (const auto& kind : kinds) {
    MemoryArbitrator::Config config;
    config.capacity = 1 * GB;
    config.kind = kind;
    if (kind == MemoryArbitrator::Kind::kNoOp) {
      ASSERT_EQ(MemoryArbitrator::create(config), nullptr);
    } else if (kind == MemoryArbitrator::Kind::kShared) {
      auto arbitrator = MemoryArbitrator::create(config);
      ASSERT_EQ(arbitrator->kind(), kind);
    } else {
      VELOX_ASSERT_THROW(MemoryArbitrator::create(config), "");
    }
  }
}

class MemoryReclaimerTest : public testing::Test {
 protected:
  MemoryReclaimerTest() {
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;
  }

  void SetUp() override {}

  void TearDown() override {}

  folly::Random::DefaultGenerator rng_;
};

TEST_F(MemoryReclaimerTest, default) {
  struct {
    int numChildren;
    int numGrandchildren;
    bool doAllocation;

    std::string debugString() const {
      return fmt::format(
          "numChildren {} numGrandchildren {} doAllocation{}",
          numChildren,
          numGrandchildren,
          doAllocation);
    }
  } testSettings[] = {
      {0, 0, false},
      {0, 0, true},
      {1, 0, false},
      {1, 0, true},
      {100, 0, false},
      {100, 0, true},
      {1, 100, false},
      {1, 100, true},
      {10, 100, false},
      {10, 100, true},
      {100, 1, false},
      {100, 1, true},
      {0, 0, false},
      {0, 0, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::vector<std::shared_ptr<MemoryPool>> pools;
    auto pool = defaultMemoryManager().addRootPool(
        "shrinkAPIs", kMaxMemory, memory::MemoryReclaimer::create());
    pools.push_back(pool);

    struct Allocation {
      void* buffer;
      size_t size;
      MemoryPool* pool;
    };
    std::vector<Allocation> allocations;
    for (int i = 0; i < testData.numChildren; ++i) {
      const bool isLeaf = testData.numGrandchildren == 0;
      auto childPool = isLeaf
          ? pool->addLeafChild(
                std::to_string(i), true, memory::MemoryReclaimer::create())
          : pool->addAggregateChild(
                std::to_string(i), memory::MemoryReclaimer::create());
      pools.push_back(childPool);
      for (int j = 0; j < testData.numGrandchildren; ++j) {
        auto grandchild = childPool->addLeafChild(
            std::to_string(j), true, memory::MemoryReclaimer::create());
        pools.push_back(grandchild);
      }
    }
    for (auto& pool : pools) {
      ASSERT_NE(pool->reclaimer(), nullptr);
      if (pool->kind() == MemoryPool::Kind::kLeaf) {
        const size_t size = 1 + folly::Random::rand32(rng_) % 1024;
        void* buffer = pool->allocate(size);
        allocations.push_back({buffer, size, pool.get()});
      }
    }
    for (auto& pool : pools) {
      uint64_t reclaimableBytes;
      ASSERT_FALSE(pool->reclaimableBytes(reclaimableBytes));
      ASSERT_EQ(reclaimableBytes, 0);
      ASSERT_EQ(pool->reclaim(0), 0);
      ASSERT_EQ(pool->reclaim(100), 0);
      ASSERT_EQ(pool->reclaim(kMaxMemory), 0);
    }
    for (const auto& allocation : allocations) {
      allocation.pool->free(allocation.buffer, allocation.size);
    }
  }
}

class MockLeafMemoryReclaimer : public MemoryReclaimer {
 public:
  explicit MockLeafMemoryReclaimer(std::atomic<uint64_t>& totalUsedBytes)
      : totalUsedBytes_(totalUsedBytes) {}

  ~MockLeafMemoryReclaimer() override {
    VELOX_CHECK(allocations_.empty());
  }

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& bytes)
      const override {
    VELOX_CHECK_EQ(pool.name(), pool_->name());
    bytes = reclaimableBytes();
    return true;
  }

  uint64_t reclaim(MemoryPool* /*unused*/, uint64_t targetBytes) noexcept
      override {
    std::lock_guard<std::mutex> l(mu_);
    uint64_t reclaimedBytes{0};
    while (!allocations_.empty() &&
           ((targetBytes == 0) || (reclaimedBytes < targetBytes))) {
      free(allocations_.front());
      reclaimedBytes += allocations_.front().size;
      allocations_.pop_front();
    }
    return reclaimedBytes;
  }

  void setPool(MemoryPool* pool) {
    VELOX_CHECK_NOT_NULL(pool);
    VELOX_CHECK_NULL(pool_);
    pool_ = pool;
  }

  void addAllocation(void* buffer, size_t size) {
    std::lock_guard<std::mutex> l(mu_);
    allocations_.push_back({buffer, size});
    totalUsedBytes_ += size;
  }

 private:
  struct Allocation {
    void* buffer;
    size_t size;
  };

  void free(const Allocation& allocation) {
    pool_->free(allocation.buffer, allocation.size);
    totalUsedBytes_ -= allocation.size;
    VELOX_CHECK_GE(totalUsedBytes_, 0);
  }

  uint64_t reclaimableBytes() const {
    std::lock_guard<std::mutex> l(mu_);
    uint64_t sumBytes{0};
    for (const auto& allocation : allocations_) {
      sumBytes += allocation.size;
    }
    return sumBytes;
  }

  std::atomic<uint64_t>& totalUsedBytes_;
  mutable std::mutex mu_;
  MemoryPool* pool_{nullptr};
  std::deque<Allocation> allocations_;
};

TEST_F(MemoryReclaimerTest, mockReclaim) {
  const int numChildren = 10;
  const int numGrandchildren = 10;
  const int numAllocationsPerLeaf = 10;
  const int allocBytes = 10;
  std::atomic<uint64_t> totalUsedBytes{0};
  auto root = defaultMemoryManager().addRootPool(
      "mockReclaim", kMaxMemory, MemoryReclaimer::create());
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int i = 0; i < numChildren; ++i) {
    auto childPool =
        root->addAggregateChild(std::to_string(i), MemoryReclaimer::create());
    childPools.push_back(childPool);
    for (int j = 0; j < numGrandchildren; ++j) {
      auto grandchildPool = childPool->addLeafChild(
          std::to_string(j),
          true,
          std::make_unique<MockLeafMemoryReclaimer>(totalUsedBytes));
      childPools.push_back(grandchildPool);
      auto* reclaimer =
          static_cast<MockLeafMemoryReclaimer*>(grandchildPool->reclaimer());
      reclaimer->setPool(grandchildPool.get());
      for (int k = 0; k < numAllocationsPerLeaf; ++k) {
        void* buffer = grandchildPool->allocate(allocBytes);
        reclaimer->addAllocation(buffer, allocBytes);
      }
    }
  }
  ASSERT_EQ(
      numGrandchildren * numChildren * numAllocationsPerLeaf * allocBytes,
      totalUsedBytes);
  uint64_t reclaimableBytes;
  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(reclaimableBytes, totalUsedBytes);
  const int numReclaims = 5;
  const int numBytesToReclaim = allocBytes * 3;
  for (int iter = 0; iter < numReclaims; ++iter) {
    const auto reclaimedBytes = root->reclaim(numBytesToReclaim);
    ASSERT_EQ(reclaimedBytes, numBytesToReclaim);
    ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
    ASSERT_EQ(reclaimableBytes, totalUsedBytes);
  }
  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(totalUsedBytes, reclaimableBytes);
  ASSERT_EQ(root->reclaim(allocBytes + 1), 2 * allocBytes);
  ASSERT_EQ(root->reclaim(allocBytes - 1), allocBytes);
  const uint64_t expectedReclaimedBytes = totalUsedBytes;
  ASSERT_EQ(root->reclaim(0), expectedReclaimedBytes);
  ASSERT_EQ(totalUsedBytes, 0);
  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(reclaimableBytes, 0);
}

TEST_F(MemoryReclaimerTest, mockReclaimMoreThanAvailable) {
  const int numChildren = 10;
  const int numAllocationsPerLeaf = 10;
  const int allocBytes = 100;
  std::atomic<uint64_t> totalUsedBytes{0};
  auto root = defaultMemoryManager().addRootPool(
      "mockReclaimMoreThanAvailable", kMaxMemory, MemoryReclaimer::create());
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int i = 0; i < numChildren; ++i) {
    auto childPool = root->addLeafChild(
        std::to_string(i),
        true,
        std::make_unique<MockLeafMemoryReclaimer>(totalUsedBytes));
    childPools.push_back(childPool);
    auto* reclaimer =
        static_cast<MockLeafMemoryReclaimer*>(childPool->reclaimer());
    reclaimer->setPool(childPool.get());
    for (int j = 0; j < numAllocationsPerLeaf; ++j) {
      void* buffer = childPool->allocate(allocBytes);
      reclaimer->addAllocation(buffer, allocBytes);
    }
  }
  ASSERT_EQ(numChildren * numAllocationsPerLeaf * allocBytes, totalUsedBytes);
  uint64_t reclaimableBytes;
  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(reclaimableBytes, totalUsedBytes);
  const uint64_t expectedReclaimedBytes = totalUsedBytes;
  ASSERT_EQ(root->reclaim(totalUsedBytes + 100), expectedReclaimedBytes);
  ASSERT_EQ(totalUsedBytes, 0);
  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(reclaimableBytes, 0);
}

TEST_F(MemoryReclaimerTest, concurrentRandomMockReclaims) {
  auto root = defaultMemoryManager().addRootPool(
      "concurrentRandomMockReclaims", kMaxMemory, MemoryReclaimer::create());

  std::atomic<uint64_t> totalUsedBytes{0};
  const int numLeafPools = 50;
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  std::vector<std::shared_ptr<MemoryPool>> leafPools;
  for (int i = 0; i < numLeafPools / 5; ++i) {
    childPools.push_back(
        root->addAggregateChild(std::to_string(i), MemoryReclaimer::create()));
    for (int j = 0; j < 5; ++j) {
      auto leafPool = childPools.back()->addLeafChild(
          std::to_string(j),
          true,
          std::make_unique<MockLeafMemoryReclaimer>(totalUsedBytes));
      leafPools.push_back(leafPool);
      auto* reclaimer =
          static_cast<MockLeafMemoryReclaimer*>(leafPool->reclaimer());
      reclaimer->setPool(leafPool.get());
    }
  }

  const int32_t kNumReclaims = 100;
  std::thread reclaimerThread([&]() {
    for (int i = 0; i < kNumReclaims; ++i) {
      const uint64_t oldUsedBytes = totalUsedBytes;
      if (oldUsedBytes == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        continue;
      }
      uint64_t bytesToReclaim = folly::Random::rand64() % oldUsedBytes;
      if (folly::Random::oneIn(10, rng_)) {
        if (folly::Random::oneIn(2, rng_)) {
          bytesToReclaim = totalUsedBytes + 1000;
        } else {
          bytesToReclaim = 0;
        }
      }
      const auto reclaimedBytes = root->reclaim(bytesToReclaim);
      if (reclaimedBytes < bytesToReclaim) {
        ASSERT_GT(bytesToReclaim, oldUsedBytes);
      }
      if (bytesToReclaim == 0) {
        ASSERT_GE(reclaimedBytes, oldUsedBytes);
      }
    }
  });

  const int32_t kNumAllocThreads = numLeafPools;
  const int32_t kNumAllocsPerThread = 1'000;
  const int maxAllocSize = 256;

  std::vector<std::thread> allocThreads;
  allocThreads.reserve(kNumAllocThreads);
  for (size_t i = 0; i < kNumAllocThreads; ++i) {
    allocThreads.emplace_back([&, pool = leafPools[i]]() {
      auto* reclaimer =
          static_cast<MockLeafMemoryReclaimer*>(pool->reclaimer());
      for (int32_t op = 0; op < kNumAllocsPerThread; ++op) {
        const int allocSize =
            std::max<int>(1, folly::Random().rand32() % maxAllocSize);
        void* buffer = pool->allocate(allocSize);
        reclaimer->addAllocation(buffer, allocSize);
      }
    });
  }

  for (auto& th : allocThreads) {
    th.join();
  }
  reclaimerThread.join();

  uint64_t reclaimableBytes;
  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(reclaimableBytes, totalUsedBytes);

  root->reclaim(0);

  ASSERT_TRUE(root->reclaimableBytes(reclaimableBytes));
  ASSERT_EQ(reclaimableBytes, 0);
  ASSERT_EQ(totalUsedBytes, 0);
}
} // namespace facebook::velox::memory
