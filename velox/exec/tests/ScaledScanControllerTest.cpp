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

#include "velox/exec/ScaledScanController.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PlanNodeStats.h"

#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::velox::exec::test {
class ScaledScanControllerTestHelper {
 public:
  explicit ScaledScanControllerTestHelper(ScaledScanController* controller)
      : controller_(controller) {
    VELOX_CHECK_NOT_NULL(controller_);
  }

  uint64_t estimatedDriverUsage() const {
    return controller_->estimatedDriverUsage_;
  }

 private:
  ScaledScanController* const controller_;
};

class ScaledScanControllerTest : public OperatorTestBase {
 protected:
  std::shared_ptr<memory::MemoryPool> rootPool(uint64_t maxCapacity) {
    return memory::memoryManager()->addRootPool("", maxCapacity);
  }
};

TEST_F(ScaledScanControllerTest, basic) {
  struct {
    // Specifies the query max capacity.
    uint64_t queryCapacity;
    // Specifies the scale up memory usage ratio.
    double scaleUpMemoryUsageRatio;
    // Specifies how much memory has been used by the query when we test for
    // scale.
    uint64_t queryMemoryUsage;
    // Specifies how much memory has been used by the scan node when we test for
    // scale.
    uint64_t nodeMemoryUsage;
    // Specifies the peak memory usage of the scan node when we test for scale.
    uint64_t nodePeakMemoryUsage;
    // Specifies the number of scan drivers in the query.
    uint32_t numDrivers;
    // Specifies the updates of the memory usage of the scan drivers. The test
    // will invoke scale controller update API for each update in the order of
    // the update vector list.
    std::vector<std::pair<uint32_t, uint64_t>> driverMemoryUsageUpdates;
    // Specifies the expected number of running scan drivers after the update or
    // the end of the test.
    uint32_t expectedNumRunningDrivers;

    std::string debugString() const {
      return fmt::format(
          "queryCapacity {}, scaleUpMemoryUsageRatio {}, queryMemoryUsage {}, nodeMemoryUsage {}, nodePeakMemoryUsage {}, numDrivers {}, expectedNumRunningDrivers {}",
          succinctBytes(queryCapacity),
          scaleUpMemoryUsageRatio,
          succinctBytes(queryMemoryUsage),
          succinctBytes(nodeMemoryUsage),
          succinctBytes(nodePeakMemoryUsage),
          numDrivers,
          expectedNumRunningDrivers);
    }
  } testSettings[] = {
      // Test case that we can't scale up because of the query memory usage
      // ratio which is set to 0.
      // 1 scan drivers in total.
      {256 << 20, 0.0, 1 << 20, 1 << 20, 1 << 20, 1, {{0, 1 << 20}}, 1},
      // 4 scan drivers in total.
      {256 << 20,
       0.0,
       32 << 20,
       16 << 20,
       16 << 20,
       4,
       {{0, 1 << 20}, {0, 4 << 20}},
       1},

      // Test case that we can only scale up to two drivers as we only update
      // stats from one driver so can't scale up beyond two.
      {256 << 20,
       0.9,
       32 << 20,
       32 << 20,
       32 << 20,
       4,
       {{0, 4 << 20}, {0, 4 << 20}},
       2},

      // Test case that we can only scale up to three drivers as we only update
      // stats from two drivers so can't scale up beyond three.
      {256 << 20,
       0.9,
       32 << 20,
       32 << 20,
       32 << 20,
       4,
       {{0, 4 << 20}, {0, 4 << 20}, {1, 4 << 20}},
       3},

      // Test case that we can only scale up to six drivers as hit the query
      // memory usage ratio limit.
      {256 << 20,
       0.5,
       64 << 20,
       32 << 20,
       32 << 20,
       8,
       {{0, 16 << 20},
        {1, 16 << 20},
        {2, 16 << 20},
        {3, 16 << 20},
        {4, 16 << 20},
        {5, 16 << 20}},
       6},

      // Test cases that we can't scale up because of the peak memory usage of
      // scan node.
      {256 << 20, 0.5, 64 << 20, 32 << 20, 128 << 20, 8, {{0, 16 << 20}}, 1},
      {256 << 20, 0.5, 64 << 20, 32 << 20, 96 << 20, 8, {{0, 16 << 20}}, 1},

      // Test cases that we can't scale up because of the sum of the estimated
      // driver memory usage.
      {256 << 20,
       0.5,
       64 << 20,
       32 << 20,
       80 << 20,
       8,
       {{0, 16 << 20},
        {1, 16 << 20},
        {2, 16 << 20},
        {3, 16 << 20},
        {4, 16 << 20},
        {5, 16 << 20}},
       6}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto root = rootPool(testData.queryCapacity);
    auto node = root->addAggregateChild("test");
    auto pool = node->addLeafChild("test");
    if (testData.nodePeakMemoryUsage > 0) {
      void* tmpBuffer = pool->allocate(testData.nodePeakMemoryUsage);
      pool->free(tmpBuffer, testData.nodePeakMemoryUsage);
    }
    void* buffer{nullptr};
    if (testData.nodeMemoryUsage > 0) {
      buffer = pool->allocate(testData.nodeMemoryUsage);
    }
    SCOPE_EXIT {
      if (buffer != nullptr) {
        pool->free(buffer, testData.nodeMemoryUsage);
      }
    };

    auto otherNode = root->addAggregateChild("other");
    auto otherPool = otherNode->addLeafChild("other");
    void* otherBuffer{nullptr};
    if (testData.queryMemoryUsage > testData.nodeMemoryUsage) {
      otherBuffer = otherPool->allocate(
          testData.queryMemoryUsage - testData.nodeMemoryUsage);
    }
    SCOPE_EXIT {
      if (otherBuffer != nullptr) {
        otherPool->free(
            otherBuffer, testData.queryMemoryUsage - testData.nodeMemoryUsage);
      }
    };

    auto controller = std::make_shared<ScaledScanController>(
        node.get(), testData.numDrivers, testData.scaleUpMemoryUsageRatio);
    for (auto& [driverIdx, memoryUsage] : testData.driverMemoryUsageUpdates) {
      controller->updateAndTryScale(driverIdx, memoryUsage);
    }

    ASSERT_EQ(
        testData.expectedNumRunningDrivers,
        controller->stats().numRunningDrivers);

    std::vector<ContinueFuture> futures;
    futures.reserve(testData.numDrivers);
    for (auto i = 0; i < testData.numDrivers; ++i) {
      ContinueFuture future{ContinueFuture::makeEmpty()};
      if (i < testData.expectedNumRunningDrivers) {
        ASSERT_FALSE(controller->shouldStop(i, &future));
        ASSERT_FALSE(future.valid());
      } else {
        ASSERT_TRUE(controller->shouldStop(i, &future));
        ASSERT_TRUE(future.valid());
        futures.push_back(std::move(future));
      }
    }

    for (const auto& future : futures) {
      ASSERT_FALSE(future.isReady());
    }

    ASSERT_TRUE(controller->close());
    ASSERT_FALSE(controller->close());
    ASSERT_FALSE(controller->close());

    for (const auto& future : futures) {
      ASSERT_TRUE(future.isReady());
    }
  }
}

TEST_F(ScaledScanControllerTest, estimateDriverUsage) {
  auto root = rootPool(256 << 20);
  auto node = root->addAggregateChild("test");
  auto pool = node->addLeafChild("test");
  const int numDrivers{4};
  auto controller =
      std::make_shared<ScaledScanController>(node.get(), numDrivers, 0.5);
  std::vector<ContinueFuture> futures;
  futures.resize(numDrivers);
  for (auto i = 0; i < numDrivers; ++i) {
    ContinueFuture future{ContinueFuture::makeEmpty()};
    if (i == 0) {
      ASSERT_FALSE(controller->shouldStop(i, &future));
      ASSERT_FALSE(future.valid());
    } else {
      ASSERT_TRUE(controller->shouldStop(i, &future));
      ASSERT_TRUE(future.valid());
      futures.push_back(std::move(future));
    }
  }

  ScaledScanControllerTestHelper helper(controller.get());
  uint64_t expectedDriverMemoryUsage{0};
  for (auto i = 0; i < numDrivers - 1; ++i) {
    controller->updateAndTryScale(i, (i + 1) << 20);
    if (i == 0) {
      expectedDriverMemoryUsage = (i + 1) << 20;
    } else {
      expectedDriverMemoryUsage =
          (expectedDriverMemoryUsage * 3 + ((i + 1) << 20)) / 4;
    }
    ASSERT_EQ(helper.estimatedDriverUsage(), expectedDriverMemoryUsage);
  }

  controller.reset();
  for (auto& future : futures) {
    ASSERT_TRUE(future.isReady());
  }
}

TEST_F(ScaledScanControllerTest, error) {
  auto root = rootPool(256 << 20);
  auto node = root->addAggregateChild("test");

  VELOX_ASSERT_THROW(
      std::make_shared<ScaledScanController>(node.get(), 0, 0.5), "");
  VELOX_ASSERT_THROW(
      std::make_shared<ScaledScanController>(node.get(), 0, -1), "");
  VELOX_ASSERT_THROW(
      std::make_shared<ScaledScanController>(node.get(), 0, 2), "");
  VELOX_ASSERT_THROW(
      std::make_shared<ScaledScanController>(root.get(), 0, 2), "");
}

TEST_F(ScaledScanControllerTest, fuzzer) {
  auto root = rootPool(256 << 20);
  auto node = root->addAggregateChild("fuzzer");
  const int numDrivers{4};
  std::vector<std::shared_ptr<memory::MemoryPool>> pools;
  for (int i = 0; i < numDrivers; ++i) {
    pools.push_back(node->addLeafChild(fmt::format("fuzzer{}", i)));
  }
  auto controller =
      std::make_shared<ScaledScanController>(node.get(), numDrivers, 0.7);
  std::vector<ContinueFuture> futures{numDrivers};
  for (int i = 0; i < numDrivers; ++i) {
    controller->shouldStop(i, &futures[i]);
  }

  std::vector<std::thread> driverThreads;
  std::atomic_int closeCount{0};
  for (int i = 0; i < numDrivers; ++i) {
    driverThreads.push_back(std::thread([&, i]() {
      auto rng = folly::Random::DefaultGenerator(i);
      auto pool = pools[i];
      if (futures[i].valid()) {
        futures[i].wait();
      }
      for (int j = 0; j < 1'000; ++j) {
        uint64_t allocationBytes{0};
        switch (folly::Random::rand32(rng) % 3) {
          case 0:
            allocationBytes = 256 / numDrivers;
            break;
          case 1:
            allocationBytes = 256 / numDrivers / 2;
            break;
          case 2:
            allocationBytes = 256 / numDrivers / 4;
            break;
        }
        void* buffer = pool->allocate(allocationBytes);
        SCOPE_EXIT {
          pool->free(buffer, allocationBytes);
        };
        controller->updateAndTryScale(i, pool->peakBytes());
      }
      if (controller->close()) {
        ++closeCount;
      }
    }));
  }
  for (auto& thread : driverThreads) {
    thread.join();
  }
  ASSERT_EQ(closeCount.load(), 1);
}
} // namespace facebook::velox::exec::test
