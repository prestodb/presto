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

#include "velox/common/base/SkewedPartitionBalancer.h"

#include <gtest/gtest.h>

#include "folly/Random.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::common::test {
class SkewedPartitionRebalancerTestHelper {
 public:
  explicit SkewedPartitionRebalancerTestHelper(
      SkewedPartitionRebalancer* balancer)
      : balancer_(balancer) {
    VELOX_CHECK_NOT_NULL(balancer_);
  }

  void verifyPartitionAssignment(
      uint32_t partition,
      const std::set<uint32_t>& expectedAssignedTasks) const {
    const std::set<uint32_t> assignedTasks(
        balancer_->partitionAssignments_[partition].begin(),
        balancer_->partitionAssignments_[partition].end());
    ASSERT_EQ(assignedTasks, expectedAssignedTasks)
        << "\nExpected: " << folly::join(",", expectedAssignedTasks)
        << "\nActual: " << folly::join(",", assignedTasks);
  }

  void verifyPartitionRowCount(uint32_t partition, uint32_t expectedRowCount)
      const {
    ASSERT_EQ(balancer_->partitionRowCount_[partition], expectedRowCount);
  }

  uint32_t taskCount() const {
    return balancer_->taskCount_;
  }

  uint32_t partitionCount() const {
    return balancer_->partitionCount_;
  }

  uint64_t processedBytes() const {
    return balancer_->processedBytes_;
  }

  bool shouldRebalance() const {
    return balancer_->shouldRebalance();
  }

 private:
  SkewedPartitionRebalancer* const balancer_;
};

class SkewedPartitionRebalancerTest : public testing::Test {
 protected:
  std::unique_ptr<SkewedPartitionRebalancer> createBalancer(
      uint32_t partitionCount = 128,
      uint32_t taskCount = 8,
      uint64_t minPartitionDataProcessedBytesRebalanceThreshold = 128,
      uint64_t minProcessedBytesRebalanceThreshold = 256) {
    return std::make_unique<SkewedPartitionRebalancer>(
        partitionCount,
        taskCount,
        minPartitionDataProcessedBytesRebalanceThreshold,
        minProcessedBytesRebalanceThreshold);
  }
};

TEST_F(SkewedPartitionRebalancerTest, basic) {
  auto balancer = createBalancer(32, 4, 128, 256);
  SkewedPartitionRebalancerTestHelper helper(balancer.get());
  for (int i = 0; i < helper.partitionCount(); ++i) {
    helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
  }
  ASSERT_EQ(balancer->stats(), SkewedPartitionRebalancer::Stats{});
  ASSERT_EQ(
      balancer->stats().toString(),
      "numBalanceTriggers 0, numScaledPartitions 0");
  balancer->rebalance();
  ASSERT_EQ(balancer->stats(), SkewedPartitionRebalancer::Stats{});

  balancer->addProcessedBytes(128);
  balancer->rebalance();
  ASSERT_EQ(balancer->stats(), SkewedPartitionRebalancer::Stats{});

  balancer->addProcessedBytes(128);
  VELOX_ASSERT_THROW(balancer->rebalance(), "");
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 1);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 0);

  for (int i = 0; i < helper.partitionCount(); ++i) {
    helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
  }

  ASSERT_EQ(helper.processedBytes(), 256);
  ASSERT_TRUE(helper.shouldRebalance());
  balancer->addPartitionRowCount(0, 100);
  ASSERT_TRUE(helper.shouldRebalance());

  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 2);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 1);
  for (int i = 0; i < helper.partitionCount(); ++i) {
    if (i == 0) {
      helper.verifyPartitionAssignment(0, {0, 1});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }

  ASSERT_FALSE(helper.shouldRebalance());
  ASSERT_EQ(helper.processedBytes(), 256);
  balancer->addProcessedBytes(128);
  ASSERT_FALSE(helper.shouldRebalance());
  ASSERT_EQ(helper.processedBytes(), 256 + 128);
  balancer->addProcessedBytes(128);
  ASSERT_TRUE(helper.shouldRebalance());
  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 3);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 2);
  for (int i = 0; i < helper.partitionCount(); ++i) {
    if (i == 0) {
      helper.verifyPartitionAssignment(0, {0, 1, 2});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }

  balancer->addProcessedBytes(512);
  balancer->addPartitionRowCount(1, 100);
  ASSERT_TRUE(helper.shouldRebalance());
  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 4);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 4);
  for (int i = 0; i < helper.partitionCount(); ++i) {
    SCOPED_TRACE(fmt::format("partition {}", i));
    if (i == 0) {
      helper.verifyPartitionAssignment(0, {0, 1, 2, 3});
    } else if (i == 1) {
      helper.verifyPartitionAssignment(1, {0, 1});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }
  ASSERT_EQ(
      balancer->stats().toString(),
      "numBalanceTriggers 4, numScaledPartitions 4");
}

TEST_F(SkewedPartitionRebalancerTest, rebalanceCondition) {
  auto balancer = createBalancer(32, 4, 128, 256);
  SkewedPartitionRebalancerTestHelper helper(balancer.get());
  ASSERT_FALSE(helper.shouldRebalance());
  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 0);
  balancer->addProcessedBytes(128);
  ASSERT_FALSE(helper.shouldRebalance());
  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 0);
  balancer->addProcessedBytes(128);
  ASSERT_TRUE(helper.shouldRebalance());
  VELOX_ASSERT_THROW(balancer->rebalance(), "");
  balancer->addPartitionRowCount(0, 1);
  balancer->addPartitionRowCount(31, 1);
  for (int i = 0; i < helper.partitionCount(); ++i) {
    if (i == 0 || i == 31) {
      helper.verifyPartitionRowCount(i, 1);
    } else {
      helper.verifyPartitionRowCount(i, 0);
    }
  }
  ASSERT_TRUE(helper.shouldRebalance());
  balancer->rebalance();
  for (int i = 0; i < helper.partitionCount(); ++i) {
    SCOPED_TRACE(fmt::format("partition {}", i));
    if (i == 0) {
      helper.verifyPartitionAssignment(0, {0, 1});
    } else if (i == 31) {
      helper.verifyPartitionAssignment(31, {2, 3});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 2);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 2);

  ASSERT_FALSE(helper.shouldRebalance());
  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 2);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 2);
}

// Verify the round-robin selection of assigned tasks.
TEST_F(SkewedPartitionRebalancerTest, assignedTaskSelection) {
  auto balancer = createBalancer(32, 4, 128, 256);
  SkewedPartitionRebalancerTestHelper helper(balancer.get());
  for (int round = 0; round < 10; ++round) {
    for (int partition = 0; partition < helper.partitionCount(); ++partition) {
      ASSERT_EQ(
          balancer->getTaskId(partition, round),
          partition % helper.taskCount());
    }
  }
  balancer->addProcessedBytes(512);
  balancer->addPartitionRowCount(0, 1);
  balancer->addPartitionRowCount(15, 1);
  balancer->addPartitionRowCount(31, 1);

  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 1);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 3);
  for (int i = 0; i < helper.partitionCount(); ++i) {
    SCOPED_TRACE(fmt::format("partition {}", i));
    if (i == 0) {
      helper.verifyPartitionAssignment(i, {0, 2});
    } else if (i == 15) {
      helper.verifyPartitionAssignment(i, {1, 3});
    } else if (i == 31) {
      helper.verifyPartitionAssignment(i, {1, 3});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }
  for (int round = 0; round < 10; ++round) {
    for (int partition = 0; partition < helper.partitionCount(); ++partition) {
      SCOPED_TRACE(fmt::format("partition {}, round {}", partition, round));
      if (partition == 0) {
        ASSERT_EQ(balancer->getTaskId(partition, round), round % 2 ? 2 : 0);
      } else if (partition == 15 || partition == 31) {
        ASSERT_EQ(balancer->getTaskId(partition, round), round % 2 ? 1 : 3);
      } else {
        ASSERT_EQ(
            balancer->getTaskId(partition, round),
            partition % helper.taskCount());
      }
    }
  }
}

TEST_F(SkewedPartitionRebalancerTest, partitionScaleProcessBytesThreshold) {
  auto balancer = createBalancer(32, 4, 128, 256);
  SkewedPartitionRebalancerTestHelper helper(balancer.get());
  balancer->addProcessedBytes(256);
  balancer->addPartitionRowCount(0, 64);
  for (int partition = 1; partition < helper.partitionCount(); ++partition) {
    balancer->addPartitionRowCount(partition, 3);
  }
  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 1);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 0);

  balancer->addProcessedBytes(256);
  balancer->addPartitionRowCount(0, 128);

  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 2);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 1);

  for (int i = 0; i < helper.partitionCount(); ++i) {
    SCOPED_TRACE(fmt::format("partition {}", i));
    if (i == 0) {
      helper.verifyPartitionAssignment(i, {0, 1});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }
}

TEST_F(SkewedPartitionRebalancerTest, skewTasksCondition) {
  auto balancer = createBalancer(32, 4, 128, 256);
  SkewedPartitionRebalancerTestHelper helper(balancer.get());
  for (int partition = 0; partition < helper.taskCount(); ++partition) {
    balancer->addProcessedBytes(400);
    balancer->addPartitionRowCount(partition, 4);
  }
  balancer->addProcessedBytes(600);
  balancer->addPartitionRowCount(0, 6);

  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 1);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 0);

  for (int partition = 0; partition < helper.taskCount(); ++partition) {
    balancer->addProcessedBytes(3000);
    balancer->addPartitionRowCount(partition, 3);
  }
  balancer->addProcessedBytes(7000);
  balancer->addPartitionRowCount(0, 10);

  balancer->rebalance();
  ASSERT_EQ(balancer->stats().numBalanceTriggers, 2);
  ASSERT_EQ(balancer->stats().numScaledPartitions, 1);

  for (int i = 0; i < helper.partitionCount(); ++i) {
    SCOPED_TRACE(fmt::format("partition {}", i));
    if (i == 0) {
      helper.verifyPartitionAssignment(i, {0, 1});
    } else {
      helper.verifyPartitionAssignment(i, {i % helper.taskCount()});
    }
  }
}

TEST_F(SkewedPartitionRebalancerTest, error) {
  auto balancer = createBalancer(32, 4, 128, 256);
  VELOX_ASSERT_THROW(balancer->addProcessedBytes(0), "");
  VELOX_ASSERT_THROW(balancer->addPartitionRowCount(32, 4), "");
  balancer->addPartitionRowCount(0, 0);
  VELOX_ASSERT_THROW(createBalancer(0, 4, 128, 256), "");
  VELOX_ASSERT_THROW(createBalancer(0, 4, 0, 0), "");
}

TEST_F(SkewedPartitionRebalancerTest, fuzz) {
  std::mt19937 rng{100};
  for (int taskCount = 1; taskCount <= 10; ++taskCount) {
    const uint64_t rebalanceThreshold = folly::Random::rand32(128, rng);
    const uint64_t perPartitionRebalanceThreshold =
        folly::Random::rand32(rebalanceThreshold / 2, rng);
    auto balancer = createBalancer(
        32, taskCount, perPartitionRebalanceThreshold, rebalanceThreshold);
    SkewedPartitionRebalancerTestHelper helper(balancer.get());
    for (int iteration = 0; iteration < 1'000; ++iteration) {
      SCOPED_TRACE(
          fmt::format("taskCount {}, iteration {}", taskCount, iteration));
      const uint64_t processedBytes = 1 + folly::Random::rand32(512, rng);
      balancer->addProcessedBytes(processedBytes);
      const auto numPartitons = folly::Random::rand32(32, rng);
      for (auto i = 0; i < numPartitons; ++i) {
        const auto partition = folly::Random::rand32(32, rng);
        const auto numRows = 1 + folly::Random::rand32(32, rng);
        balancer->addPartitionRowCount(partition, numRows);
      }
      balancer->rebalance();
      for (int round = 0; round < 10; ++round) {
        for (int partition = 0; partition < helper.partitionCount();
             ++partition) {
          ASSERT_LT(balancer->getTaskId(partition, round), taskCount);
        }
      }
    }
  }
}
} // namespace facebook::velox::common::test
