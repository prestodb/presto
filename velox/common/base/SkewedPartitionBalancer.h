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
#pragma once

#include "velox/common/base/IndexedPriorityQueue.h"

namespace facebook::velox::common {

namespace test {
class SkewedPartitionRebalancerTestHelper;
}

/// This class is used to auto-scale partition processing by assigning more
/// tasks to busy partition measured by processed data size. This is used by
/// local partition to scale table writers for now.
class SkewedPartitionRebalancer {
 public:
  /// 'numPartitions' is the number of partitions to process. 'numTasks' is
  /// number of tasks for execution.
  /// 'minProcessedBytesRebalanceThresholdPerPartition' is the processed bytes
  /// threshold to trigger task scaling for a single partition.
  /// 'minProcessedBytesRebalanceThreshold' is the threshold to trigger
  /// partition assignment rebalancing across tasks, which assigns under-loaded
  /// tasks to busy partitions from the busy tasks. A partition load is measured
  /// as the number of processed data size in bytes. Similarly, a task load is
  /// measured in the total number of processed data size from all its serving
  /// partitions.
  SkewedPartitionRebalancer(
      uint32_t numPartitions,
      uint32_t numTasks,
      uint64_t minProcessedBytesRebalanceThresholdPerPartition,
      uint64_t minProcessedBytesRebalanceThreshold);

  ~SkewedPartitionRebalancer() {
    VELOX_CHECK(!rebalancing_);
  }

  /// Invoked to rebalance the partition assignments if applicable.
  void rebalance();

  /// Gets the assigned task id for a given 'partition'. 'index' is used to
  /// choose one of multiple assigned tasks in a round-robin order.
  uint32_t getTaskId(uint32_t partition, uint64_t index) const {
    auto& taskList = partitionAssignments_[partition];
    return taskList.nextTaskId(index);
  }

  /// Adds the processed partition row count. This is used to estimate the
  /// processed bytes of a partition.
  void addPartitionRowCount(uint32_t partition, uint32_t numRows) {
    VELOX_CHECK_LT(partition, numPartitions_);
    partitionRowCount_[partition] += numRows;
  }

  /// Adds the total processed bytes from all the partitions.
  void addProcessedBytes(int64_t bytes) {
    VELOX_CHECK_GT(bytes, 0);
    processedBytes_ += bytes;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  uint32_t numTasks() const {
    return numTasks_;
  }

  /// The rebalancer internal stats.
  struct Stats {
    /// The number of times that triggers rebalance.
    size_t numBalanceTriggers{0};
    /// The number of times that we scale a partition processing.
    size_t numScaledPartitions{0};

    std::string toString() const;

    inline bool operator==(const Stats& other) const {
      return std::tie(numBalanceTriggers, numScaledPartitions) ==
          std::tie(other.numBalanceTriggers, other.numScaledPartitions);
    }
  };

  Stats stats() const {
    return Stats{
        .numBalanceTriggers = numBalanceTriggers_.load(),
        .numScaledPartitions = numScaledPartitions_.load()};
  }

 private:
  bool shouldRebalance(int64_t processedBytes) const;

  void rebalancePartitions(int64_t processedBytes);

  // Calculates the partition processed data size based on the number of
  // processed rows and the averaged row size.
  void calculatePartitionProcessedBytes();

  template <bool MaxQueue>
  uint64_t calculateTaskDataSizeSinceLastRebalance(
      const IndexedPriorityQueue<uint32_t, MaxQueue>& maxPartitions) {
    uint64_t estimatedDataBytesSinceLastRebalance{0};
    for (int i = 0; i < maxPartitions.size(); ++i) {
      estimatedDataBytesSinceLastRebalance +=
          partitionBytesSinceLastRebalancePerTask_[maxPartitions.values()[i]];
    }
    return estimatedDataBytesSinceLastRebalance;
  }

  // Tries to rebalance by assigning 'minTasks' to busy partitions in
  // 'maxTasks'. 'taskMaxPartitions' tracks the partitions served by eack task
  // in a max priority queue.
  void rebalanceBasedOnTaskSkewness(
      IndexedPriorityQueue<uint32_t, true>& maxTasks,
      IndexedPriorityQueue<uint32_t, false>& minTasks,
      std::vector<IndexedPriorityQueue<uint32_t, true>>& taskMaxPartitions);

  // Finds the skew min tasks compared with the max task as specified by
  // 'maxTaskId'.
  std::vector<uint32_t> findSkewedMinTasks(
      uint32_t maxTaskId,
      IndexedPriorityQueue<uint32_t, false>& minTasks) const;

  // Tries to assign 'targetTaskId' to 'rebalancePartition' for rebalancing.
  // Returns true if rebalanced, otherwise false.
  bool rebalancePartition(
      uint32_t rebalancePartition,
      uint32_t targetTaskId,
      IndexedPriorityQueue<uint32_t, true>& maxTasks,
      IndexedPriorityQueue<uint32_t, false>& minTasks);

  static constexpr double kTaskSkewnessThreshod_{0.7};

  const uint32_t numPartitions_;
  const uint32_t numTasks_;
  const uint64_t minProcessedBytesRebalanceThresholdPerPartition_;
  const uint64_t minProcessedBytesRebalanceThreshold_;

  // The accumulated number of rows processed by each partition.
  std::vector<std::atomic_uint64_t> partitionRowCount_;

  // Indicates if the rebalancer is running or not.
  std::atomic_bool rebalancing_{false};

  // The accumulated number of bytes processed by all the partitions.
  std::atomic_int64_t processedBytes_{0};
  // 'processedBytes_' at the last rebalance. It is used to calculate the
  // processed bytes changes since the last rebalance.
  std::atomic_int64_t processedBytesAtLastRebalance_{0};
  // The accumulated number of bytes processed by each partition.
  std::vector<uint64_t> partitionBytes_;
  // 'partitionBytes_' at the last rebalance. It is used to calculate the
  // processed bytes changes for each partition since the last rebalance.
  std::vector<uint64_t> partitionBytesAtLastRebalance_;
  // The average processed bytes for each partition on its assigned tasks since
  // the last rebalance. It is used to calculate the processed byte changes for
  // each task since the last rebalance.
  std::vector<uint64_t> partitionBytesSinceLastRebalancePerTask_;
  // The estimated task processed bytes since the last rebalance.
  std::vector<uint64_t> estimatedTaskBytesSinceLastRebalance_;

  // The assigned task ids for a partition
  class PartitionAssignment {
   public:
    PartitionAssignment() = default;

    void addTaskId(uint32_t taskId);

    uint32_t nextTaskId(uint64_t index) const;

    const std::vector<uint32_t>& taskIds() const;

    uint32_t size() const;

   private:
    mutable folly::SharedMutex lock_;
    std::vector<uint32_t> taskIds_;
  };
  std::vector<PartitionAssignment> partitionAssignments_;

  // The number of times that triggers rebalance.
  std::atomic_uint64_t numBalanceTriggers_{0};
  // The number of times that a scaled partition processing.
  std::atomic_uint32_t numScaledPartitions_{0};

  friend class test::SkewedPartitionRebalancerTestHelper;
};
} // namespace facebook::velox::common
