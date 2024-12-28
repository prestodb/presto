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

#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::common {
SkewedPartitionRebalancer::SkewedPartitionRebalancer(
    uint32_t numPartitions,
    uint32_t numTasks,
    uint64_t minProcessedBytesRebalanceThresholdPerPartition,
    uint64_t minProcessedBytesRebalanceThreshold)
    : numPartitions_(numPartitions),
      numTasks_(numTasks),
      minProcessedBytesRebalanceThresholdPerPartition_(
          minProcessedBytesRebalanceThresholdPerPartition),
      minProcessedBytesRebalanceThreshold_(std::max(
          minProcessedBytesRebalanceThreshold,
          minProcessedBytesRebalanceThresholdPerPartition_)),
      partitionRowCount_(numPartitions_),
      partitionAssignments_(numPartitions_) {
  VELOX_CHECK_GT(numPartitions_, 0);
  VELOX_CHECK_GT(numTasks_, 0);

  partitionBytes_.resize(numPartitions_, 0);
  partitionBytesAtLastRebalance_.resize(numPartitions_, 0);
  partitionBytesSinceLastRebalancePerTask_.resize(numPartitions_, 0);
  estimatedTaskBytesSinceLastRebalance_.resize(numTasks_, 0);

  // Assigns one task for each partition intitially.
  for (auto partition = 0; partition < numPartitions_; ++partition) {
    const uint32_t taskId = partition % numTasks_;
    partitionAssignments_[partition].addTaskId(taskId);
  }
}

void SkewedPartitionRebalancer::PartitionAssignment::addTaskId(
    uint32_t taskId) {
  std::unique_lock guard{lock_};
  taskIds_.push_back(taskId);
}

uint32_t SkewedPartitionRebalancer::PartitionAssignment::nextTaskId(
    uint64_t index) const {
  std::shared_lock guard{lock_};
  const auto taskIndex = index % taskIds_.size();
  return taskIds_[taskIndex];
}

uint32_t SkewedPartitionRebalancer::PartitionAssignment::size() const {
  std::shared_lock guard{lock_};
  return taskIds_.size();
}

const std::vector<uint32_t>&
SkewedPartitionRebalancer::PartitionAssignment::taskIds() const {
  std::shared_lock guard{lock_};
  return taskIds_;
}

void SkewedPartitionRebalancer::rebalance() {
  const int64_t processedBytes = processedBytes_.load();
  if (shouldRebalance(processedBytes)) {
    rebalancePartitions(processedBytes);
  }
}

bool SkewedPartitionRebalancer::shouldRebalance(int64_t processedBytes) const {
  return (processedBytes - processedBytesAtLastRebalance_) >=
      minProcessedBytesRebalanceThreshold_;
}

void SkewedPartitionRebalancer::rebalancePartitions(int64_t processedBytes) {
  if (rebalancing_.exchange(true)) {
    return;
  }

  SCOPE_EXIT {
    VELOX_CHECK(rebalancing_);
    rebalancing_ = false;
  };
  ++numBalanceTriggers_;

  TestValue::adjust(
      "facebook::velox::common::SkewedPartitionRebalancer::rebalancePartitions",
      this);

  // Updates the processed bytes for each partition.
  calculatePartitionProcessedBytes();

  // Updates 'partitionBytesSinceLastRebalancePerTask_'.
  for (auto partition = 0; partition < numPartitions_; ++partition) {
    const auto totalAssignedTasks = partitionAssignments_[partition].size();
    const auto partitionBytes = partitionBytes_[partition];
    partitionBytesSinceLastRebalancePerTask_[partition] =
        (partitionBytes - partitionBytesAtLastRebalance_[partition]) /
        totalAssignedTasks;
    partitionBytesAtLastRebalance_[partition] = partitionBytes;
  }

  // Builds the max partition queue for each partition with the partition having
  // max processed bytes since last rebalance at the top of the queue for
  // rebalance later.
  std::vector<IndexedPriorityQueue<uint32_t, /*MaxQueue=*/true>>
      taskMaxPartitions{numTasks_};
  for (auto partition = 0; partition < numPartitions_; ++partition) {
    auto& taskAssignments = partitionAssignments_[partition];
    for (uint32_t taskId : taskAssignments.taskIds()) {
      auto& taskQueue = taskMaxPartitions[taskId];
      taskQueue.addOrUpdate(
          partition, partitionBytesSinceLastRebalancePerTask_[partition]);
    }
  }

  // Builds max and min task queue based on the estimated processed bytes since
  // last rebalance.
  IndexedPriorityQueue<uint32_t, /*MaxQueue=*/true> maxTasks;
  IndexedPriorityQueue<uint32_t, /*MaxQueue=*/false> minTasks;
  for (auto taskId = 0; taskId < numTasks_; ++taskId) {
    estimatedTaskBytesSinceLastRebalance_[taskId] =
        calculateTaskDataSizeSinceLastRebalance(taskMaxPartitions[taskId]);
    maxTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
    minTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
  }

  rebalanceBasedOnTaskSkewness(maxTasks, minTasks, taskMaxPartitions);
  processedBytesAtLastRebalance_.store(processedBytes);
}

void SkewedPartitionRebalancer::rebalanceBasedOnTaskSkewness(
    IndexedPriorityQueue<uint32_t, true>& maxTasks,
    IndexedPriorityQueue<uint32_t, false>& minTasks,
    std::vector<IndexedPriorityQueue<uint32_t, true>>& taskMaxPartitions) {
  std::unordered_set<uint32_t> scaledPartitions;

  while (!maxTasks.empty()) {
    const auto maxTaskId = maxTasks.pop();

    auto& maxPartitions = taskMaxPartitions[maxTaskId];
    if (maxPartitions.empty()) {
      continue;
    }

    std::vector<uint32_t> minSkewedTasks =
        findSkewedMinTasks(maxTaskId, minTasks);
    if (minSkewedTasks.empty()) {
      break;
    }

    while (!maxPartitions.empty()) {
      const auto maxPartition = maxPartitions.pop();

      // Rebalance partition only once in a single cycle to avoid aggressive
      // rebalancing.
      if (scaledPartitions.count(maxPartition) != 0) {
        continue;
      }

      const uint32_t totalAssignedTasks =
          partitionAssignments_[maxPartition].size();
      if (partitionBytes_[maxPartition] <
          (minProcessedBytesRebalanceThresholdPerPartition_ *
           totalAssignedTasks)) {
        break;
      }

      for (uint32_t minTaskId : minSkewedTasks) {
        if (rebalancePartition(maxPartition, minTaskId, maxTasks, minTasks)) {
          scaledPartitions.insert(maxPartition);
          break;
        }
      }
    }
  }

  numScaledPartitions_ += scaledPartitions.size();
}

bool SkewedPartitionRebalancer::rebalancePartition(
    uint32_t rebalancePartition,
    uint32_t targetTaskId,
    IndexedPriorityQueue<uint32_t, true>& maxTasks,
    IndexedPriorityQueue<uint32_t, false>& minTasks) {
  auto& taskAssignments = partitionAssignments_[rebalancePartition];
  for (auto taskId : taskAssignments.taskIds()) {
    if (taskId == targetTaskId) {
      return false;
    }
  }

  taskAssignments.addTaskId(targetTaskId);
  VELOX_CHECK_GT(partitionAssignments_[rebalancePartition].size(), 1);

  const auto newNumTasks = taskAssignments.size();
  const auto oldNumTasks = newNumTasks - 1;
  // Since a partition is rebalanced from max to min skewed tasks,
  // decrease the priority of max taskBucket as well as increase the priority
  // of min taskBucket.
  for (uint32_t taskId : taskAssignments.taskIds()) {
    if (taskId == targetTaskId) {
      estimatedTaskBytesSinceLastRebalance_[taskId] +=
          (partitionBytesSinceLastRebalancePerTask_[rebalancePartition] *
           oldNumTasks) /
          newNumTasks;
    } else {
      estimatedTaskBytesSinceLastRebalance_[taskId] -=
          partitionBytesSinceLastRebalancePerTask_[rebalancePartition] /
          newNumTasks;
    }

    maxTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
    minTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
  }

  VLOG(3) << "Rebalanced partition " << rebalancePartition << " to task "
          << targetTaskId << " with num of assigned tasks " << newNumTasks;
  return true;
}

void SkewedPartitionRebalancer::calculatePartitionProcessedBytes() {
  uint64_t totalPartitionRowCount{0};
  for (auto partition = 0; partition < numPartitions_; ++partition) {
    totalPartitionRowCount += partitionRowCount_[partition];
  }
  VELOX_CHECK_GT(totalPartitionRowCount, 0);

  for (auto partition = 0; partition < numPartitions_; ++partition) {
    // Since we estimate 'partitionBytes_' based on 'partitionRowCount_' and
    // total 'processedBytes_'. It is possible that the estimated
    // 'partitionBytes_' is slightly less than it was estimated at the last
    // rebalance cycle 'partitionBytesAtLastRebalance_'. That's because for
    // a given partition, 'partitionRowCount_' has increased, however overall
    // 'processedBytes_' hasn't increased that much. Therefore, we need to make
    // sure that the estimated 'partitionBytes_' should be at least
    // 'partitionBytesAtLastRebalance_'. Otherwise, it will affect the ordering
    // of 'minTasks' priority queue.
    partitionBytes_[partition] = std::max(
        (partitionRowCount_[partition] * processedBytes_) /
            totalPartitionRowCount,
        partitionBytes_[partition]);
  }
}

std::vector<uint32_t> SkewedPartitionRebalancer::findSkewedMinTasks(
    uint32_t maxTaskId,
    IndexedPriorityQueue<uint32_t, false>& minTasks) const {
  std::vector<uint32_t> minSkewedTaskIds;
  while (!minTasks.empty()) {
    const auto minTaskId = minTasks.top();
    if (minTaskId == maxTaskId) {
      break;
    }
    const double skewness =
        ((double)(estimatedTaskBytesSinceLastRebalance_[maxTaskId] -
                  estimatedTaskBytesSinceLastRebalance_[minTaskId])) /
        estimatedTaskBytesSinceLastRebalance_[maxTaskId];
    if (skewness <= kTaskSkewnessThreshod_ || std::isnan(skewness)) {
      break;
    }

    minTasks.pop();
    minSkewedTaskIds.push_back(minTaskId);
  }

  for (auto taskId : minSkewedTaskIds) {
    minTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
  }
  return minSkewedTaskIds;
}

std::string SkewedPartitionRebalancer::Stats::toString() const {
  return fmt::format(
      "numBalanceTriggers {}, numScaledPartitions {}",
      numBalanceTriggers,
      numScaledPartitions);
}
} // namespace facebook::velox::common
