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

namespace facebook::velox::common {
SkewedPartitionRebalancer::SkewedPartitionRebalancer(
    uint32_t partitionCount,
    uint32_t taskCount,
    uint64_t minProcessedBytesRebalanceThresholdPerPartition,
    uint64_t minProcessedBytesRebalanceThreshold)
    : partitionCount_(partitionCount),
      taskCount_(taskCount),
      minProcessedBytesRebalanceThresholdPerPartition_(
          minProcessedBytesRebalanceThresholdPerPartition),
      minProcessedBytesRebalanceThreshold_(std::max(
          minProcessedBytesRebalanceThreshold,
          minProcessedBytesRebalanceThresholdPerPartition_)) {
  VELOX_CHECK_GT(partitionCount_, 0);
  VELOX_CHECK_GT(taskCount_, 0);

  partitionRowCount_.resize(partitionCount_, 0);
  partitionBytes_.resize(partitionCount_, 0);
  partitionBytesAtLastRebalance_.resize(partitionCount_, 0);
  partitionBytesSinceLastRebalancePerTask_.resize(partitionCount_, 0);
  estimatedTaskBytesSinceLastRebalance_.resize(taskCount_, 0);

  partitionAssignments_.resize(partitionCount_);

  // Assigns one task for each partition intitially.
  for (auto partition = 0; partition < partitionCount_; ++partition) {
    const uint32_t taskId = partition % taskCount;
    partitionAssignments_[partition].emplace_back(taskId);
  }
}

void SkewedPartitionRebalancer::rebalance() {
  if (shouldRebalance()) {
    rebalancePartitions();
  }
}

bool SkewedPartitionRebalancer::shouldRebalance() const {
  VELOX_CHECK_GE(processedBytes_, processedBytesAtLastRebalance_);
  return (processedBytes_ - processedBytesAtLastRebalance_) >=
      minProcessedBytesRebalanceThreshold_;
}

void SkewedPartitionRebalancer::rebalancePartitions() {
  VELOX_DCHECK(shouldRebalance());
  ++stats_.numBalanceTriggers;

  // Updates the processed bytes for each partition.
  calculatePartitionProcessedBytes();

  // Updates 'partitionBytesSinceLastRebalancePerTask_'.
  for (auto partition = 0; partition < partitionCount_; ++partition) {
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
      taskMaxPartitions{taskCount_};
  for (auto partition = 0; partition < partitionCount_; ++partition) {
    auto& taskAssignments = partitionAssignments_[partition];
    for (uint32_t taskId : taskAssignments) {
      auto& taskQueue = taskMaxPartitions[taskId];
      taskQueue.addOrUpdate(
          partition, partitionBytesSinceLastRebalancePerTask_[partition]);
    }
  }

  // Builds max and min task queue based on the estimated processed bytes since
  // last rebalance.
  IndexedPriorityQueue<uint32_t, /*MaxQueue=*/true> maxTasks;
  IndexedPriorityQueue<uint32_t, /*MaxQueue=*/false> minTasks;
  for (auto taskId = 0; taskId < taskCount_; ++taskId) {
    estimatedTaskBytesSinceLastRebalance_[taskId] =
        calculateTaskDataSizeSinceLastRebalance(taskMaxPartitions[taskId]);
    maxTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
    minTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
  }

  rebalanceBasedOnTaskSkewness(maxTasks, minTasks, taskMaxPartitions);
  processedBytesAtLastRebalance_ = processedBytes_;
}

void SkewedPartitionRebalancer::rebalanceBasedOnTaskSkewness(
    IndexedPriorityQueue<uint32_t, true>& maxTasks,
    IndexedPriorityQueue<uint32_t, false>& minTasks,
    std::vector<IndexedPriorityQueue<uint32_t, true>>& taskMaxPartitions) {
  std::unordered_set<uint32_t> scaledPartitions;

  while (true) {
    const auto maxTaskIdOpt = maxTasks.pop();
    if (!maxTaskIdOpt.has_value()) {
      break;
    }
    const uint32_t maxTaskId = maxTaskIdOpt.value();

    auto& maxPartitions = taskMaxPartitions[maxTaskId];
    if (maxPartitions.empty()) {
      continue;
    }

    std::vector<uint32_t> minSkewedTasks =
        findSkewedMinTasks(maxTaskId, minTasks);
    if (minSkewedTasks.empty()) {
      break;
    }

    while (true) {
      const auto maxPartitionOpt = maxPartitions.pop();
      if (!maxPartitionOpt.has_value()) {
        break;
      }
      const uint32_t maxPartition = maxPartitionOpt.value();

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

  stats_.numScaledPartitions += scaledPartitions.size();
}

bool SkewedPartitionRebalancer::rebalancePartition(
    uint32_t rebalancePartition,
    uint32_t targetTaskId,
    IndexedPriorityQueue<uint32_t, true>& maxTasks,
    IndexedPriorityQueue<uint32_t, false>& minTasks) {
  auto& taskAssignments = partitionAssignments_[rebalancePartition];
  for (auto taskId : taskAssignments) {
    if (taskId == targetTaskId) {
      return false;
    }
  }

  taskAssignments.push_back(targetTaskId);
  VELOX_CHECK_GT(partitionAssignments_[rebalancePartition].size(), 1);

  const auto newTaskCount = taskAssignments.size();
  const auto oldTaskCount = newTaskCount - 1;
  // Since a partition is rebalanced from max to min skewed tasks,
  // decrease the priority of max taskBucket as well as increase the priority
  // of min taskBucket.
  for (uint32_t taskId : taskAssignments) {
    if (taskId == targetTaskId) {
      estimatedTaskBytesSinceLastRebalance_[taskId] +=
          (partitionBytesSinceLastRebalancePerTask_[rebalancePartition] *
           oldTaskCount) /
          newTaskCount;
    } else {
      estimatedTaskBytesSinceLastRebalance_[taskId] -=
          partitionBytesSinceLastRebalancePerTask_[rebalancePartition] /
          newTaskCount;
    }

    maxTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
    minTasks.addOrUpdate(taskId, estimatedTaskBytesSinceLastRebalance_[taskId]);
  }

  LOG(INFO) << "Rebalanced partition " << rebalancePartition << " to task "
            << targetTaskId << " with taskCount " << newTaskCount;
  return true;
}

void SkewedPartitionRebalancer::calculatePartitionProcessedBytes() {
  uint64_t totalPartitionRowCount{0};
  for (auto partition = 0; partition < partitionCount_; ++partition) {
    totalPartitionRowCount += partitionRowCount_[partition];
  }
  VELOX_CHECK_GT(totalPartitionRowCount, 0);

  for (auto partition = 0; partition < partitionCount_; ++partition) {
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
    const IndexedPriorityQueue<uint32_t, false>& minTasks) const {
  std::vector<uint32_t> minSkewdTaskIds;
  for (uint32_t minTaskId : minTasks) {
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
    minSkewdTaskIds.push_back(minTaskId);
  }
  return minSkewdTaskIds;
}

std::string SkewedPartitionRebalancer::Stats::toString() const {
  return fmt::format(
      "numBalanceTriggers {}, numScaledPartitions {}",
      numBalanceTriggers,
      numScaledPartitions);
}
} // namespace facebook::velox::common
