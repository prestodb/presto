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

#include "velox/common/base/SkewedPartitionBalancer.h"
#include "velox/exec/LocalPartition.h"

namespace facebook::velox::exec {

using namespace facebook::velox::common;

/// Customized local partition for writer scaling for (non-bucketed) partitioned
/// table write.
class ScaleWriterPartitioningLocalPartition : public LocalPartition {
 public:
  ScaleWriterPartitioningLocalPartition(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::LocalPartitionNode>& planNode);

  std::string toString() const override {
    return fmt::format(
        "ScaleWriterPartitioningLocalPartition({})", numPartitions_);
  }

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  void close() override;

  /// The name of the runtime stats of writer scaling.
  /// The number of times that we triggers the rebalance of table partitions.
  static inline const std::string kRebalanceTriggers{"rebalanceTriggers"};
  /// The number of times that we scale a partition processing.
  static inline const std::string kScaledPartitions{"scaledPartitions"};

 private:
  void prepareForWriterAssignments(vector_size_t numInput);

  uint32_t getNextWriterId(uint32_t partitionId);

  // The max query memory usage ratio before we stop writer scaling.
  const double maxQueryMemoryUsageRatio_;
  // The max number of logical table partitions that can be assigned to a single
  // table writer thread. Multiple physical table partitions can be mapped to
  // one logical table partition.
  const uint32_t maxTablePartitionsPerWriter_;
  // The total number of logical table partitions that can be served by all the
  // table writer threads.
  const uint32_t numTablePartitions_;

  memory::MemoryPool* const queryPool_;

  // The skewed partition balancer for writer scaling.
  const std::shared_ptr<SkewedPartitionRebalancer> tablePartitionRebalancer_;

  std::shared_ptr<LocalExchangeMemoryManager> memoryManager_;

  // Reusable memory for writer assignment processing.
  std::vector<uint32_t> tablePartitionRowCounts_;
  std::vector<int32_t> tablePartitionWriterIds_;
  std::vector<uint32_t> tablePartitionWriterIndexes_;

  // Reusable memory for writer assignment processing.
  std::vector<vector_size_t> writerAssignmentCounts_;
  std::vector<BufferPtr> writerAssignmmentIndicesBuffers_;
  std::vector<vector_size_t*> rawWriterAssignmmentIndicesBuffers_;
};

/// Customized local partition for writer scaling for un-partitioned table
/// write.
class ScaleWriterLocalPartition : public LocalPartition {
 public:
  ScaleWriterLocalPartition(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::LocalPartitionNode>& planNode);

  void addInput(RowVectorPtr input) override;

  void initialize() override;

  void close() override;

  /// The name of the runtime stats of writer scaling.
  /// The number of scaled writers.
  static inline const std::string kScaledWriters{"scaledWriters"};

 private:
  // Gets the writer id to process the next input in a round-robin manner.
  uint32_t getNextWriterId();

  // The max query memory usage ratio before we stop writer scaling.
  const double maxQueryMemoryUsageRatio_;
  memory::MemoryPool* const queryPool_;
  // The minimal amount of processed data bytes before we trigger next writer
  // scaling.
  const uint64_t minDataProcessedBytes_;

  std::shared_ptr<LocalExchangeMemoryManager> memoryManager_;

  // The number of assigned writers.
  uint32_t numWriters_{1};
  // The monotonically increasing writer index to find the next writer id in a
  // round-robin manner.
  uint32_t nextWriterIndex_{0};
  // The total processed data bytes from all writers.
  uint64_t processedDataBytes_{0};
  // The total processed data bytes at the last writer scaling.
  uint64_t processedBytesAtLastScale_{0};
};
} // namespace facebook::velox::exec
