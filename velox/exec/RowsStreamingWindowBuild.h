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

#include "velox/exec/WindowBuild.h"

namespace facebook::velox::exec {

/// Unlike PartitionStreamingWindowBuild, RowsStreamingWindowBuild is capable of
/// processing window functions as rows arrive within a single partition,
/// without the need to wait for the entirewindow partition to be ready. This
/// approach can significantly reduce memory usage, especially when a single
/// partition contains a large amount of data. It is particularly suited for
/// optimizing rank, dense_rank and row_number functions, as well as aggregate
/// window functions with a default frame.
class RowsStreamingWindowBuild : public WindowBuild {
 public:
  RowsStreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      velox::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection);

  void addInput(RowVectorPtr input) override;

  void spill() override {
    VELOX_UNREACHABLE();
  }

  std::optional<common::SpillStats> spilledStats() const override {
    return std::nullopt;
  }

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

  bool needsInput() override;

 private:
  // Adds input rows to the current partition, or creates a new partition if it
  // does not exist.
  void addPartitionInputs(bool finished);

  // Invoked before add input to ensure there is an open (in-complete) partition
  // to accept new input. The function creates a new one at the tail of
  // 'windowPartitions_' if it is empty or the last partition is already
  // completed.
  void ensureInputPartition();

  // Sets to true if this window node has range frames.
  const bool hasRangeFrame_;

  // Points to the input rows in the current partition.
  std::vector<char*> inputRows_;

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  /// The output gets next partition from the head of 'windowPartitions_' and
  /// input adds to the next partition from the tail of 'windowPartitions_'.
  std::deque<std::shared_ptr<WindowPartition>> windowPartitions_;
};

} // namespace facebook::velox::exec
