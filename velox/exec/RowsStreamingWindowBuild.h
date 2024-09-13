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

  bool needsInput() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return windowPartitions_.empty() ||
        outputPartition_ == windowPartitions_.size() - 1;
  }

 private:
  // Adds input rows to the current partition, or creates a new partition if it
  // does not exist.
  void addPartitionInputs(bool finished);

  // Sets to true if this window node has range frames.
  const bool hasRangeFrame_;

  // Points to the input rows in the current partition.
  std::vector<char*> inputRows_;

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  // Point to the current output partition if not -1.
  vector_size_t outputPartition_ = -1;

  // Current input partition that receives inputs.
  vector_size_t inputPartition_ = 0;

  // Holds all the built window partitions.
  std::vector<std::shared_ptr<WindowPartition>> windowPartitions_;
};

} // namespace facebook::velox::exec
