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

#include "velox/exec/PartitionStreamingWindowBuild.h"

namespace facebook::velox::exec {

PartitionStreamingWindowBuild::PartitionStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection)
    : WindowBuild(windowNode, pool, spillConfig, nonReclaimableSection) {}

void PartitionStreamingWindowBuild::buildNextPartition() {
  partitionStartRows_.push_back(sortedRows_.size());
  sortedRows_.insert(sortedRows_.end(), inputRows_.begin(), inputRows_.end());
  inputRows_.clear();
}

void PartitionStreamingWindowBuild::addInput(RowVectorPtr input) {
  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    if (previousRow_ != nullptr &&
        compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_)) {
      buildNextPartition();
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

void PartitionStreamingWindowBuild::noMoreInput() {
  buildNextPartition();

  // Help for last partition related calculations.
  partitionStartRows_.push_back(sortedRows_.size());
}

std::shared_ptr<WindowPartition>
PartitionStreamingWindowBuild::nextPartition() {
  VELOX_CHECK_GT(
      partitionStartRows_.size(), 0, "No window partitions available");

  ++currentPartition_;
  VELOX_CHECK_LE(
      currentPartition_,
      partitionStartRows_.size() - 2,
      "All window partitions consumed");

  // Erase previous partition.
  if (currentPartition_ > 0) {
    const auto numPreviousPartitionRows =
        partitionStartRows_[currentPartition_];
    data_->eraseRows(
        folly::Range<char**>(sortedRows_.data(), numPreviousPartitionRows));
    sortedRows_.erase(
        sortedRows_.begin(), sortedRows_.begin() + numPreviousPartitionRows);
    sortedRows_.shrink_to_fit();
    for (int i = currentPartition_; i < partitionStartRows_.size(); ++i) {
      partitionStartRows_[i] =
          partitionStartRows_[i] - numPreviousPartitionRows;
    }
  }

  const auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  const auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);

  return std::make_shared<WindowPartition>(
      data_.get(), partition, inversedInputChannels_, sortKeyInfo_);
}

bool PartitionStreamingWindowBuild::hasNextPartition() {
  return partitionStartRows_.size() > 0 &&
      currentPartition_ < int(partitionStartRows_.size() - 2);
}

} // namespace facebook::velox::exec
