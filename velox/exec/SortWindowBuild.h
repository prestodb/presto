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

#include "velox/exec/Spiller.h"
#include "velox/exec/WindowBuild.h"

namespace facebook::velox::exec {

// Sorts input data of the Window by {partition keys, sort keys}
// to identify window partitions. This sort fully orders
// rows as needed for window function computation.
class SortWindowBuild : public WindowBuild {
 public:
  SortWindowBuild(
      const std::shared_ptr<const core::WindowNode>& node,
      velox::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection);

  bool needsInput() override {
    // No partitions are available yet, so can consume input rows.
    return partitionStartRows_.size() == 0;
  }

  void addInput(RowVectorPtr input) override;

  void spill() override;

  std::optional<common::SpillStats> spilledStats() const override {
    if (spiller_ == nullptr) {
      return std::nullopt;
    }
    return spiller_->stats();
  }

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::unique_ptr<WindowPartition> nextPartition() override;

 private:
  void ensureInputFits(const RowVectorPtr& input);

  void setupSpiller();

  // Main sorting function loop done after all input rows are received
  // by WindowBuild.
  void sortPartitions();

  // Function to compute the partitionStartRows_ structure.
  // partitionStartRows_ is vector of the starting rows index
  // of each partition in the data. This is an auxiliary
  // structure that helps simplify the window function computations.
  void computePartitionStartRows();

  // Find the next partition start row from start.
  vector_size_t findNextPartitionStartRow(vector_size_t start);

  // Reads next partition from spilled data into 'data_' and 'sortedRows_'.
  void loadNextPartitionFromSpill();

  const size_t numPartitionKeys_;

  // Compare flags for partition and sorting keys. Compare flags for partition
  // keys are set to default values. Compare flags for sorting keys match
  // sorting order specified in the plan node.
  //
  // Used to sort 'data_' while spilling.
  const std::vector<CompareFlags> spillCompareFlags_;

  memory::MemoryPool* const pool_;

  // allKeyInfo_ is a combination of (partitionKeyInfo_ and sortKeyInfo_).
  // It is used to perform a full sorting of the input rows to be able to
  // separate partitions and sort the rows in it. The rows are output in
  // this order by the operator.
  std::vector<std::pair<column_index_t, core::SortOrder>> allKeyInfo_;

  // Vector of pointers to each input row in the data_ RowContainer.
  // The rows are sorted by partitionKeys + sortKeys. This total
  // ordering can be used to split partitions (with the correct
  // order by) for the processing.
  std::vector<char*> sortedRows_;

  // This is a vector that gives the index of the start row
  // (in sortedRows_) of each partition in the RowContainer data_.
  // This auxiliary structure helps demarcate partitions.
  std::vector<vector_size_t> partitionStartRows_;

  // Current partition being output. Used to construct WindowPartitions
  // during resetPartition.
  vector_size_t currentPartition_ = -1;

  // Spiller for contents of the 'data_'.
  std::unique_ptr<Spiller> spiller_;

  // Used to sort-merge spilled data.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> merge_;
};

} // namespace facebook::velox::exec
