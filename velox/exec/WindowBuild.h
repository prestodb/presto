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

#include "velox/exec/RowContainer.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

// The Window operator needs to see all input rows, and separate them into
// partitions based on a partitioning key. There are many approaches to do
// this. e.g with a full-sort, HashTable, streaming etc. This abstraction
// is used by the Window operator to hold the input rows and provide
// partitions to it for processing. Varied implementations of the
// WindowBuild can use different algorithms.
class WindowBuild {
 public:
  WindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      velox::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection);

  virtual ~WindowBuild() = default;

  // The Window operator invokes this function to check if the WindowBuild can
  // accept input. The Streaming Window build doesn't accept input if it has a
  // partition to output.
  virtual bool needsInput() = 0;

  // Adds new input rows to the WindowBuild.
  virtual void addInput(RowVectorPtr input) = 0;

  // Can be called any time before noMoreInput().
  virtual void spill() = 0;

  /// Returns the spiller stats including total bytes and rows spilled so far.
  virtual std::optional<common::SpillStats> spilledStats() const = 0;

  // The Window operator invokes this function to indicate that no
  // more input rows will be passed from the Window operator to the
  // WindowBuild.
  // When using a sort based build, all input rows need to be
  // seen before any partitions are determined. So this function is
  // used to indicate to the WindowBuild that it can proceed with
  // building partitions.
  virtual void noMoreInput() = 0;

  // Returns true if a new Window partition is available for the Window
  // operator to consume.
  virtual bool hasNextPartition() = 0;

  // The Window operator invokes this function to get the next Window partition
  // to pass along to the WindowFunction. The WindowPartition has APIs to access
  // the underlying columns of Window partition data.
  // Check hasNextPartition() before invoking this function. This function fails
  // if called when no partition is available.
  virtual std::unique_ptr<WindowPartition> nextPartition() = 0;

  // Returns the average size of input rows in bytes stored in the
  // data container of the WindowBuild.
  std::optional<int64_t> estimateRowSize() {
    return data_->estimateRowSize();
  }

 protected:
  bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keys);

  // The below 2 vectors represent the ChannelIndex of the partition keys
  // and the order by keys. These keyInfo are used for sorting by those
  // key combinations during the processing.
  // partitionKeyInfo_ is used to separate partitions in the rows.
  // sortKeyInfo_ is used to identify peer rows in a partition.
  std::vector<std::pair<column_index_t, core::SortOrder>> partitionKeyInfo_;
  std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;

  // Input columns in the order of: partition keys, sorting keys, the rest.
  std::vector<column_index_t> inputChannels_;

  // The mapping from original input column index to the index after column
  // reordering. This is the inversed mapping of inputChannels_.
  std::vector<column_index_t> inversedInputChannels_;

  // Input column types in 'inputChannels_' order.
  RowTypePtr inputType_;

  const common::SpillConfig* const spillConfig_;
  tsan_atomic<bool>* const nonReclaimableSection_;

  // The RowContainer holds all the input rows in WindowBuild. Columns are
  // already reordered according to inputChannels_.
  std::unique_ptr<RowContainer> data_;

  // The decodedInputVectors_ are reused across addInput() calls to decode
  // the partition and sort keys for the above RowContainer.
  std::vector<DecodedVector> decodedInputVectors_;

  // Number of input rows.
  vector_size_t numRows_ = 0;
};

} // namespace facebook::velox::exec
