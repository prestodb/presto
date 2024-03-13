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

#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/Spill.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {

/// A utility class to accumulate data inside and output the sorted result.
/// Spilling would be triggered if spilling is enabled and memory usage exceeds
/// limit.
class SortBuffer {
 public:
  SortBuffer(
      const RowTypePtr& input,
      const std::vector<column_index_t>& sortColumnIndices,
      const std::vector<CompareFlags>& sortCompareFlags,
      velox::memory::MemoryPool* pool,
      tsan_atomic<bool>* nonReclaimableSection,
      const common::SpillConfig* spillConfig = nullptr);

  void addInput(const VectorPtr& input);

  /// Indicates no more input and triggers either of:
  ///  - In-memory sorting on rows stored in 'data_' if spilling is not enabled.
  ///  - Finish spilling and setup the sort merge reader for the un-spilling
  ///  processing for the output.
  void noMoreInput();

  /// Returns the sorted output rows in batch.
  RowVectorPtr getOutput(uint32_t maxOutputRows);

  /// Indicates if this sort buffer can spill or not.
  bool canSpill() const {
    return spillConfig_ != nullptr;
  }

  /// Invoked to spill all the rows from 'data_'.
  void spill();

  memory::MemoryPool* pool() const {
    return pool_;
  }

  /// Returns the spiller stats including total bytes and rows spilled so far.
  std::optional<common::SpillStats> spilledStats() const {
    if (spiller_ == nullptr) {
      return std::nullopt;
    }
    return spiller_->stats();
  }

  std::optional<uint64_t> estimateOutputRowSize() const;

 private:
  // Ensures there is sufficient memory reserved to process 'input'.
  void ensureInputFits(const VectorPtr& input);
  void updateEstimatedOutputRowSize();
  // Invoked to initialize or reset the reusable output buffer to get output.
  void prepareOutput(uint32_t maxOutputRows);
  void getOutputWithoutSpill();
  void getOutputWithSpill();
  // Spill during input stage.
  void spillInput();
  // Spill during output stage.
  void spillOutput();
  // Finish spill, and we shouldn't get any rows from non-spilled partition as
  // there is only one hash partition for SortBuffer.
  void finishSpill();

  const RowTypePtr input_;
  const std::vector<CompareFlags> sortCompareFlags_;
  velox::memory::MemoryPool* const pool_;
  // The flag is passed from the associated operator such as OrderBy or
  // TableWriter to indicate if this sort buffer object is under non-reclaimable
  // execution section or not.
  tsan_atomic<bool>* const nonReclaimableSection_;
  const common::SpillConfig* const spillConfig_;

  // The column projection map between 'input_' and 'spillerStoreType_' as sort
  // buffer stores the sort columns first in 'data_'.
  std::vector<IdentityProjection> columnMap_;

  // Indicates no more input. Once it is set, addInput() can't be called on this
  // sort buffer object.
  bool noMoreInput_ = false;
  // The number of received input rows.
  size_t numInputRows_ = 0;
  // Used to store the input data in row format.
  std::unique_ptr<RowContainer> data_;
  std::vector<char*> sortedRows_;

  // The data type of the rows stored in 'data_' and spilled on disk. The
  // sort key columns are stored first then the non-sorted data columns.
  RowTypePtr spillerStoreType_;
  std::unique_ptr<Spiller> spiller_;
  // Used to merge the sorted runs from in-memory rows and spilled rows on disk.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> spillMerger_;
  // Records the source rows to copy to 'output_' in order.
  std::vector<const RowVector*> spillSources_;
  std::vector<vector_size_t> spillSourceRows_;

  // Reusable output vector.
  RowVectorPtr output_;
  // Estimated size of a single output row by using the max
  // 'data_->estimateRowSize()' across all accumulated data set.
  std::optional<uint64_t> estimatedOutputRowSize_{};
  // The number of rows that has been returned.
  size_t numOutputRows_{0};
};

} // namespace facebook::velox::exec
