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
#include "velox/exec/RowContainer.h"
#include "velox/exec/Spiller.h"

namespace facebook::velox::exec {

/// OrderBy operator implementation: OrderBy stores all its inputs in a
/// RowContainer as the inputs are added. Until all inputs are available,
/// it blocks the pipeline. Once all inputs are available, it sorts pointers
/// to the rows using the RowContainer's compare() function. And finally it
/// constructs and returns the sorted output RowVector using the data in the
/// RowContainer.
/// Limitations:
/// * It memcopies twice: 1) input to RowContainer and 2) RowContainer to
/// output.
class OrderBy : public Operator {
 public:
  OrderBy(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL driverCtx,
      const std::shared_ptr<const core::OrderByNode>& orderByNode);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* FOLLY_NULLABLE /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  static const int32_t kBatchSizeInBytes{2 * 1024 * 1024};

  // Checks if input will fit in the existing memory and increases
  // reservation if not. If reservation cannot be increased, spills enough to
  // make 'input' fit.
  void ensureInputFits(const RowVectorPtr& input);

  // Prepare the reusable output buffer based on the output batch size and the
  // remaining rows to return.
  void prepareOutput();

  void getOutputWithoutSpill();
  void getOutputWithSpill();

  // Spills content until under 'targetRows' and under 'targetBytes' of out of
  // line data are left. If 'targetRows' is 0, spills everything and physically
  // frees the data in the 'data_'. This is called by ensureInputFits or by
  // external memory management. In the latter case, the Driver of this will be
  // in a paused state and off thread.
  void spill(int64_t targetRows, int64_t targetBytes);

  memory::MappedMemory* FOLLY_NONNULL const mappedMemory_;

  const int32_t numSortKeys_;

  // The maximum memory usage that an order by can hold before spilling.
  // If it is zero, then there is no such limit.
  const uint64_t spillMemoryThreshold_;

  // Filesystem path for spill files, empty if spilling is disabled.
  // The disk spilling related configs if spilling is enabled, otherwise null.
  const std::optional<Spiller::Config> spillConfig_;

  // The map from column channel in 'output_' to the corresponding one stored in
  // 'data_'. The column channel might be reordered to ensure the sorting key
  // columns stored first in 'data_'.
  std::vector<IdentityProjection> columnMap_;

  std::vector<CompareFlags> keyCompareFlags_;

  std::unique_ptr<RowContainer> data_;

  // The row type used to store input data in row container and for spilling
  // internally.
  RowTypePtr internalStoreType_;

  // Maximum number of rows in the output batch.
  uint32_t outputBatchSize_;

  // Possibly reusable output vector.
  RowVectorPtr output_;

  // The number of input rows.
  size_t numRows_ = 0;

  // The number of rows has been returned for output.
  size_t numRowsReturned_ = 0;

  // Used to collect sorted rows from 'data_' on non-spilling output path.
  std::vector<char*> returningRows_;

  std::unique_ptr<Spiller> spiller_;

  // Counts input batches and triggers spilling if folly hash of this % 100 <=
  // 'testSpillPct_';.
  uint64_t spillTestCounter_{0};

  // Set to read back spilled data if disk spilling has been triggered.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> spillMerge_;

  // Record the source rows to copy to 'output_' in order.
  std::vector<const RowVector*> spillSources_;
  std::vector<vector_size_t> spillSourceRows_;

  bool finished_ = false;
};
} // namespace facebook::velox::exec
