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

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class RowNumber : public Operator {
 public:
  RowNumber(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::RowNumberNode>& rowNumberNode);

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && !finishedEarly_;
  }

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return (noMoreInput_ && input_ == nullptr &&
            spillInputReader_ == nullptr) ||
        finishedEarly_;
  }

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

 private:
  bool spillEnabled() const {
    return spillConfig_.has_value();
  }

  void setupInputSpiller(const SpillPartitionNumSet& spillPartitionSet);

  void ensureInputFits(const RowVectorPtr& input);

  void spillInput(const RowVectorPtr& input, memory::MemoryPool* pool);

  void spill();

  void addSpillInput();

  void restoreNextSpillPartition();

  SpillPartitionNumSet spillHashTable();

  int64_t numRows(char* partition);

  void setNumRows(char* partition, int64_t numRows);

  RowVectorPtr getOutputForSinglePartition();

  FlatVector<int64_t>& getOrCreateRowNumberVector(vector_size_t size);

  // Used by recursive spill processing to read the spilled input data from the
  // previous spill run through 'spillInputReader_' and then spill them back
  // into a number of sub-partitions. After that, the function restores one of
  // the newly spilled partitions and resets 'spillInputReader_' accordingly.
  void recursiveSpillInput();

  // Set 'spillPartitionBits_' for (recursive) spill. If 'restoredPartitionId'
  // is not null, use it to set 'spillPartitionBits_', otherwise use
  // 'spillConfig_'. If the new 'spillPartitionBits_' exceeds the
  // 'maxSpillLevel', set 'exceededMaxSpillLevelLimit_' to true.
  //
  // NOTE: we don't increment 'spillMaxLevelExceededCount' here, as the actual
  // increment happens in the 'reclaim()' method if
  // 'exceededMaxSpillLevelLimit_' is true.
  void setSpillPartitionBits(
      const SpillPartitionId* restoredPartitionId = nullptr);

  const std::optional<int32_t> limit_;
  const bool generateRowNumber_;

  // Hash table to store number of rows seen so far per partition. Not used if
  // there are no partitioning keys.
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  int32_t numRowsOffset_;

  // Total number of input rows. Used when there are no partitioning keys and
  // therefore no hash table.
  int64_t numTotalInput_{0};

  // Boolean indicating that the operator finished processing before seeing all
  // the input. This happens when there are no partitioning keys and the
  // operator already received 'limit_' rows.
  bool finishedEarly_{false};

  RowTypePtr inputType_;

  // The spill partition bits used by both hash table content spill and input
  // data spill.
  HashBitRange spillPartitionBits_;

  // Used to restore previously spilled hash table.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> spillHashTableReader_;

  SpillPartitionSet spillHashTablePartitionSet_;

  // Spiller for input received after spilling has been triggered.
  std::unique_ptr<Spiller> inputSpiller_;

  // Used to restore previously spilled input.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> spillInputReader_;

  SpillPartitionSet spillInputPartitionSet_;

  // Used to calculate the spill partition numbers of the inputs.
  std::unique_ptr<HashPartitionFunction> spillHashFunction_;

  // The cpu may be voluntarily yield after running too long when processing
  // input from spilled file.
  bool yield_{false};

  bool exceededMaxSpillLevelLimit_{false};
};
} // namespace facebook::velox::exec
