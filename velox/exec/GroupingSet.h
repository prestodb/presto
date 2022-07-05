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

#include "velox/exec/AggregationMasks.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

class Aggregate;

class GroupingSet {
 public:
  GroupingSet(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      std::vector<column_index_t>&& preGroupedKeys,
      std::vector<std::unique_ptr<Aggregate>>&& aggregates,
      std::vector<std::optional<column_index_t>>&& aggrMaskChannels,
      std::vector<std::vector<column_index_t>>&& channelLists,
      std::vector<std::vector<VectorPtr>>&& constantLists,
      std::vector<TypePtr>&& intermediateTypes,
      bool ignoreNullKeys,
      bool isPartial,
      bool isRawInput,
      OperatorCtx* FOLLY_NONNULL operatorCtx);

  void addInput(const RowVectorPtr& input, bool mayPushdown);

  void noMoreInput();

  /// Typically, the output is not available until all input has been added.
  /// However, in case when input is clustered on some of the grouping keys, the
  /// output becomes available every time one of these grouping keys changes
  /// value. This method returns true if no-more-input message has been received
  /// or if some groups are ready for output because pre-grouped keys values
  /// have changed.
  bool hasOutput();

  /// Called if partial aggregation has reached memory limit or if hasOutput()
  /// returns true.
  bool getOutput(
      int32_t batchSize,
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  uint64_t allocatedBytes() const;

  void resetPartial();

  const HashLookup& hashLookup() const;

  /// Spills content until under 'targetRows' and under 'targetBytes'
  /// of out of line data are left. If targetRows is 0, spills
  /// everything and physically frees the data in the
  /// 'table_->rows()'. This leaves 'table_' initialized and 'this'
  /// ready to accumulate more input. This is called by ensureInputFits
  /// or by external memory management. In the latter case, the Driver
  /// of this will be in a paused state and off thread.
  void spill(int64_t targetRows, int64_t targetBytes);

  /// Returns the total bytes and rows spilled so far.
  std::pair<int64_t, int64_t> spilledBytesAndRows() const {
    return spiller_ ? spiller_->spilledBytesAndRows()
                    : std::pair<int64_t, int64_t>(0, 0);
  }

  /// Return the number of rows kept in memory.
  int64_t numRows() const {
    return table_ ? table_->rows()->numRows() : 0;
  }

 private:
  void addInputForActiveRows(const RowVectorPtr& input, bool mayPushdown);

  void addRemainingInput();

  void initializeGlobalAggregation();

  void addGlobalAggregationInput(const RowVectorPtr& input, bool mayPushdown);

  bool getGlobalAggregationOutput(
      int32_t batchSize,
      bool isPartial,
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  void createHashTable();

  void populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

  // If the given aggregation has mask, the method returns reference to the
  // selectivity vector from the maskedActiveRows_ (based on the mask channel
  // index for this aggregation), otherwise it returns reference to activeRows_.
  const SelectivityVector& getSelectivityVector(size_t aggregateIndex) const;

  // Checks if input will fit in the existing memory and increases
  // reservation if not. If reservation cannot be increased, spills
  // enough to make 'input' fit.
  void ensureInputFits(const RowVectorPtr& input);

  // Copies the grouping keys and aggregates for 'groups' into 'result' If
  // partial output, extracts the intermediate type for aggregates, final result
  // otherwise.
  void extractGroups(folly::Range<char**> groups, const RowVectorPtr& result);

  /// Produces output in if spilling has occurred. First produces data
  /// from non-spilled partitions, then merges spill runs and
  /// unspilled data form spilled partitions. Returns nullptr when at
  /// end.
  bool getOutputWithSpill(const RowVectorPtr& result);

  /// Reads rows from the current spilled partition until producing a batch of
  /// final results in 'result'. Returns false and leaves 'result' empty when
  /// the partition is fully read.
  bool mergeNext(const RowVectorPtr& result);

  // Initializes a new row in 'mergeRows' with the keys from the
  // current element from 'keys'. Accumulators are left in the initial
  // state with no data accumulated. This is called each time a new
  // key is received from a merge of spilled data. After this
  // updateRow() is called on the same element and on every subsequent
  // element read from the stream until a new key is seen, at which
  // time we again call initializeRow(). When enough rows have been
  // accumulated and we have a new key, we produce the output and
  // clear 'mergeRows_' with extractSpillResult() and only then do
  // initializeRow().
  void initializeRow(SpillStream& keys, char* FOLLY_NONNULL row);

  // Updates the accumulators in 'row' with the intermediate type data from
  // 'keys'. This is called for each row received from a merge of spilled data.
  void updateRow(SpillStream& keys, char* FOLLY_NONNULL row);

  // Copies the finalized state from 'mergeRows' to 'result' and clears
  // 'mergeRows'. Used for producing a batch of results when aggregating spilled
  // groups.
  void extractSpillResult(const RowVectorPtr& result);

  std::vector<column_index_t> keyChannels_;

  /// A subset of grouping keys on which the input is clustered.
  const std::vector<column_index_t> preGroupedKeyChannels_;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  const bool isGlobal_;
  const bool isPartial_;
  const bool isRawInput_;
  std::vector<std::unique_ptr<Aggregate>> aggregates_;
  AggregationMasks masks_;
  // Argument list for the corresponding element of 'aggregates_'.
  const std::vector<std::vector<column_index_t>> channelLists_;
  // Constant arguments to aggregates. Corresponds pairwise to
  // 'channelLists_'. This is used when channelLists_[i][j] ==
  // kConstantChannel.
  const std::vector<std::vector<VectorPtr>> constantLists_;

  // Types for extracting accumulators for spilling.
  const std::vector<TypePtr> intermediateTypes_;

  const bool ignoreNullKeys_;
  memory::MappedMemory* FOLLY_NONNULL const mappedMemory_;

  // Boolean indicating whether accumulators for a global aggregation (i.e.
  // aggregation with no grouping keys) have been initialized.
  bool globalAggregationInitialized_{false};

  std::vector<bool> mayPushdown_;

  // Place for the arguments of the aggregate being updated.
  std::vector<VectorPtr> tempVectors_;
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  SelectivityVector activeRows_;

  // Used to allocate memory for a single row accumulating results of global
  // aggregation
  HashStringAllocator stringAllocator_;
  AllocationPool rows_;
  const bool isAdaptive_;

  core::ExecCtx& execCtx_;

  bool noMoreInput_{false};

  /// In case of partial streaming aggregation, the input vector passed to
  /// addInput(). A set of rows that belong to the last group of pre-grouped
  /// keys need to be processed after flushing the hash table and accumulators.
  RowVectorPtr remainingInput_;

  /// First row in remainingInput_ that needs to be processed.
  vector_size_t firstRemainingRow_;

  /// The value of mayPushdown flag specified in addInput() for the
  /// 'remainingInput_'.
  bool remainingMayPushdown_;

  uint64_t maxBatchBytes_;

  // Filesystem path for spill files, empty if spilling is disabled.
  const std::optional<std::string> spillPath_;

  std::unique_ptr<Spiller> spiller_;
  std::unique_ptr<TreeOfLosers<SpillStream>> merge_;
  RowContainerIterator spillIterator_;

  // Container for materializing batches of output from spilling.
  std::unique_ptr<RowContainer> mergeRows_;

  // The row with the current merge state, allocated from 'mergeRow_'.
  char* FOLLY_NULLABLE mergeState_ = nullptr;

  // The currently running spill partition in producing spilld output.
  int32_t outputPartition_{-1};

  // Intermediate vector for passing arguments to aggregate in merging spill.
  std::vector<VectorPtr> mergeArgs_;

  // Indicates the element in mergeArgs_[0] that corresponds to the accumulator
  // to merge.
  SelectivityVector mergeSelection_;

  // True if 'merge_' indicates that the next key is the same as the current
  // one.
  bool nextKeyIsEqual_{false};

  // The set of rows that are outside of the spillable hash number
  // ranges. Used when producing output.
  std::optional<Spiller::SpillRows> nonSpilledRows_;

  // Index of first in 'nonSpilledRows_' that has not been added to output.
  size_t nonSpilledIndex_ = 0;
  // Pool of the OperatorCtx. Used for spilling.
  memory::MemoryPool& pool_;

  // Executor for spilling. If nullptr spilling writes on the Driver's thread.
  folly::Executor* FOLLY_NULLABLE const spillExecutor_;

  // The RowContainer of 'table_' is moved here before freeing
  // 'table_' when starting to read spill output.
  std::unique_ptr<RowContainer> rowsWhileReadingSpill_;

  // Percentage of input batches to be spilled for testing. 0 means no spilling
  // for test.
  const int32_t testSpillPct_;

  // Counts input batches and triggers spilling if folly hash of this % 100 <=
  // 'testSpillPct_';.
  uint64_t spillTestCounter_{0};
};

} // namespace facebook::velox::exec
