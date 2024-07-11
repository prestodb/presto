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

#include "velox/exec/AggregateInfo.h"
#include "velox/exec/AggregationMasks.h"
#include "velox/exec/DistinctAggregations.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/SortedAggregations.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

class GroupingSet {
 public:
  GroupingSet(
      const RowTypePtr& inputType,
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      std::vector<column_index_t>&& preGroupedKeys,
      std::vector<AggregateInfo>&& aggregates,
      bool ignoreNullKeys,
      bool isPartial,
      bool isRawInput,
      const std::vector<vector_size_t>& globalGroupingSets,
      const std::optional<column_index_t>& groupIdChannel,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      OperatorCtx* operatorCtx,
      folly::Synchronized<common::SpillStats>* spillStats);

  ~GroupingSet();

  // Used by MarkDistinct operator to identify rows with unique values.
  static std::unique_ptr<GroupingSet> createForMarkDistinct(
      const RowTypePtr& inputType,
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      OperatorCtx* operatorCtx,
      tsan_atomic<bool>* nonReclaimableSection);

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
  /// returns true. 'maxOutputRows' and 'maxOutputBytes' specify the max number
  /// of rows/bytes to return in 'result' respectively. The function stops
  /// producing output if it exceeds either limit.
  bool getOutput(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  uint64_t allocatedBytes() const;

  /// Resets the hash table inside the grouping set when partial aggregation
  /// is full or reclaims memory from distinct aggregation after it has received
  /// all the inputs.
  void resetTable();

  /// Returns true if 'this' should start producing partial
  /// aggregation results. Checks the memory consumption against
  /// 'maxBytes'. If exceeding 'maxBytes', sees if changing hash mode
  /// can free up space and rehashes and returns false if significant
  /// space was recovered. In specific, changing from an array hash
  /// based on value ranges to one based on value ids can save a lot.
  bool isPartialFull(int64_t maxBytes);

  /// Returns the count of the hash table, if any.
  int64_t numDistinct() const {
    return table_ ? table_->numDistinct() : 0;
  }

  /// Returns number of global grouping sets rows if there is default output.
  std::optional<vector_size_t> numDefaultGlobalGroupingSetRows() const {
    if (hasDefaultGlobalGroupingSetOutput()) {
      return globalGroupingSets_.size();
    }
    return std::nullopt;
  }

  const HashLookup& hashLookup() const;

  /// Spills all the rows in container.
  void spill();

  /// Spills all the rows in container starting from the offset specified by
  /// 'rowIterator'.
  void spill(const RowContainerIterator& rowIterator);

  /// Returns the spiller stats including total bytes and rows spilled so far.
  std::optional<common::SpillStats> spilledStats() const {
    if (spiller_ == nullptr) {
      return std::nullopt;
    }
    return spiller_->stats();
  }

  /// Returns true if spilling has triggered on this grouping set.
  bool hasSpilled() const;

  /// Returns the hashtable stats.
  HashTableStats hashTableStats() const {
    return table_ ? table_->stats() : HashTableStats{};
  }

  /// Return the number of rows kept in memory.
  int64_t numRows() const {
    return table_ ? table_->rows()->numRows() : 0;
  }

  // Frees hash tables and other state when giving up partial aggregation as
  // non-productive. Must be called before toIntermediate() is used.
  void abandonPartialAggregation();

  /// Translates the raw input in input to accumulators initialized from a
  /// single input row. Passes grouping keys through.
  void toIntermediate(const RowVectorPtr& input, RowVectorPtr& result);

  /// Returns default global grouping sets output if there are no input rows.
  /// The default global grouping set output is a single row per global grouping
  /// set with the groupId key and the default aggregate value.
  /// This function can also be used with distinct aggregations.
  bool getDefaultGlobalGroupingSetOutput(
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  memory::MemoryPool& testingPool() const {
    return pool_;
  }

  std::optional<int64_t> estimateOutputRowSize() const;

 private:
  bool isDistinct() const {
    return aggregates_.empty();
  }

  void addInputForActiveRows(const RowVectorPtr& input, bool mayPushdown);

  void addRemainingInput();

  void initializeGlobalAggregation();

  void destroyGlobalAggregations();

  void addGlobalAggregationInput(const RowVectorPtr& input, bool mayPushdown);

  bool getGlobalAggregationOutput(
      RowContainerIterator& iterator,
      RowVectorPtr& result);

  // If there are global grouping sets, then returns if they have default
  // output in case no input rows were received.
  bool hasDefaultGlobalGroupingSetOutput() const {
    return noMoreInput_ && numInputRows_ == 0 && !globalGroupingSets_.empty() &&
        isRawInput_;
  }

  void createHashTable();

  void populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

  // If the given aggregation has mask, the method returns reference to the
  // selectivity vector from the maskedActiveRows_ (based on the mask channel
  // index for this aggregation), otherwise it returns reference to activeRows_.
  const SelectivityVector& getSelectivityVector(size_t aggregateIndex) const;

  // Checks if input will fit in the existing memory and increases reservation
  // if not. If reservation cannot be increased, spills enough to make 'input'
  // fit.
  void ensureInputFits(const RowVectorPtr& input);

  // Reserves memory for output processing. If reservation cannot be increased,
  // spills enough to make output fit.
  void ensureOutputFits();

  // Copies the grouping keys and aggregates for 'groups' into 'result' If
  // partial output, extracts the intermediate type for aggregates, final result
  // otherwise.
  void extractGroups(folly::Range<char**> groups, const RowVectorPtr& result);

  // Produces output in if spilling has occurred. First produces data
  // from non-spilled partitions, then merges spill runs and unspilled data
  // form spilled partitions. Returns nullptr when at end. 'maxOutputRows' and
  // 'maxOutputBytes' specifies the max number of output rows and bytes in
  // 'result'.
  bool getOutputWithSpill(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  // Prepares for the next spill partition for output. It sets
  // 'outputSpillPartition_' to the number of the next spill partition, and
  // creates 'merge_' to read from it. The function returns false if all the
  // spilled partitions have been processed.
  bool prepareNextSpillPartitionOutput();

  // Reads from spilled rows until producing a batch of final results in
  // 'result'. Returns false and leaves 'result' empty when the spilled data is
  // fully read. 'maxOutputRows' and 'maxOutputBytes' specify the max number of
  // output rows and bytes in 'result'.
  bool mergeNext(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  // Reads from spilled rows for group by with aggregates.
  bool mergeNextWithAggregates(
      int32_t maxOutputRows,
      int32_t maxOutputBytes,
      const RowVectorPtr& result);

  // Reads from spilled rows for group by without aggregates.
  bool mergeNextWithoutAggregates(
      int32_t maxOutputRows,
      const RowVectorPtr& result);

  // Initializes a new row in 'mergeRows' with the keys from the
  // current element from 'stream'. Accumulators are left in the initial
  // state with no data accumulated. This is called each time a new
  // key is received from a merge of spilled data. After this
  // updateRow() is called on the same element and on every subsequent
  // element read from the stream until a new key is seen, at which
  // time we again call initializeRow(). When enough rows have been
  // accumulated and we have a new key, we produce the output and
  // clear 'mergeRows_' with extractSpillResult() and only then do
  // initializeRow().
  void initializeRow(SpillMergeStream& stream, char* row);

  // Updates the accumulators in 'row' with the intermediate type data from
  // 'keys'. This is called for each row received from a merge of spilled data.
  void updateRow(SpillMergeStream& keys, char* row);

  // Returns a RowType of the spilled data.
  RowTypePtr makeSpillType() const;

  // Copies the finalized state from 'mergeRows' to 'result' and clears
  // 'mergeRows'. Used for producing a batch of results when aggregating spilled
  // groups.
  void extractSpillResult(const RowVectorPtr& result);

  // Return a list of accumulators for 'aggregates_', plus one more accumulator
  // for 'sortedAggregations_', and one for each 'distinctAggregations_'.  When
  // 'excludeToIntermediate' is true, skip the functions that support
  // 'toIntermediate'.
  std::vector<Accumulator> accumulators(bool excludeToIntermediate);

  std::vector<column_index_t> keyChannels_;

  /// A subset of grouping keys on which the input is clustered.
  const std::vector<column_index_t> preGroupedKeyChannels_;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  const bool isGlobal_;
  const bool isPartial_;
  const bool isRawInput_;
  const core::QueryConfig& queryConfig_;

  std::vector<AggregateInfo> aggregates_;
  AggregationMasks masks_;
  std::unique_ptr<SortedAggregations> sortedAggregations_;
  std::vector<std::unique_ptr<DistinctAggregations>> distinctAggregations_;

  const bool ignoreNullKeys_;

  uint64_t numInputRows_ = 0;

  // List of global grouping set numbers, if being used with a GROUPING SET.
  const std::vector<vector_size_t> globalGroupingSets_;
  // Column for groupId for a GROUPING SET.
  std::optional<column_index_t> groupIdChannel_;

  const common::SpillConfig* const spillConfig_;

  // Indicates if this grouping set and the associated hash aggregation operator
  // is under non-reclaimable execution section or not.
  tsan_atomic<bool>* const nonReclaimableSection_;

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
  memory::AllocationPool rows_;
  const bool isAdaptive_;

  bool noMoreInput_{false};

  // In case of partial streaming aggregation, the input vector passed to
  // addInput(). A set of rows that belong to the last group of pre-grouped
  // keys need to be processed after flushing the hash table and accumulators.
  RowVectorPtr remainingInput_;

  // First row in remainingInput_ that needs to be processed.
  vector_size_t firstRemainingRow_;

  // The value of mayPushdown flag specified in addInput() for the
  // 'remainingInput_'.
  bool remainingMayPushdown_;

  std::unique_ptr<Spiller> spiller_;

  // The current spill partition in producing spill output. If it is -1, then we
  // haven't started yet.
  int32_t outputSpillPartition_{-1};

  SpillPartitionSet spillPartitionSet_;

  // Sets to the number of files storing the spilled distinct hash table per
  // each spill partition. These are the files generated by the first spill
  // call. This only applies for distinct hash aggregation.
  std::vector<size_t> numDistinctSpillFilesPerPartition_;
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> merge_;

  // Container for materializing batches of output from spilling.
  std::unique_ptr<RowContainer> mergeRows_;

  // The row with the current merge state, allocated from 'mergeRow_'.
  char* mergeState_ = nullptr;

  // Intermediate vector for passing arguments to aggregate in merging spill.
  std::vector<VectorPtr> mergeArgs_;

  // Indicates the element in mergeArgs_[0] that corresponds to the accumulator
  // to merge.
  SelectivityVector mergeSelection_;

  // Pool of the OperatorCtx. Used for spilling.
  memory::MemoryPool& pool_;

  // True if partial aggregation has been given up as non-productive.
  bool abandonedPartialAggregation_{false};

  // True if partial aggregation and all aggregates have a fast path from raw
  // input to intermediate. Initialized in abandonPartialAggregation().
  bool allSupportToIntermediate_;

  // RowContainer for toIntermediate for aggregates that do not have a
  // toIntermediate() fast path
  std::unique_ptr<RowContainer> intermediateRows_;
  std::vector<char*> intermediateGroups_;
  std::vector<vector_size_t> intermediateRowNumbers_;
  // Temporary for case where an aggregate in toIntermediate() outputs post-init
  // state of aggregate for all rows.
  std::vector<char*> firstGroup_;

  folly::Synchronized<common::SpillStats>* const spillStats_;
};

} // namespace facebook::velox::exec
