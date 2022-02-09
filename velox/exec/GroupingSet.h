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
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

class Aggregate;

class GroupingSet {
 public:
  GroupingSet(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      std::vector<ChannelIndex>&& preGroupedKeys,
      std::vector<std::unique_ptr<Aggregate>>&& aggregates,
      std::vector<std::optional<ChannelIndex>>&& aggrMaskChannels,
      std::vector<std::vector<ChannelIndex>>&& channelLists,
      std::vector<std::vector<VectorPtr>>&& constantLists,
      bool ignoreNullKeys,
      bool isRawInput,
      OperatorCtx* driverCtx);

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
      bool isPartial,
      RowContainerIterator* iterator,
      RowVectorPtr& result);

  uint64_t allocatedBytes() const;

  void resetPartial();

  const HashLookup& hashLookup() const;

 private:
  void addInputForActiveRows(const RowVectorPtr& input, bool mayPushdown);

  void addRemainingInput();

  void initializeGlobalAggregation();

  void addGlobalAggregationInput(const RowVectorPtr& input, bool mayPushdown);

  bool getGlobalAggregationOutput(
      int32_t batchSize,
      bool isPartial,
      RowContainerIterator* iterator,
      RowVectorPtr& result);

  void createHashTable();

  void populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

  // If the given aggregation has mask, the method returns reference to the
  // selectivity vector from the maskedActiveRows_ (based on the mask channel
  // index for this aggregation), otherwise it returns reference to activeRows_.
  const SelectivityVector& getSelectivityVector(size_t aggregateIndex) const;

  std::vector<ChannelIndex> keyChannels_;

  /// A subset of grouping keys on which the input is clustered.
  const std::vector<ChannelIndex> preGroupedKeyChannels_;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  const bool isGlobal_;
  const bool isRawInput_;
  std::vector<std::unique_ptr<Aggregate>> aggregates_;
  AggregationMasks masks_;
  // Argument list for the corresponding element of 'aggregates_'.
  const std::vector<std::vector<ChannelIndex>> channelLists_;
  // Constant arguments to aggregates. Corresponds pairwise to
  // 'channelLists_'. This is used when channelLists_[i][j] ==
  // kConstantChannel.
  const std::vector<std::vector<VectorPtr>> constantLists_;
  const bool ignoreNullKeys_;
  memory::MappedMemory* const mappedMemory_;

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
};

} // namespace facebook::velox::exec
