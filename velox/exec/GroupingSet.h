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

#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

class Aggregate;

class GroupingSet {
 public:
  GroupingSet(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      std::vector<std::unique_ptr<Aggregate>>&& aggregates,
      std::vector<std::optional<ChannelIndex>>&& aggrMaskChannels,
      std::vector<std::vector<ChannelIndex>>&& channelLists,
      std::vector<std::vector<VectorPtr>>&& constantLists,
      bool ignoreNullKeys,
      OperatorCtx* driverCtx);

  void addInput(const RowVectorPtr& input, bool mayPushdown);

  bool getOutput(
      int32_t batchSize,
      bool isPartial,
      RowContainerIterator* iterator,
      RowVectorPtr& result);

  uint64_t allocatedBytes() const;

  void resetPartial();

  const HashLookup& hashLookup() const;

 private:
  void initializeGlobalAggregation();

  void populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

  // If the given aggregation has mask, the method copies the activeRows_ to
  // maskedActiveRows_, updates maskedActiveRows_ from the mask column and
  // returns pointer to the maskedActiveRows_. Otherwise it simply returns
  // pointer to activeRows_.
  const SelectivityVector* prepareSelectivityVector(
      size_t aggregateIndex,
      const RowVectorPtr& input);

  std::vector<ChannelIndex> keyChannels_;
  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  const bool isGlobal_;
  std::vector<std::unique_ptr<Aggregate>> aggregates_;
  // For each aggregation, can hold an index to a boolean channel (projection in
  // the input row vector), that acts as row mask for the aggregation.
  std::vector<std::optional<ChannelIndex>> aggrMaskChannels_;
  // Argument list for the corresponding element of 'aggregates_'.
  const std::vector<std::vector<ChannelIndex>> channelLists_;
  // Constant arguments to aggregates. Corresponds pairwise to
  // 'channelLists_'. This is used when channelLists_[i][j] ==
  // kConstantChannel.
  const std::vector<std::vector<VectorPtr>> constantLists_;
  const bool ignoreNullKeys_;
  DriverCtx* const driverCtx_;
  memory::MappedMemory* const mappedMemory_;

  std::vector<bool> mayPushdown_;

  // Place for the arguments of the aggregate being updated.
  std::vector<VectorPtr> tempVectors_;
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  uint64_t numAdded_ = 0;
  SelectivityVector activeRows_;
  // In case of an aggregation using a mask, this selectivity vector is updated
  // from the mask boolean projection and is used to run aggregations, instead
  // of the activeRows_.
  SelectivityVector maskedActiveRows_;

  // We use this vector to decode mask boolean projections to update aggregation
  // masks.
  DecodedVector decodedMask_;

  // Used to allocate memory for a single row accumulating results of global
  // aggregation
  HashStringAllocator stringAllocator_;
  AllocationPool rows_;
  const bool isAdaptive_;
};

} // namespace facebook::velox::exec
