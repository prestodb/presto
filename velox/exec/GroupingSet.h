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
      bool isRawInput,
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

  // For each aggregation, if that aggregation has mask, the method prepares the
  // selectivity vector by copying activeRows_ and then updating it from the
  // mask column. The selectivity vectors are reused, if more than one
  // aggregation is using it (we keep the map keyed by channel index).
  void prepareMaskedSelectivityVectors(const RowVectorPtr& input);

  // If the given aggregation has mask, the method returns reference to the
  // selectivity vector from the maskedActiveRows_ (based on the mask channel
  // index for this aggregation), otherwise it returns reference to activeRows_.
  const SelectivityVector& getSelectivityVector(size_t aggregateIndex) const;

  std::vector<ChannelIndex> keyChannels_;
  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  const bool isGlobal_;
  const bool isRawInput_;
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
  memory::MappedMemory* const mappedMemory_;

  std::vector<bool> mayPushdown_;

  // Place for the arguments of the aggregate being updated.
  std::vector<VectorPtr> tempVectors_;
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  uint64_t numAdded_ = 0;
  SelectivityVector activeRows_;
  // For aggregations that use masks we keep selectivity vectors in this map,
  // keyed by the channel index, so the selectivity vectors can be reused.
  struct MaskedRows {
    bool prepared{false}; // true, if already prepared for the current batch.
    SelectivityVector rows;
  };
  std::unordered_map<ChannelIndex, MaskedRows> maskedActiveRows_;

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
