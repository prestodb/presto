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
#include "velox/exec/GroupingSet.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

namespace {
bool allAreSinglyReferenced(
    const std::vector<ChannelIndex>& argList,
    const std::unordered_map<ChannelIndex, int>& channelUseCount) {
  return std::all_of(argList.begin(), argList.end(), [&](auto channel) {
    return channelUseCount.find(channel)->second == 1;
  });
}

// Returns true if all vectors are Lazy vectors, possibly wrapped, that haven't
// been loaded yet.
bool areAllLazyNotLoaded(const std::vector<VectorPtr>& vectors) {
  return std::all_of(vectors.begin(), vectors.end(), [](const auto& vector) {
    return isLazyNotLoaded(*vector);
  });
}
} // namespace

GroupingSet::GroupingSet(
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    std::vector<ChannelIndex>&& preGroupedKeys,
    std::vector<std::unique_ptr<Aggregate>>&& aggregates,
    std::vector<std::optional<ChannelIndex>>&& aggrMaskChannels,
    std::vector<std::vector<ChannelIndex>>&& channelLists,
    std::vector<std::vector<VectorPtr>>&& constantLists,
    bool ignoreNullKeys,
    bool isRawInput,
    OperatorCtx* operatorCtx)
    : preGroupedKeyChannels_(std::move(preGroupedKeys)),
      hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      isRawInput_(isRawInput),
      aggregates_(std::move(aggregates)),
      masks_{std::move(aggrMaskChannels)},
      channelLists_(std::move(channelLists)),
      constantLists_(std::move(constantLists)),
      ignoreNullKeys_(ignoreNullKeys),
      mappedMemory_(operatorCtx->mappedMemory()),
      stringAllocator_(mappedMemory_),
      rows_(mappedMemory_),
      isAdaptive_(
          operatorCtx->task()->queryCtx()->config().hashAdaptivityEnabled()),
      execCtx_(*operatorCtx->execCtx()) {
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }
  std::unordered_map<ChannelIndex, int> channelUseCount;
  for (const std::vector<ChannelIndex>& argList : channelLists_) {
    for (ChannelIndex channel : argList) {
      ++channelUseCount[channel];
    }
  }
  for (const std::vector<ChannelIndex>& argList : channelLists_) {
    mayPushdown_.push_back(allAreSinglyReferenced(argList, channelUseCount));
  }
}

namespace {
bool equalKeys(
    const std::vector<ChannelIndex>& keys,
    const RowVectorPtr& vector,
    vector_size_t index,
    vector_size_t otherIndex) {
  for (auto key : keys) {
    const auto& child = vector->childAt(key);
    if (!child->equalValueAt(child.get(), index, otherIndex)) {
      return false;
    }
  }

  return true;
}
} // namespace

void GroupingSet::addInput(const RowVectorPtr& input, bool mayPushdown) {
  if (isGlobal_) {
    addGlobalAggregationInput(input, mayPushdown);
    return;
  }

  auto numRows = input->size();
  if (!preGroupedKeyChannels_.empty()) {
    if (remainingInput_) {
      addRemainingInput();
    }
    // Look for the last group of pre-grouped keys.
    for (auto i = input->size() - 2; i >= 0; --i) {
      if (!equalKeys(preGroupedKeyChannels_, input, i, i + 1)) {
        // Process that many rows, flush the accumulators and the hash
        // table, then add remaining rows.
        numRows = i + 1;

        remainingInput_ = input;
        firstRemainingRow_ = numRows;
        remainingMayPushdown_ = mayPushdown;
        break;
      }
    }
  }

  activeRows_.resize(numRows);
  activeRows_.setAll();

  addInputForActiveRows(input, mayPushdown);
}

void GroupingSet::noMoreInput() {
  noMoreInput_ = true;

  if (remainingInput_) {
    addRemainingInput();
  }
}

bool GroupingSet::hasOutput() {
  return noMoreInput_ || remainingInput_;
}

void GroupingSet::addInputForActiveRows(
    const RowVectorPtr& input,
    bool mayPushdown) {
  bool rehash = false;
  if (!table_) {
    rehash = true;
    createHashTable();
  }
  auto& hashers = lookup_->hashers;
  lookup_->reset(activeRows_.end());
  auto mode = table_->hashMode();
  if (ignoreNullKeys_) {
    // A null in any of the keys disables the row.
    deselectRowsWithNulls(*input, keyChannels_, activeRows_, execCtx_);
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input->loadedChildAt(hashers[i]->channel());
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(*key, activeRows_, lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(*key, activeRows_, i > 0, lookup_->hashes);
      }
    }
  } else {
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input->loadedChildAt(hashers[i]->channel());
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(*key, activeRows_, lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(*key, activeRows_, i > 0, lookup_->hashes);
      }
    }
  }

  if (activeRows_.isAllSelected()) {
    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);
  } else {
    lookup_->rows.clear();
    bits::forEachSetBit(
        activeRows_.asRange().bits(), 0, activeRows_.size(), [&](auto row) {
          lookup_->rows.push_back(row);
        });
  }

  if (rehash) {
    if (table_->hashMode() != BaseHashTable::HashMode::kHash) {
      table_->decideHashMode(input->size());
    }
    addInputForActiveRows(input, mayPushdown);
    return;
  }
  table_->groupProbe(*lookup_);
  masks_.addInput(input, activeRows_);
  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& rows = getSelectivityVector(i);
    // TODO(spershin): We disable the pushdown at the moment if selectivity
    // vector has changed after groups generation, we might want to revisit
    // this.
    const bool canPushdown = (&rows == &activeRows_) && mayPushdown &&
        mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_);
    populateTempVectors(i, input);
    if (isRawInput_) {
      aggregates_[i]->addRawInput(
          lookup_->hits.data(), rows, tempVectors_, canPushdown);
    } else {
      aggregates_[i]->addIntermediateResults(
          lookup_->hits.data(), rows, tempVectors_, canPushdown);
    }
  }
  tempVectors_.clear();
}

void GroupingSet::addRemainingInput() {
  activeRows_.clearAll();
  activeRows_.resize(remainingInput_->size());
  activeRows_.setValidRange(firstRemainingRow_, remainingInput_->size(), true);
  activeRows_.updateBounds();

  addInputForActiveRows(remainingInput_, remainingMayPushdown_);
  remainingInput_.reset();
}

void GroupingSet::createHashTable() {
  if (ignoreNullKeys_) {
    table_ = HashTable<true>::createForAggregation(
        std::move(hashers_), aggregates_, mappedMemory_);
  } else {
    table_ = HashTable<false>::createForAggregation(
        std::move(hashers_), aggregates_, mappedMemory_);
  }
  lookup_ = std::make_unique<HashLookup>(table_->hashers());
  if (!isAdaptive_ && table_->hashMode() != BaseHashTable::HashMode::kHash) {
    table_->forceGenericHashMode();
  }
}

void GroupingSet::initializeGlobalAggregation() {
  if (globalAggregationInitialized_) {
    return;
  }

  lookup_ = std::make_unique<HashLookup>(hashers_);
  lookup_->reset(1);

  // Row layout is:
  //  - null flags - one bit per aggregate,
  // - uint32_t row size,
  //  - fixed-width accumulators - one per aggregate
  //
  // Here we always make space for a row size since we only have one
  // row and no RowContainer.
  int32_t rowSizeOffset = bits::nbytes(aggregates_.size());
  int32_t offset = rowSizeOffset + sizeof(int32_t);
  int32_t nullOffset = 0;

  for (auto& aggregate : aggregates_) {
    aggregate->setAllocator(&stringAllocator_);
    aggregate->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset),
        rowSizeOffset);
    offset += aggregate->accumulatorFixedWidthSize();
    ++nullOffset;
  }

  auto singleGroup = std::vector<vector_size_t>{0};
  lookup_->hits[0] = rows_.allocateFixed(offset);
  for (auto& aggregate : aggregates_) {
    aggregate->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }

  globalAggregationInitialized_ = true;
}

void GroupingSet::addGlobalAggregationInput(
    const RowVectorPtr& input,
    bool mayPushdown) {
  initializeGlobalAggregation();

  auto numRows = input->size();
  activeRows_.resize(numRows);
  activeRows_.setAll();

  masks_.addInput(input, activeRows_);
  for (auto i = 0; i < aggregates_.size(); ++i) {
    populateTempVectors(i, input);
    const auto& rows = getSelectivityVector(i);
    const bool canPushdown =
        mayPushdown && mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_);
    if (isRawInput_) {
      aggregates_[i]->addSingleGroupRawInput(
          lookup_->hits[0], rows, tempVectors_, canPushdown);
    } else {
      aggregates_[i]->addSingleGroupIntermediateResults(
          lookup_->hits[0], rows, tempVectors_, canPushdown);
    }
  }
  tempVectors_.clear();
}

bool GroupingSet::getGlobalAggregationOutput(
    int32_t batchSize,
    bool isPartial,
    RowContainerIterator* iterator,
    RowVectorPtr& result) {
  VELOX_CHECK_EQ(batchSize, 1);
  if (iterator->allocationIndex != 0) {
    return false;
  }

  initializeGlobalAggregation();

  auto groups = lookup_->hits.data();
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i]->finalize(groups, 1);
    if (isPartial) {
      aggregates_[i]->extractAccumulators(groups, 1, &result->childAt(i));
    } else {
      aggregates_[i]->extractValues(groups, 1, &result->childAt(i));
    }
  }
  iterator->allocationIndex = std::numeric_limits<int32_t>::max();
  return true;
}

void GroupingSet::populateTempVectors(
    int32_t aggregateIndex,
    const RowVectorPtr& input) {
  auto& channels = channelLists_[aggregateIndex];
  tempVectors_.resize(channels.size());
  for (auto i = 0; i < channels.size(); ++i) {
    if (channels[i] == kConstantChannel) {
      tempVectors_[i] = BaseVector::wrapInConstant(
          input->size(), 0, constantLists_[aggregateIndex][i]);
    } else {
      // No load of lazy vectors; The aggregate may decide to push down.
      tempVectors_[i] = input->childAt(channels[i]);
    }
  }
}

const SelectivityVector& GroupingSet::getSelectivityVector(
    size_t aggregateIndex) const {
  auto* rows = masks_.activeRows(aggregateIndex);

  // No mask? Use the current selectivity vector for this aggregation.
  if (not rows) {
    return activeRows_;
  }

  return *rows;
}

bool GroupingSet::getOutput(
    int32_t batchSize,
    bool isPartial,
    RowContainerIterator* iterator,
    RowVectorPtr& result) {
  if (isGlobal_) {
    return getGlobalAggregationOutput(batchSize, isPartial, iterator, result);
  }

  // @lint-ignore CLANGTIDY
  char* groups[batchSize];
  int32_t numGroups =
      table_ ? table_->rows()->listRows(iterator, batchSize, groups) : 0;
  if (!numGroups) {
    if (table_) {
      table_->clear();
    }
    if (remainingInput_) {
      addRemainingInput();
    }
    return false;
  }
  result->resize(numGroups);
  auto totalKeys = lookup_->hashers.size();
  for (int32_t i = 0; i < totalKeys; ++i) {
    auto keyVector = result->childAt(i);
    table_->rows()->extractColumn(groups, numGroups, i, keyVector);
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i]->finalize(groups, numGroups);
    auto& aggregateVector = result->childAt(i + totalKeys);
    if (isPartial) {
      aggregates_[i]->extractAccumulators(groups, numGroups, &aggregateVector);
    } else {
      aggregates_[i]->extractValues(groups, numGroups, &aggregateVector);
    }
  }
  return true;
}

void GroupingSet::resetPartial() {
  if (table_) {
    table_->clear();
  }
}

uint64_t GroupingSet::allocatedBytes() const {
  if (table_) {
    return table_->allocatedBytes();
  }

  return stringAllocator_.retainedSize() + rows_.allocatedBytes();
}

const HashLookup& GroupingSet::hashLookup() const {
  return *lookup_;
}

} // namespace facebook::velox::exec
