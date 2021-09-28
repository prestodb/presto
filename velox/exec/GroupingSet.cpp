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
  for (auto channel : argList) {
    if (channelUseCount.find(channel)->second > 1) {
      return false;
    }
  }
  return true;
}

// Returns true if all vectors are Lazy vectors, possibly wrapped, that haven't
// been loaded yet.
bool areAllLazyNotLoaded(const std::vector<VectorPtr>& vectors) {
  for (const auto& vector : vectors) {
    if (!isLazyNotLoaded(*vector)) {
      return false;
    }
  }
  return true;
}
} // namespace

GroupingSet::GroupingSet(
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    std::vector<std::unique_ptr<Aggregate>>&& aggregates,
    std::vector<std::optional<ChannelIndex>>&& aggrMaskChannels,
    std::vector<std::vector<ChannelIndex>>&& channelLists,
    std::vector<std::vector<VectorPtr>>&& constantLists,
    bool ignoreNullKeys,
    bool isRawInput,
    OperatorCtx* operatorCtx)
    : hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      isRawInput_(isRawInput),
      aggregates_(std::move(aggregates)),
      aggrMaskChannels_(std::move(aggrMaskChannels)),
      channelLists_(std::move(channelLists)),
      constantLists_(std::move(constantLists)),
      ignoreNullKeys_(ignoreNullKeys),
      driverCtx_(operatorCtx->driverCtx()),
      mappedMemory_(operatorCtx->mappedMemory()),
      stringAllocator_(mappedMemory_),
      rows_(mappedMemory_),
      isAdaptive_(operatorCtx->task()->queryCtx()->hashAdaptivityEnabled()) {
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

void GroupingSet::addInput(const RowVectorPtr& input, bool mayPushdown) {
  auto numRows = input->size();
  activeRows_.resize(numRows);
  activeRows_.setAll();
  if (isGlobal_) {
    // global aggregation
    if (numAdded_ == 0) {
      initializeGlobalAggregation();
    }
    prepareMaskedSelectivityVectors(input);
    numAdded_ += numRows;
    for (auto i = 0; i < aggregates_.size(); ++i) {
      populateTempVectors(i, input);
      const SelectivityVector& rows = getSelectivityVector(i);
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
    return;
  }

  bool rehash = false;
  if (!table_) {
    rehash = true;
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
  auto& hashers = lookup_->hashers;
  lookup_->reset(numRows);
  auto mode = table_->hashMode();
  if (ignoreNullKeys_) {
    // A null in any of the keys disables the row.
    deselectRowsWithNulls(*input, keyChannels_, activeRows_);
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input->loadedChildAt(hashers[i]->channel());
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(*key, activeRows_, &lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(*key, activeRows_, i > 0, &lookup_->hashes);
      }
    }
    lookup_->rows.clear();
    bits::forEachSetBit(
        activeRows_.asRange().bits(),
        0,
        activeRows_.size(),
        [&](vector_size_t row) { lookup_->rows.push_back(row); });
  } else {
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input->loadedChildAt(hashers[i]->channel());
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(*key, activeRows_, &lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(*key, activeRows_, i > 0, &lookup_->hashes);
      }
    }
    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);
  }
  if (rehash) {
    if (table_->hashMode() != BaseHashTable::HashMode::kHash) {
      table_->decideHashMode(input->size());
    }
    addInput(input, mayPushdown);
    return;
  }
  numAdded_ += lookup_->rows.size();
  table_->groupProbe(*lookup_);
  prepareMaskedSelectivityVectors(input);
  for (auto i = 0; i < aggregates_.size(); ++i) {
    const SelectivityVector& rows = getSelectivityVector(i);
    // TODO(spershin): We disable the pushdown at the moment if selectivity
    // vector has changed after groups generation, we might want to revisit
    // this.
    const bool canPushdown = (&rows != &activeRows_) && mayPushdown &&
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

void GroupingSet::initializeGlobalAggregation() {
  lookup_ = std::make_unique<HashLookup>(hashers_);
  lookup_->reset(1);

  // Row layout is:
  //  - null flags - one bit per aggregate,
  //  - fixed-width accumulators - one per aggregate
  int32_t offset = bits::nbytes(aggregates_.size());
  int32_t nullOffset = 0;

  for (auto& aggregate : aggregates_) {
    aggregate->setAllocator(&stringAllocator_);
    aggregate->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset));
    offset += aggregate->accumulatorFixedWidthSize();
    ++nullOffset;
  }

  auto singleGroup = std::vector<vector_size_t>{0};
  lookup_->hits[0] = rows_.allocateFixed(offset);
  for (auto& aggregate : aggregates_) {
    aggregate->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }
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

void GroupingSet::prepareMaskedSelectivityVectors(const RowVectorPtr& input) {
  // Clear the flag for existing selectivity vectors (from the previous batch),
  // so we know if we need to recalculate them.
  for (auto& it : maskedActiveRows_) {
    it.second.prepared = false;
  }
  for (auto aggrIndex = 0; aggrIndex < aggrMaskChannels_.size(); ++aggrIndex) {
    if (not aggrMaskChannels_[aggrIndex].has_value()) {
      continue;
    }
    const auto maskChannelIndex = aggrMaskChannels_[aggrIndex].value();

    // See, if we already prepared 'rows' for this channel or not.
    MaskedRows& maskedRows = maskedActiveRows_[maskChannelIndex];
    if (not maskedRows.prepared) {
      // Get the projection column vector that would be our mask.
      const auto& maskVector = input->childAt(maskChannelIndex);

      maskedRows.rows = activeRows_;
      // Get decoded vector and update the masked selectivity vector.
      decodedMask_.decode(*maskVector, activeRows_);
      activeRows_.applyToSelected([&](vector_size_t i) {
        if (maskVector->isNullAt(i) || !decodedMask_.valueAt<bool>(i)) {
          maskedRows.rows.setValid(i, false);
        }
      });
      maskedRows.prepared = true;
      maskedRows.rows.updateBounds();
    }
  }
}

const SelectivityVector& GroupingSet::getSelectivityVector(
    size_t aggregateIndex) const {
  // No mask? Use the current selectivity vector for this aggregation.
  if (not aggrMaskChannels_[aggregateIndex].has_value()) {
    return activeRows_;
  }

  // Get the projection column vector that would be our mask.
  auto it = maskedActiveRows_.find(aggrMaskChannels_[aggregateIndex].value());
  DCHECK(it != maskedActiveRows_.end());
  return it->second.rows;
}

bool GroupingSet::getOutput(
    int32_t batchSize,
    bool isPartial,
    RowContainerIterator* iterator,
    RowVectorPtr& result) {
  if (isGlobal_) {
    // global aggregation
    VELOX_CHECK_EQ(batchSize, 1);
    if (iterator->allocationIndex != 0) {
      return false;
    }

    if (numAdded_ == 0) {
      initializeGlobalAggregation();
    }

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

  // @lint-ignore CLANGTIDY
  char* groups[batchSize];
  int32_t numGroups =
      table_ ? table_->rows()->listRows(iterator, batchSize, groups) : 0;
  if (!numGroups) {
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
    auto aggregateVector = result->childAt(i + totalKeys);
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
