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
    const std::vector<column_index_t>& argList,
    const std::unordered_map<column_index_t, int>& channelUseCount) {
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

std::optional<std::string> makeSpillPath(
    bool isPartial,
    const OperatorCtx& operatorCtx) {
  if (isPartial) {
    return std::nullopt;
  }
  auto path = operatorCtx.task()->queryCtx()->config().spillPath();
  if (path.has_value()) {
    return path.value() + "/" + operatorCtx.task()->taskId();
  }
  return std::nullopt;
}
} // namespace

GroupingSet::GroupingSet(
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
    OperatorCtx* FOLLY_NONNULL operatorCtx)
    : preGroupedKeyChannels_(std::move(preGroupedKeys)),
      hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      isPartial_(isPartial),
      isRawInput_(isRawInput),
      aggregates_(std::move(aggregates)),
      masks_{std::move(aggrMaskChannels)},
      channelLists_(std::move(channelLists)),
      constantLists_(std::move(constantLists)),
      intermediateTypes_(std::move(intermediateTypes)),
      ignoreNullKeys_(ignoreNullKeys),
      spillableReservationGrowthPct_(operatorCtx->driverCtx()
                                         ->queryConfig()
                                         .spillableReservationGrowthPct()),
      spillPartitionBits_(
          operatorCtx->driverCtx()->queryConfig().spillPartitionBits()),
      spillFileSizeFactor_(
          operatorCtx->driverCtx()->queryConfig().spillFileSizeFactor()),
      mappedMemory_(operatorCtx->mappedMemory()),
      stringAllocator_(mappedMemory_),
      rows_(mappedMemory_),
      isAdaptive_(
          operatorCtx->task()->queryCtx()->config().hashAdaptivityEnabled()),
      spillPath_(makeSpillPath(isPartial, *operatorCtx)),
      pool_(*operatorCtx->pool()),
      spillExecutor_(operatorCtx->task()->queryCtx()->spillExecutor()),
      testSpillPct_(
          operatorCtx->task()->queryCtx()->config().testingSpillPct()) {
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }
  std::unordered_map<column_index_t, int> channelUseCount;
  for (const std::vector<column_index_t>& argList : channelLists_) {
    for (column_index_t channel : argList) {
      ++channelUseCount[channel];
    }
  }
  for (const std::vector<column_index_t>& argList : channelLists_) {
    mayPushdown_.push_back(allAreSinglyReferenced(argList, channelUseCount));
  }
}

namespace {
bool equalKeys(
    const std::vector<column_index_t>& keys,
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
  VELOX_CHECK(!isGlobal_);
  bool rehash = false;
  if (!table_) {
    rehash = true;
    createHashTable();
  }
  auto& hashers = lookup_->hashers;
  lookup_->reset(activeRows_.end());
  auto mode = table_->hashMode();
  ensureInputFits(input);

  for (auto i = 0; i < hashers.size(); ++i) {
    auto key = input->childAt(hashers[i]->channel())->loadedVector();
    hashers[i]->decode(*key, activeRows_);
  }

  if (ignoreNullKeys_) {
    // A null in any of the keys disables the row.
    deselectRowsWithNulls(hashers, activeRows_);
  }

  for (int32_t i = 0; i < hashers.size(); ++i) {
    if (mode != BaseHashTable::HashMode::kHash) {
      if (!hashers[i]->computeValueIds(activeRows_, lookup_->hashes)) {
        rehash = true;
      }
    } else {
      hashers[i]->hash(activeRows_, i > 0, lookup_->hashes);
    }
  }

  if (rehash) {
    if (table_->hashMode() != BaseHashTable::HashMode::kHash) {
      table_->decideHashMode(input->size());
    }
    addInputForActiveRows(input, mayPushdown);
    return;
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

  table_->groupProbe(*lookup_);
  masks_.addInput(input, activeRows_);
  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& rows = getSelectivityVector(i);

    // Check is mask is false for all rows.
    if (!rows.hasSelections()) {
      continue;
    }

    populateTempVectors(i, input);
    // TODO(spershin): We disable the pushdown at the moment if selectivity
    // vector has changed after groups generation, we might want to revisit
    // this.
    const bool canPushdown = (&rows == &activeRows_) && mayPushdown &&
        mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_);
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
  activeRows_.resize(remainingInput_->size());
  activeRows_.clearAll();
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

  lookup_->hits[0] = rows_.allocateFixed(offset);
  const auto singleGroup = std::vector<vector_size_t>{0};
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
    const auto& rows = getSelectivityVector(i);

    // Check is mask is false for all rows.
    if (!rows.hasSelections()) {
      continue;
    }

    populateTempVectors(i, input);
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
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  VELOX_CHECK_EQ(batchSize, 1);
  if (iterator.allocationIndex != 0) {
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
  iterator.allocationIndex = std::numeric_limits<int32_t>::max();
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
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  if (isGlobal_) {
    return getGlobalAggregationOutput(batchSize, isPartial_, iterator, result);
  }
  if (spiller_) {
    return getOutputWithSpill(result);
  }

  // @lint-ignore CLANGTIDY
  char* groups[batchSize];
  int32_t numGroups =
      table_ ? table_->rows()->listRows(&iterator, batchSize, groups) : 0;
  if (!numGroups) {
    if (table_) {
      table_->clear();
    }
    if (remainingInput_) {
      addRemainingInput();
    }
    return false;
  }
  extractGroups(folly::Range<char**>(groups, numGroups), result);
  return true;
}

void GroupingSet::extractGroups(
    folly::Range<char**> groups,
    const RowVectorPtr& result) {
  result->resize(groups.size());
  RowContainer& rows = table_ ? *table_->rows() : *rowsWhileReadingSpill_;
  auto totalKeys = rows.keyTypes().size();
  for (int32_t i = 0; i < totalKeys; ++i) {
    auto keyVector = result->childAt(i);
    rows.extractColumn(groups.data(), groups.size(), i, keyVector);
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i]->finalize(groups.data(), groups.size());
    auto& aggregateVector = result->childAt(i + totalKeys);
    if (isPartial_) {
      aggregates_[i]->extractAccumulators(
          groups.data(), groups.size(), &aggregateVector);
    } else {
      aggregates_[i]->extractValues(
          groups.data(), groups.size(), &aggregateVector);
    }
  }
}

void GroupingSet::resetPartial() {
  if (table_ != nullptr) {
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

void GroupingSet::ensureInputFits(const RowVectorPtr& input) {
  // Spilling is considered if this is a final or single aggregation and
  // spillPath is set.
  if (isPartial_ || !spillPath_.has_value()) {
    return;
  }
  auto numDistinct = table_->numDistinct();
  if (!numDistinct) {
    // Table is empty. Nothing to spill.
    return;
  }

  auto tableIncrement = table_->hashTableSizeIncrease(input->size());

  auto rows = table_->rows();

  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  auto outOfLineBytesPerRow = outOfLineBytes / numDistinct;

  int64_t flatBytes = input->estimateFlatSize();

  // Test-only spill path.
  if (testSpillPct_ &&
      (folly::hasher<uint64_t>()(++spillTestCounter_)) % 100 <= testSpillPct_) {
    const auto rowsToSpill = std::max<int64_t>(1, numDistinct / 10);
    spill(
        numDistinct - rowsToSpill,
        outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow));
    return;
  }

  if (!tableIncrement && freeRows > input->size() &&
      (outOfLineBytes == 0 || outOfLineFreeBytes >= flatBytes)) {
    // Enough free rows for input rows and enough variable length free
    // space for the flat size of the whole vector. If outOfLineBytes
    // is 0 there is no need for variable length space.
    return;
  }
  // If there is variable length data we take the flat size of the
  // input as a cap on the new variable length data needed.
  auto increment =
      rows->sizeIncrement(input->size(), outOfLineBytes ? flatBytes : 0) +
      tableIncrement;
  auto tracker = mappedMemory_->tracker();
  // There must be at least 2x the increment in reservation.
  if (tracker->getAvailableReservation() > 2 * increment) {
    return;
  }
  // Check if can increase reservation. The increment is the larger of
  // twice the maximum increment from this input and
  // 'spillableReservationGrowthPct_' of the current reservation.
  auto targetIncrement = std::max<int64_t>(
      increment * 2,
      tracker->getCurrentUserBytes() * spillableReservationGrowthPct_ / 100);
  if (tracker->maybeReserve(targetIncrement)) {
    return;
  }
  auto rowsToSpill = std::max<int64_t>(
      1, targetIncrement / (rows->fixedRowSize() + outOfLineBytesPerRow));
  spill(
      std::max<int64_t>(0, numDistinct - rowsToSpill),
      std::max<int64_t>(
          0, outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow)));
}

void GroupingSet::spill(int64_t targetRows, int64_t targetBytes) {
  if (!spiller_) {
    auto rows = table_->rows();
    auto types = rows->keyTypes();
    types.insert(
        types.end(), intermediateTypes_.begin(), intermediateTypes_.end());
    std::vector<std::string> names;
    for (auto i = 0; i < types.size(); ++i) {
      names.push_back(fmt::format("s{}", i));
    }
    assert(mappedMemory_->tracker()); // lint
    const auto fileSize =
        mappedMemory_->tracker()->getCurrentUserBytes() / spillFileSizeFactor_;
    spiller_ = std::make_unique<Spiller>(
        Spiller::Type::kAggregate,
        *rows,
        [&](folly::Range<char**> rows) { table_->erase(rows); },
        ROW(std::move(names), std::move(types)),
        // Spill up to 8 partitions based on bits starting from 29th of the hash
        // number. Any from one to three bits would do.
        HashBitRange(29, 29 + spillPartitionBits_),
        rows->keyTypes().size(),
        std::vector<CompareFlags>(),
        spillPath_.value(),
        fileSize,
        Spiller::spillPool(),
        spillExecutor_);
  }
  spiller_->spill(targetRows, targetBytes);
}

bool GroupingSet::getOutputWithSpill(const RowVectorPtr& result) {
  if (outputPartition_ == -1) {
    mergeArgs_.resize(1);
    std::vector<TypePtr> keyTypes;
    for (auto& hasher : table_->hashers()) {
      keyTypes.push_back(hasher->type());
    }
    mergeRows_ = std::make_unique<RowContainer>(
        keyTypes,
        !ignoreNullKeys_,
        aggregates_,
        std::vector<TypePtr>(),
        false,
        false,
        false,
        false,
        mappedMemory_,
        ContainerRowSerde::instance());
    // Take ownership of the rows and free the hash table. The table will not be
    // needed for producing spill output.
    rowsWhileReadingSpill_ = table_->moveRows();
    table_.reset();
    outputPartition_ = 0;
    nonSpilledRows_ = spiller_->finishSpill();
  }

  if (nonSpilledIndex_ < nonSpilledRows_.value().size()) {
    uint64_t bytes = 0;
    vector_size_t numGroups = 0;
    // Produce non-spilled content at max 1000 rows at a time.
    auto limit = std::min<size_t>(
        1000, nonSpilledRows_.value().size() - nonSpilledIndex_);
    for (; numGroups < limit; ++numGroups) {
      bytes += rowsWhileReadingSpill_->rowSize(
          nonSpilledRows_.value()[nonSpilledIndex_ + numGroups]);
      if (bytes > maxBatchBytes_) {
        ++numGroups;
        break;
      }
    }
    extractGroups(
        folly::Range<char**>(
            nonSpilledRows_.value().data() + nonSpilledIndex_, numGroups),
        result);
    nonSpilledIndex_ += numGroups;
    return true;
  }
  while (outputPartition_ < spiller_->state().maxPartitions()) {
    if (!merge_) {
      merge_ = spiller_->startMerge(outputPartition_);
    }
    // NOTE: 'merge_' might be nullptr if 'outputPartition_' is empty.
    if (merge_ == nullptr || !mergeNext(result)) {
      ++outputPartition_;
      merge_ = nullptr;
      continue;
    }
    return true;
  }
  return false;
}

bool GroupingSet::mergeNext(const RowVectorPtr& result) {
  constexpr int32_t kBatchBytes = 1 << 20; // 1MB
  for (;;) {
    auto next = merge_->nextWithEquals();
    if (!next.first) {
      extractSpillResult(result);
      return result->size() > 0;
    }
    if (!nextKeyIsEqual_) {
      mergeState_ = mergeRows_->newRow();
      initializeRow(*next.first, mergeState_);
    }
    updateRow(*next.first, mergeState_);
    nextKeyIsEqual_ = next.second;
    next.first->pop();
    if (!nextKeyIsEqual_ && mergeRows_->allocatedBytes() > kBatchBytes) {
      extractSpillResult(result);
      return true;
    }
  }
}

void GroupingSet::initializeRow(SpillStream& keys, char* FOLLY_NONNULL row) {
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    mergeRows_->store(keys.decoded(i), keys.currentIndex(), mergeState_, i);
  }
  vector_size_t zero = 0;
  for (auto& aggregate : aggregates_) {
    aggregate->initializeNewGroups(
        &row, folly::Range<const vector_size_t*>(&zero, 1));
  }
}

void GroupingSet::extractSpillResult(const RowVectorPtr& result) {
  std::vector<char*> rows(mergeRows_->numRows());
  RowContainerIterator iter;
  mergeRows_->listRows(
      &iter, rows.size(), RowContainer::kUnlimited, rows.data());
  extractGroups(folly::Range<char**>(rows.data(), rows.size()), result);
  mergeRows_->clear();
}

void GroupingSet::updateRow(SpillStream& input, char* FOLLY_NONNULL row) {
  if (input.currentIndex() >= mergeSelection_.size()) {
    mergeSelection_.resize(bits::roundUp(input.currentIndex() + 1, 64));
    mergeSelection_.clearAll();
  }
  mergeSelection_.setValid(input.currentIndex(), true);
  mergeSelection_.updateBounds();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    mergeArgs_[0] = input.current().childAt(i + keyChannels_.size());
    aggregates_[i]->addSingleGroupIntermediateResults(
        row, mergeSelection_, mergeArgs_, false);
  }
  mergeSelection_.setValid(input.currentIndex(), false);
}

} // namespace facebook::velox::exec
