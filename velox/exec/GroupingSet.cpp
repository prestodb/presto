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
#include "velox/exec/Aggregate.h"
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

std::vector<std::optional<column_index_t>> maskChannels(
    const std::vector<AggregateInfo>& aggregates) {
  std::vector<std::optional<column_index_t>> masks;
  masks.reserve(aggregates.size());
  for (const auto& aggregate : aggregates) {
    masks.push_back(aggregate.mask);
  }
  return masks;
}
} // namespace

GroupingSet::GroupingSet(
    const RowTypePtr& inputType,
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    std::vector<column_index_t>&& preGroupedKeys,
    std::vector<AggregateInfo>&& aggregates,
    bool ignoreNullKeys,
    bool isPartial,
    bool isRawInput,
    const Spiller::Config* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    OperatorCtx* operatorCtx)
    : preGroupedKeyChannels_(std::move(preGroupedKeys)),
      hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      isPartial_(isPartial),
      isRawInput_(isRawInput),
      aggregates_(std::move(aggregates)),
      masks_(maskChannels(aggregates_)),
      ignoreNullKeys_(ignoreNullKeys),
      spillMemoryThreshold_(operatorCtx->driverCtx()
                                ->queryConfig()
                                .aggregationSpillMemoryThreshold()),
      spillConfig_(spillConfig),
      nonReclaimableSection_(nonReclaimableSection),
      stringAllocator_(operatorCtx->pool()),
      rows_(operatorCtx->pool()),
      isAdaptive_(operatorCtx->task()
                      ->queryCtx()
                      ->queryConfig()
                      .hashAdaptivityEnabled()),
      pool_(*operatorCtx->pool()) {
  VELOX_CHECK_NOT_NULL(nonReclaimableSection_);
  VELOX_CHECK(pool_.trackUsage());
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }
  std::unordered_map<column_index_t, int> channelUseCount;
  for (const auto& aggregate : aggregates_) {
    for (auto channel : aggregate.inputs) {
      ++channelUseCount[channel];
    }
  }

  for (const auto& aggregate : aggregates_) {
    mayPushdown_.push_back(
        allAreSinglyReferenced(aggregate.inputs, channelUseCount));
  }

  // Setup SortedAggregations.
  std::vector<AggregateInfo*> sortedAggs;
  for (auto& aggregate : aggregates_) {
    if (!aggregate.sortingKeys.empty()) {
      sortedAggs.push_back(&aggregate);
    }
  }

  if (!sortedAggs.empty()) {
    sortedAggregations_ =
        std::make_unique<SortedAggregations>(sortedAggs, inputType, &pool_);
  }
}

GroupingSet::~GroupingSet() {
  if (isGlobal_) {
    destroyGlobalAggregations();
  }
}

std::unique_ptr<GroupingSet> GroupingSet::createForMarkDistinct(
    const RowTypePtr& inputType,
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    OperatorCtx* FOLLY_NONNULL operatorCtx,
    tsan_atomic<bool>* nonReclaimableSection) {
  return std::make_unique<GroupingSet>(
      inputType,
      std::move(hashers),
      /*preGroupedKeys*/ std::vector<column_index_t>{},
      /*aggregates*/ std::vector<AggregateInfo>{},
      /*ignoreNullKeys*/ false,
      /*isPartial*/ false,
      /*isRawInput*/ false,
      /*spillConfig*/ nullptr,
      nonReclaimableSection,
      operatorCtx);
};

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

  if (sortedAggregations_) {
    sortedAggregations_->noMoreInput();
  }
}

bool GroupingSet::hasOutput() {
  return noMoreInput_ || remainingInput_;
}

void GroupingSet::addInputForActiveRows(
    const RowVectorPtr& input,
    bool mayPushdown) {
  VELOX_CHECK(!isGlobal_);
  if (!table_) {
    createHashTable();
  }
  ensureInputFits(input);

  // Prevents the memory arbitrator to reclaim memory from this grouping set
  // during the execution below.
  //
  // NOTE: 'nonReclaimableSection_' points to the corresponding flag in the
  // associated aggregation operator.
  auto guard = folly::makeGuard([this]() { *nonReclaimableSection_ = false; });
  *nonReclaimableSection_ = true;

  table_->prepareForProbe(*lookup_, input, activeRows_, ignoreNullKeys_);
  table_->groupProbe(*lookup_);
  masks_.addInput(input, activeRows_);

  auto* groups = lookup_->hits.data();
  const auto& newGroups = lookup_->newGroups;

  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }

    auto& function = aggregates_[i].function;
    if (!newGroups.empty()) {
      function->initializeNewGroups(groups, newGroups);
    }

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
      function->addRawInput(groups, rows, tempVectors_, canPushdown);
    } else {
      function->addIntermediateResults(groups, rows, tempVectors_, canPushdown);
    }
  }
  tempVectors_.clear();

  if (sortedAggregations_) {
    if (!newGroups.empty()) {
      sortedAggregations_->initializeNewGroups(groups, newGroups);
    }
    sortedAggregations_->addInput(groups, input);
  }
}

void GroupingSet::addRemainingInput() {
  activeRows_.resize(remainingInput_->size());
  activeRows_.clearAll();
  activeRows_.setValidRange(firstRemainingRow_, remainingInput_->size(), true);
  activeRows_.updateBounds();

  addInputForActiveRows(remainingInput_, remainingMayPushdown_);
  remainingInput_.reset();
}

namespace {

void initializeAggregates(
    const std::vector<AggregateInfo>& aggregates,
    RowContainer& rows) {
  const auto numKeys = rows.keyTypes().size();
  for (auto i = 0; i < aggregates.size(); ++i) {
    auto& function = aggregates[i].function;
    function->setAllocator(&rows.stringAllocator());

    const auto rowColumn = rows.columnAt(numKeys + i);
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rows.rowSizeOffset());
  }
}
} // namespace

std::vector<Accumulator> GroupingSet::accumulators() {
  std::vector<Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (auto& aggregate : aggregates_) {
    accumulators.push_back(aggregate.function.get());
  }

  if (sortedAggregations_ != nullptr) {
    accumulators.push_back(sortedAggregations_->accumulator());
  }
  return accumulators;
}

void GroupingSet::createHashTable() {
  if (ignoreNullKeys_) {
    table_ = HashTable<true>::createForAggregation(
        std::move(hashers_), accumulators(), &pool_);
  } else {
    table_ = HashTable<false>::createForAggregation(
        std::move(hashers_), accumulators(), &pool_);
  }

  RowContainer& rows = *table_->rows();
  initializeAggregates(aggregates_, rows);

  if (sortedAggregations_) {
    sortedAggregations_->setAllocator(&rows.stringAllocator());

    const auto rowColumn =
        rows.columnAt(rows.keyTypes().size() + aggregates_.size());
    sortedAggregations_->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rows.rowSizeOffset());
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
  //  - uint32_t row size,
  //  - fixed-width accumulators - one per aggregate
  //
  // Here we always make space for a row size since we only have one row and no
  // RowContainer.  The whole row is allocated to guarantee that alignment
  // requirements of all aggregate functions are satisfied.
  int32_t rowSizeOffset = bits::nbytes(aggregates_.size());
  int32_t offset = rowSizeOffset + sizeof(int32_t);
  int32_t nullOffset = 0;
  int32_t alignment = 1;

  for (auto& aggregate : aggregates_) {
    auto& function = aggregate.function;

    Accumulator accumulator{aggregate.function.get()};

    // Accumulator offset must be aligned by their alignment size.
    offset = bits::roundUp(offset, accumulator.alignment());

    function->setAllocator(&stringAllocator_);
    function->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset),
        rowSizeOffset);

    offset += accumulator.fixedWidthSize();
    ++nullOffset;
    alignment =
        RowContainer::combineAlignments(accumulator.alignment(), alignment);
  }

  if (sortedAggregations_) {
    auto accumulator = sortedAggregations_->accumulator();

    offset = bits::roundUp(offset, accumulator.alignment());

    sortedAggregations_->setAllocator(&stringAllocator_);
    sortedAggregations_->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset),
        rowSizeOffset);

    offset += accumulator.fixedWidthSize();
    ++nullOffset;
    alignment =
        RowContainer::combineAlignments(accumulator.alignment(), alignment);
  }

  lookup_->hits[0] = rows_.allocateFixed(offset, alignment);
  const auto singleGroup = std::vector<vector_size_t>{0};
  for (auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }

  if (sortedAggregations_) {
    sortedAggregations_->initializeNewGroups(lookup_->hits.data(), singleGroup);
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

  auto* group = lookup_->hits[0];

  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }
    const auto& rows = getSelectivityVector(i);

    // Check is mask is false for all rows.
    if (!rows.hasSelections()) {
      continue;
    }

    auto& function = aggregates_[i].function;

    populateTempVectors(i, input);
    const bool canPushdown =
        mayPushdown && mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_);
    if (isRawInput_) {
      function->addSingleGroupRawInput(group, rows, tempVectors_, canPushdown);
    } else {
      function->addSingleGroupIntermediateResults(
          group, rows, tempVectors_, canPushdown);
    }
  }
  tempVectors_.clear();

  if (sortedAggregations_) {
    sortedAggregations_->addSingleGroupInput(group, input);
  }
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
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }

    auto& function = aggregates_[i].function;
    if (isPartial) {
      function->extractAccumulators(groups, 1, &result->childAt(i));
    } else {
      function->extractValues(groups, 1, &result->childAt(i));
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->extractValues(folly::Range(groups, 1), result);
  }

  iterator.allocationIndex = std::numeric_limits<int32_t>::max();
  return true;
}

void GroupingSet::destroyGlobalAggregations() {
  if (!globalAggregationInitialized_) {
    return;
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    if (function->accumulatorUsesExternalMemory()) {
      auto groups = lookup_->hits.data();
      function->destroy(folly::Range(groups, 1));
    }
  }
}

void GroupingSet::populateTempVectors(
    int32_t aggregateIndex,
    const RowVectorPtr& input) {
  const auto& channels = aggregates_[aggregateIndex].inputs;
  const auto& constants = aggregates_[aggregateIndex].constantInputs;
  tempVectors_.resize(channels.size());
  for (auto i = 0; i < channels.size(); ++i) {
    if (channels[i] == kConstantChannel) {
      tempVectors_[i] =
          BaseVector::wrapInConstant(input->size(), 0, constants[i]);
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
    return getOutputWithSpill(batchSize, result);
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
  if (groups.empty()) {
    return;
  }
  RowContainer& rows = table_ ? *table_->rows() : *rowsWhileReadingSpill_;
  auto totalKeys = rows.keyTypes().size();
  for (int32_t i = 0; i < totalKeys; ++i) {
    auto keyVector = result->childAt(i);
    rows.extractColumn(groups.data(), groups.size(), i, keyVector);
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }

    auto& function = aggregates_[i].function;
    auto& aggregateVector = result->childAt(i + totalKeys);
    if (isPartial_) {
      function->extractAccumulators(
          groups.data(), groups.size(), &aggregateVector);
    } else {
      function->extractValues(groups.data(), groups.size(), &aggregateVector);
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->extractValues(groups, result);
  }
}

void GroupingSet::resetPartial() {
  if (table_ != nullptr) {
    table_->clear();
  }
}

bool GroupingSet::isPartialFull(int64_t maxBytes) {
  VELOX_CHECK(isPartial_);
  if (!table_ || allocatedBytes() <= maxBytes) {
    return false;
  }
  if (table_->hashMode() != BaseHashTable::HashMode::kArray) {
    // Not a kArray table, no rehashing will shrink this.
    return true;
  }
  auto stats = table_->stats();
  // If we have a large array with sparse data, we rehash this in a
  // mode that turns off value ranges for kArray mode. Large means
  // over 1/16 of the space budget and sparse means under 1 entry
  // per 32 buckets.
  if (stats.capacity * sizeof(void*) > maxBytes / 16 &&
      stats.numDistinct < stats.capacity / 32) {
    table_->decideHashMode(0, true);
  }
  return allocatedBytes() > maxBytes;
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
  if (isPartial_ || spillConfig_ == nullptr) {
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
  if (spillConfig_->testSpillPct > 0 &&
      (folly::hasher<uint64_t>()(++spillTestCounter_)) % 100 <=
          spillConfig_->testSpillPct) {
    const auto rowsToSpill = std::max<int64_t>(1, numDistinct / 10);
    spill(
        numDistinct - rowsToSpill,
        outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow));
    return;
  }

  const auto currentUsage = pool_.currentBytes();
  if (spillMemoryThreshold_ != 0 && currentUsage > spillMemoryThreshold_) {
    const int64_t bytesToSpill =
        currentUsage * spillConfig_->spillableReservationGrowthPct / 100;
    auto rowsToSpill = std::max<int64_t>(
        1, bytesToSpill / (rows->fixedRowSize() + outOfLineBytesPerRow));
    spill(
        std::max<int64_t>(0, numDistinct - rowsToSpill),
        std::max<int64_t>(
            0, outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow)));
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
  // There must be at least 2x the increment in reservation.
  if (pool_.availableReservation() > 2 * increment) {
    return;
  }
  // Check if can increase reservation. The increment is the larger of
  // twice the maximum increment from this input and
  // 'spillableReservationGrowthPct_' of the current reservation.
  auto targetIncrement = std::max<int64_t>(
      increment * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);
  if (pool_.maybeReserve(targetIncrement)) {
    return;
  }

  // NOTE: disk spilling use the system disk spilling memory pool instead of
  // the operator memory pool.
  auto rowsToSpill = std::max<int64_t>(
      1, targetIncrement / (rows->fixedRowSize() + outOfLineBytesPerRow));
  spill(
      std::max<int64_t>(0, numDistinct - rowsToSpill),
      std::max<int64_t>(
          0, outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow)));
}

void GroupingSet::spill(int64_t targetRows, int64_t targetBytes) {
  // NOTE: if the disk spilling is triggered by the memory arbitrator, then it
  // is possible that the grouping set hasn't processed any input data yet.
  // Correspondingly, 'table_' will not be initialized at that point.
  if (table_ == nullptr) {
    return;
  }
  if (spiller_ == nullptr) {
    auto rows = table_->rows();
    auto types = rows->keyTypes();
    for (const auto& aggregate : aggregates_) {
      types.push_back(aggregate.intermediateType);
    }
    std::vector<std::string> names;
    for (auto i = 0; i < types.size(); ++i) {
      names.push_back(fmt::format("s{}", i));
    }
    VELOX_DCHECK(pool_.trackUsage());
    spiller_ = std::make_unique<Spiller>(
        Spiller::Type::kAggregate,
        rows,
        [&](folly::Range<char**> rows) { table_->erase(rows); },
        ROW(std::move(names), std::move(types)),
        // Spill up to 8 partitions based on bits starting from 29th of the hash
        // number. Any from one to three bits would do.
        spillConfig_->hashBitRange,
        rows->keyTypes().size(),
        std::vector<CompareFlags>(),
        spillConfig_->filePath,
        spillConfig_->maxFileSize,
        spillConfig_->minSpillRunSize,
        Spiller::spillPool(),
        spillConfig_->executor);
  }
  spiller_->spill(targetRows, targetBytes);
  if (table_->rows()->numRows() == 0) {
    table_->clear();
  }
}

bool GroupingSet::getOutputWithSpill(
    int32_t batchSize,
    const RowVectorPtr& result) {
  if (outputPartition_ == -1) {
    mergeArgs_.resize(1);
    std::vector<TypePtr> keyTypes;
    for (auto& hasher : table_->hashers()) {
      keyTypes.push_back(hasher->type());
    }

    mergeRows_ = std::make_unique<RowContainer>(
        keyTypes,
        !ignoreNullKeys_,
        accumulators(),
        std::vector<TypePtr>(),
        false,
        false,
        false,
        false,
        &pool_,
        ContainerRowSerde::instance());

    initializeAggregates(aggregates_, *mergeRows_);

    // Take ownership of the rows and free the hash table. The table will not be
    // needed for producing spill output.
    rowsWhileReadingSpill_ = table_->moveRows();
    table_.reset();
    outputPartition_ = 0;
    nonSpilledRows_ = spiller_->finishSpill();
  }

  if (nonSpilledIndex_ < nonSpilledRows_.value().size()) {
    const size_t numGroups = std::min<vector_size_t>(
        batchSize, nonSpilledRows_.value().size() - nonSpilledIndex_);
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
    if (merge_ == nullptr || !mergeNext(batchSize, result)) {
      ++outputPartition_;
      merge_ = nullptr;
      continue;
    }
    return true;
  }
  return false;
}

bool GroupingSet::mergeNext(int32_t batchSize, const RowVectorPtr& result) {
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
    if (!nextKeyIsEqual_ && mergeRows_->numRows() >= batchSize) {
      extractSpillResult(result);
      return true;
    }
  }
}

void GroupingSet::initializeRow(
    SpillMergeStream& keys,
    char* FOLLY_NONNULL row) {
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    mergeRows_->store(keys.decoded(i), keys.currentIndex(), mergeState_, i);
  }
  vector_size_t zero = 0;
  for (auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(
        &row, folly::Range<const vector_size_t*>(&zero, 1));
  }
}

void GroupingSet::extractSpillResult(const RowVectorPtr& result) {
  std::vector<char*> rows(mergeRows_->numRows());
  RowContainerIterator iter;
  if (!rows.empty()) {
    mergeRows_->listRows(
        &iter, rows.size(), RowContainer::kUnlimited, rows.data());
  }
  extractGroups(folly::Range<char**>(rows.data(), rows.size()), result);
  mergeRows_->clear();
}

void GroupingSet::updateRow(SpillMergeStream& input, char* FOLLY_NONNULL row) {
  if (input.currentIndex() >= mergeSelection_.size()) {
    mergeSelection_.resize(bits::roundUp(input.currentIndex() + 1, 64));
    mergeSelection_.clearAll();
  }
  mergeSelection_.setValid(input.currentIndex(), true);
  mergeSelection_.updateBounds();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    mergeArgs_[0] = input.current().childAt(i + keyChannels_.size());
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, mergeSelection_, mergeArgs_, false);
  }
  mergeSelection_.setValid(input.currentIndex(), false);
}

void GroupingSet::abandonPartialAggregation() {
  abandonedPartialAggregation_ = true;
  allSupportToIntermediate_ = true;
  for (auto& aggregate : aggregates_) {
    if (!aggregate.function->supportsToIntermediate()) {
      allSupportToIntermediate_ = false;
    }
  }
  if (!allSupportToIntermediate_) {
    VELOX_CHECK_EQ(table_->rows()->numRows(), 0)
    intermediateRows_ = table_->moveRows();
    intermediateRows_->clear();
  }
  table_ = nullptr;
}

void GroupingSet::toIntermediate(
    const RowVectorPtr& input,
    RowVectorPtr& result) {
  VELOX_CHECK(abandonedPartialAggregation_);
  VELOX_CHECK(result.unique());
  if (!isRawInput_) {
    result = input;
    return;
  }
  auto numRows = input->size();
  activeRows_.resize(numRows);
  activeRows_.setAll();
  masks_.addInput(input, activeRows_);

  result->resize(numRows);
  if (intermediateRows_) {
    intermediateGroups_.resize(numRows);
    for (auto i = 0; i < numRows; ++i) {
      intermediateGroups_[i] = intermediateRows_->newRow();
      intermediateRows_->setAllNull(intermediateGroups_[i]);
    }
    intermediateRowNumbers_.resize(numRows);
    std::iota(
        intermediateRowNumbers_.begin(), intermediateRowNumbers_.end(), 0);
  }

  for (auto i = 0; i < keyChannels_.size(); ++i) {
    result->childAt(i) = input->childAt(keyChannels_[i]);
  }
  for (auto i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    auto& aggregateVector = result->childAt(i + keyChannels_.size());
    VELOX_CHECK(aggregateVector.unique());
    aggregateVector->resize(input->size());
    const auto& rows = getSelectivityVector(i);
    // Check if mask is false for all rows.
    if (!rows.hasSelections()) {
      // The aggregate produces its initial state for all
      // rows. Initialize one, then read the same data into each
      // element of flat result. This is most often a null but for
      // example count produces a zero, so we use the per-aggregate
      // functions.
      function->initializeNewGroups(
          intermediateGroups_.data(),
          folly::Range<const vector_size_t*>(
              intermediateRowNumbers_.data(), 1));
      firstGroup_.resize(numRows);
      std::fill(firstGroup_.begin(), firstGroup_.end(), intermediateGroups_[0]);
      function->extractAccumulators(
          firstGroup_.data(), intermediateGroups_.size(), &aggregateVector);
      function->clear();
      continue;
    }

    populateTempVectors(i, input);

    if (function->supportsToIntermediate()) {
      function->toIntermediate(rows, tempVectors_, aggregateVector);
      continue;
    }
    function->initializeNewGroups(
        intermediateGroups_.data(), intermediateRowNumbers_);

    function->addRawInput(
        intermediateGroups_.data(), rows, tempVectors_, false);

    function->extractAccumulators(
        intermediateGroups_.data(),
        intermediateGroups_.size(),
        &aggregateVector);
    function->clear();
  }
  if (intermediateRows_) {
    intermediateRows_->eraseRows(folly::Range<char**>(
        intermediateGroups_.data(), intermediateGroups_.size()));
  }
  tempVectors_.clear();
}

std::optional<int64_t> GroupingSet::estimateRowSize() const {
  const RowContainer* rows =
      table_ ? table_->rows() : rowsWhileReadingSpill_.get();
  return rows && rows->estimateRowSize() >= 0
      ? std::optional<int64_t>(rows->estimateRowSize())
      : std::nullopt;
};

} // namespace facebook::velox::exec
