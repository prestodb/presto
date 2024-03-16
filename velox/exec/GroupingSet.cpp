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
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Task.h"

using facebook::velox::common::testutil::TestValue;

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

} // namespace

GroupingSet::GroupingSet(
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
    OperatorCtx* operatorCtx)
    : preGroupedKeyChannels_(std::move(preGroupedKeys)),
      hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      isPartial_(isPartial),
      isRawInput_(isRawInput),
      queryConfig_(operatorCtx->task()->queryCtx()->queryConfig()),
      aggregates_(std::move(aggregates)),
      masks_(extractMaskChannels(aggregates_)),
      ignoreNullKeys_(ignoreNullKeys),
      globalGroupingSets_(globalGroupingSets),
      groupIdChannel_(groupIdChannel),
      spillConfig_(spillConfig),
      nonReclaimableSection_(nonReclaimableSection),
      stringAllocator_(operatorCtx->pool()),
      rows_(operatorCtx->pool()),
      isAdaptive_(queryConfig_.hashAdaptivityEnabled()),
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

  sortedAggregations_ =
      SortedAggregations::create(aggregates_, inputType, &pool_);
  if (isPartial_) {
    VELOX_USER_CHECK_NULL(
        sortedAggregations_,
        "Partial aggregations over sorted inputs are not supported");
  }

  for (auto& aggregate : aggregates_) {
    if (aggregate.distinct) {
      VELOX_USER_CHECK(
          !isPartial_,
          "Partial aggregations over distinct inputs are not supported");
      distinctAggregations_.emplace_back(
          DistinctAggregations::create({&aggregate}, inputType, &pool_));
    } else {
      distinctAggregations_.push_back(nullptr);
    }
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
    OperatorCtx* operatorCtx,
    tsan_atomic<bool>* nonReclaimableSection) {
  return std::make_unique<GroupingSet>(
      inputType,
      std::move(hashers),
      /*preGroupedKeys*/ std::vector<column_index_t>{},
      /*aggregates*/ std::vector<AggregateInfo>{},
      /*ignoreNullKeys*/ false,
      /*isPartial*/ false,
      /*isRawInput*/ false,
      /*globalGroupingSets*/ std::vector<vector_size_t>{},
      /*groupIdColumn*/ std::nullopt,
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
  numInputRows_ += numRows;
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

  // Spill the remaining in-memory state to disk if spilling has been triggered
  // on this grouping set. This is to simplify query OOM prevention when
  // producing output as we don't support to spill during that stage as for now.
  if (hasSpilled()) {
    spill();
  }

  ensureOutputFits();
}

bool GroupingSet::hasSpilled() const {
  return spiller_ != nullptr;
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

  TestValue::adjust(
      "facebook::velox::exec::GroupingSet::addInputForActiveRows", this);

  table_->prepareForGroupProbe(
      *lookup_,
      input,
      activeRows_,
      ignoreNullKeys_,
      BaseHashTable::kNoSpillInputStartPartitionBit);
  if (lookup_->rows.empty()) {
    // No rows to probe. Can happen when ignoreNullKeys_ is true and all rows
    // have null keys.
    return;
  }

  table_->groupProbe(*lookup_);
  masks_.addInput(input, activeRows_);

  auto* groups = lookup_->hits.data();
  const auto& newGroups = lookup_->newGroups;

  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }

    const auto& rows = getSelectivityVector(i);

    if (aggregates_[i].distinct) {
      if (!newGroups.empty()) {
        distinctAggregations_[i]->initializeNewGroups(groups, newGroups);
      }

      if (rows.hasSelections()) {
        distinctAggregations_[i]->addInput(groups, input, rows);
      }
      continue;
    }

    auto& function = aggregates_[i].function;
    if (!newGroups.empty()) {
      function->initializeNewGroups(groups, newGroups);
    }

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
    RowContainer& rows,
    bool excludeToIntermediate) {
  const auto numKeys = rows.keyTypes().size();
  int i = 0;
  for (auto& aggregate : aggregates) {
    auto& function = aggregate.function;
    if (excludeToIntermediate && function->supportsToIntermediate()) {
      continue;
    }
    function->setAllocator(&rows.stringAllocator());

    const auto rowColumn = rows.columnAt(numKeys + i);
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rows.rowSizeOffset());
    ++i;
  }
}
} // namespace

std::vector<Accumulator> GroupingSet::accumulators(bool excludeToIntermediate) {
  std::vector<Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (auto& aggregate : aggregates_) {
    if (!excludeToIntermediate ||
        !aggregate.function->supportsToIntermediate()) {
      accumulators.push_back(
          Accumulator{aggregate.function.get(), aggregate.intermediateType});
    }
  }

  if (sortedAggregations_ != nullptr) {
    accumulators.push_back(sortedAggregations_->accumulator());
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      accumulators.push_back(aggregation->accumulator());
    }
  }
  return accumulators;
}

void GroupingSet::createHashTable() {
  if (ignoreNullKeys_) {
    table_ = HashTable<true>::createForAggregation(
        std::move(hashers_), accumulators(false), &pool_);
  } else {
    table_ = HashTable<false>::createForAggregation(
        std::move(hashers_), accumulators(false), &pool_);
  }

  RowContainer& rows = *table_->rows();
  initializeAggregates(aggregates_, rows, false);

  auto numColumns = rows.keyTypes().size() + aggregates_.size();

  if (sortedAggregations_) {
    sortedAggregations_->setAllocator(&rows.stringAllocator());

    const auto rowColumn = rows.columnAt(numColumns);
    sortedAggregations_->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rows.rowSizeOffset());

    ++numColumns;
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->setAllocator(&rows.stringAllocator());

      const auto rowColumn = rows.columnAt(numColumns);
      aggregation->setOffsets(
          rowColumn.offset(),
          rowColumn.nullByte(),
          rowColumn.nullMask(),
          rows.rowSizeOffset());
      ++numColumns;
    }
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

    Accumulator accumulator{
        aggregate.function.get(), aggregate.intermediateType};

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

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      auto accumulator = aggregation->accumulator();

      offset = bits::roundUp(offset, accumulator.alignment());

      aggregation->setAllocator(&stringAllocator_);
      aggregation->setOffsets(
          offset,
          RowContainer::nullByte(nullOffset),
          RowContainer::nullMask(nullOffset),
          rowSizeOffset);

      offset += accumulator.fixedWidthSize();
      ++nullOffset;
      alignment =
          RowContainer::combineAlignments(accumulator.alignment(), alignment);
    }
  }

  lookup_->hits[0] = rows_.allocateFixed(offset, alignment);
  const auto singleGroup = std::vector<vector_size_t>{0};
  for (auto& aggregate : aggregates_) {
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }
    aggregate.function->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }

  if (sortedAggregations_) {
    sortedAggregations_->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->initializeNewGroups(lookup_->hits.data(), singleGroup);
    }
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

    if (aggregates_[i].distinct) {
      distinctAggregations_[i]->addSingleGroupInput(group, input, rows);
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
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
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
    auto& resultVector = result->childAt(aggregates_[i].output);
    if (isPartial_) {
      function->extractAccumulators(groups, 1, &resultVector);
    } else {
      function->extractValues(groups, 1, &resultVector);
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->extractValues(folly::Range(groups, 1), result);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->extractValues(folly::Range(groups, 1), result);
    }
  }

  iterator.allocationIndex = std::numeric_limits<int32_t>::max();
  return true;
}

bool GroupingSet::getDefaultGlobalGroupingSetOutput(
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  VELOX_CHECK(hasDefaultGlobalGroupingSetOutput());

  if (iterator.allocationIndex != 0) {
    return false;
  }

  auto globalAggregatesRow =
      BaseVector::create<RowVector>(result->type(), 1, &pool_);

  VELOX_CHECK(getGlobalAggregationOutput(iterator, globalAggregatesRow));

  // There is one output row for each global GroupingSet.
  const auto numGroupingSets = globalGroupingSets_.size();
  result->resize(numGroupingSets);
  VELOX_CHECK(groupIdChannel_.has_value());

  // First columns in 'result' are for grouping keys (which could include the
  // GroupId column). For a global grouping set row :
  // i) Non-groupId grouping keys are null.
  // ii) GroupId column is populated with the global grouping set number.

  column_index_t firstAggregateCol = result->type()->size();
  for (const auto& aggregate : aggregates_) {
    firstAggregateCol = std::min(firstAggregateCol, aggregate.output);
  }

  for (auto i = 0; i < firstAggregateCol; i++) {
    auto column = result->childAt(i);
    if (i == groupIdChannel_.value()) {
      column->resize(numGroupingSets);
      auto* groupIdVector = column->asFlatVector<int64_t>();
      for (auto j = 0; j < numGroupingSets; j++) {
        groupIdVector->set(j, globalGroupingSets_.at(j));
      }
    } else {
      column->resize(numGroupingSets, false);
      for (auto j = 0; j < numGroupingSets; j++) {
        column->setNull(j, true);
      }
    }
  }

  // The remaining aggregate columns are filled from the computed global
  // aggregates.
  for (const auto& aggregate : aggregates_) {
    auto resultAggregateColumn = result->childAt(aggregate.output);
    resultAggregateColumn->resize(numGroupingSets);
    auto sourceAggregateColumn = globalAggregatesRow->childAt(aggregate.output);
    for (auto i = 0; i < numGroupingSets; i++) {
      resultAggregateColumn->copy(sourceAggregateColumn.get(), i, 0, 1);
    }
  }

  return true;
}

void GroupingSet::destroyGlobalAggregations() {
  if (!globalAggregationInitialized_) {
    return;
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    if (function->accumulatorUsesExternalMemory()) {
      auto* groups = lookup_->hits.data();
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
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    RowContainerIterator& iterator,
    RowVectorPtr& result) {
  TestValue::adjust("facebook::velox::exec::GroupingSet::getOutput", this);

  if (isGlobal_) {
    return getGlobalAggregationOutput(iterator, result);
  }

  if (hasDefaultGlobalGroupingSetOutput()) {
    return getDefaultGlobalGroupingSetOutput(iterator, result);
  }

  if (hasSpilled()) {
    return getOutputWithSpill(maxOutputRows, maxOutputBytes, result);
  }
  VELOX_CHECK(!isDistinct());

  // @lint-ignore CLANGTIDY
  char* groups[maxOutputRows];
  const int32_t numGroups = table_
      ? table_->rows()->listRows(
            &iterator, maxOutputRows, maxOutputBytes, groups)
      : 0;
  if (numGroups == 0) {
    if (table_ != nullptr) {
      table_->clear();
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
  RowContainer& rows = *table_->rows();
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

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->extractValues(groups, result);
    }
  }
}

void GroupingSet::resetTable() {
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

  const auto numDistinct = table_->numDistinct();
  if (numDistinct == 0) {
    // Table is empty. Nothing to spill.
    return;
  }

  auto* rows = table_->rows();
  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  const auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow = outOfLineBytes / numDistinct;
  const int64_t flatBytes = input->estimateFlatSize();

  // Test-only spill path.
  if (testingTriggerSpill()) {
    spill();
    return;
  }

  const auto currentUsage = pool_.currentBytes();
  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool_.availableReservation();
  const auto tableIncrementBytes = table_->hashTableSizeIncrease(input->size());
  const auto incrementBytes =
      rows->sizeIncrement(input->size(), outOfLineBytes ? flatBytes * 2 : 0) +
      tableIncrementBytes;

  // First to check if we have sufficient minimal memory reservation.
  if (availableReservationBytes >= minReservationBytes) {
    if ((tableIncrementBytes == 0) && (freeRows > input->size()) &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatBytes * 2)) {
      // Enough free rows for input rows and enough variable length free space
      // for double the flat size of the whole vector. If outOfLineBytes is 0
      // there is no need for variable length space. Double the flat size is a
      // stopgap because the real increase can be higher, specially with
      // aggregates that have stl or folly containers. Make a way to raise the
      // reservation in the spill protected section instead.
      return;
    }

    // If there is variable length data we take double the flat size of the
    // input as a cap on the new variable length data needed. Same condition as
    // in first check. Completely arbitrary. Allow growth in spill protected
    // area instead.
    // There must be at least 2x the increment in reservation.
    if (availableReservationBytes > 2 * incrementBytes) {
      return;
    }
  }

  // Check if we can increase reservation. The increment is the larger of twice
  // the maximum increment from this input and 'spillableReservationGrowthPct_'
  // of the current memory usage.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_.maybeReserve(targetIncrementBytes)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool_.name()
               << ", usage: " << succinctBytes(pool_.currentBytes())
               << ", reservation: " << succinctBytes(pool_.reservedBytes());
}

void GroupingSet::ensureOutputFits() {
  // If spilling has already been triggered on this operator, then we don't need
  // to reserve memory for the output as we can't reclaim much memory from this
  // operator itself. The output processing can reclaim memory from the other
  // operator or query through memory arbitration.
  if (isPartial_ || spillConfig_ == nullptr || hasSpilled()) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill()) {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    memory::testingRunArbitration(&pool_);
    return;
  }

  const uint64_t outputBufferSizeToReserve =
      queryConfig_.preferredOutputBatchBytes() * 1.2;
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_.maybeReserve(outputBufferSizeToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve "
               << succinctBytes(outputBufferSizeToReserve)
               << " for memory pool " << pool_.name()
               << ", usage: " << succinctBytes(pool_.currentBytes())
               << ", reservation: " << succinctBytes(pool_.reservedBytes());
}

RowTypePtr GroupingSet::makeSpillType() const {
  auto rows = table_->rows();
  auto types = rows->keyTypes();

  for (const auto& accumulator : rows->accumulators()) {
    types.push_back(accumulator.spillType());
  }

  std::vector<std::string> names;
  for (auto i = 0; i < types.size(); ++i) {
    names.push_back(fmt::format("s{}", i));
  }

  return ROW(std::move(names), std::move(types));
}

void GroupingSet::spill() {
  // NOTE: if the disk spilling is triggered by the memory arbitrator, then it
  // is possible that the grouping set hasn't processed any input data yet.
  // Correspondingly, 'table_' will not be initialized at that point.
  if (table_ == nullptr || table_->numDistinct() == 0) {
    return;
  }

  if (!hasSpilled()) {
    auto rows = table_->rows();
    VELOX_DCHECK(pool_.trackUsage());
    spiller_ = std::make_unique<Spiller>(
        Spiller::Type::kAggregateInput,
        rows,
        makeSpillType(),
        rows->keyTypes().size(),
        std::vector<CompareFlags>(),
        spillConfig_);
  }
  spiller_->spill();
  if (sortedAggregations_) {
    sortedAggregations_->clear();
  }
  table_->clear();
}

void GroupingSet::spill(const RowContainerIterator& rowIterator) {
  VELOX_CHECK(!hasSpilled());

  if (table_ == nullptr) {
    return;
  }

  auto* rows = table_->rows();
  VELOX_CHECK(pool_.trackUsage());
  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kAggregateOutput, rows, makeSpillType(), spillConfig_);

  spiller_->spill(rowIterator);
  table_->clear();
}

bool GroupingSet::getOutputWithSpill(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  if (merge_ == nullptr) {
    VELOX_CHECK_NULL(mergeRows_);
    VELOX_CHECK(mergeArgs_.empty());

    if (!isDistinct()) {
      mergeArgs_.resize(1);
      std::vector<TypePtr> keyTypes;
      for (auto& hasher : table_->hashers()) {
        keyTypes.push_back(hasher->type());
      }

      mergeRows_ = std::make_unique<RowContainer>(
          keyTypes,
          !ignoreNullKeys_,
          accumulators(false),
          std::vector<TypePtr>(),
          false,
          false,
          false,
          false,
          &pool_,
          table_->rows()->stringAllocatorShared());

      initializeAggregates(aggregates_, *mergeRows_, false);
    }

    VELOX_CHECK_EQ(table_->rows()->numRows(), 0);

    VELOX_CHECK_NULL(merge_);
    auto spillPartition = spiller_->finishSpill();
    merge_ = spillPartition.createOrderedReader(&pool_);
  }
  VELOX_CHECK_EQ(spiller_->state().maxPartitions(), 1);
  if (merge_ == nullptr) {
    return false;
  }
  return mergeNext(maxOutputRows, maxOutputBytes, result);
}

bool GroupingSet::mergeNext(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  if (isDistinct()) {
    return mergeNextWithoutAggregates(maxOutputRows, result);
  } else {
    return mergeNextWithAggregates(maxOutputRows, maxOutputBytes, result);
  }
}

bool GroupingSet::mergeNextWithAggregates(
    int32_t maxOutputRows,
    int32_t maxOutputBytes,
    const RowVectorPtr& result) {
  VELOX_CHECK(!isDistinct());
  VELOX_CHECK_NOT_NULL(merge_);

  // True if 'merge_' indicates that the next key is the same as the current
  // one.
  bool nextKeyIsEqual{false};
  for (;;) {
    auto next = merge_->nextWithEquals();
    if (next.first == nullptr) {
      extractSpillResult(result);
      return result->size() > 0;
    }
    if (!nextKeyIsEqual) {
      mergeState_ = mergeRows_->newRow();
      initializeRow(*next.first, mergeState_);
    }
    updateRow(*next.first, mergeState_);
    nextKeyIsEqual = next.second;
    next.first->pop();
    if (!nextKeyIsEqual &&
        ((mergeRows_->numRows() >= maxOutputRows) ||
         (mergeRows_->allocatedBytes() >= maxOutputBytes))) {
      extractSpillResult(result);
      return true;
    }
  }
  VELOX_UNREACHABLE();
}

bool GroupingSet::mergeNextWithoutAggregates(
    int32_t maxOutputRows,
    const RowVectorPtr& result) {
  VELOX_CHECK_NOT_NULL(merge_);
  VELOX_CHECK(isDistinct());

  // We are looping over sorted rows produced by tree-of-losers. We logically
  // split the stream into runs of duplicate rows. As we process each run we
  // track whether one of the values comes from stream 0, in which case we
  // should not produce a result from that run. Otherwise, we produce a result
  // at the end of the run (when we know for sure whether the run contains a row
  // from stream 0 or not).
  //
  // NOTE: stream 0 contains rows which has already been output as distinct
  // before we trigger spilling.
  bool newDistinct{true};
  int32_t numOutputRows{0};
  while (numOutputRows < maxOutputRows) {
    const auto next = merge_->nextWithEquals();
    auto* stream = next.first;
    if (stream == nullptr) {
      break;
    }
    if (stream->id() == 0) {
      newDistinct = false;
    }
    if (next.second) {
      stream->pop();
      continue;
    }
    if (newDistinct) {
      // Yield result for new distinct.
      result->copy(
          &stream->current(), numOutputRows++, stream->currentIndex(), 1);
    }
    stream->pop();
    newDistinct = true;
  }
  result->resize(numOutputRows);
  return numOutputRows > 0;
}

void GroupingSet::initializeRow(SpillMergeStream& stream, char* row) {
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    mergeRows_->store(stream.decoded(i), stream.currentIndex(), mergeState_, i);
  }
  vector_size_t zero = 0;
  for (auto& aggregate : aggregates_) {
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }
    aggregate.function->initializeNewGroups(
        &row, folly::Range<const vector_size_t*>(&zero, 1));
  }

  if (sortedAggregations_ != nullptr) {
    sortedAggregations_->initializeNewGroups(
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

void GroupingSet::updateRow(SpillMergeStream& input, char* row) {
  if (input.currentIndex() >= mergeSelection_.size()) {
    mergeSelection_.resize(bits::roundUp(input.currentIndex() + 1, 64));
    mergeSelection_.clearAll();
  }
  mergeSelection_.setValid(input.currentIndex(), true);
  mergeSelection_.updateBounds();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }
    mergeArgs_[0] = input.current().childAt(i + keyChannels_.size());
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, mergeSelection_, mergeArgs_, false);
  }
  mergeSelection_.setValid(input.currentIndex(), false);

  if (sortedAggregations_ != nullptr) {
    const auto& vector =
        input.current().childAt(aggregates_.size() + keyChannels_.size());
    sortedAggregations_->addSingleGroupSpillInput(
        row, vector, input.currentIndex());
  }
}

void GroupingSet::abandonPartialAggregation() {
  abandonedPartialAggregation_ = true;
  allSupportToIntermediate_ = true;
  for (auto& aggregate : aggregates_) {
    if (!aggregate.function->supportsToIntermediate()) {
      allSupportToIntermediate_ = false;
    }
  }

  VELOX_CHECK_EQ(table_->rows()->numRows(), 0);
  intermediateRows_ = std::make_unique<RowContainer>(
      table_->rows()->keyTypes(),
      !ignoreNullKeys_,
      accumulators(true),
      std::vector<TypePtr>(),
      false,
      false,
      false,
      false,
      &pool_,
      table_->rows()->stringAllocatorShared());
  initializeAggregates(aggregates_, *intermediateRows_, true);
  table_.reset();
}

namespace {
// Recursive resize all children.

void recursiveResizeChildren(VectorPtr& vector, vector_size_t newSize) {
  VELOX_CHECK(vector.unique());
  if (vector->typeKind() == TypeKind::ROW) {
    auto rowVector = vector->asUnchecked<RowVector>();
    for (auto& child : rowVector->children()) {
      recursiveResizeChildren(child, newSize);
    }
  }
  vector->resize(newSize);
}

} // namespace

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
  if (!allSupportToIntermediate_) {
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
    recursiveResizeChildren(aggregateVector, input->size());
    const auto& rows = getSelectivityVector(i);

    if (function->supportsToIntermediate()) {
      populateTempVectors(i, input);
      VELOX_DCHECK(aggregateVector);
      function->toIntermediate(rows, tempVectors_, aggregateVector);
      continue;
    }

    // Initialize all groups, even if we only need just one, to make sure bulk
    // free (intermediateRows_->eraseRows) is safe. It is not legal to free a
    // group that hasn't been initialized.
    function->initializeNewGroups(
        intermediateGroups_.data(), intermediateRowNumbers_);

    // Check if mask is false for all rows.
    if (!rows.hasSelections()) {
      // The aggregate produces its initial state for all
      // rows. Initialize one, then read the same data into each
      // element of flat result. This is most often a null but for
      // example count produces a zero, so we use the per-aggregate
      // functions.
      firstGroup_.resize(numRows);
      std::fill(firstGroup_.begin(), firstGroup_.end(), intermediateGroups_[0]);
      function->extractAccumulators(
          firstGroup_.data(), intermediateGroups_.size(), &aggregateVector);
      continue;
    }

    populateTempVectors(i, input);

    function->addRawInput(
        intermediateGroups_.data(), rows, tempVectors_, false);

    function->extractAccumulators(
        intermediateGroups_.data(),
        intermediateGroups_.size(),
        &aggregateVector);
  }
  if (intermediateRows_) {
    intermediateRows_->eraseRows(folly::Range<char**>(
        intermediateGroups_.data(), intermediateGroups_.size()));
    if (intermediateRows_->checkFree()) {
      intermediateRows_->stringAllocator().checkEmpty();
    }
  }

  // It's unnecessary to call function->clear() to reset the internal states of
  // aggregation functions because toIntermediate() is already called at the end
  // of HashAggregation::getOutput(). When toIntermediate() is called, the
  // aggregaiton function instances won't be reused after it returns.
  tempVectors_.clear();
}

std::optional<int64_t> GroupingSet::estimateOutputRowSize() const {
  if (table_ == nullptr) {
    return std::nullopt;
  }
  return table_->rows()->estimateRowSize();
}
} // namespace facebook::velox::exec
