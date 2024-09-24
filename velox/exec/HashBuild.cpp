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

#include "velox/exec/HashBuild.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
namespace {
// Map HashBuild 'state' to the corresponding driver blocking reason.
BlockingReason fromStateToBlockingReason(HashBuild::State state) {
  switch (state) {
    case HashBuild::State::kRunning:
      [[fallthrough]];
    case HashBuild::State::kFinish:
      return BlockingReason::kNotBlocked;
    case HashBuild::State::kYield:
      return BlockingReason::kYield;
    case HashBuild::State::kWaitForSpill:
      return BlockingReason::kWaitForSpill;
    case HashBuild::State::kWaitForBuild:
      return BlockingReason::kWaitForJoinBuild;
    case HashBuild::State::kWaitForProbe:
      return BlockingReason::kWaitForJoinProbe;
    default:
      VELOX_UNREACHABLE(HashBuild::stateName(state));
  }
}
} // namespace

HashBuild::HashBuild(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    : Operator(
          driverCtx,
          nullptr,
          operatorId,
          joinNode->id(),
          "HashBuild",
          joinNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      joinNode_(std::move(joinNode)),
      joinType_{joinNode_->joinType()},
      nullAware_{joinNode_->isNullAware()},
      needProbedFlagSpill_{needRightSideJoin(joinType_)},
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      keyChannelMap_(joinNode_->rightKeys().size()) {
  VELOX_CHECK(pool()->trackUsage());
  VELOX_CHECK_NOT_NULL(joinBridge_);

  joinBridge_->addBuilder();

  auto inputType = joinNode_->sources()[1]->outputType();

  const auto numKeys = joinNode_->rightKeys().size();
  keyChannels_.reserve(numKeys);
  std::vector<std::string> names;
  names.reserve(inputType->size());
  std::vector<TypePtr> types;
  types.reserve(inputType->size());

  for (int i = 0; i < numKeys; ++i) {
    auto& key = joinNode_->rightKeys()[i];
    auto channel = exprToChannel(key.get(), inputType);
    keyChannelMap_[channel] = i;
    keyChannels_.emplace_back(channel);
    names.emplace_back(inputType->nameOf(channel));
    types.emplace_back(inputType->childAt(channel));
  }

  // Identify the non-key build side columns and make a decoder for each.
  const int32_t numDependents = inputType->size() - numKeys;
  if (numDependents > 0) {
    // Number of join keys (numKeys) may be less then number of input columns
    // (inputType->size()). In this case numDependents is negative and cannot be
    // used to call 'reserve'. This happens when we join different probe side
    // keys with the same build side key: SELECT * FROM t LEFT JOIN u ON t.k1 =
    // u.k AND t.k2 = u.k.
    dependentChannels_.reserve(numDependents);
    decoders_.reserve(numDependents);
  }
  for (auto i = 0; i < inputType->size(); ++i) {
    if (keyChannelMap_.find(i) == keyChannelMap_.end()) {
      dependentChannels_.emplace_back(i);
      decoders_.emplace_back(std::make_unique<DecodedVector>());
      names.emplace_back(inputType->nameOf(i));
      types.emplace_back(inputType->childAt(i));
    }
  }

  tableType_ = ROW(std::move(names), std::move(types));
  setupTable();
  setupSpiller();
  stateCleared_ = false;
}

void HashBuild::initialize() {
  Operator::initialize();

  if (isAntiJoin(joinType_) && joinNode_->filter()) {
    setupFilterForAntiJoins(keyChannelMap_);
  }
}

void HashBuild::setupTable() {
  VELOX_CHECK_NULL(table_);

  const auto numKeys = keyChannels_.size();
  std::vector<std::unique_ptr<VectorHasher>> keyHashers;
  keyHashers.reserve(numKeys);
  for (vector_size_t i = 0; i < numKeys; ++i) {
    keyHashers.emplace_back(
        VectorHasher::create(tableType_->childAt(i), keyChannels_[i]));
  }

  const auto numDependents = tableType_->size() - numKeys;
  std::vector<TypePtr> dependentTypes;
  dependentTypes.reserve(numDependents);
  for (int i = numKeys; i < tableType_->size(); ++i) {
    dependentTypes.emplace_back(tableType_->childAt(i));
  }
  if (joinNode_->isRightJoin() || joinNode_->isFullJoin() ||
      joinNode_->isRightSemiProjectJoin()) {
    // Do not ignore null keys.
    table_ = HashTable<false>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true, // allowDuplicates
        true, // hasProbedFlag
        operatorCtx_->driverCtx()
            ->queryConfig()
            .minTableRowsForParallelJoinBuild(),
        pool());
  } else {
    // (Left) semi and anti join with no extra filter only needs to know whether
    // there is a match. Hence, no need to store entries with duplicate keys.
    const bool dropDuplicates = !joinNode_->filter() &&
        (joinNode_->isLeftSemiFilterJoin() ||
         joinNode_->isLeftSemiProjectJoin() || isAntiJoin(joinType_));
    // Right semi join needs to tag build rows that were probed.
    const bool needProbedFlag = joinNode_->isRightSemiFilterJoin();
    if (isLeftNullAwareJoinWithFilter(joinNode_)) {
      // We need to check null key rows in build side in case of null-aware anti
      // or left semi project join with filter set.
      table_ = HashTable<false>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          !dropDuplicates, // allowDuplicates
          needProbedFlag, // hasProbedFlag
          operatorCtx_->driverCtx()
              ->queryConfig()
              .minTableRowsForParallelJoinBuild(),
          pool());
    } else {
      // Ignore null keys
      table_ = HashTable<true>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          !dropDuplicates, // allowDuplicates
          needProbedFlag, // hasProbedFlag
          operatorCtx_->driverCtx()
              ->queryConfig()
              .minTableRowsForParallelJoinBuild(),
          pool());
    }
  }
  analyzeKeys_ = table_->hashMode() != BaseHashTable::HashMode::kHash;
}

void HashBuild::setupSpiller(SpillPartition* spillPartition) {
  VELOX_CHECK_NULL(spiller_);
  VELOX_CHECK_NULL(spillInputReader_);

  if (!spillEnabled()) {
    return;
  }
  if (spillType_ == nullptr) {
    spillType_ = hashJoinTableSpillType(tableType_, joinType_);
    if (needProbedFlagSpill_) {
      spillProbedFlagChannel_ = spillType_->size() - 1;
      VELOX_CHECK_NULL(spillProbedFlagVector_);
      // Creates a constant probed flag vector with all values false for build
      // side table spilling.
      spillProbedFlagVector_ = std::make_shared<ConstantVector<bool>>(
          pool(), 0, /*isNull=*/false, BOOLEAN(), false);
    }
  }

  const auto* config = spillConfig();
  uint8_t startPartitionBit = config->startPartitionBit;
  if (spillPartition != nullptr) {
    spillInputReader_ = spillPartition->createUnorderedReader(
        config->readBufferSize, pool(), &spillStats_);
    startPartitionBit =
        spillPartition->id().partitionBitOffset() + config->numPartitionBits;
    // Disable spilling if exceeding the max spill level and the query might run
    // out of memory if the restored partition still can't fit in memory.
    if (config->exceedSpillLevelLimit(startPartitionBit)) {
      RECORD_METRIC_VALUE(kMetricMaxSpillLevelExceededCount);
      FB_LOG_EVERY_MS(WARNING, 1'000)
          << "Exceeded spill level limit: " << config->maxSpillLevel
          << ", and disable spilling for memory pool: " << pool()->name();
      ++spillStats_.wlock()->spillMaxLevelExceededCount;
      exceededMaxSpillLevelLimit_ = true;
      return;
    }
    exceededMaxSpillLevelLimit_ = false;
  }

  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinBuild,
      joinType_,
      table_->rows(),
      spillType_,
      HashBitRange(
          startPartitionBit, startPartitionBit + config->numPartitionBits),
      config,
      &spillStats_);

  const int32_t numPartitions = spiller_->hashBits().numPartitions();
  spillInputIndicesBuffers_.resize(numPartitions);
  rawSpillInputIndicesBuffers_.resize(numPartitions);
  numSpillInputs_.resize(numPartitions, 0);
  spillChildVectors_.resize(spillType_->size());
}

bool HashBuild::isInputFromSpill() const {
  return spillInputReader_ != nullptr;
}

RowTypePtr HashBuild::inputType() const {
  return isInputFromSpill() ? tableType_
                            : joinNode_->sources()[1]->outputType();
}

void HashBuild::setupFilterForAntiJoins(
    const folly::F14FastMap<column_index_t, column_index_t>& keyChannelMap) {
  VELOX_DCHECK(
      std::is_sorted(dependentChannels_.begin(), dependentChannels_.end()));

  ExprSet exprs({joinNode_->filter()}, operatorCtx_->execCtx());
  VELOX_DCHECK_EQ(exprs.exprs().size(), 1);
  const auto& expr = exprs.expr(0);
  filterPropagatesNulls_ = expr->propagatesNulls();
  if (filterPropagatesNulls_) {
    const auto inputType = joinNode_->sources()[1]->outputType();
    for (const auto& field : expr->distinctFields()) {
      const auto index = inputType->getChildIdxIfExists(field->field());
      if (!index.has_value()) {
        continue;
      }
      auto keyIter = keyChannelMap.find(*index);
      if (keyIter != keyChannelMap.end()) {
        keyFilterChannels_.push_back(keyIter->second);
      } else {
        auto dependentIter = std::lower_bound(
            dependentChannels_.begin(), dependentChannels_.end(), *index);
        VELOX_DCHECK(
            dependentIter != dependentChannels_.end() &&
            *dependentIter == *index);
        dependentFilterChannels_.push_back(
            dependentIter - dependentChannels_.begin());
      }
    }
  }
}

void HashBuild::removeInputRowsForAntiJoinFilter() {
  bool changed = false;
  auto* rawActiveRows = activeRows_.asMutableRange().bits();
  auto removeNulls = [&](DecodedVector& decoded) {
    if (decoded.mayHaveNulls()) {
      changed = true;
      // NOTE: the true value of a raw null bit indicates non-null so we AND
      // 'rawActiveRows' with the raw bit.
      bits::andBits(
          rawActiveRows, decoded.nulls(&activeRows_), 0, activeRows_.end());
    }
  };
  for (auto channel : keyFilterChannels_) {
    removeNulls(table_->hashers()[channel]->decodedVector());
  }
  for (auto channel : dependentFilterChannels_) {
    removeNulls(*decoders_[channel]);
  }
  if (changed) {
    activeRows_.updateBounds();
  }
}

void HashBuild::addInput(RowVectorPtr input) {
  checkRunning();
  ensureInputFits(input);

  TestValue::adjust("facebook::velox::exec::HashBuild::addInput", this);

  activeRows_.resize(input->size());
  activeRows_.setAll();

  auto& hashers = table_->hashers();

  for (auto i = 0; i < hashers.size(); ++i) {
    auto key = input->childAt(hashers[i]->channel())->loadedVector();
    hashers[i]->decode(*key, activeRows_);
  }

  // Update statistics for null keys in join operator.
  // We use activeRows_ to store which rows have some null keys,
  // and reset it after using it.
  // Only process when input is not spilled, to avoid overcounting.
  if (!isInputFromSpill()) {
    auto lockedStats = stats_.wlock();
    deselectRowsWithNulls(hashers, activeRows_);
    lockedStats->numNullKeys +=
        activeRows_.size() - activeRows_.countSelected();
    activeRows_.setAll();
  }

  if (!isRightJoin(joinType_) && !isFullJoin(joinType_) &&
      !isRightSemiProjectJoin(joinType_) &&
      !isLeftNullAwareJoinWithFilter(joinNode_)) {
    deselectRowsWithNulls(hashers, activeRows_);
    if (nullAware_ && !joinHasNullKeys_ &&
        activeRows_.countSelected() < input->size()) {
      joinHasNullKeys_ = true;
    }
  } else if (nullAware_ && !joinHasNullKeys_) {
    for (auto& hasher : hashers) {
      auto& decoded = hasher->decodedVector();
      if (decoded.mayHaveNulls()) {
        auto* nulls = decoded.nulls(&activeRows_);
        if (nulls && bits::countNulls(nulls, 0, activeRows_.end()) > 0) {
          joinHasNullKeys_ = true;
          break;
        }
      }
    }
  }

  for (auto i = 0; i < dependentChannels_.size(); ++i) {
    decoders_[i]->decode(
        *input->childAt(dependentChannels_[i])->loadedVector(), activeRows_);
  }

  if (isAntiJoin(joinType_) && joinNode_->filter()) {
    if (filterPropagatesNulls_) {
      removeInputRowsForAntiJoinFilter();
    }
  } else if (isAntiJoin(joinType_) && nullAware_ && joinHasNullKeys_) {
    // Null-aware anti join with no extra filter returns no rows if build side
    // has nulls in join keys. Hence, we can stop processing on first null.
    noMoreInput();
    return;
  }

  spillInput(input);
  if (!activeRows_.hasSelections()) {
    return;
  }

  if (analyzeKeys_ && hashes_.size() < activeRows_.end()) {
    hashes_.resize(activeRows_.end());
  }

  // As long as analyzeKeys is true, we keep running the keys through
  // the Vectorhashers so that we get a possible mapping of the keys
  // to small ints for array or normalized key. When mayUseValueIds is
  // false for the first time we stop. We do not retain the value ids
  // since the final ones will only be known after all data is
  // received.
  for (auto& hasher : hashers) {
    // TODO: Load only for active rows, except if right/full outer join.
    if (analyzeKeys_) {
      hasher->computeValueIds(activeRows_, hashes_);
      analyzeKeys_ = hasher->mayUseValueIds();
    }
  }
  auto rows = table_->rows();
  FlatVector<bool>* spillProbedFlagVector{nullptr};
  if (isInputFromSpill() && needProbedFlagSpill_) {
    spillProbedFlagVector =
        input->childAt(spillProbedFlagChannel_)->asFlatVector<bool>();
  }

  activeRows_.applyToSelected([&](auto rowIndex) {
    char* newRow = rows->newRow();
    // Store the columns for each row in sequence. At probe time
    // strings of the row will probably be in consecutive places, so
    // reading one will prime the cache for the next.
    for (auto i = 0; i < hashers.size(); ++i) {
      rows->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
    }
    for (auto i = 0; i < dependentChannels_.size(); ++i) {
      rows->store(*decoders_[i], rowIndex, newRow, i + hashers.size());
    }
    if (spillProbedFlagVector != nullptr) {
      VELOX_CHECK(!spillProbedFlagVector->isNullAt(rowIndex));
      if (spillProbedFlagVector->valueAt(rowIndex)) {
        rows->setProbedFlag(&newRow, 1);
      }
    }
  });
}

void HashBuild::ensureInputFits(RowVectorPtr& input) {
  // NOTE: we don't need memory reservation if all the partitions are spilling
  // as we spill all the input rows to disk directly.
  if (!spillEnabled() || spiller_ == nullptr || spiller_->isAllSpilled()) {
    return;
  }

  // NOTE: we simply reserve memory all inputs even though some of them are
  // spilling directly. It is okay as we will accumulate the extra reservation
  // in the operator's memory pool, and won't make any new reservation if there
  // is already sufficient reservations.
  VELOX_CHECK(spillEnabled());

  auto* rows = table_->rows();
  const auto numRows = rows->numRows();

  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  const auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow =
      std::max<uint64_t>(1, numRows == 0 ? 0 : outOfLineBytes / numRows);
  const auto currentUsage = pool()->usedBytes();

  if (numRows != 0) {
    // Test-only spill path.
    if (testingTriggerSpill(pool()->name())) {
      Operator::ReclaimableSectionGuard guard(this);
      memory::testingRunArbitration(pool());
      return;
    }
  }

  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool()->availableReservation();
  const auto tableIncrementBytes = table_->hashTableSizeIncrease(input->size());
  const int64_t flatBytes = input->estimateFlatSize();
  const auto rowContainerIncrementBytes = numRows == 0
      ? flatBytes * 2
      : rows->sizeIncrement(
            input->size(), outOfLineBytes > 0 ? flatBytes * 2 : 0);
  const auto incrementBytes = rowContainerIncrementBytes + tableIncrementBytes;

  // First to check if we have sufficient minimal memory reservation.
  if (availableReservationBytes >= minReservationBytes) {
    if (freeRows > input->size() &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatBytes)) {
      // Enough free rows for input rows and enough variable length free
      // space for the flat size of the whole vector. If outOfLineBytes
      // is 0 there is no need for variable length space.
      return;
    }

    // If there is variable length data we take the flat size of the
    // input as a cap on the new variable length data needed. There must be at
    // least 2x the increments in reservation.
    if (pool()->availableReservation() > 2 * incrementBytes) {
      return;
    }
  }

  // Check if we can increase reservation. The increment is the larger of
  // twice the maximum increment from this input and
  // 'spillableReservationGrowthPct_' of the current reservation.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);

  {
    Operator::ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(targetIncrementBytes)) {
      // If above reservation triggers the spilling of 'HashBuild' operator
      // itself, we will no longer need the reserved memory for building hash
      // table as the table is spilled, and the input will be directly spilled,
      // too.
      if (spiller_->isAllSpilled()) {
        pool()->release();
      }
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->usedBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

void HashBuild::spillInput(const RowVectorPtr& input) {
  VELOX_CHECK_EQ(input->size(), activeRows_.size());

  if (!spillEnabled() || spiller_ == nullptr || !spiller_->isAnySpilled() ||
      !activeRows_.hasSelections()) {
    return;
  }

  const auto numInput = input->size();
  prepareInputIndicesBuffers(numInput, spiller_->spilledPartitionSet());
  computeSpillPartitions(input);

  vector_size_t numSpillInputs = 0;
  for (auto row = 0; row < numInput; ++row) {
    const auto partition = spillPartitions_[row];
    if (FOLLY_UNLIKELY(!activeRows_.isValid(row))) {
      continue;
    }
    if (!spiller_->isSpilled(partition)) {
      continue;
    }
    activeRows_.setValid(row, false);
    ++numSpillInputs;
    rawSpillInputIndicesBuffers_[partition][numSpillInputs_[partition]++] = row;
  }
  if (numSpillInputs == 0) {
    return;
  }

  maybeSetupSpillChildVectors(input);

  for (uint32_t partition = 0; partition < numSpillInputs_.size();
       ++partition) {
    const int numInputs = numSpillInputs_[partition];
    if (numInputs == 0) {
      continue;
    }
    VELOX_CHECK(spiller_->isSpilled(partition));
    spillPartition(
        partition, numInputs, spillInputIndicesBuffers_[partition], input);
  }
  activeRows_.updateBounds();
}

void HashBuild::maybeSetupSpillChildVectors(const RowVectorPtr& input) {
  if (isInputFromSpill()) {
    return;
  }
  int32_t spillChannel = 0;
  for (const auto& channel : keyChannels_) {
    spillChildVectors_[spillChannel++] = input->childAt(channel);
  }
  for (const auto& channel : dependentChannels_) {
    spillChildVectors_[spillChannel++] = input->childAt(channel);
  }
  if (needProbedFlagSpill_) {
    VELOX_CHECK_NOT_NULL(spillProbedFlagVector_);
    spillProbedFlagVector_->resize(input->size());
    spillChildVectors_[spillChannel] = spillProbedFlagVector_;
  }
}

void HashBuild::prepareInputIndicesBuffers(
    vector_size_t numInput,
    const SpillPartitionNumSet& spillPartitions) {
  const auto maxIndicesBufferBytes = numInput * sizeof(vector_size_t);
  for (const auto& partition : spillPartitions) {
    if (spillInputIndicesBuffers_[partition] == nullptr ||
        (spillInputIndicesBuffers_[partition]->size() <
         maxIndicesBufferBytes)) {
      spillInputIndicesBuffers_[partition] = allocateIndices(numInput, pool());
      rawSpillInputIndicesBuffers_[partition] =
          spillInputIndicesBuffers_[partition]->asMutable<vector_size_t>();
    }
  }
  std::fill(numSpillInputs_.begin(), numSpillInputs_.end(), 0);
}

void HashBuild::computeSpillPartitions(const RowVectorPtr& input) {
  if (hashes_.size() < activeRows_.end()) {
    hashes_.resize(activeRows_.end());
  }
  const auto& hashers = table_->hashers();
  for (auto i = 0; i < hashers.size(); ++i) {
    auto& hasher = hashers[i];
    if (hasher->channel() != kConstantChannel) {
      hashers[i]->hash(activeRows_, i > 0, hashes_);
    } else {
      hashers[i]->hashPrecomputed(activeRows_, i > 0, hashes_);
    }
  }

  spillPartitions_.resize(input->size());
  activeRows_.applyToSelected([&](int32_t row) {
    spillPartitions_[row] = spiller_->hashBits().partition(hashes_[row]);
  });
}

void HashBuild::spillPartition(
    uint32_t partition,
    vector_size_t size,
    const BufferPtr& indices,
    const RowVectorPtr& input) {
  VELOX_DCHECK(spillEnabled());

  if (isInputFromSpill()) {
    spiller_->spill(partition, wrap(size, indices, input));
  } else {
    spiller_->spill(
        partition,
        wrap(size, indices, spillType_, spillChildVectors_, input->pool()));
  }
}

void HashBuild::noMoreInput() {
  checkRunning();

  if (noMoreInput_) {
    return;
  }
  Operator::noMoreInput();

  noMoreInputInternal();
}

void HashBuild::noMoreInputInternal() {
  if (!finishHashBuild()) {
    return;
  }

  postHashBuildProcess();
}

bool HashBuild::finishHashBuild() {
  checkRunning();

  // Release the unused memory reservation before building the merged join
  // table.
  pool()->release();

  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  // The last Driver to hit HashBuild::finish gathers the data from
  // all build Drivers and hands it over to the probe side. At this
  // point all build Drivers are continued and will free their
  // state. allPeersFinished is true only for the last Driver of the
  // build pipeline.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    setState(State::kWaitForBuild);
    return false;
  }

  TestValue::adjust("facebook::velox::exec::HashBuild::finishHashBuild", this);

  SCOPE_EXIT {
    // Realize the promises so that the other Drivers (which were not
    // the last to finish) can continue from the barrier and finish.
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }
  };

  if (joinHasNullKeys_ && isAntiJoin(joinType_) && nullAware_ &&
      !joinNode_->filter()) {
    joinBridge_->setAntiJoinHasNullKeys();
    return true;
  }

  std::vector<HashBuild*> otherBuilds;
  otherBuilds.reserve(peers.size());
  uint64_t numRows{0};
  {
    std::lock_guard<std::mutex> l(mutex_);
    numRows += table_->rows()->numRows();
  }
  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    HashBuild* build = dynamic_cast<HashBuild*>(op);
    VELOX_CHECK_NOT_NULL(build);
    if (build->joinHasNullKeys_) {
      joinHasNullKeys_ = true;
      if (isAntiJoin(joinType_) && nullAware_ && !joinNode_->filter()) {
        joinBridge_->setAntiJoinHasNullKeys();
        return true;
      }
    }
    {
      std::lock_guard<std::mutex> l(build->mutex_);
      VELOX_CHECK(
          !build->stateCleared_,
          "Internal state for a peer is empty. It might have already"
          " been closed.");
      numRows += build->table_->rows()->numRows();
    }
    otherBuilds.push_back(build);
  }

  ensureTableFits(numRows);

  std::vector<std::unique_ptr<BaseHashTable>> otherTables;
  otherTables.reserve(peers.size());
  SpillPartitionSet spillPartitions;
  for (auto* build : otherBuilds) {
    std::unique_ptr<Spiller> spiller;
    {
      std::lock_guard<std::mutex> l(build->mutex_);
      VELOX_CHECK(
          !build->stateCleared_,
          "Internal state for a peer is empty. It might have already"
          " been closed.");
      build->stateCleared_ = true;
      VELOX_CHECK_NOT_NULL(build->table_);
      otherTables.push_back(std::move(build->table_));
      spiller = std::move(build->spiller_);
    }
    if (spiller != nullptr) {
      spiller->finishSpill(spillPartitions);
    }
  }

  if (spiller_ != nullptr) {
    spiller_->finishSpill(spillPartitions);
    removeEmptyPartitions(spillPartitions);
  }

  // TODO: Get accurate signal if parallel join build is going to be applied
  //  from hash table. Currently there is still a chance inside hash table that
  //  it might decide it is not going to trigger parallel join build.
  const bool allowParallelJoinBuild =
      !otherTables.empty() && spillPartitions.empty();

  SCOPE_EXIT {
    // Make a guard to release the unused memory reservation since we have
    // finished the merged table build.
    pool()->release();
  };

  // TODO: Re-enable parallel join build with spilling triggered after
  //  https://github.com/facebookincubator/velox/issues/3567 is fixed.
  CpuWallTiming timing;
  {
    // If there is a chance the join build is parallel, we suspend the driver
    // while the hash table is being built. This is because off-driver thread
    // memory allocations inside parallel join build might trigger memory
    // arbitration.
    std::unique_ptr<SuspendedSection> suspendedSection;
    if (allowParallelJoinBuild) {
      suspendedSection = std::make_unique<SuspendedSection>(
          driverThreadContext()->driverCtx.driver);
    }

    CpuWallTimer cpuWallTimer{timing};
    table_->prepareJoinTable(
        std::move(otherTables),
        isInputFromSpill() ? spillConfig()->startPartitionBit
                           : BaseHashTable::kNoSpillInputStartPartitionBit,
        allowParallelJoinBuild ? operatorCtx_->task()->queryCtx()->executor()
                               : nullptr);
  }
  stats_.wlock()->addRuntimeStat(
      BaseHashTable::kBuildWallNanos,
      RuntimeCounter(timing.wallNanos, RuntimeCounter::Unit::kNanos));

  addRuntimeStats();
  joinBridge_->setHashTable(
      std::move(table_), std::move(spillPartitions), joinHasNullKeys_);
  if (spillEnabled()) {
    stateCleared_ = true;
  }
  return true;
}

void HashBuild::ensureTableFits(uint64_t numRows) {
  // NOTE: we don't need memory reservation if all the partitions have been
  // spilled as nothing need to be built.
  if (!spillEnabled() || spiller_ == nullptr || spiller_->isAllSpilled() ||
      numRows == 0) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool()->name())) {
    Operator::ReclaimableSectionGuard guard(this);
    memory::testingRunArbitration(pool());
    return;
  }

  TestValue::adjust("facebook::velox::exec::HashBuild::ensureTableFits", this);

  // NOTE: reserve a bit more memory to consider the extra memory used for
  // parallel table build operation.
  //
  // TODO: make this query configurable.
  const uint64_t memoryBytesToReserve =
      table_->estimateHashTableSize(numRows) * 1.1;
  {
    Operator::ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(memoryBytesToReserve)) {
      // If reservation triggers the spilling of 'HashBuild' operator itself, we
      // will no longer need the reserved memory for building hash table as the
      // table is spilled.
      if (spiller_->isAllSpilled()) {
        pool()->release();
      }
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(memoryBytesToReserve)
               << " for join table build from last hash build operator "
               << pool()->name()
               << ", usage: " << succinctBytes(pool()->usedBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

void HashBuild::postHashBuildProcess() {
  checkRunning();

  if (!spillEnabled()) {
    setState(State::kFinish);
    return;
  }

  auto spillInput = joinBridge_->spillInputOrFuture(&future_);
  if (!spillInput.has_value()) {
    VELOX_CHECK(future_.valid());
    setState(State::kWaitForProbe);
    return;
  }
  setupSpillInput(std::move(spillInput.value()));
}

void HashBuild::setupSpillInput(HashJoinBridge::SpillInput spillInput) {
  checkRunning();

  if (spillInput.spillPartition == nullptr) {
    setState(State::kFinish);
    return;
  }

  table_.reset();
  spiller_.reset();
  spillInputReader_.reset();

  // Reset the key and dependent channels as the spilled data columns have
  // already been ordered.
  std::iota(keyChannels_.begin(), keyChannels_.end(), 0);
  std::iota(
      dependentChannels_.begin(),
      dependentChannels_.end(),
      keyChannels_.size());

  setupTable();
  setupSpiller(spillInput.spillPartition.get());
  stateCleared_ = false;

  // Start to process spill input.
  processSpillInput();
}

void HashBuild::processSpillInput() {
  checkRunning();

  while (spillInputReader_->nextBatch(spillInput_)) {
    addInput(std::move(spillInput_));
    if (!isRunning()) {
      return;
    }
    if (operatorCtx_->driver()->shouldYield()) {
      state_ = State::kYield;
      future_ = ContinueFuture{folly::Unit{}};
      return;
    }
  }
  noMoreInputInternal();
}

void HashBuild::addRuntimeStats() {
  // Report range sizes and number of distinct values for the join keys.
  const auto& hashers = table_->hashers();
  const auto hashTableStats = table_->stats();
  uint64_t asRange;
  uint64_t asDistinct;
  auto lockedStats = stats_.wlock();

  lockedStats->addInputTiming.add(table_->offThreadBuildTiming());
  for (auto i = 0; i < hashers.size(); i++) {
    hashers[i]->cardinality(0, asRange, asDistinct);
    if (asRange != VectorHasher::kRangeTooLarge) {
      lockedStats->addRuntimeStat(
          fmt::format("rangeKey{}", i), RuntimeCounter(asRange));
    }
    if (asDistinct != VectorHasher::kRangeTooLarge) {
      lockedStats->addRuntimeStat(
          fmt::format("distinctKey{}", i), RuntimeCounter(asDistinct));
    }
  }

  lockedStats->runtimeStats[BaseHashTable::kCapacity] =
      RuntimeMetric(hashTableStats.capacity);
  lockedStats->runtimeStats[BaseHashTable::kNumRehashes] =
      RuntimeMetric(hashTableStats.numRehashes);
  lockedStats->runtimeStats[BaseHashTable::kNumDistinct] =
      RuntimeMetric(hashTableStats.numDistinct);
  if (hashTableStats.numTombstones != 0) {
    lockedStats->runtimeStats[BaseHashTable::kNumTombstones] =
        RuntimeMetric(hashTableStats.numTombstones);
  }

  // Add max spilling level stats if spilling has been triggered.
  if (spiller_ != nullptr && spiller_->isAnySpilled()) {
    lockedStats->addRuntimeStat(
        "maxSpillLevel",
        RuntimeCounter(
            spillConfig()->spillLevel(spiller_->hashBits().begin())));
  }
}

BlockingReason HashBuild::isBlocked(ContinueFuture* future) {
  switch (state_) {
    case State::kRunning:
      if (isInputFromSpill()) {
        processSpillInput();
      }
      break;
    case State::kYield:
      setRunning();
      VELOX_CHECK(isInputFromSpill());
      processSpillInput();
      break;
    case State::kFinish:
      break;
    case State::kWaitForSpill:
      if (!future_.valid()) {
        setRunning();
        VELOX_CHECK_NOT_NULL(input_);
        addInput(std::move(input_));
      }
      break;
    case State::kWaitForBuild:
      [[fallthrough]];
    case State::kWaitForProbe:
      if (!future_.valid()) {
        setRunning();
        postHashBuildProcess();
      }
      break;
    default:
      VELOX_UNREACHABLE("Unexpected state: {}", stateName(state_));
      break;
  }
  if (future_.valid()) {
    VELOX_CHECK(!isRunning() && !isFinished());
    *future = std::move(future_);
  }
  return fromStateToBlockingReason(state_);
}

bool HashBuild::isFinished() {
  return state_ == State::kFinish;
}

bool HashBuild::isRunning() const {
  return state_ == State::kRunning;
}

void HashBuild::checkRunning() const {
  VELOX_CHECK(isRunning(), stateName(state_));
}

void HashBuild::setRunning() {
  setState(State::kRunning);
}

void HashBuild::setState(State state) {
  checkStateTransition(state);
  state_ = state;
}

void HashBuild::checkStateTransition(State state) {
  VELOX_CHECK_NE(state_, state);
  switch (state) {
    case State::kRunning:
      if (!spillEnabled()) {
        VELOX_CHECK_EQ(state_, State::kWaitForBuild);
      } else {
        VELOX_CHECK_NE(state_, State::kFinish);
      }
      break;
    case State::kWaitForBuild:
      [[fallthrough]];
    case State::kWaitForSpill:
      [[fallthrough]];
    case State::kWaitForProbe:
      [[fallthrough]];
    case State::kFinish:
      VELOX_CHECK_EQ(state_, State::kRunning);
      break;
    default:
      VELOX_UNREACHABLE(stateName(state_));
      break;
  }
}

std::string HashBuild::stateName(State state) {
  switch (state) {
    case State::kRunning:
      return "RUNNING";
    case State::kYield:
      return "YIELD";
    case State::kWaitForSpill:
      return "WAIT_FOR_SPILL";
    case State::kWaitForBuild:
      return "WAIT_FOR_BUILD";
    case State::kWaitForProbe:
      return "WAIT_FOR_PROBE";
    case State::kFinish:
      return "FINISH";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(state));
  }
}

bool HashBuild::canReclaim() const {
  return canSpill() && !operatorCtx_->task()->hasMixedExecutionGroup();
}

void HashBuild::reclaim(
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  auto* driver = operatorCtx_->driver();
  VELOX_CHECK_NOT_NULL(driver);
  VELOX_CHECK(!nonReclaimableSection_);

  TestValue::adjust("facebook::velox::exec::HashBuild::reclaim", this);

  if (exceededMaxSpillLevelLimit_) {
    return;
  }

  // NOTE: a hash build operator is reclaimable if it is in the middle of table
  // build processing and is not under non-reclaimable execution section.
  if (nonReclaimableState()) {
    // TODO: reduce the log frequency if it is too verbose.
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    ++stats.numNonReclaimableAttempts;
    FB_LOG_EVERY_MS(WARNING, 1'000)
        << "Can't reclaim from hash build operator, state_["
        << stateName(state_) << "], nonReclaimableSection_["
        << nonReclaimableSection_ << "], spiller_["
        << (stateCleared_
                ? "cleared"
                : (spiller_->finalized() ? "finalized" : "non-finalized"))
        << "] " << pool()->name()
        << ", usage: " << succinctBytes(pool()->usedBytes());
    return;
  }

  const auto& task = driver->task();
  VELOX_CHECK(task->pauseRequested());
  const std::vector<Operator*> operators =
      task->findPeerOperators(operatorCtx_->driverCtx()->pipelineId, this);
  for (auto* op : operators) {
    HashBuild* buildOp = dynamic_cast<HashBuild*>(op);
    VELOX_CHECK_NOT_NULL(buildOp);
    VELOX_CHECK(buildOp->canReclaim());
    if (buildOp->nonReclaimableState()) {
      // TODO: reduce the log frequency if it is too verbose.
      RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
      ++stats.numNonReclaimableAttempts;
      FB_LOG_EVERY_MS(WARNING, 1'000)
          << "Can't reclaim from hash build operator, state_["
          << stateName(buildOp->state_) << "], nonReclaimableSection_["
          << buildOp->nonReclaimableSection_ << "], " << buildOp->pool()->name()
          << ", usage: " << succinctBytes(buildOp->pool()->usedBytes());
      return;
    }
  }

  struct SpillResult {
    const std::exception_ptr error{nullptr};

    explicit SpillResult(std::exception_ptr _error) : error(_error) {}
  };

  std::vector<std::shared_ptr<AsyncSource<SpillResult>>> spillTasks;
  auto* spillExecutor = spillConfig()->executor;
  for (auto* op : operators) {
    HashBuild* buildOp = static_cast<HashBuild*>(op);
    spillTasks.push_back(
        memory::createAsyncMemoryReclaimTask<SpillResult>([buildOp]() {
          try {
            buildOp->spiller_->spill();
            buildOp->table_->clear();
            // Release the minimum reserved memory.
            buildOp->pool()->release();
            return std::make_unique<SpillResult>(nullptr);
          } catch (const std::exception& e) {
            LOG(ERROR) << "Spill from hash build pool "
                       << buildOp->pool()->name() << " failed: " << e.what();
            // The exception is captured and thrown by the caller.
            return std::make_unique<SpillResult>(std::current_exception());
          }
        }));
    if ((operators.size() > 1) && (spillExecutor != nullptr)) {
      spillExecutor->add([source = spillTasks.back()]() { source->prepare(); });
    }
  }

  auto syncGuard = folly::makeGuard([&]() {
    for (auto& spillTask : spillTasks) {
      // We consume the result for the pending tasks. This is a cleanup in the
      // guard and must not throw. The first error is already captured before
      // this runs.
      try {
        spillTask->move();
      } catch (const std::exception&) {
      }
    }
  });

  for (auto& spillTask : spillTasks) {
    const auto result = spillTask->move();
    if (result->error) {
      std::rethrow_exception(result->error);
    }
  }
}

bool HashBuild::nonReclaimableState() const {
  // Apart from being in the nonReclaimable section, it's also not reclaimable
  // if:
  // 1) the hash table has been built by the last build thread (indicated by
  //    state_)
  // 2) the last build operator has transferred ownership of 'this operator's
  //    internal state (table_ and spiller_) to itself.
  // 3) it has completed spilling before reaching either of the previous
  //    two states.
  return ((state_ != State::kRunning) && (state_ != State::kWaitForBuild) &&
          (state_ != State::kYield)) ||
      nonReclaimableSection_ || !spiller_ || spiller_->finalized();
}

void HashBuild::close() {
  Operator::close();

  {
    // Free up major memory usage. Gate access to them as they can be accessed
    // by the last build thread that finishes building the hash table.
    std::lock_guard<std::mutex> l(mutex_);
    stateCleared_ = true;
    joinBridge_.reset();
    spiller_.reset();
    table_.reset();
  }
}
} // namespace facebook::velox::exec
