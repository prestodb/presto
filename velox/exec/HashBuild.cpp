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
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      spillMemoryThreshold_(
          operatorCtx_->driverCtx()->queryConfig().joinSpillMemoryThreshold()),
      keyChannelMap_(joinNode_->rightKeys().size()) {
  VELOX_CHECK(pool()->trackUsage());
  VELOX_CHECK_NOT_NULL(joinBridge_);

  spillGroup_ = spillEnabled()
      ? operatorCtx_->task()->getSpillOperatorGroupLocked(
            operatorCtx_->driverCtx()->splitGroupId, planNodeId())
      : nullptr;

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

  const auto& spillConfig = spillConfig_.value();
  HashBitRange hashBits(
      spillConfig.startPartitionBit,
      spillConfig.startPartitionBit + spillConfig.joinPartitionBits);
  if (spillPartition == nullptr) {
    spillGroup_->addOperator(
        *this,
        operatorCtx_->driver()->shared_from_this(),
        [&](const std::vector<Operator*>& operators) { runSpill(operators); });
  } else {
    LOG(INFO) << "Setup reader to read spilled input from "
              << spillPartition->toString()
              << ", memory pool: " << pool()->name();

    spillInputReader_ = spillPartition->createUnorderedReader(pool());

    const auto startBit = spillPartition->id().partitionBitOffset() +
        spillConfig.joinPartitionBits;
    // Disable spilling if exceeding the max spill level and the query might run
    // out of memory if the restored partition still can't fit in memory.
    if (spillConfig.exceedJoinSpillLevelLimit(startBit)) {
      RECORD_METRIC_VALUE(kMetricMaxSpillLevelExceededCount);
      LOG(WARNING) << "Exceeded spill level limit: "
                   << spillConfig.maxSpillLevel
                   << ", and disable spilling for memory pool: "
                   << pool()->name();
      exceededMaxSpillLevelLimit_ = true;
      return;
    }
    hashBits = HashBitRange(startBit, startBit + spillConfig.joinPartitionBits);
  }

  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinBuild,
      table_->rows(),
      tableType_,
      std::move(hashBits),
      &spillConfig,
      spillConfig.maxFileSize);

  const int32_t numPartitions = spiller_->hashBits().numPartitions();
  spillInputIndicesBuffers_.resize(numPartitions);
  rawSpillInputIndicesBuffers_.resize(numPartitions);
  numSpillInputs_.resize(numPartitions, 0);
  spillChildVectors_.resize(tableType_->size());
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
      bits::andBits(rawActiveRows, decoded.nulls(), 0, activeRows_.end());
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

  if (!ensureInputFits(input)) {
    VELOX_CHECK_NOT_NULL(input_);
    VELOX_CHECK(future_.valid());
    return;
  }

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
        auto* nulls = decoded.nulls();
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
  auto nextOffset = rows->nextOffset();
  activeRows_.applyToSelected([&](auto rowIndex) {
    char* newRow = rows->newRow();
    if (nextOffset) {
      *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
    }
    // Store the columns for each row in sequence. At probe time
    // strings of the row will probably be in consecutive places, so
    // reading one will prime the cache for the next.
    for (auto i = 0; i < hashers.size(); ++i) {
      rows->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
    }
    for (auto i = 0; i < dependentChannels_.size(); ++i) {
      rows->store(*decoders_[i], rowIndex, newRow, i + hashers.size());
    }
  });
}

bool HashBuild::ensureInputFits(RowVectorPtr& input) {
  // NOTE: we don't need memory reservation if all the partitions are spilling
  // as we spill all the input rows to disk directly.
  if (!spillEnabled() || spiller_ == nullptr || spiller_->isAllSpilled()) {
    return true;
  }

  // NOTE: we simply reserve memory all inputs even though some of them are
  // spilling directly. It is okay as we will accumulate the extra reservation
  // in the operator's memory pool, and won't make any new reservation if there
  // is already sufficient reservations.
  if (!reserveMemory(input)) {
    if (!requestSpill(input)) {
      return false;
    }
  } else {
    // Check if any other peer operator has requested group spill.
    if (waitSpill(input)) {
      return false;
    }
  }
  return true;
}

bool HashBuild::reserveMemory(const RowVectorPtr& input) {
  VELOX_CHECK(spillEnabled());

  numSpillRows_ = 0;
  numSpillBytes_ = 0;

  auto* rows = table_->rows();
  const auto numRows = rows->numRows();

  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  const auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow =
      std::max<uint64_t>(1, numRows == 0 ? 0 : outOfLineBytes / numRows);
  const auto currentUsage = pool()->currentBytes();

  if (numRows != 0) {
    // Test-only spill path.
    if (testingTriggerSpill()) {
      numSpillRows_ = std::max<int64_t>(1, numRows / 10);
      numSpillBytes_ = numSpillRows_ * outOfLineBytesPerRow;
      return false;
    }

    // We check usage from the parent pool to take peers' allocations into
    // account.
    const auto nodeUsage = pool()->parent()->currentBytes();
    if (spillMemoryThreshold_ != 0 && nodeUsage > spillMemoryThreshold_) {
      const int64_t bytesToSpill =
          nodeUsage * spillConfig()->spillableReservationGrowthPct / 100;
      numSpillRows_ = std::max<int64_t>(
          1, bytesToSpill / (rows->fixedRowSize() + outOfLineBytesPerRow));
      numSpillBytes_ = numSpillRows_ * outOfLineBytesPerRow;
      return false;
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
      return true;
    }

    // If there is variable length data we take the flat size of the
    // input as a cap on the new variable length data needed. There must be at
    // least 2x the increments in reservation.
    if (pool()->availableReservation() > 2 * incrementBytes) {
      return true;
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
      return true;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->currentBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
  return true;
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
  for (auto i = 0; i < spillPartitions_.size(); ++i) {
    spillPartitions_[i] = spiller_->hashBits().partition(hashes_[i]);
  }
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
        wrap(size, indices, tableType_, spillChildVectors_, input->pool()));
  }
}

bool HashBuild::requestSpill(RowVectorPtr& input) {
  VELOX_CHECK_GT(numSpillRows_, 0);
  VELOX_CHECK_GT(numSpillBytes_, 0);

  // If all the partitions have been spilled, then nothing to spill.
  if (spiller_->isAllSpilled()) {
    return true;
  }

  input_ = std::move(input);
  if (spillGroup_->requestSpill(*this, future_)) {
    VELOX_CHECK(future_.valid());
    setState(State::kWaitForSpill);
    return false;
  }
  input = std::move(input_);
  return true;
}

bool HashBuild::waitSpill(RowVectorPtr& input) {
  if (!spillGroup_->needSpill()) {
    return false;
  }

  if (spillGroup_->waitSpill(*this, future_)) {
    VELOX_CHECK(future_.valid());
    input_ = std::move(input);
    setState(State::kWaitForSpill);
    return true;
  }
  return false;
}

void HashBuild::runSpill(const std::vector<Operator*>& spillOperators) {
  VELOX_CHECK(spillEnabled());
  VELOX_CHECK(!spiller_->state().isAllPartitionSpilled());

  uint64_t targetRows = 0;
  uint64_t targetBytes = 0;
  for (auto& spillOp : spillOperators) {
    HashBuild* build = dynamic_cast<HashBuild*>(spillOp);
    VELOX_CHECK_NOT_NULL(build);
    build->addAndClearSpillTarget(targetRows, targetBytes);
  }
  VELOX_CHECK_GT(targetRows, 0);
  VELOX_CHECK_GT(targetBytes, 0);

  // TODO: consider to offload the partition spill processing to an executor to
  // run in parallel.
  for (auto& spillOp : spillOperators) {
    HashBuild* build = dynamic_cast<HashBuild*>(spillOp);
    build->spiller_->spill();
    build->table_->clear();
    build->pool()->release();
  }
}

void HashBuild::addAndClearSpillTarget(uint64_t& numRows, uint64_t& numBytes) {
  numRows += numSpillRows_;
  numSpillRows_ = 0;
  numBytes += numSpillBytes_;
  numSpillBytes_ = 0;
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
  if (spillEnabled()) {
    spillGroup_->operatorStopped(*this);
  }

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

  auto promisesGuard = folly::makeGuard([&]() {
    // Realize the promises so that the other Drivers (which were not
    // the last to finish) can continue from the barrier and finish.
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }
  });

  if (joinHasNullKeys_ && isAntiJoin(joinType_) && nullAware_ &&
      !joinNode_->filter()) {
    joinBridge_->setAntiJoinHasNullKeys();
    return true;
  }

  std::vector<HashBuild*> otherBuilds;
  otherBuilds.reserve(peers.size());
  uint64_t numRows = table_->rows()->numRows();
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
    numRows += build->table_->rows()->numRows();
    otherBuilds.push_back(build);
  }

  ensureTableFits(numRows);

  std::vector<std::unique_ptr<BaseHashTable>> otherTables;
  otherTables.reserve(peers.size());
  SpillPartitionSet spillPartitions;
  for (auto* build : otherBuilds) {
    VELOX_CHECK_NOT_NULL(build->table_);
    otherTables.push_back(std::move(build->table_));
    if (build->spiller_ != nullptr) {
      build->spiller_->finishSpill(spillPartitions);
    }
    build->recordSpillStats();
  }

  if (spiller_ != nullptr) {
    spiller_->finishSpill(spillPartitions);

    // Remove the spilled partitions which are empty so as we don't need to
    // trigger unnecessary spilling at hash probe side.
    auto iter = spillPartitions.begin();
    while (iter != spillPartitions.end()) {
      if (iter->second->numFiles() > 0) {
        ++iter;
      } else {
        iter = spillPartitions.erase(iter);
      }
    }
  }
  recordSpillStats();

  // TODO: re-enable parallel join build with spilling triggered after
  // https://github.com/facebookincubator/velox/issues/3567 is fixed.
  const bool allowParallelJoinBuild =
      !otherTables.empty() && spillPartitions.empty();
  table_->prepareJoinTable(
      std::move(otherTables),
      allowParallelJoinBuild ? operatorCtx_->task()->queryCtx()->executor()
                             : nullptr,
      isInputFromSpill() ? spillConfig()->startPartitionBit
                         : BaseHashTable::kNoSpillInputStartPartitionBit);
  addRuntimeStats();
  if (joinBridge_->setHashTable(
          std::move(table_), std::move(spillPartitions), joinHasNullKeys_)) {
    spillGroup_->restart();
  }

  // Release the unused memory reservation since we have finished the merged
  // table build.
  pool()->release();
  return true;
}

void HashBuild::recordSpillStats() {
  if (spiller_ != nullptr) {
    const auto spillStats = spiller_->stats();
    VELOX_CHECK_EQ(spillStats.spillSortTimeUs, 0);
    Operator::recordSpillStats(spillStats);
  } else if (exceededMaxSpillLevelLimit_) {
    exceededMaxSpillLevelLimit_ = false;
    common::SpillStats spillStats;
    spillStats.spillMaxLevelExceededCount = 1;
    Operator::recordSpillStats(spillStats);
  }
}

void HashBuild::ensureTableFits(uint64_t numRows) {
  // NOTE: we don't need memory reservation if all the partitions have been
  // spilled as nothing need to be built.
  if (!spillEnabled() || spiller_ == nullptr || spiller_->isAllSpilled()) {
    return;
  }

  TestValue::adjust("facebook::velox::exec::HashBuild::ensureTableFits", this);

  // NOTE: reserve a bit more memory to consider the extra memory used for
  // parallel table build operation.
  const uint64_t bytesToReserve = table_->estimateHashTableSize(numRows) * 1.1;
  {
    Operator::ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(bytesToReserve)) {
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(bytesToReserve)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->currentBytes())
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

  // Start to process spill input.
  processSpillInput();
}

void HashBuild::processSpillInput() {
  checkRunning();

  while (spillInputReader_->nextBatch(input_)) {
    addInput(std::move(input_));
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

  lockedStats->runtimeStats["hashtable.capacity"] =
      RuntimeMetric(hashTableStats.capacity);
  lockedStats->runtimeStats["hashtable.numRehashes"] =
      RuntimeMetric(hashTableStats.numRehashes);
  lockedStats->runtimeStats["hashtable.numDistinct"] =
      RuntimeMetric(hashTableStats.numDistinct);
  if (hashTableStats.numTombstones != 0) {
    lockedStats->runtimeStats["hashtable.numTombstones"] =
        RuntimeMetric(hashTableStats.numTombstones);
  }

  // Add max spilling level stats if spilling has been triggered.
  if (spiller_ != nullptr && spiller_->isAnySpilled()) {
    lockedStats->addRuntimeStat(
        "maxSpillLevel",
        RuntimeCounter(
            spillConfig()->joinSpillLevel(spiller_->hashBits().begin())));
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

bool HashBuild::testingTriggerSpill() {
  // Test-only spill path.
  if (spillConfig()->testSpillPct == 0) {
    return false;
  }
  return folly::hasher<uint64_t>()(++spillTestCounter_) % 100 <=
      spillConfig()->testSpillPct;
}

void HashBuild::reclaim(
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  auto* driver = operatorCtx_->driver();
  VELOX_CHECK_NOT_NULL(driver);
  VELOX_CHECK(!nonReclaimableSection_);

  TestValue::adjust("facebook::velox::exec::HashBuild::reclaim", this);

  if (spiller_ == nullptr) {
    // NOTE: we might have reached to the max spill limit.
    return;
  }

  // NOTE: a hash build operator is reclaimable if it is in the middle of table
  // build processing and is not under non-reclaimable execution section.
  if (nonReclaimableState()) {
    // TODO: reduce the log frequency if it is too verbose.
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    ++stats.numNonReclaimableAttempts;
    LOG(WARNING) << "Can't reclaim from hash build operator, state_["
                 << stateName(state_) << "], nonReclaimableSection_["
                 << nonReclaimableSection_ << "], spiller_["
                 << (spiller_->finalized() ? "finalized" : "non-finalized")
                 << "] " << pool()->name()
                 << ", usage: " << succinctBytes(pool()->currentBytes());
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
      LOG(WARNING) << "Can't reclaim from hash build operator, state_["
                   << stateName(buildOp->state_) << "], nonReclaimableSection_["
                   << buildOp->nonReclaimableSection_ << "], "
                   << buildOp->pool()->name() << ", usage: "
                   << succinctBytes(buildOp->pool()->currentBytes());
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
        std::make_shared<AsyncSource<SpillResult>>([buildOp]() {
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
      } catch (const std::exception& e) {
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
  return ((state_ != State::kRunning) && (state_ != State::kWaitForBuild) &&
          (state_ != State::kYield)) ||
      nonReclaimableSection_ || spiller_->finalized();
}

void HashBuild::abort() {
  Operator::abort();

  // Free up major memory usage.
  joinBridge_.reset();
  spiller_.reset();
  table_.reset();
}
} // namespace facebook::velox::exec
