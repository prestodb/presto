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
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {
namespace {
// Map HashBuild 'state' to the corresponding driver blocking reason.
BlockingReason fromStateToBlockingReason(HashBuild::State state) {
  switch (state) {
    case HashBuild::State::kRunning:
      FOLLY_FALLTHROUGH;
    case HashBuild::State::kFinish:
      return BlockingReason::kNotBlocked;
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
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "HashBuild"),
      joinNode_(std::move(joinNode)),
      joinType_{joinNode_->joinType()},
      mappedMemory_(operatorCtx_->mappedMemory()),
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      spillConfig_(makeOperatorSpillConfig(
          *operatorCtx_->task()->queryCtx(),
          *operatorCtx_,
          core::QueryConfig::kJoinSpillEnabled,
          operatorId)),
      spillGroup_(
          spillEnabled() ? operatorCtx_->task()->getSpillOperatorGroupLocked(
                               operatorCtx_->driverCtx()->splitGroupId,
                               planNodeId())
                         : nullptr) {
  VELOX_CHECK_NOT_NULL(joinBridge_);
  joinBridge_->addBuilder();

  auto outputType = joinNode_->sources()[1]->outputType();

  auto numKeys = joinNode_->rightKeys().size();
  keyChannels_.reserve(numKeys);
  folly::F14FastMap<column_index_t, column_index_t> keyChannelMap(numKeys);
  std::vector<std::string> names;
  names.reserve(outputType->size());
  std::vector<TypePtr> types;
  types.reserve(outputType->size());

  for (int i = 0; i < joinNode_->rightKeys().size(); ++i) {
    auto& key = joinNode_->rightKeys()[i];
    auto channel = exprToChannel(key.get(), outputType);
    keyChannelMap[channel] = i;
    keyChannels_.emplace_back(channel);
    names.emplace_back(outputType->nameOf(channel));
    types.emplace_back(outputType->childAt(channel));
  }

  // Identify the non-key build side columns and make a decoder for each.
  const auto numDependents = outputType->size() - numKeys;
  dependentChannels_.reserve(numDependents);
  decoders_.reserve(numDependents);
  for (auto i = 0; i < outputType->size(); ++i) {
    if (keyChannelMap.find(i) == keyChannelMap.end()) {
      dependentChannels_.emplace_back(i);
      decoders_.emplace_back(std::make_unique<DecodedVector>());
      names.emplace_back(outputType->nameOf(i));
      types.emplace_back(outputType->childAt(i));
    }
  }

  tableType_ = ROW(std::move(names), std::move(types));
  setupTable();
  setupSpiller();

  if (isAntiJoins(joinType_) && joinNode_->filter()) {
    setupFilterForAntiJoins(keyChannelMap);
  }
}

void HashBuild::setupTable() {
  VELOX_CHECK_NULL(table_);

  const auto numKeys = keyChannels_.size();
  std::vector<std::unique_ptr<VectorHasher>> keyHashers;
  keyHashers.reserve(numKeys);
  for (vector_size_t i = 0; i < numKeys; ++i) {
    keyHashers.emplace_back(std::make_unique<VectorHasher>(
        tableType_->childAt(i), keyChannels_[i]));
  }

  const auto numDependents = tableType_->size() - numKeys;
  std::vector<TypePtr> dependentTypes;
  dependentTypes.reserve(numDependents);
  for (int i = numKeys; i < tableType_->size(); ++i) {
    dependentTypes.emplace_back(tableType_->childAt(i));
  }
  if (joinNode_->isRightJoin() || joinNode_->isFullJoin()) {
    // Do not ignore null keys.
    table_ = HashTable<false>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true, // allowDuplicates
        true, // hasProbedFlag
        mappedMemory_);
  } else {
    // (Left) semi and anti join with no extra filter only needs to know whether
    // there is a match. Hence, no need to store entries with duplicate keys.
    const bool dropDuplicates = !joinNode_->filter() &&
        (joinNode_->isLeftSemiJoin() || isAntiJoins(joinType_));
    // Right semi join needs to tag build rows that were probed.
    const bool needProbedFlag = joinNode_->isRightSemiJoin();
    if (joinNode_->isNullAwareAntiJoin() && joinNode_->filter()) {
      // We need to check null key rows in build side in case of null-aware anti
      // join with filter set.
      table_ = HashTable<false>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          !dropDuplicates, // allowDuplicates
          needProbedFlag, // hasProbedFlag
          mappedMemory_);
    } else {
      // Ignore null keys
      table_ = HashTable<true>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          !dropDuplicates, // allowDuplicates
          needProbedFlag, // hasProbedFlag
          mappedMemory_);
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
  HashBitRange hashBits = spillConfig.hashBitRange;
  if (spillPartition != nullptr) {
    const auto startBit = spillPartition->id().partitionBitOffset() +
        spillConfig.hashBitRange.numBits();
    hashBits =
        HashBitRange(startBit, startBit + spillConfig.hashBitRange.numBits());
  }
  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinBuild,
      table_->rows(),
      [&](folly::Range<char**> rows) { table_->rows()->eraseRows(rows); },
      tableType_,
      std::move(hashBits),
      keyChannels_.size(),
      std::vector<CompareFlags>(),
      spillConfig.filePath,
      operatorCtx_->task()
              ->queryCtx()
              ->pool()
              ->getMemoryUsageTracker()
              ->maxTotalBytes() *
          spillConfig.fileSizeFactor,
      Spiller::spillPool(),
      spillConfig.executor);

  if (spillPartition == nullptr) {
    spillGroup_->addOperator(
        *this,
        [&](const std::vector<Operator*>& operators) { runSpill(operators); });
  } else {
    spillInputReader_ = spillPartition->createReader();
  }
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
    const auto outputType = joinNode_->sources()[1]->outputType();
    for (const auto& field : expr->distinctFields()) {
      const auto index = outputType->getChildIdxIfExists(field->field());
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

  activeRows_.resize(input->size());
  activeRows_.setAll();

  auto& hashers = table_->hashers();

  for (auto i = 0; i < hashers.size(); ++i) {
    auto key = input->childAt(hashers[i]->channel())->loadedVector();
    hashers[i]->decode(*key, activeRows_);
  }

  if (!isRightJoin(joinType_) && !isFullJoin(joinType_) &&
      !(isNullAwareAntiJoin(joinType_) && joinNode_->filter())) {
    deselectRowsWithNulls(hashers, activeRows_);
  }

  for (auto i = 0; i < dependentChannels_.size(); ++i) {
    decoders_[i]->decode(
        *input->childAt(dependentChannels_[i])->loadedVector(), activeRows_);
  }

  if (isAntiJoins(joinType_) && joinNode_->filter()) {
    if (filterPropagatesNulls_) {
      removeInputRowsForAntiJoinFilter();
    }
  } else if (
      isNullAwareAntiJoin(joinType_) &&
      activeRows_.countSelected() < input->size()) {
    // Null-aware anti join with no extra filter returns no rows if build side
    // has nulls in join keys. Hence, we can stop processing on first null.
    antiJoinHasNullKeys_ = true;
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
  if (!spillEnabled() || spiller_->isAllSpilled()) {
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

  auto rows = table_->rows();
  auto numRows = rows->numRows();
  if (numRows == 0) {
    // Skip the memory reservation for the first input as we are lack of memory
    // usage stats for estimation. It is safe to skip as the query should have
    // sufficient memory initially.
    return true;
  }

  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  auto outOfLineBytesPerRow = std::max<uint64_t>(1, outOfLineBytes / numRows);
  int64_t flatBytes = input->estimateFlatSize();

  // Test-only spill path.
  if (testingTriggerSpill()) {
    numSpillRows_ = std::max<int64_t>(1, numRows / 10);
    numSpillBytes_ = numSpillRows_ * outOfLineBytesPerRow;
    return false;
  }

  if (freeRows > input->size() &&
      (outOfLineBytes == 0 || outOfLineFreeBytes >= flatBytes)) {
    // Enough free rows for input rows and enough variable length free
    // space for the flat size of the whole vector. If outOfLineBytes
    // is 0 there is no need for variable length space.
    return true;
  }

  // If there is variable length data we take the flat size of the
  // input as a cap on the new variable length data needed.
  const auto increment =
      rows->sizeIncrement(input->size(), outOfLineBytes ? flatBytes : 0);

  auto tracker = CHECK_NOTNULL(mappedMemory_->tracker());
  // There must be at least 2x the increments in reservation.
  if (tracker->getAvailableReservation() > 2 * increment) {
    return true;
  }

  // Check if we can increase reservation. The increment is the larger of
  // twice the maximum increment from this input and
  // 'spillableReservationGrowthPct_' of the current reservation.
  auto targetIncrement = std::max<int64_t>(
      increment * 2,
      tracker->getCurrentUserBytes() *
          spillConfig()->spillableReservationGrowthPct / 100);
  if (tracker->maybeReserve(targetIncrement)) {
    return true;
  }
  numSpillRows_ = std::max<int64_t>(
      1, targetIncrement / (rows->fixedRowSize() + outOfLineBytesPerRow));
  numSpillBytes_ = numSpillRows_ * outOfLineBytesPerRow;
  return false;
}

void HashBuild::spillInput(const RowVectorPtr& input) {
  VELOX_CHECK_EQ(input->size(), activeRows_.size());

  if (!spillEnabled() || !spiller_->isAnySpilled() ||
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
  for (const auto& partition : spillPartitions) {
    if (spillInputIndicesBuffers_[partition] == nullptr ||
        (spillInputIndicesBuffers_[partition]->size() < numInput)) {
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
  std::vector<Spiller*> spillers;
  spillers.reserve(spillOperators.size());
  for (auto& spillOp : spillOperators) {
    HashBuild* build = dynamic_cast<HashBuild*>(spillOp);
    VELOX_CHECK_NOT_NULL(build);
    spillers.push_back(build->spiller_.get());
    build->addAndClearSpillTarget(targetRows, targetBytes);
  }
  VELOX_CHECK_GT(targetRows, 0);
  VELOX_CHECK_GT(targetBytes, 0);

  std::vector<Spiller::SpillableStats> spillableStats(
      spiller_->hashBits().numPartitions());
  for (auto* spiller : spillers) {
    spiller->fillSpillRuns(spillableStats);
  }

  // Sort the partitions based on the amount of spillable data.
  SpillPartitionNumSet partitionsToSpill;
  std::vector<int32_t> partitionIndices(spillableStats.size());
  std::iota(partitionIndices.begin(), partitionIndices.end(), 0);
  std::sort(
      partitionIndices.begin(),
      partitionIndices.end(),
      [&](int32_t lhs, int32_t rhs) {
        return spillableStats[lhs].numBytes > spillableStats[rhs].numBytes;
      });
  int64_t numRows = 0;
  int64_t numBytes = 0;
  for (auto partitionNum : partitionIndices) {
    if (spillableStats[partitionNum].numBytes == 0) {
      break;
    }
    partitionsToSpill.insert(partitionNum);
    numRows += spillableStats[partitionNum].numRows;
    numBytes += spillableStats[partitionNum].numBytes;
    if (numRows >= targetRows && numBytes >= targetBytes) {
      break;
    }
  }
  VELOX_CHECK(!partitionsToSpill.empty());

  // TODO: consider to offload the partition spill processing to an executor to
  // run in parallel.
  for (auto* spiller : spillers) {
    spiller->spill(partitionsToSpill);
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

  std::vector<std::unique_ptr<BaseHashTable>> otherTables;
  otherTables.reserve(peers.size());
  SpillPartitionSet spillPartitions;
  Spiller::Stats spillStats;
  if (!antiJoinHasNullKeys_) {
    for (auto& peer : peers) {
      auto op = peer->findOperator(planNodeId());
      HashBuild* build = dynamic_cast<HashBuild*>(op);
      VELOX_CHECK(build);
      if (build->antiJoinHasNullKeys_) {
        antiJoinHasNullKeys_ = true;
        break;
      }
      otherTables.push_back(std::move(build->table_));
      if (build->spiller_ != nullptr) {
        spillStats += build->spiller_->stats();
        build->spiller_->finishSpill(spillPartitions);
      }
    }

    if (spiller_ != nullptr) {
      spillStats += spiller_->stats();

      stats_.spilledBytes = spillStats.spilledBytes;
      stats_.spilledRows = spillStats.spilledRows;
      stats_.spilledPartitions = spillStats.spilledPartitions;

      spiller_->finishSpill(spillPartitions);

      // Remove empty partitions.
      auto iter = spillPartitions.begin();
      while (iter != spillPartitions.end()) {
        if (iter->second->numFiles() == 0) {
          iter = spillPartitions.erase(iter);
        } else {
          ++iter;
        }
      }
    }
    table_->prepareJoinTable(std::move(otherTables));

    addRuntimeStats();
    if (joinBridge_->setHashTable(
            std::move(table_), std::move(spillPartitions))) {
      spillGroup_->restart();
    }
  } else {
    joinBridge_->setAntiJoinHasNullKeys();
  }

  // Realize the promises so that the other Drivers (which were not
  // the last to finish) can continue from the barrier and finish.
  peers.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }
  return true;
}

void HashBuild::postHashBuildProcess() {
  checkRunning();

  // Release the unused memory reservation since we have finished the table
  // build.
  operatorCtx_->mappedMemory()->tracker()->release();

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
  }
  noMoreInputInternal();
}

void HashBuild::addRuntimeStats() {
  // Report range sizes and number of distinct values for the join keys.
  const auto& hashers = table_->hashers();
  uint64_t asRange;
  uint64_t asDistinct;
  for (auto i = 0; i < hashers.size(); i++) {
    hashers[i]->cardinality(0, asRange, asDistinct);
    if (asRange != VectorHasher::kRangeTooLarge) {
      stats_.addRuntimeStat(
          fmt::format("rangeKey{}", i), RuntimeCounter(asRange));
    }
    if (asDistinct != VectorHasher::kRangeTooLarge) {
      stats_.addRuntimeStat(
          fmt::format("distinctKey{}", i), RuntimeCounter(asDistinct));
    }
  }
}

BlockingReason HashBuild::isBlocked(ContinueFuture* future) {
  switch (state_) {
    case State::kRunning:
      if (isInputFromSpill()) {
        processSpillInput();
      }
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
      FOLLY_FALLTHROUGH;
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
  stateTransitionCheck(state);
  state_ = state;
}

void HashBuild::stateTransitionCheck(State state) {
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
      FOLLY_FALLTHROUGH;
    case State::kWaitForSpill:
      FOLLY_FALLTHROUGH;
    case State::kWaitForProbe:
      FOLLY_FALLTHROUGH;
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

} // namespace facebook::velox::exec
