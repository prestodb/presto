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

#include "velox/exec/HashProbe.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {

namespace {

// Batch size used when iterating the row container.
constexpr int kBatchSize = 1024;

// Returns the type for the hash table row. Build side keys first,
// then dependent build side columns.
RowTypePtr makeTableType(
    const RowType* type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        keys) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<column_index_t> keyChannels(keys.size());
  names.reserve(type->size());
  types.reserve(type->size());
  for (const auto& key : keys) {
    auto channel = type->getChildIdx(key->name());
    names.emplace_back(type->nameOf(channel));
    types.emplace_back(type->childAt(channel));
    keyChannels.insert(channel);
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (keyChannels.find(i) == keyChannels.end()) {
      names.emplace_back(type->nameOf(i));
      types.emplace_back(type->childAt(i));
    }
  }
  return ROW(std::move(names), std::move(types));
}

// Copy values from 'rows' of 'table' according to 'projections' in
// 'result'. Reuses 'result' children where possible.
void extractColumns(
    BaseHashTable* table,
    folly::Range<char**> rows,
    folly::Range<const IdentityProjection*> projections,
    memory::MemoryPool* pool,
    const RowVectorPtr& result) {
  for (auto projection : projections) {
    auto& child = result->childAt(projection.outputChannel);
    // TODO: Consider reuse of complex types.
    if (!child || !BaseVector::isVectorWritable(child) ||
        !child->isFlatEncoding()) {
      child = BaseVector::create(
          result->type()->childAt(projection.outputChannel), rows.size(), pool);
    }
    child->resize(rows.size());
    table->rows()->extractColumn(
        rows.data(), rows.size(), projection.inputChannel, child);
  }
}

folly::Range<vector_size_t*> initializeRowNumberMapping(
    BufferPtr& mapping,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (!mapping || !mapping->unique() ||
      mapping->size() < sizeof(vector_size_t) * size) {
    mapping = allocateIndices(size, pool);
  }
  return folly::Range(mapping->asMutable<vector_size_t>(), size);
}

BlockingReason fromStateToBlockingReason(HashProbe::State state) {
  switch (state) {
    case HashProbe::State::kRunning:
      FOLLY_FALLTHROUGH;
    case HashProbe::State::kFinish:
      return BlockingReason::kNotBlocked;
    case HashProbe::State::kWaitForBuild:
      return BlockingReason::kWaitForJoinBuild;
    case HashProbe::State::kWaitForPeers:
      return BlockingReason::kWaitForJoinProbe;
    default:
      VELOX_UNREACHABLE("Unexpected state: ", HashProbe::stateName(state));
  }
}

// Generate partition number set from spill partition id set.
SpillPartitionNumSet toPartitionNumSet(
    const SpillPartitionIdSet& partitionIdSet) {
  SpillPartitionNumSet partitionNumSet;
  partitionNumSet.reserve(partitionIdSet.size());
  for (const auto& partitionId : partitionIdSet) {
    partitionNumSet.insert(partitionId.partitionNumber());
  }
  return partitionNumSet;
}
} // namespace

HashProbe::HashProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::HashJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "HashProbe"),
      outputBatchSize_{driverCtx->queryConfig().preferredOutputBatchSize()},
      joinNode_(std::move(joinNode)),
      joinType_{joinNode_->joinType()},
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      spillConfig_(
          isSpillAllowed()
              ? operatorCtx_->makeSpillConfig(Spiller::Type::kHashJoinProbe)
              : std::nullopt),
      probeType_(joinNode_->sources()[0]->outputType()),
      filterResult_(1),
      outputTableRows_(outputBatchSize_) {
  VELOX_CHECK_NOT_NULL(joinBridge_);

  auto numKeys = joinNode_->leftKeys().size();
  keyChannels_.reserve(numKeys);
  hashers_.reserve(numKeys);
  for (auto& key : joinNode_->leftKeys()) {
    auto channel = exprToChannel(key.get(), probeType_);
    keyChannels_.emplace_back(channel);
    hashers_.push_back(
        std::make_unique<VectorHasher>(probeType_->childAt(channel), channel));
  }
  lookup_ = std::make_unique<HashLookup>(hashers_);
  auto buildType = joinNode_->sources()[1]->outputType();
  auto tableType = makeTableType(buildType.get(), joinNode_->rightKeys());
  if (joinNode_->filter()) {
    initializeFilter(joinNode_->filter(), probeType_, tableType);
  }

  size_t numIdentityProjections = 0;
  for (auto i = 0; i < probeType_->size(); ++i) {
    auto name = probeType_->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      identityProjections_.emplace_back(i, outIndex.value());
      if (outIndex.value() == i) {
        ++numIdentityProjections;
      }
    }
  }

  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    auto tableChannel = tableType->getChildIdxIfExists(outputType_->nameOf(i));
    if (tableChannel.has_value()) {
      tableOutputProjections_.emplace_back(tableChannel.value(), i);
    }
  }

  if (numIdentityProjections == probeType_->size() &&
      tableOutputProjections_.empty()) {
    isIdentityProjection_ = true;
  }

  if (isNullAwareAntiJoin(joinType_)) {
    filterTableResult_.resize(1);
  }
}

void HashProbe::initializeFilter(
    const core::TypedExprPtr& filter,
    const RowTypePtr& probeType,
    const RowTypePtr& tableType) {
  std::vector<core::TypedExprPtr> filters = {filter};
  filter_ =
      std::make_unique<ExprSet>(std::move(filters), operatorCtx_->execCtx());

  column_index_t filterChannel = 0;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  auto numFields = filter_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);
  for (auto& field : filter_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = probeType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterInputProjections_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(probeType->nameOf(channelValue));
      types.emplace_back(probeType->childAt(channelValue));
      continue;
    }
    channel = tableType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterTableProjections_.emplace_back(channelValue, filterChannel);
      names.emplace_back(tableType->nameOf(channelValue));
      types.emplace_back(tableType->childAt(channelValue));
      if (isNullAwareAntiJoin(joinType_)) {
        filterTableProjectionMap_[channelValue] = filterChannel;
      }
      ++filterChannel;
      continue;
    }
    VELOX_FAIL(
        "Join filter field {} not in probe or build input", field->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

void HashProbe::maybeSetupSpillInput(
    const std::optional<SpillPartitionId>& restoredPartitionId,
    const SpillPartitionIdSet& spillPartitionIds) {
  VELOX_CHECK_NULL(spillInputReader_);

  // If 'restoredPartitionId' is not null, then 'table_' is built from the
  // spilled build data. Create an unsorted reader to read the probe inputs from
  // the corresponding spilled probe partition on disk.
  if (restoredPartitionId.has_value()) {
    auto iter = spillPartitionSet_.find(restoredPartitionId.value());
    VELOX_CHECK(iter != spillPartitionSet_.end());
    auto partition = std::move(iter->second);
    VELOX_CHECK_EQ(partition->id(), restoredPartitionId.value());
    spillInputReader_ = partition->createReader();
    spillPartitionSet_.erase(iter);
  }

  VELOX_CHECK_NULL(spiller_);
  spillInputPartitionIds_ = spillPartitionIds;
  if (spillInputPartitionIds_.empty()) {
    return;
  }

  // If 'spillInputPartitionIds_' is not empty, then we set up a spiller to
  // spill the incoming probe inputs.
  const auto& spillConfig = spillConfig_.value();
  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinProbe,
      probeType_,
      HashBitRange(
          spillInputPartitionIds_.begin()->partitionBitOffset(),
          spillInputPartitionIds_.begin()->partitionBitOffset() +
              spillConfig.hashBitRange.numBits()),
      spillConfig.filePath,
      operatorCtx_->task()
              ->queryCtx()
              ->pool()
              ->getMemoryUsageTracker()
              ->maxTotalBytes() *
          spillConfig.fileSizeFactor,
      Spiller::spillPool(),
      spillConfig.executor);
  // Set the spill partitions to the corresponding ones at the build side. The
  // hash probe operator itself won't trigger any spilling.
  spiller_->setPartitionsSpilled(toPartitionNumSet(spillInputPartitionIds_));

  spillHashFunction_ = std::make_unique<HashPartitionFunction>(
      spiller_->hashBits(), probeType_, keyChannels_);
  spillInputIndicesBuffers_.resize(spillHashFunction_->numPartitions());
  rawSpillInputIndicesBuffers_.resize(spillHashFunction_->numPartitions());
  numSpillInputs_.resize(spillHashFunction_->numPartitions(), 0);
}

void HashProbe::asyncWaitForHashTable() {
  checkRunning();
  VELOX_CHECK_NULL(table_);

  auto hashBuildResult = joinBridge_->tableOrFuture(&future_);
  if (!hashBuildResult.has_value()) {
    VELOX_CHECK(future_.valid());
    setState(State::kWaitForBuild);
    return;
  }

  if (hashBuildResult->antiJoinHasNullKeys) {
    // Null-aware anti join with null keys on the build side without a filter
    // always returns nothing.
    VELOX_CHECK(isNullAwareAntiJoin(joinType_));
    // 'antiJoinHasNullKeys' flag shall only be set on the first built 'table_'
    VELOX_CHECK(spillPartitionSet_.empty());
    noMoreInput();
    return;
  }

  table_ = std::move(hashBuildResult->table);
  VELOX_CHECK_NOT_NULL(table_);

  maybeSetupSpillInput(
      hashBuildResult->restoredPartitionId, hashBuildResult->spillPartitionIds);

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      if (!needSpillInput()) {
        noMoreInput();
      }
    }
  } else if (
      (isInnerJoin(joinType_) || isLeftSemiJoin(joinType_) ||
       isRightSemiJoin(joinType_)) &&
      table_->hashMode() != BaseHashTable::HashMode::kHash && !isSpillInput() &&
      !hasMoreSpillData()) {
    // Find out whether there are any upstream operators that can accept
    // dynamic filters on all or a subset of the join keys. Create dynamic
    // filters to push down.
    //
    // NOTE: this optimization is not applied in the following cases: (1) if the
    // probe input is read from spilled data and there is no upstream operators
    // involved; (2) if there is spill data to restore, then we can't filter
    // probe inputs solely based on the current table's join keys.
    const auto& buildHashers = table_->hashers();
    auto channels = operatorCtx_->driverCtx()->driver->canPushdownFilters(
        this, keyChannels_);
    for (auto i = 0; i < keyChannels_.size(); i++) {
      if (channels.find(keyChannels_[i]) != channels.end()) {
        if (auto filter = buildHashers[i]->getFilter(false)) {
          dynamicFilters_.emplace(keyChannels_[i], std::move(filter));
        }
      }
    }
  }
}

bool HashProbe::isSpillInput() const {
  return spillInputReader_ != nullptr;
}

void HashProbe::prepareForSpillRestore() {
  checkRunning();
  VELOX_CHECK(spillEnabled());
  VELOX_CHECK(hasMoreSpillData());

  // Reset the internal states which are relevant to the previous probe run.
  noMoreSpillInput_ = false;
  table_.reset();
  spiller_.reset();
  spillInputReader_.reset();
  spillInputPartitionIds_.clear();
  lastProbeIterator_.reset();

  VELOX_CHECK(promises_.empty() || lastProber_);
  if (!lastProber_) {
    return;
  }
  lastProber_ = false;
  // Notify the hash build operators to build the next hash table.
  joinBridge_->probeFinished();

  // Wake up the peer hash probe operators to wait for table build.
  auto promises = std::move(promises_);
  for (auto& promise : promises) {
    promise.setValue();
  }
}

void HashProbe::addSpillInput() {
  checkRunning();

  if (input_ != nullptr || noMoreSpillInput_) {
    return;
  }
  if (FOLLY_UNLIKELY(!spillInputReader_->nextBatch(input_))) {
    noMoreInputInternal();
    return;
  }

  addInput(std::move(input_));
}

void HashProbe::spillInput(RowVectorPtr& input) {
  VELOX_CHECK(needSpillInput());

  const auto numInput = input->size();
  prepareInputIndicesBuffers(
      input->size(), spiller_->state().spilledPartitionSet());
  spillHashFunction_->partition(*input, spillPartitions_);

  vector_size_t numNonSpillingInput = 0;
  for (auto row = 0; row < numInput; ++row) {
    const auto partition = spillPartitions_[row];
    if (!spiller_->isSpilled(partition)) {
      rawNonSpillInputIndicesBuffer_[numNonSpillingInput++] = row;
      continue;
    }
    rawSpillInputIndicesBuffers_[partition][numSpillInputs_[partition]++] = row;
  }
  if (numNonSpillingInput == numInput) {
    return;
  }

  // Ensure vector are lazy loaded before spilling.
  for (int32_t i = 0; i < input_->childrenSize(); ++i) {
    input_->childAt(i)->loadedVector();
  }

  for (int32_t partition = 0; partition < numSpillInputs_.size(); ++partition) {
    const auto numSpillInputs = numSpillInputs_[partition];
    if (numSpillInputs == 0) {
      continue;
    }
    VELOX_CHECK(spiller_->isSpilled(partition));
    spiller_->spill(
        partition,
        wrap(numSpillInputs, spillInputIndicesBuffers_[partition], input));
  }

  if (numNonSpillingInput == 0) {
    input = nullptr;
  } else {
    input = wrap(numNonSpillingInput, nonSpillInputIndicesBuffer_, input);
  }
}

void HashProbe::prepareInputIndicesBuffers(
    vector_size_t numInput,
    const folly::F14FastSet<uint32_t>& spillPartitions) {
  VELOX_DCHECK(spillEnabled());
  const auto maxIndicesBufferBytes = numInput * sizeof(vector_size_t);
  if (nonSpillInputIndicesBuffer_ == nullptr ||
      nonSpillInputIndicesBuffer_->size() < maxIndicesBufferBytes) {
    nonSpillInputIndicesBuffer_ = allocateIndices(numInput, pool());
    rawNonSpillInputIndicesBuffer_ =
        nonSpillInputIndicesBuffer_->asMutable<vector_size_t>();
  }
  for (const auto& partition : spillPartitions) {
    if (spillInputIndicesBuffers_[partition] == nullptr ||
        spillInputIndicesBuffers_[partition]->size() < maxIndicesBufferBytes) {
      spillInputIndicesBuffers_[partition] = allocateIndices(numInput, pool());
      rawSpillInputIndicesBuffers_[partition] =
          spillInputIndicesBuffers_[partition]->asMutable<vector_size_t>();
    }
  }
  std::fill(numSpillInputs_.begin(), numSpillInputs_.end(), 0);
}

BlockingReason HashProbe::isBlocked(ContinueFuture* future) {
  switch (state_) {
    case State::kWaitForBuild:
      VELOX_CHECK_NULL(table_);
      if (!future_.valid()) {
        setRunning();
        asyncWaitForHashTable();
      }
      break;
    case State::kRunning:
      VELOX_CHECK_NOT_NULL(table_);
      if (spillInputReader_ != nullptr) {
        addSpillInput();
      }
      break;
    case State::kWaitForPeers:
      VELOX_CHECK(hasMoreSpillData());
      if (!future_.valid()) {
        setRunning();
      }
      break;
    case State::kFinish:
      break;
    default:
      VELOX_UNREACHABLE(stateName(state_));
      break;
  }

  if (future_.valid()) {
    VELOX_CHECK(!isRunning());
    *future = std::move(future_);
  }
  return fromStateToBlockingReason(state_);
}

void HashProbe::clearDynamicFilters() {
  VELOX_CHECK(!hasMoreSpillData());
  VELOX_CHECK(!needSpillInput());

  // The join can be completely replaced with a pushed down
  // filter when the following conditions are met:
  //  * hash table has a single key with unique values,
  //  * build side has no dependent columns.
  if (keyChannels_.size() == 1 && !table_->hasDuplicateKeys() &&
      tableOutputProjections_.empty() && !filter_ && !dynamicFilters_.empty()) {
    canReplaceWithDynamicFilter_ = true;
  }

  Operator::clearDynamicFilters();
}

void HashProbe::addInput(RowVectorPtr input) {
  input_ = std::move(input);

  if (canReplaceWithDynamicFilter_) {
    replacedWithDynamicFilter_ = true;
    return;
  }

  if (needSpillInput()) {
    spillInput(input_);
    // Check if all the probe input rows have been spilled.
    if (input_ == nullptr) {
      return;
    }
  }

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      VELOX_CHECK(needSpillInput());
      input_ = nullptr;
      return;
    }
    // Build side is empty. This state is valid only for anti, left and full
    // joins.
    VELOX_CHECK(
        isAntiJoins(joinType_) || isLeftJoin(joinType_) ||
        isFullJoin(joinType_));
    if (!isAntiJoins(joinType_) || (filter_ == nullptr)) {
      return;
    }
    // For anti join types we need to decode the join keys columns to initialize
    // 'nonNullInputRows_' if the filter is also present. The filter evaluation
    // will access 'nonNullInputRows_' later.
  }

  nonNullInputRows_.resize(input_->size());
  nonNullInputRows_.setAll();

  for (auto i = 0; i < hashers_.size(); ++i) {
    auto key = input_->childAt(hashers_[i]->channel())->loadedVector();
    hashers_[i]->decode(*key, nonNullInputRows_);
  }

  deselectRowsWithNulls(hashers_, nonNullInputRows_);

  if (table_->numDistinct() == 0) {
    VELOX_CHECK(isAntiJoins(joinType_));
    VELOX_CHECK_NOT_NULL(filter_);
    return;
  }

  activeRows_ = nonNullInputRows_;
  lookup_->hashes.resize(input_->size());
  auto mode = table_->hashMode();
  auto& buildHashers = table_->hashers();
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    if (mode != BaseHashTable::HashMode::kHash) {
      auto key = input_->childAt(keyChannels_[i]);
      buildHashers[i]->lookupValueIds(
          *key, activeRows_, scratchMemory_, lookup_->hashes);
    } else {
      hashers_[i]->hash(activeRows_, i > 0, lookup_->hashes);
    }
  }
  lookup_->rows.clear();
  if (activeRows_.isAllSelected()) {
    lookup_->rows.resize(activeRows_.size());
    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);
  } else {
    bits::forEachSetBit(
        activeRows_.asRange().bits(),
        0,
        activeRows_.size(),
        [&](vector_size_t row) { lookup_->rows.push_back(row); });
  }

  passingInputRowsInitialized_ = false;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_) ||
      isAntiJoins(joinType_)) {
    // Make sure to allocate an entry in 'hits' for every input row to allow for
    // including rows without a match in the output. Also, make sure to
    // initialize all 'hits' to nullptr as HashTable::joinProbe will only
    // process activeRows_.
    auto numInput = input_->size();
    auto& hits = lookup_->hits;
    hits.resize(numInput);
    std::fill(hits.data(), hits.data() + numInput, nullptr);
    if (!lookup_->rows.empty()) {
      table_->joinProbe(*lookup_);
    }

    // Update lookup_->rows to include all input rows, not just
    // activeRows_ as we need to include all rows in the output.
    auto& rows = lookup_->rows;
    rows.resize(numInput);
    std::iota(rows.begin(), rows.end(), 0);
  } else {
    if (lookup_->rows.empty()) {
      input_ = nullptr;
      return;
    }
    lookup_->hits.resize(lookup_->rows.back() + 1);
    table_->joinProbe(*lookup_);
  }
  results_.reset(*lookup_);
}

void HashProbe::prepareOutput(vector_size_t size) {
  // Try to re-use memory for the output vectors that contain build-side data.
  // We expect output vectors containing probe-side data to be null (reset in
  // clearIdentityProjectedOutput). BaseVector::prepareForReuse keeps null
  // children unmodified and makes non-null (build side) children reusable.
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, size);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, size, pool()));
  }
}

void HashProbe::fillOutput(vector_size_t size) {
  prepareOutput(size);

  for (auto projection : identityProjections_) {
    // Load input vector if it is being split into multiple batches. It is not
    // safe to wrap unloaded LazyVector into two different dictionaries.
    ensureLoadedIfNotAtEnd(projection.inputChannel);
    auto inputChild = input_->childAt(projection.inputChannel);

    output_->childAt(projection.outputChannel) =
        wrapChild(size, outputRowMapping_, inputChild);
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows_.data(), size),
      tableOutputProjections_,
      pool(),
      output_);
}

RowVectorPtr HashProbe::getBuildSideOutput() {
  outputTableRows_.resize(outputBatchSize_);
  int32_t numOut;
  if (isRightSemiJoin(joinType_)) {
    numOut = table_->listProbedRows(
        &lastProbeIterator_,
        outputBatchSize_,
        RowContainer::kUnlimited,
        outputTableRows_.data());
  } else {
    // Must be a right join or full join.
    numOut = table_->listNotProbedRows(
        &lastProbeIterator_,
        outputBatchSize_,
        RowContainer::kUnlimited,
        outputTableRows_.data());
  }
  if (!numOut) {
    return nullptr;
  }

  prepareOutput(numOut);

  // Populate probe-side columns of the output with nulls.
  for (auto projection : identityProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::createNullConstant(
        outputType_->childAt(projection.outputChannel), numOut, pool());
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows_.data(), numOut),
      tableOutputProjections_,
      pool(),
      output_);
  return output_;
}

void HashProbe::clearIdentityProjectedOutput() {
  if (!output_ || !output_.unique()) {
    return;
  }
  for (auto& projection : identityProjections_) {
    output_->childAt(projection.outputChannel) = nullptr;
  }
}

bool HashProbe::needLastProbe() const {
  return isRightJoin(joinType_) || isFullJoin(joinType_) ||
      isRightSemiJoin(joinType_);
}

bool HashProbe::skipProbeOnEmptyBuild() const {
  return isInnerJoin(joinType_) || isLeftSemiJoin(joinType_) ||
      isRightJoin(joinType_) || isRightSemiJoin(joinType_);
}

bool HashProbe::spillEnabled() const {
  return spillConfig_.has_value();
}

bool HashProbe::hasMoreSpillData() const {
  VELOX_CHECK(spillPartitionSet_.empty() || spillEnabled());
  return !spillPartitionSet_.empty() || needSpillInput();
}

bool HashProbe::needSpillInput() const {
  VELOX_CHECK(spillInputPartitionIds_.empty() || spillEnabled());
  VELOX_CHECK_EQ(spillInputPartitionIds_.empty(), spiller_ == nullptr);

  return !spillInputPartitionIds_.empty();
}

void HashProbe::setState(State state) {
  checkStateTransition(state);
  state_ = state;
}

void HashProbe::checkStateTransition(State state) {
  VELOX_CHECK_NE(state_, state);
  switch (state) {
    case State::kRunning:
      if (!hasMoreSpillData()) {
        VELOX_CHECK_EQ(state_, State::kWaitForBuild);
      } else {
        VELOX_CHECK(
            state_ == State::kWaitForBuild || state_ == State::kWaitForPeers)
      }
      break;
    case State::kWaitForPeers:
      VELOX_CHECK(hasMoreSpillData());
      FOLLY_FALLTHROUGH;
    case State::kWaitForBuild:
      FOLLY_FALLTHROUGH;
    case State::kFinish:
      VELOX_CHECK_EQ(state_, State::kRunning);
      break;
    default:
      VELOX_UNREACHABLE(stateName(state_));
      break;
  }
}

RowVectorPtr HashProbe::getOutput() {
  checkRunning();

  clearIdentityProjectedOutput();
  if (!input_) {
    if (!hasMoreInput()) {
      if (needLastProbe() && lastProber_) {
        auto output = getBuildSideOutput();
        if (output != nullptr) {
          return output;
        }
      }
      if (hasMoreSpillData()) {
        prepareForSpillRestore();
        asyncWaitForHashTable();
      } else {
        setState(State::kFinish);
      }
      return nullptr;
    }
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (replacedWithDynamicFilter_) {
    stats_.addRuntimeStat(
        "replacedWithDynamicFilterRows", RuntimeCounter(inputSize));
    auto output = Operator::fillOutput(inputSize, nullptr);
    input_ = nullptr;
    return output;
  }

  const bool isLeftSemiOrAntiJoinNoFilter =
      !filter_ && (isLeftSemiJoin(joinType_) || isAntiJoins(joinType_));

  const bool emptyBuildSide = (table_->numDistinct() == 0);

  // Left semi and anti joins are always cardinality reducing, e.g. for a given
  // row of input they produce zero or 1 row of output. Therefore, if there is
  // no extra filter we can process each batch of input in one go.
  auto outputBatchSize = (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide)
      ? inputSize
      : outputBatchSize_;
  auto mapping =
      initializeRowNumberMapping(outputRowMapping_, outputBatchSize, pool());
  outputTableRows_.resize(outputBatchSize);

  for (;;) {
    int numOut = 0;

    if (emptyBuildSide) {
      // When build side is empty, anti and left joins return all probe side
      // rows, including ones with null join keys.
      std::iota(mapping.begin(), mapping.end(), 0);
      std::fill(outputTableRows_.begin(), outputTableRows_.end(), nullptr);
      numOut = inputSize;
    } else if (isNullAwareAntiJoin(joinType_) && !filter_) {
      // When build side is not empty, anti join without a filter returns probe
      // rows with no nulls in the join key and no match in the build side.
      for (auto i = 0; i < inputSize; ++i) {
        if (nonNullInputRows_.isValid(i) &&
            (!activeRows_.isValid(i) || !lookup_->hits[i])) {
          mapping[numOut] = i;
          ++numOut;
        }
      }
    } else if (isAntiJoin(joinType_) && !filter_) {
      for (auto i = 0; i < inputSize; ++i) {
        if (!nonNullInputRows_.isValid(i) ||
            (!activeRows_.isValid(i) || !lookup_->hits[i])) {
          mapping[numOut] = i;
          ++numOut;
        }
      }
    } else {
      numOut = table_->listJoinResults(
          results_,
          isLeftJoin(joinType_) || isFullJoin(joinType_) ||
              isAntiJoins(joinType_),
          mapping,
          folly::Range(outputTableRows_.data(), outputTableRows_.size()));
    }

    if (!numOut) {
      input_ = nullptr;
      return nullptr;
    }
    VELOX_CHECK_LE(numOut, outputTableRows_.size());

    numOut = evalFilter(numOut);
    if (!numOut) {
      // The filter was false on all rows.
      if (isLeftSemiOrAntiJoinNoFilter) {
        input_ = nullptr;
        return nullptr;
      }
      continue;
    }

    if (needLastProbe()) {
      // Mark build-side rows that have a match on the join condition.
      table_->rows()->setProbedFlag(outputTableRows_.data(), numOut);
    }

    // Right semi join only returns the build side output when the probe side
    // is fully complete. Do not return anything here.
    if (isRightSemiJoin(joinType_)) {
      if (results_.atEnd()) {
        input_ = nullptr;
      }
      return nullptr;
    }

    fillOutput(numOut);

    if (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide) {
      input_ = nullptr;
    }
    return output_;
  }
}

void HashProbe::fillFilterInput(vector_size_t size) {
  if (!filterInput_) {
    filterInput_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(filterInputType_, 1, pool()));
  }
  filterInput_->resize(size);
  for (auto projection : filterInputProjections_) {
    ensureLoadedIfNotAtEnd(projection.inputChannel);
    filterInput_->childAt(projection.outputChannel) = wrapChild(
        size, outputRowMapping_, input_->childAt(projection.inputChannel));
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows_.data(), size),
      filterTableProjections_,
      pool(),
      filterInput_);
}

void HashProbe::prepareFilterRowsForNullAwareAntiJoin(
    vector_size_t numRows,
    bool filterPropagateNulls) {
  VELOX_CHECK_LE(numRows, kBatchSize);
  if (filterTableInput_ == nullptr) {
    filterTableInput_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(filterInputType_, kBatchSize, pool()));
  }

  if (filterPropagateNulls) {
    nullFilterInputRows_.resizeFill(numRows, false);
    auto* rawNullRows = nullFilterInputRows_.asMutableRange().bits();
    for (auto& projection : filterInputProjections_) {
      filterInputColumnDecodedVector_.decode(
          *filterInput_->childAt(projection.outputChannel), filterInputRows_);
      if (filterInputColumnDecodedVector_.mayHaveNulls()) {
        // NOTE: the false value of a raw null bit indicates null so we OR with
        // negative of the raw bit.
        bits::orWithNegatedBits(
            rawNullRows, filterInputColumnDecodedVector_.nulls(), 0, numRows);
      }
    }
    nullFilterInputRows_.updateBounds();
    // TODO: consider to skip filtering on 'nullFilterInputRows_' as we know it
    // will never pass the filtering.
  }

  // NOTE: for null-aware anti join, we will skip filtering on the probe rows
  // with null join key columns(s) as we can apply filtering after they cross
  // join with the table rows later.g
  if (!nonNullInputRows_.isAllSelected()) {
    auto* rawMapping = outputRowMapping_->asMutable<vector_size_t>();
    for (int i = 0; i < numRows; ++i) {
      filterInputRows_.setValid(i, nonNullInputRows_.isValid(rawMapping[i]));
    }
    filterInputRows_.updateBounds();
  }
}

void HashProbe::applyFilterOnTableRowsForNullAwareAntiJoin(
    SelectivityVector& rows,
    bool nullKeyRowsOnly) {
  if (!rows.hasSelections()) {
    return;
  }
  auto tableRows = table_->rows();
  if (tableRows == nullptr) {
    return;
  }
  RowContainerIterator iter;
  char* data[kBatchSize];
  while (auto numRows = tableRows->listRows(
             &iter, kBatchSize, RowContainer::kUnlimited, data)) {
    filterTableInput_->resize(numRows);
    filterTableInputRows_.resizeFill(numRows, true);
    auto* rawNonNullRows = filterTableInputRows_.asMutableRange().bits();

    for (column_index_t columnIndex = 0;
         columnIndex < tableRows->columnTypes().size();
         ++columnIndex) {
      // NOTE: extracts the build side columns in 'filterTableInput_' from the
      // table. 'filterTableInput_' has type of 'filterInputType_', and we will
      // fill the probe side columns later before applying the filter.
      VectorPtr columnVector;
      auto it = filterTableProjectionMap_.find(columnIndex);
      if (it != filterTableProjectionMap_.end()) {
        columnVector = filterTableInput_->childAt(it->second);
        tableRows->extractColumn(data, numRows, columnIndex, columnVector);
      }
      if (nullKeyRowsOnly && columnIndex < tableRows->keyTypes().size()) {
        if (columnVector == nullptr) {
          columnVector = BaseVector::create(
              tableRows->keyTypes()[columnIndex], numRows, pool());
          tableRows->extractColumn(data, numRows, columnIndex, columnVector);
        }
        filterInputColumnDecodedVector_.decode(
            *columnVector, filterTableInputRows_);
        if (filterInputColumnDecodedVector_.mayHaveNulls()) {
          // NOTE: the true value of a raw null bit indicates non-null so we AND
          // with the raw bit.
          bits::andBits(
              rawNonNullRows,
              filterInputColumnDecodedVector_.nulls(),
              0,
              numRows);
        }
      }
    }

    if (nullKeyRowsOnly) {
      bits::negate(reinterpret_cast<char*>(rawNonNullRows), numRows);
      filterTableInputRows_.updateBounds();
    }

    rows.applyToSelected([&](vector_size_t row) {
      // Fill up 'filterTableInput_' with probe side filter columns.
      for (auto& projection : filterInputProjections_) {
        filterTableInput_->childAt(projection.outputChannel) =
            BaseVector::wrapInConstant(
                numRows, row, input_->childAt(projection.inputChannel));
      }
      EvalCtx evalCtx(
          operatorCtx_->execCtx(), filter_.get(), filterTableInput_.get());
      filter_->eval(filterTableInputRows_, evalCtx, filterTableResult_);
      decodedFilterTableResult_.decode(
          *filterTableResult_[0], filterTableInputRows_);
      const bool passed =
          !filterTableInputRows_.testSelected([&](vector_size_t j) {
            return decodedFilterTableResult_.isNullAt(j) ||
                !decodedFilterTableResult_.valueAt<bool>(j);
          });
      if (passed) {
        rows.setValid(row, false);
      }
    });
  }
  rows.updateBounds();
}

vector_size_t HashProbe::evalFilterForNullAwareAntiJoin(
    vector_size_t numRows,
    const bool filterPropagateNulls) {
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  SelectivityVector filterPassedRows(numRows, false);
  SelectivityVector nullKeyProbeRows(input_->size(), false);
  SelectivityVector crossJoinProbeRows(input_->size(), false);

  for (auto i = 0; i < numRows; ++i) {
    // Skip filter input row if it has any null probe side filter column.
    if (filterPropagateNulls && nullFilterInputRows_.isValid(i)) {
      continue;
    }

    const auto probeRow = rawOutputProbeRowMapping[i];
    if (nonNullInputRows_.isValid(probeRow)) {
      if (!decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i)) {
        filterPassedRows.setValid(i, true);
      } else {
        nullKeyProbeRows.setValid(probeRow, true);
      }
    } else {
      crossJoinProbeRows.setValid(probeRow, true);
    }
  }

  // Skip filtering on the probe rows which have passed the filter on any one of
  // its matched row with the build side.
  filterPassedRows.updateBounds();
  filterPassedRows.applyToSelected([&](vector_size_t row) {
    auto probeRow = rawOutputProbeRowMapping[row];
    nullKeyProbeRows.setValid(probeRow, false);
    crossJoinProbeRows.setValid(probeRow, false);
  });

  // TODO: consider to combine the two filter processes into one to avoid scan
  // the table rows twice.
  nullKeyProbeRows.updateBounds();
  applyFilterOnTableRowsForNullAwareAntiJoin(nullKeyProbeRows, true);

  crossJoinProbeRows.updateBounds();
  applyFilterOnTableRowsForNullAwareAntiJoin(crossJoinProbeRows, false);

  vector_size_t numPassed = 0;
  auto addMiss = [&](auto row) {
    outputTableRows_[numPassed] = nullptr;
    rawOutputProbeRowMapping[numPassed++] = row;
  };
  for (auto i = 0; i < numRows; ++i) {
    auto probeRow = rawOutputProbeRowMapping[i];
    bool passed;
    if (filterPropagateNulls && nullFilterInputRows_.isValid(i)) {
      passed = false;
    } else if (nonNullInputRows_.isValid(probeRow)) {
      if (!decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i)) {
        passed = true;
      } else {
        passed = !nullKeyProbeRows.isValid(probeRow);
      }
    } else {
      passed = !crossJoinProbeRows.isValid(probeRow);
    }
    noMatchDetector_.advance(probeRow, passed, addMiss);
  }
  if (results_.atEnd()) {
    noMatchDetector_.finish(addMiss);
  }
  return numPassed;
}

int32_t HashProbe::evalFilter(int32_t numRows) {
  if (!filter_) {
    return numRows;
  }
  const bool filterPropagateNulls = filter_->expr(0)->propagatesNulls();
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  fillFilterInput(numRows);
  filterInputRows_.resizeFill(numRows);

  if (isNullAwareAntiJoin(joinType_)) {
    prepareFilterRowsForNullAwareAntiJoin(numRows, filterPropagateNulls);
  }

  EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput_.get());
  filter_->eval(0, 1, true, filterInputRows_, evalCtx, filterResult_);

  decodedFilterResult_.decode(*filterResult_[0], filterInputRows_);

  int32_t numPassed = 0;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
    // Identify probe rows which got filtered out and add them back with nulls
    // for build side.
    auto addMiss = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      const bool passed = !decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i);
      noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
      if (passed) {
        outputTableRows_[numPassed] = outputTableRows_[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }
    if (results_.atEnd()) {
      noMatchDetector_.finish(addMiss);
    }
  } else if (isLeftSemiJoin(joinType_)) {
    auto addLastMatch = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      if (!decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i)) {
        leftSemiJoinTracker_.advance(rawOutputProbeRowMapping[i], addLastMatch);
      }
    }
    if (results_.atEnd()) {
      leftSemiJoinTracker_.finish(addLastMatch);
    }
  } else if (isNullAwareAntiJoin(joinType_)) {
    numPassed = evalFilterForNullAwareAntiJoin(numRows, filterPropagateNulls);
  } else if (isAntiJoin(joinType_)) {
    auto addMiss = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      auto probeRow = rawOutputProbeRowMapping[i];
      bool passed = nonNullInputRows_.isValid(probeRow) &&
          !decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i);
      noMatchDetector_.advance(probeRow, passed, addMiss);
    }
    if (results_.atEnd()) {
      noMatchDetector_.finish(addMiss);
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      if (!decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i)) {
        outputTableRows_[numPassed] = outputTableRows_[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }
  }
  return numPassed;
}

void HashProbe::ensureLoadedIfNotAtEnd(column_index_t channel) {
  if (isLeftSemiJoin(joinType_) || isAntiJoins(joinType_) || results_.atEnd()) {
    return;
  }

  if (!passingInputRowsInitialized_) {
    passingInputRowsInitialized_ = true;
    passingInputRows_.resize(input_->size());
    if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
      passingInputRows_.setAll();
    } else {
      passingInputRows_.clearAll();
      auto hitsSize = lookup_->hits.size();
      auto hits = lookup_->hits.data();
      for (auto i = 0; i < hitsSize; ++i) {
        if (hits[i]) {
          passingInputRows_.setValid(i, true);
        }
      }
    }
    passingInputRows_.updateBounds();
  }

  LazyVector::ensureLoadedRows(input_->childAt(channel), passingInputRows_);
}

void HashProbe::noMoreInput() {
  Operator::noMoreInput();
  noMoreInputInternal();
}

bool HashProbe::hasMoreInput() const {
  return !noMoreInput_ || (spillInputReader_ != nullptr && !noMoreSpillInput_);
}

void HashProbe::noMoreInputInternal() {
  checkRunning();

  noMoreSpillInput_ = true;
  if (!spillInputPartitionIds_.empty()) {
    VELOX_CHECK_EQ(
        spillInputPartitionIds_.size(), spiller_->spilledPartitionSet().size());
    spiller_->finishSpill(spillPartitionSet_);
  }

  // Setup spill partition data.
  const bool hasSpillData = hasMoreSpillData();
  if (!needLastProbe() && !hasSpillData) {
    return;
  }

  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  // The last operator to finish processing inputs is responsible for producing
  // build-side rows based on the join.
  ContinueFuture future;
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(),
          operatorCtx_->driver(),
          hasSpillData ? &future_ : &future,
          hasSpillData ? promises_ : promises,
          peers)) {
    if (hasSpillData) {
      VELOX_CHECK(future_.valid());
      setState(State::kWaitForPeers);
    }
    return;
  }

  lastProber_ = true;
}

bool HashProbe::isFinished() {
  return state_ == State::kFinish;
}

bool HashProbe::isRunning() const {
  return state_ == State::kRunning;
}

void HashProbe::checkRunning() const {
  VELOX_CHECK(isRunning(), stateName(state_));
}

void HashProbe::setRunning() {
  setState(State::kRunning);
}

std::string HashProbe::stateName(State state) {
  switch (state) {
    case State::kWaitForBuild:
      return "WAIT_FOR_BUILD";
    case State::kRunning:
      return "RUNNING";
    case State::kWaitForPeers:
      return "WAIT_FOR_PEERS";
    case State::kFinish:
      return "FINISH";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(state));
  }
}

} // namespace facebook::velox::exec
