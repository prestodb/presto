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
    const std::vector<TypePtr>& resultTypes,
    std::vector<VectorPtr>& resultVectors) {
  VELOX_CHECK_EQ(resultTypes.size(), resultVectors.size())
  for (auto projection : projections) {
    const auto resultChannel = projection.outputChannel;
    VELOX_CHECK_LT(resultChannel, resultVectors.size())

    auto& child = resultVectors[resultChannel];
    // TODO: Consider reuse of complex types.
    if (!child || !BaseVector::isVectorWritable(child) ||
        !child->isFlatEncoding()) {
      child = BaseVector::create(resultTypes[resultChannel], rows.size(), pool);
    }
    child->resize(rows.size());
    table->rows()->extractColumn(
        rows.data(), rows.size(), projection.inputChannel, child);
  }
}

BlockingReason fromStateToBlockingReason(ProbeOperatorState state) {
  switch (state) {
    case ProbeOperatorState::kRunning:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      return BlockingReason::kNotBlocked;
    case ProbeOperatorState::kWaitForBuild:
      return BlockingReason::kWaitForJoinBuild;
    case ProbeOperatorState::kWaitForPeers:
      return BlockingReason::kWaitForJoinProbe;
    default:
      VELOX_UNREACHABLE("Unexpected state: ", probeOperatorStateName(state));
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
          "HashProbe",
          joinNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      outputBatchSize_{outputBatchRows()},
      joinNode_(std::move(joinNode)),
      joinType_{joinNode_->joinType()},
      nullAware_{joinNode_->isNullAware()},
      probeType_(joinNode_->sources()[0]->outputType()),
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      filterResult_(1),
      outputTableRows_(outputBatchSize_) {
  VELOX_CHECK_NOT_NULL(joinBridge_);
}

void HashProbe::initialize() {
  Operator::initialize();

  VELOX_CHECK(hashers_.empty());
  hashers_ = createVectorHashers(probeType_, joinNode_->leftKeys());

  const auto numKeys = hashers_.size();
  keyChannels_.reserve(numKeys);
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }

  VELOX_CHECK_NULL(lookup_);
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

  if (nullAware_) {
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
    spillInputReader_ = partition->createUnorderedReader(pool());
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
              spillConfig.joinPartitionBits),
      &spillConfig,
      spillConfig.maxFileSize);
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
    setState(ProbeOperatorState::kWaitForBuild);
    return;
  }

  if (hashBuildResult->hasNullKeys) {
    VELOX_CHECK(nullAware_);
    if (isAntiJoin(joinType_) && !joinNode_->filter()) {
      // Null-aware anti join with null keys on the build side without a filter
      // always returns nothing.
      // The flag must be set on the first (and only) built 'table_'.
      VELOX_CHECK(spillPartitionSet_.empty());
      noMoreInput();
      return;
    }
    buildSideHasNullKeys_ = true;
  }

  table_ = std::move(hashBuildResult->table);
  VELOX_CHECK_NOT_NULL(table_);

  maybeSetupSpillInput(
      hashBuildResult->restoredPartitionId, hashBuildResult->spillPartitionIds);

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      if (!needSpillInput()) {
        if (isSpillInput() ||
            operatorCtx_->driverCtx()
                ->queryConfig()
                .hashProbeFinishEarlyOnEmptyBuild()) {
          noMoreInput();
        } else {
          skipInput_ = true;
        }
      }
    }
  } else if (
      (isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
       isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_)) &&
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

    // Null aware Right Semi Project join needs to know whether there are any
    // nulls on the probe side. Hence, cannot filter these out.
    const auto nullAllowed = isRightSemiProjectJoin(joinType_) && nullAware_;

    for (auto i = 0; i < keyChannels_.size(); i++) {
      if (channels.find(keyChannels_[i]) != channels.end()) {
        if (auto filter = buildHashers[i]->getFilter(nullAllowed)) {
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
  const auto singlePartition =
      spillHashFunction_->partition(*input, spillPartitions_);

  vector_size_t numNonSpillingInput = 0;
  for (auto row = 0; row < numInput; ++row) {
    const auto partition = singlePartition.has_value() ? singlePartition.value()
                                                       : spillPartitions_[row];
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
  for (int32_t i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
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
    case ProbeOperatorState::kWaitForBuild:
      VELOX_CHECK_NULL(table_);
      if (!future_.valid()) {
        setRunning();
        asyncWaitForHashTable();
      }
      break;
    case ProbeOperatorState::kRunning:
      VELOX_CHECK_NOT_NULL(table_);
      if (spillInputReader_ != nullptr) {
        addSpillInput();
      }
      break;
    case ProbeOperatorState::kWaitForPeers:
      VELOX_CHECK(hasMoreSpillData());
      if (!future_.valid()) {
        setRunning();
      }
      break;
    case ProbeOperatorState::kFinish:
      break;
    default:
      VELOX_UNREACHABLE(probeOperatorStateName(state_));
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

void HashProbe::decodeAndDetectNonNullKeys() {
  nonNullInputRows_.resize(input_->size());
  nonNullInputRows_.setAll();

  for (auto i = 0; i < hashers_.size(); ++i) {
    auto key = input_->childAt(hashers_[i]->channel())->loadedVector();
    hashers_[i]->decode(*key, nonNullInputRows_);
  }

  deselectRowsWithNulls(hashers_, nonNullInputRows_);
  if (isRightSemiProjectJoin(joinType_) &&
      nonNullInputRows_.countSelected() < input_->size()) {
    probeSideHasNullKeys_ = true;
  }
}

void HashProbe::addInput(RowVectorPtr input) {
  if (skipInput_) {
    VELOX_CHECK_NULL(input_);
    return;
  }
  input_ = std::move(input);

  const auto numInput = input_->size();

  if (numInput > 0) {
    noInput_ = false;
  }

  if (canReplaceWithDynamicFilter_) {
    replacedWithDynamicFilter_ = true;
    return;
  }

  bool hasDecoded = false;

  if (needSpillInput()) {
    if (isRightSemiProjectJoin(joinType_) && !probeSideHasNullKeys_) {
      decodeAndDetectNonNullKeys();
      hasDecoded = true;
    }

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
        isAntiJoin(joinType_) || isLeftJoin(joinType_) ||
        isFullJoin(joinType_) || isLeftSemiProjectJoin(joinType_));
    if (isLeftSemiProjectJoin(joinType_) ||
        (isAntiJoin(joinType_) && filter_)) {
      // For anti join with filter and semi project join we need to decode the
      // join keys columns to initialize 'nonNullInputRows_'. The anti join
      // filter evaluation and semi project join output generation will access
      // 'nonNullInputRows_' later.
      decodeAndDetectNonNullKeys();
    }
    return;
  }

  if (!hasDecoded) {
    decodeAndDetectNonNullKeys();
  }
  activeRows_ = nonNullInputRows_;

  // Update statistics for null keys in join operator.
  // Updating here means we will report 0 null keys when build side is empty.
  // If we want more accurate stats, we will have to decode input vector
  // even when not needed. So we tradeoff less accurate stats for more
  // performance.
  {
    auto lockedStats = stats_.wlock();
    lockedStats->numNullKeys +=
        activeRows_.size() - activeRows_.countSelected();
  }

  table_->prepareForJoinProbe(*lookup_.get(), input_, activeRows_, false);

  passingInputRowsInitialized_ = false;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_) || isAntiJoin(joinType_) ||
      isLeftSemiProjectJoin(joinType_)) {
    // Make sure to allocate an entry in 'hits' for every input row to allow for
    // including rows without a match in the output. Also, make sure to
    // initialize all 'hits' to nullptr as HashTable::joinProbe will only
    // process activeRows_.
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
    output_ = BaseVector::create<RowVector>(outputType_, size, pool());
  }
}

namespace {
VectorPtr createConstantFalse(vector_size_t size, memory::MemoryPool* pool) {
  return std::make_shared<ConstantVector<bool>>(
      pool, size, false /*isNull*/, BOOLEAN(), false /*value*/);
}
} // namespace

void HashProbe::fillLeftSemiProjectMatchColumn(vector_size_t size) {
  if (emptyBuildSide()) {
    // Build side is empty or all rows have null join keys.
    if (nullAware_ && buildSideHasNullKeys_) {
      matchColumn() = BaseVector::createNullConstant(BOOLEAN(), size, pool());
    } else {
      matchColumn() = createConstantFalse(size, pool());
    }
  } else {
    auto flatMatch = matchColumn()->as<FlatVector<bool>>();
    flatMatch->resize(size);
    auto rawValues = flatMatch->mutableRawValues<uint64_t>();
    for (auto i = 0; i < size; ++i) {
      if (nullAware_) {
        // Null-aware join may produce TRUE, FALSE or NULL.
        if (filter_) {
          if (leftSemiProjectIsNull_.isValid(i)) {
            flatMatch->setNull(i, true);
          } else {
            bool hasMatch = outputTableRows_[i] != nullptr;
            bits::setBit(rawValues, i, hasMatch);
          }
        } else {
          if (!nonNullInputRows_.isValid(i)) {
            // Probe key is null.
            flatMatch->setNull(i, true);
          } else {
            // Probe key is not null.
            bool hasMatch = outputTableRows_[i] != nullptr;
            if (!hasMatch && buildSideHasNullKeys_) {
              flatMatch->setNull(i, true);
            } else {
              bits::setBit(rawValues, i, hasMatch);
            }
          }
        }
      } else {
        bool hasMatch = outputTableRows_[i] != nullptr;
        bits::setBit(rawValues, i, hasMatch);
      }
    }
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

  if (isLeftSemiProjectJoin(joinType_)) {
    fillLeftSemiProjectMatchColumn(size);
  } else {
    extractColumns(
        table_.get(),
        folly::Range<char**>(outputTableRows_.data(), size),
        tableOutputProjections_,
        pool(),
        outputType_->children(),
        output_->children());
  }
}

RowVectorPtr HashProbe::getBuildSideOutput() {
  outputTableRows_.resize(outputBatchSize_);
  int32_t numOut;
  if (isRightSemiFilterJoin(joinType_)) {
    numOut = table_->listProbedRows(
        &lastProbeIterator_,
        outputBatchSize_,
        RowContainer::kUnlimited,
        outputTableRows_.data());
  } else if (isRightSemiProjectJoin(joinType_)) {
    numOut = table_->listAllRows(
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
      outputType_->children(),
      output_->children());

  if (isRightSemiProjectJoin(joinType_)) {
    // Populate 'match' column.
    if (noInput_) {
      // Probe side is empty. All rows should return 'match = false', even ones
      // with a null join key.
      matchColumn() = createConstantFalse(numOut, pool());
    } else {
      table_->rows()->extractProbedFlags(
          outputTableRows_.data(),
          numOut,
          nullAware_,
          nullAware_ && probeSideHasNullKeys_,
          matchColumn());
    }
  }

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
  return !skipInput_ &&
      (isRightJoin(joinType_) || isFullJoin(joinType_) ||
       isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_));
}

bool HashProbe::skipProbeOnEmptyBuild() const {
  return isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
      isRightJoin(joinType_) || isRightSemiFilterJoin(joinType_) ||
      isRightSemiProjectJoin(joinType_);
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

void HashProbe::setState(ProbeOperatorState state) {
  checkStateTransition(state);
  state_ = state;
}

void HashProbe::checkStateTransition(ProbeOperatorState state) {
  VELOX_CHECK_NE(state_, state);
  switch (state) {
    case ProbeOperatorState::kRunning:
      if (!hasMoreSpillData()) {
        VELOX_CHECK_EQ(state_, ProbeOperatorState::kWaitForBuild);
      } else {
        VELOX_CHECK(
            state_ == ProbeOperatorState::kWaitForBuild ||
            state_ == ProbeOperatorState::kWaitForPeers)
      }
      break;
    case ProbeOperatorState::kWaitForPeers:
      VELOX_CHECK(hasMoreSpillData());
      [[fallthrough]];
    case ProbeOperatorState::kWaitForBuild:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      VELOX_CHECK_EQ(state_, ProbeOperatorState::kRunning);
      break;
    default:
      VELOX_UNREACHABLE(probeOperatorStateName(state_));
      break;
  }
}

RowVectorPtr HashProbe::getOutput() {
  if (isFinished()) {
    return nullptr;
  }
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
        setState(ProbeOperatorState::kFinish);
      }
      return nullptr;
    }
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (replacedWithDynamicFilter_) {
    addRuntimeStat("replacedWithDynamicFilterRows", RuntimeCounter(inputSize));
    auto output = Operator::fillOutput(inputSize, nullptr);
    input_ = nullptr;
    return output;
  }

  const bool isLeftSemiOrAntiJoinNoFilter = !filter_ &&
      (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
       isAntiJoin(joinType_));

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
    } else if (isAntiJoin(joinType_) && !filter_) {
      if (nullAware_) {
        // When build side is not empty, anti join without a filter returns
        // probe rows with no nulls in the join key and no match in the build
        // side.
        for (auto i = 0; i < inputSize; ++i) {
          if (nonNullInputRows_.isValid(i) &&
              (!activeRows_.isValid(i) || !lookup_->hits[i])) {
            mapping[numOut] = i;
            ++numOut;
          }
        }
      } else {
        for (auto i = 0; i < inputSize; ++i) {
          if (!nonNullInputRows_.isValid(i) ||
              (!activeRows_.isValid(i) || !lookup_->hits[i])) {
            mapping[numOut] = i;
            ++numOut;
          }
        }
      }
    } else {
      numOut = table_->listJoinResults(
          results_,
          isLeftJoin(joinType_) || isFullJoin(joinType_) ||
              isAntiJoin(joinType_) || isLeftSemiProjectJoin(joinType_),
          mapping,
          folly::Range(outputTableRows_.data(), outputTableRows_.size()));
    }

    // We are done processing the input batch if there are no more joined rows
    // to process and the NoMatchDetector isn't carrying forward a row that
    // still needs to be written to the output.
    if (!numOut && !noMatchDetector_.hasLastMissedRow()) {
      input_ = nullptr;
      return nullptr;
    }
    VELOX_CHECK_LE(numOut, outputTableRows_.size());

    numOut = evalFilter(numOut);

    if (!numOut) {
      continue;
    }

    if (needLastProbe()) {
      // Mark build-side rows that have a match on the join condition.
      table_->rows()->setProbedFlag(outputTableRows_.data(), numOut);
    }

    // Right semi join only returns the build side output when the probe side
    // is fully complete. Do not return anything here.
    if (isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_)) {
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
  std::vector<VectorPtr> filterColumns(filterInputType_->size());
  for (auto projection : filterInputProjections_) {
    ensureLoadedIfNotAtEnd(projection.inputChannel);
    filterColumns[projection.outputChannel] = wrapChild(
        size, outputRowMapping_, input_->childAt(projection.inputChannel));
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows_.data(), size),
      filterTableProjections_,
      pool(),
      filterInputType_->children(),
      filterColumns);

  filterInput_ = std::make_shared<RowVector>(
      pool(), filterInputType_, nullptr, size, std::move(filterColumns));
}

void HashProbe::prepareFilterRowsForNullAwareJoin(
    vector_size_t numRows,
    bool filterPropagateNulls) {
  VELOX_CHECK_LE(numRows, kBatchSize);
  if (filterTableInput_ == nullptr) {
    filterTableInput_ =
        BaseVector::create<RowVector>(filterInputType_, kBatchSize, pool());
  }

  if (filterPropagateNulls) {
    nullFilterInputRows_.resizeFill(numRows, false);
    auto* rawNullRows = nullFilterInputRows_.asMutableRange().bits();
    for (auto& projection : filterInputProjections_) {
      filterInputColumnDecodedVector_.decode(
          *filterInput_->childAt(projection.outputChannel), filterInputRows_);
      if (filterInputColumnDecodedVector_.mayHaveNulls()) {
        SelectivityVector nullsInActiveRows(numRows);
        memcpy(
            nullsInActiveRows.asMutableRange().bits(),
            filterInputColumnDecodedVector_.nulls(&filterInputRows_),
            bits::nbytes(numRows));
        // All rows that are not active count as non-null here.
        bits::orWithNegatedBits(
            nullsInActiveRows.asMutableRange().bits(),
            filterInputRows_.asRange().bits(),
            0,
            numRows);
        // NOTE: the false value of a raw null bit indicates null so we OR with
        // negative of the raw bit.
        bits::orWithNegatedBits(
            rawNullRows, nullsInActiveRows.asRange().bits(), 0, numRows);
      }
    }
    nullFilterInputRows_.updateBounds();
    // TODO: consider to skip filtering on 'nullFilterInputRows_' as we know it
    // will never pass the filtering.
  }

  // NOTE: for null-aware anti join, we will skip filtering on the probe rows
  // with null join key columns(s) as we can apply filtering after they cross
  // join with the table rows later.
  if (!nonNullInputRows_.isAllSelected()) {
    auto* rawMapping = outputRowMapping_->asMutable<vector_size_t>();
    for (int i = 0; i < numRows; ++i) {
      if (filterInputRows_.isValid(i) &&
          !nonNullInputRows_.isValid(rawMapping[i])) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }
}

namespace {

const uint64_t* getFlatFilterResult(VectorPtr& result) {
  if (!result->isFlatEncoding()) {
    return nullptr;
  }
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  if (!flat->mayHaveNulls()) {
    return flat->rawValues<uint64_t>();
  }
  if (!flat->rawValues<uint64_t>()) {
    return flat->rawNulls();
  }
  if (!result.unique()) {
    return nullptr;
  }
  auto* values = flat->mutableRawValues<uint64_t>();
  bits::andBits(values, flat->rawNulls(), 0, flat->size());
  return values;
}

} // namespace

void HashProbe::applyFilterOnTableRowsForNullAwareJoin(
    const SelectivityVector& rows,
    SelectivityVector& filterPassedRows,
    std::function<int32_t(char**, int32_t)> iterator) {
  if (!rows.hasSelections()) {
    return;
  }
  auto* tableRows = table_->rows();
  VELOX_CHECK(tableRows, "Should not move rows in hash joins");
  char* data[kBatchSize];
  while (auto numRows = iterator(data, kBatchSize)) {
    filterTableInput_->resize(numRows);
    filterTableInputRows_.resizeFill(numRows, true);
    for (auto& projection : filterTableProjections_) {
      tableRows->extractColumn(
          data,
          numRows,
          projection.inputChannel,
          filterTableInput_->childAt(projection.outputChannel));
    }
    rows.applyToSelected([&](vector_size_t row) {
      for (auto& projection : filterInputProjections_) {
        filterTableInput_->childAt(projection.outputChannel) =
            BaseVector::wrapInConstant(
                numRows, row, input_->childAt(projection.inputChannel));
      }
      EvalCtx evalCtx(
          operatorCtx_->execCtx(), filter_.get(), filterTableInput_.get());
      filter_->eval(filterTableInputRows_, evalCtx, filterTableResult_);
      if (auto* values = getFlatFilterResult(filterTableResult_[0])) {
        if (!bits::testSetBits(
                values, 0, numRows, [](vector_size_t) { return false; })) {
          filterPassedRows.setValid(row, true);
        }
      } else {
        decodedFilterTableResult_.decode(
            *filterTableResult_[0], filterTableInputRows_);
        if (decodedFilterTableResult_.isConstantMapping()) {
          if (!decodedFilterTableResult_.isNullAt(0) &&
              decodedFilterTableResult_.valueAt<bool>(0)) {
            filterPassedRows.setValid(row, true);
          }
        } else {
          for (vector_size_t i = 0; i < numRows; ++i) {
            if (!decodedFilterTableResult_.isNullAt(i) &&
                decodedFilterTableResult_.valueAt<bool>(i)) {
              filterPassedRows.setValid(row, true);
              break;
            }
          }
        }
      }
    });
  }
}

SelectivityVector HashProbe::evalFilterForNullAwareJoin(
    vector_size_t numRows,
    bool filterPropagateNulls) {
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  // Subset of probe-side rows with a match that passed the filter.
  SelectivityVector filterPassedRows(input_->size(), false);

  // Subset of probe-side rows with non-null probe key and either no match or no
  // match that passed the filter. We need to combine these with all build-side
  // rows with null keys to see if a filter passes on any of these.
  SelectivityVector nullKeyProbeRows(input_->size(), false);

  // Subset of probe-sie rows with null probe key. We need to combine these with
  // all build-side rows to see if a filter passes on any of these.
  SelectivityVector crossJoinProbeRows(input_->size(), false);

  for (auto i = 0; i < numRows; ++i) {
    // Skip filter input row if it has any null probe side filter column.
    if (filterPropagateNulls && nullFilterInputRows_.isValid(i)) {
      continue;
    }

    const auto probeRow = rawOutputProbeRowMapping[i];
    if (nonNullInputRows_.isValid(probeRow)) {
      if (filterPassed(i)) {
        filterPassedRows.setValid(probeRow, true);
      } else {
        nullKeyProbeRows.setValid(probeRow, true);
      }
    } else {
      crossJoinProbeRows.setValid(probeRow, true);
    }
  }

  if (buildSideHasNullKeys_) {
    BaseHashTable::NullKeyRowsIterator iter;
    nullKeyProbeRows.deselect(filterPassedRows);
    applyFilterOnTableRowsForNullAwareJoin(
        nullKeyProbeRows, filterPassedRows, [&](char** data, int32_t maxRows) {
          return table_->listNullKeyRows(&iter, maxRows, data);
        });
  }
  BaseHashTable::RowsIterator iter;
  crossJoinProbeRows.deselect(filterPassedRows);
  applyFilterOnTableRowsForNullAwareJoin(
      crossJoinProbeRows, filterPassedRows, [&](char** data, int32_t maxRows) {
        return table_->listAllRows(
            &iter, maxRows, RowContainer::kUnlimited, data);
      });
  filterPassedRows.updateBounds();

  return filterPassedRows;
}

int32_t HashProbe::evalFilter(int32_t numRows) {
  if (!filter_) {
    return numRows;
  }

  const bool filterPropagateNulls = filter_->expr(0)->propagatesNulls();
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  filterInputRows_.resizeFill(numRows);

  // Do not evaluate filter on rows with no match to (1) avoid
  // false-positives when filter evaluates to true for rows with NULLs on the
  // build side; (2) avoid errors in filter evaluation that would fail the query
  // unnecessarily.
  // TODO Apply the same to left joins.
  if (isAntiJoin(joinType_) || isLeftSemiProjectJoin(joinType_)) {
    for (auto i = 0; i < numRows; ++i) {
      if (outputTableRows_[i] == nullptr) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }

  fillFilterInput(numRows);

  if (nullAware_) {
    prepareFilterRowsForNullAwareJoin(numRows, filterPropagateNulls);
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
      const bool passed = filterPassed(i);
      noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
      if (passed) {
        outputTableRows_[numPassed] = outputTableRows_[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }

    noMatchDetector_.finishIteration(
        addMiss, results_.atEnd(), outputTableRows_.size() - numPassed);
  } else if (isLeftSemiFilterJoin(joinType_)) {
    auto addLastMatch = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        leftSemiFilterJoinTracker_.advance(
            rawOutputProbeRowMapping[i], addLastMatch);
      }
    }
    if (results_.atEnd()) {
      leftSemiFilterJoinTracker_.finish(addLastMatch);
    }
  } else if (isLeftSemiProjectJoin(joinType_)) {
    // NOTE: Set output table row to point to a fake string to indicate there
    // is a match for this probe 'row'. 'fillOutput' populates the match
    // column based on the nullable of this pointer.
    static const char* kPassed = "passed";

    if (nullAware_) {
      leftSemiProjectIsNull_.resize(numRows);
      leftSemiProjectIsNull_.clearAll();

      auto addLast = [&](auto row, std::optional<bool> passed) {
        if (passed.has_value()) {
          outputTableRows_[numPassed] =
              passed.value() ? const_cast<char*>(kPassed) : nullptr;
        } else {
          leftSemiProjectIsNull_.setValid(numPassed, true);
        }
        rawOutputProbeRowMapping[numPassed++] = row;
      };

      auto passedRows =
          evalFilterForNullAwareJoin(numRows, filterPropagateNulls);
      for (auto i = 0; i < numRows; ++i) {
        // filterPassed(i) -> TRUE
        // else passed -> NULL
        // else FALSE
        auto probeRow = rawOutputProbeRowMapping[i];
        std::optional<bool> passed = filterPassed(i)
            ? std::optional(true)
            : (passedRows.isValid(probeRow) ? std::nullopt
                                            : std::optional(false));
        leftSemiProjectJoinTracker_.advance(probeRow, passed, addLast);
      }
      leftSemiProjectIsNull_.updateBounds();
      if (results_.atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    } else {
      auto addLast = [&](auto row, std::optional<bool> passed) {
        outputTableRows_[numPassed] =
            passed.value() ? const_cast<char*>(kPassed) : nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        leftSemiProjectJoinTracker_.advance(
            rawOutputProbeRowMapping[i], filterPassed(i), addLast);
      }
      if (results_.atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    }
  } else if (isAntiJoin(joinType_)) {
    auto addMiss = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    if (nullAware_) {
      auto passedRows =
          evalFilterForNullAwareJoin(numRows, filterPropagateNulls);
      for (auto i = 0; i < numRows; ++i) {
        auto probeRow = rawOutputProbeRowMapping[i];
        bool passed = passedRows.isValid(probeRow);
        noMatchDetector_.advance(probeRow, passed, addMiss);
      }
    } else {
      for (auto i = 0; i < numRows; ++i) {
        auto probeRow = rawOutputProbeRowMapping[i];
        noMatchDetector_.advance(probeRow, filterPassed(i), addMiss);
      }
    }

    noMatchDetector_.finishIteration(
        addMiss, results_.atEnd(), outputTableRows_.size() - numPassed);
  } else {
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        outputTableRows_[numPassed] = outputTableRows_[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }
  }
  return numPassed;
}

void HashProbe::ensureLoadedIfNotAtEnd(column_index_t channel) {
  if ((!filter_ &&
       (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
        isAntiJoin(joinType_))) ||
      results_.atEnd()) {
    return;
  }

  if (!passingInputRowsInitialized_) {
    passingInputRowsInitialized_ = true;
    passingInputRows_.resize(input_->size());
    if (isLeftJoin(joinType_) || isFullJoin(joinType_) ||
        isLeftSemiProjectJoin(joinType_)) {
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
    recordSpillStats();
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
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(),
          operatorCtx_->driver(),
          hasSpillData ? &future_ : nullptr,
          hasSpillData ? promises_ : promises,
          peers)) {
    if (hasSpillData) {
      VELOX_CHECK(future_.valid());
      setState(ProbeOperatorState::kWaitForPeers);
    }
    DCHECK(promises.empty());
    return;
  }
  // NOTE: if 'hasSpillData' is false, then this is the last built table to
  // probe. Correspondingly, the hash probe operator except the last one can
  // simply finish its processing without waiting the other peers to reach the
  // barrier.
  VELOX_CHECK(promises.empty());
  VELOX_CHECK(hasSpillData || peers.empty());
  lastProber_ = true;
}

void HashProbe::recordSpillStats() {
  VELOX_CHECK_NOT_NULL(spiller_);
  const auto spillStats = spiller_->stats();
  VELOX_CHECK_EQ(spillStats.spillSortTimeUs, 0);
  VELOX_CHECK_EQ(spillStats.spillFillTimeUs, 0);
  Operator::recordSpillStats(spillStats);
}

bool HashProbe::isFinished() {
  return state_ == ProbeOperatorState::kFinish;
}

bool HashProbe::isRunning() const {
  return state_ == ProbeOperatorState::kRunning;
}

void HashProbe::checkRunning() const {
  VELOX_CHECK(isRunning(), probeOperatorStateName(state_));
}

void HashProbe::setRunning() {
  setState(ProbeOperatorState::kRunning);
}

void HashProbe::close() {
  Operator::close();

  // Free up major memory usage.
  joinBridge_.reset();
  spiller_.reset();
  table_.reset();
  outputRowMapping_.reset();
  output_.reset();
  nonSpillInputIndicesBuffer_.reset();
  spillInputIndicesBuffers_.clear();
  spillInputReader_.reset();
}

} // namespace facebook::velox::exec
