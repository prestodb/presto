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
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

using facebook::velox::common::testutil::TestValue;

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
    folly::Range<char* const*> rows,
    folly::Range<const IdentityProjection*> projections,
    memory::MemoryPool* pool,
    const std::vector<TypePtr>& resultTypes,
    std::vector<VectorPtr>& resultVectors) {
  VELOX_CHECK_EQ(resultTypes.size(), resultVectors.size());
  for (auto projection : projections) {
    const auto resultChannel = projection.outputChannel;
    VELOX_CHECK_LT(resultChannel, resultVectors.size());

    auto& child = resultVectors[resultChannel];
    // TODO: Consider reuse of complex types.
    if (!child || !BaseVector::isVectorWritable(child) ||
        !child->isFlatEncoding()) {
      child = BaseVector::create(resultTypes[resultChannel], rows.size(), pool);
    }
    child->resize(rows.size());
    table->extractColumn(rows, projection.inputChannel, child);
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

template <typename T>
T* initBuffer(BufferPtr& buffer, vector_size_t size, memory::MemoryPool* pool) {
  VELOX_CHECK(!buffer || buffer->isMutable());
  if (!buffer || buffer->size() < size * sizeof(T)) {
    buffer = AlignedBuffer::allocate<T>(size, pool);
  }
  return buffer->asMutable<T>();
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
      outputTableRowsCapacity_(outputBatchSize_) {
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
      projectedInputColumns_.insert(i);
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

void HashProbe::maybeSetupInputSpiller(
    const SpillPartitionIdSet& spillPartitionIds) {
  VELOX_CHECK_NULL(inputSpiller_);
  VELOX_CHECK(spillInputPartitionIds_.empty());

  spillInputPartitionIds_ = spillPartitionIds;
  if (spillInputPartitionIds_.empty()) {
    return;
  }

  // If 'spillInputPartitionIds_' is not empty, then we set up a spiller to
  // spill the incoming probe inputs.
  inputSpiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinProbe,
      probeType_,
      HashBitRange(
          spillInputPartitionIds_.begin()->partitionBitOffset(),
          spillInputPartitionIds_.begin()->partitionBitOffset() +
              spillConfig()->numPartitionBits),
      spillConfig(),
      &spillStats_);
  // Set the spill partitions to the corresponding ones at the build side. The
  // hash probe operator itself won't trigger any spilling.
  inputSpiller_->setPartitionsSpilled(
      toPartitionNumSet(spillInputPartitionIds_));

  spillHashFunction_ = std::make_unique<HashPartitionFunction>(
      inputSpiller_->hashBits(), probeType_, keyChannels_);
  spillInputIndicesBuffers_.resize(spillHashFunction_->numPartitions());
  rawSpillInputIndicesBuffers_.resize(spillHashFunction_->numPartitions());
  numSpillInputs_.resize(spillHashFunction_->numPartitions(), 0);
  // If we have received no more input signal from either source or restored
  // spill input, then we shall just finish the spiller and records the spilled
  // partition set accordingly.
  if (noMoreSpillInput_) {
    inputSpiller_->finishSpill(spillPartitionSet_);
  }
}

void HashProbe::maybeSetupSpillInputReader(
    const std::optional<SpillPartitionId>& restoredPartitionId) {
  VELOX_CHECK_NULL(spillInputReader_);
  if (!restoredPartitionId.has_value()) {
    return;
  }
  // If 'restoredPartitionId' is not null, then 'table_' is built from the
  // spilled build data. Create an unsorted reader to read the probe inputs from
  // the corresponding spilled probe partition on disk.
  auto iter = spillPartitionSet_.find(restoredPartitionId.value());
  VELOX_CHECK(iter != spillPartitionSet_.end());
  auto partition = std::move(iter->second);
  VELOX_CHECK_EQ(partition->id(), restoredPartitionId.value());
  spillInputReader_ = partition->createUnorderedReader(
      spillConfig_->readBufferSize, pool(), &spillStats_);
  spillPartitionSet_.erase(iter);
}

void HashProbe::initializeResultIter() {
  VELOX_CHECK_NOT_NULL(table_);
  if (resultIter_ != nullptr) {
    return;
  }
  std::vector<vector_size_t> listColumns;
  listColumns.reserve(tableOutputProjections_.size());
  for (const auto& projection : tableOutputProjections_) {
    listColumns.push_back(projection.inputChannel);
  }
  std::vector<vector_size_t> varSizeListColumns;
  uint64_t fixedSizeListColumnsSizeSum{0};
  varSizeListColumns.reserve(tableOutputProjections_.size());
  for (const auto column : listColumns) {
    if (table_->rows()->columnTypes()[column]->isFixedWidth()) {
      fixedSizeListColumnsSizeSum += table_->rows()->fixedSizeAt(column);
    } else {
      varSizeListColumns.push_back(column);
    }
  }
  resultIter_ = std::make_unique<BaseHashTable::JoinResultIterator>(
      std::move(varSizeListColumns), fixedSizeListColumnsSizeSum);
}

void HashProbe::asyncWaitForHashTable() {
  checkRunning();
  VELOX_CHECK_NULL(table_);

  // Release any reserved memory before wait for next round of hash join in case
  // of disk spilling has been triggered.
  pool()->release();

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
  initializeResultIter();

  VELOX_CHECK_NOT_NULL(table_);

  maybeSetupSpillInputReader(hashBuildResult->restoredPartitionId);
  maybeSetupInputSpiller(hashBuildResult->spillPartitionIds);
  prepareTableSpill(hashBuildResult->restoredPartitionId);

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
    // Find out whether there are any upstream operators that can accept dynamic
    // filters on all or a subset of the join keys. Create dynamic filters to
    // push down.
    //
    // NOTE: this optimization is not applied in the following cases: (1) if the
    // probe input is read from spilled data and there is no upstream operators
    // involved; (2) if there is spill data to restore, then we can't filter
    // probe inputs solely based on the current table's join keys.
    const auto& buildHashers = table_->hashers();
    const auto channels = operatorCtx_->driverCtx()->driver->canPushdownFilters(
        this, keyChannels_);

    // Null aware Right Semi Project join needs to know whether there are any
    // nulls on the probe side. Hence, cannot filter these out.
    const auto nullAllowed = isRightSemiProjectJoin(joinType_) && nullAware_;

    for (auto i = 0; i < keyChannels_.size(); ++i) {
      if (channels.find(keyChannels_[i]) != channels.end()) {
        if (auto filter = buildHashers[i]->getFilter(nullAllowed)) {
          dynamicFilters_.emplace(keyChannels_[i], std::move(filter));
        }
      }
    }
    hasGeneratedDynamicFilters_ = !dynamicFilters_.empty();
  }
}

bool HashProbe::isSpillInput() const {
  return spillInputReader_ != nullptr;
}

void HashProbe::prepareForSpillRestore() {
  checkRunning();
  VELOX_CHECK(canSpill());
  VELOX_CHECK(hasMoreSpillData());

  // Reset the internal states which are relevant to the previous probe run.
  noMoreSpillInput_ = false;
  if (lastProber_) {
    table_->clear(true);
  }
  table_.reset();
  inputSpiller_.reset();
  spillInputReader_.reset();
  spillInputPartitionIds_.clear();
  spillOutputReader_.reset();
  lastProbeIterator_.reset();

  VELOX_CHECK(promises_.empty() || lastProber_);
  if (!lastProber_) {
    return;
  }
  // Notify the hash build operators to build the next hash table.
  joinBridge_->probeFinished();

  wakeupPeerOperators();

  lastProber_ = false;
}

void HashProbe::wakeupPeerOperators() {
  VELOX_CHECK(lastProber_);
  auto promises = std::move(promises_);
  for (auto& promise : promises) {
    promise.setValue();
  }
}

std::vector<HashProbe*> HashProbe::findPeerOperators() {
  auto task = operatorCtx_->task();
  const std::vector<Operator*> operators =
      task->findPeerOperators(operatorCtx_->driverCtx()->pipelineId, this);
  std::vector<HashProbe*> probeOps;
  probeOps.reserve(operators.size());
  for (auto* op : operators) {
    auto* probeOp = dynamic_cast<HashProbe*>(op);
    probeOps.push_back(probeOp);
  }
  return probeOps;
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
      input->size(), inputSpiller_->state().spilledPartitionSet());
  const auto singlePartition =
      spillHashFunction_->partition(*input, spillPartitions_);

  vector_size_t numNonSpillingInput = 0;
  for (auto row = 0; row < numInput; ++row) {
    const auto partition = singlePartition.has_value() ? singlePartition.value()
                                                       : spillPartitions_[row];
    if (!inputSpiller_->isSpilled(partition)) {
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
    VELOX_CHECK(inputSpiller_->isSpilled(partition));
    inputSpiller_->spill(
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
  VELOX_DCHECK(canSpill());
  const auto maxIndicesBufferBytes = numInput * sizeof(vector_size_t);
  if (nonSpillInputIndicesBuffer_ == nullptr ||
      !nonSpillInputIndicesBuffer_->isMutable() ||
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
      VELOX_CHECK(canSpill());
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
  // The join can be completely replaced with a pushed down filter when the
  // following conditions are met:
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

  // Reset passingInputRowsInitialized_ as input_ as changed.
  passingInputRowsInitialized_ = false;

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
    VELOX_CHECK(joinIncludesMissesFromLeft(joinType_));
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

  if (joinIncludesMissesFromLeft(joinType_)) {
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

  resultIter_->reset(*lookup_);
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
    auto* outputTableRows = outputTableRows_->as<char*>();
    for (auto i = 0; i < size; ++i) {
      if (nullAware_) {
        // Null-aware join may produce TRUE, FALSE or NULL.
        if (filter_) {
          if (leftSemiProjectIsNull_.isValid(i)) {
            flatMatch->setNull(i, true);
          } else {
            const bool hasMatch = outputTableRows[i] != nullptr;
            bits::setBit(rawValues, i, hasMatch);
          }
        } else {
          if (!nonNullInputRows_.isValid(i)) {
            // Probe key is null.
            flatMatch->setNull(i, true);
          } else {
            // Probe key is not null.
            const bool hasMatch = outputTableRows[i] != nullptr;
            if (!hasMatch && buildSideHasNullKeys_) {
              flatMatch->setNull(i, true);
            } else {
              bits::setBit(rawValues, i, hasMatch);
            }
          }
        }
      } else {
        const bool hasMatch = outputTableRows[i] != nullptr;
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
        folly::Range<char* const*>(outputTableRows_->as<char*>(), size),
        tableOutputProjections_,
        pool(),
        outputType_->children(),
        output_->children());
  }
}

RowVectorPtr HashProbe::getBuildSideOutput() {
  auto* outputTableRows =
      initBuffer<char*>(outputTableRows_, outputTableRowsCapacity_, pool());
  int32_t numOut;
  if (isRightSemiFilterJoin(joinType_)) {
    numOut = table_->listProbedRows(
        &lastProbeIterator_,
        outputTableRowsCapacity_,
        RowContainer::kUnlimited,
        outputTableRows);
  } else if (isRightSemiProjectJoin(joinType_)) {
    numOut = table_->listAllRows(
        &lastProbeIterator_,
        outputTableRowsCapacity_,
        RowContainer::kUnlimited,
        outputTableRows);
  } else {
    // Must be a right join or full join.
    numOut = table_->listNotProbedRows(
        &lastProbeIterator_,
        outputTableRowsCapacity_,
        RowContainer::kUnlimited,
        outputTableRows);
  }
  if (numOut == 0) {
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
      folly::Range<char**>(outputTableRows, numOut),
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
          outputTableRows,
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
  return !skipInput_ && needRightSideJoin(joinType_);
}

bool HashProbe::skipProbeOnEmptyBuild() const {
  return isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
      isRightJoin(joinType_) || isRightSemiFilterJoin(joinType_) ||
      isRightSemiProjectJoin(joinType_);
}

bool HashProbe::canSpill() const {
  return Operator::canSpill() &&
      !operatorCtx_->task()->hasMixedExecutionGroup();
}

bool HashProbe::hasMoreSpillData() const {
  VELOX_CHECK(spillPartitionSet_.empty() || canSpill());
  return !spillPartitionSet_.empty() || needSpillInput();
}

bool HashProbe::needSpillInput() const {
  VELOX_CHECK(spillInputPartitionIds_.empty() || canSpill());
  VELOX_CHECK_EQ(spillInputPartitionIds_.empty(), inputSpiller_ == nullptr);

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
      if (!canSpill()) {
        VELOX_CHECK_EQ(state_, ProbeOperatorState::kWaitForBuild);
      } else {
        VELOX_CHECK(
            state_ == ProbeOperatorState::kWaitForBuild ||
            state_ == ProbeOperatorState::kWaitForPeers);
      }
      break;
    case ProbeOperatorState::kWaitForPeers:
      VELOX_CHECK(canSpill());
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
  // Release the extra unused memory reserved for output processing.
  SCOPE_EXIT {
    pool()->release();
  };
  return getOutputInternal(/*toSpillOutput=*/false);
}

RowVectorPtr HashProbe::getOutputInternal(bool toSpillOutput) {
  if (isFinished()) {
    return nullptr;
  }
  checkRunning();

  if (!toSpillOutput) {
    // Avoid memory reservation if it is triggered by memory arbitration to
    // spill pending output.
    ensureOutputFits();
  }

  if (maybeReadSpillOutput()) {
    return output_;
  }

  clearIdentityProjectedOutput();

  if (!input_) {
    if (hasMoreInput()) {
      return nullptr;
    }

    if (needLastProbe() && lastProber_) {
      auto output = getBuildSideOutput();
      if (output != nullptr) {
        return output;
      }
    }

    // NOTE: if getOutputInternal() is called from memory arbitration to spill
    // the produced output from pending 'input_', then we should not proceed
    // with the rest of procedure, and let the next driver getOutput() call to
    // handle the probe finishing process properly.
    if (toSpillOutput) {
      VELOX_CHECK(memory::underMemoryArbitration());
      VELOX_CHECK(canReclaim());
      return nullptr;
    }

    if (hasMoreSpillData()) {
      prepareForSpillRestore();
      asyncWaitForHashTable();
    } else {
      if (lastProber_ && canSpill()) {
        joinBridge_->probeFinished();
        wakeupPeerOperators();
      }
      setState(ProbeOperatorState::kFinish);
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

  // Left semi and anti joins are always cardinality reducing, e.g. for a
  // given row of input they produce zero or 1 row of output. Therefore, if
  // there is no extra filter we can process each batch of input in one go.
  auto outputBatchSize = (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide)
      ? inputSize
      : outputBatchSize_;
  outputTableRowsCapacity_ = outputBatchSize;
  if (filter_ &&
      (isLeftJoin(joinType_) || isFullJoin(joinType_) ||
       isAntiJoin(joinType_))) {
    // If we need non-matching probe side row, there is a possibility that such
    // row exists at end of an input batch and being carried over in the next
    // output batch, so we need to make extra room of one row in output.
    ++outputTableRowsCapacity_;
  }
  auto mapping = initializeRowNumberMapping(
      outputRowMapping_, outputTableRowsCapacity_, pool());
  auto* outputTableRows =
      initBuffer<char*>(outputTableRows_, outputTableRowsCapacity_, pool());

  for (;;) {
    int numOut = 0;

    if (emptyBuildSide) {
      // When build side is empty, anti and left joins return all probe side
      // rows, including ones with null join keys.
      std::iota(mapping.begin(), mapping.begin() + inputSize, 0);
      std::fill(outputTableRows, outputTableRows + inputSize, nullptr);
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
          *resultIter_,
          joinIncludesMissesFromLeft(joinType_),
          folly::Range(mapping.data(), outputBatchSize),
          folly::Range(outputTableRows, outputBatchSize),
          operatorCtx_->driverCtx()->queryConfig().preferredOutputBatchBytes());
    }

    // We are done processing the input batch if there are no more joined rows
    // to process and the NoMatchDetector isn't carrying forward a row that
    // still needs to be written to the output.
    if (!numOut && !noMatchDetector_.hasLastMissedRow()) {
      input_ = nullptr;
      return nullptr;
    }
    VELOX_CHECK_LE(numOut, outputBatchSize);

    numOut = evalFilter(numOut);

    if (numOut == 0) {
      continue;
    }

    if (needLastProbe()) {
      // Mark build-side rows that have a match on the join condition.
      table_->rows()->setProbedFlag(outputTableRows, numOut);
    }

    // Right semi join only returns the build side output when the probe side
    // is fully complete. Do not return anything here.
    if (isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_)) {
      if (resultIter_->atEnd()) {
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

bool HashProbe::maybeReadSpillOutput() {
  maybeSetupSpillOutputReader();

  if (spillOutputReader_ == nullptr) {
    return false;
  }

  VELOX_DCHECK_EQ(table_->numDistinct(), 0);

  if (!spillOutputReader_->nextBatch(output_)) {
    spillOutputReader_.reset();
    return false;
  }
  return true;
}

RowVectorPtr HashProbe::createFilterInput(vector_size_t size) {
  std::vector<VectorPtr> filterColumns(filterInputType_->size());
  for (auto projection : filterInputProjections_) {
    if (projectedInputColumns_.find(projection.inputChannel) !=
        projectedInputColumns_.end()) {
      // If the column is projected to the output, ensure it's loaded if it's
      // lazy in case the filter only loads an incomplete subset of the rows
      // that will be output.
      ensureLoaded(projection.inputChannel);
    } else {
      // If the column isn't projected to the output, the Vector will only be
      // reused if we've broken the input batch into multiple output batches,
      // i.e. if results_ is not at the end of the iterator.
      ensureLoadedIfNotAtEnd(projection.inputChannel);
    }

    filterColumns[projection.outputChannel] = wrapChild(
        size, outputRowMapping_, input_->childAt(projection.inputChannel));
  }

  extractColumns(
      table_.get(),
      folly::Range<char* const*>(outputTableRows_->as<char*>(), size),
      filterTableProjections_,
      pool(),
      filterInputType_->children(),
      filterColumns);

  return std::make_shared<RowVector>(
      pool(), filterInputType_, nullptr, size, std::move(filterColumns));
}

void HashProbe::prepareFilterRowsForNullAwareJoin(
    RowVectorPtr& filterInput,
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
          *filterInput->childAt(projection.outputChannel), filterInputRows_);
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
        // NOTE: the false value of a raw null bit indicates null so we OR
        // with negative of the raw bit.
        bits::orWithNegatedBits(
            rawNullRows, nullsInActiveRows.asRange().bits(), 0, numRows);
      }
    }
    nullFilterInputRows_.updateBounds();
    // TODO: consider to skip filtering on 'nullFilterInputRows_' as we know
    // it will never pass the filtering.
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
  VELOX_CHECK(table_->rows(), "Should not move rows in hash joins");
  char* data[kBatchSize];
  while (auto numRows = iterator(data, kBatchSize)) {
    filterTableInput_->resize(numRows);
    filterTableInputRows_.resizeFill(numRows, true);
    for (auto& projection : filterTableProjections_) {
      table_->extractColumn(
          folly::Range<char* const*>(data, numRows),
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

  // Subset of probe-side rows with non-null probe key and either no match or
  // no match that passed the filter. We need to combine these with all
  // build-side rows with null keys to see if a filter passes on any of these.
  SelectivityVector nullKeyProbeRows(input_->size(), false);

  // Subset of probe-sie rows with null probe key. We need to combine these
  // with all build-side rows to see if a filter passes on any of these.
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
  auto* outputTableRows = outputTableRows_->asMutable<char*>();

  filterInputRows_.resizeFill(numRows);

  // Do not evaluate filter on rows with no match to (1) avoid
  // false-positives when filter evaluates to true for rows with NULLs on the
  // build side; (2) avoid errors in filter evaluation that would fail the
  // query unnecessarily.
  // TODO Apply the same to left joins.
  if (isAntiJoin(joinType_) || isLeftSemiProjectJoin(joinType_)) {
    for (auto i = 0; i < numRows; ++i) {
      if (outputTableRows[i] == nullptr) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }

  RowVectorPtr filterInput = createFilterInput(numRows);

  if (nullAware_) {
    prepareFilterRowsForNullAwareJoin(
        filterInput, numRows, filterPropagateNulls);
  }

  EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput.get());
  filter_->eval(0, 1, true, filterInputRows_, evalCtx, filterResult_);

  decodedFilterResult_.decode(*filterResult_[0], filterInputRows_);

  int32_t numPassed = 0;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
    // Identify probe rows which got filtered out and add them back with nulls
    // for build side.
    if (noMatchDetector_.hasLastMissedRow()) {
      auto* tempOutputTableRows = initBuffer<char*>(
          tempOutputTableRows_, outputTableRowsCapacity_, pool());
      auto* tempOutputRowMapping = initBuffer<vector_size_t>(
          tempOutputRowMapping_, outputTableRowsCapacity_, pool());
      auto addMiss = [&](auto row) {
        tempOutputTableRows[numPassed] = nullptr;
        tempOutputRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        const bool passed = filterPassed(i);
        noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
        if (passed) {
          tempOutputTableRows[numPassed] = outputTableRows[i];
          tempOutputRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
        }
      }
      if (resultIter_->atEnd()) {
        noMatchDetector_.finish(addMiss);
      }
      std::copy(
          tempOutputTableRows,
          tempOutputTableRows + numPassed,
          outputTableRows);
      std::copy(
          tempOutputRowMapping,
          tempOutputRowMapping + numPassed,
          rawOutputProbeRowMapping);
    } else {
      auto addMiss = [&](auto row) {
        outputTableRows[numPassed] = nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        const bool passed = filterPassed(i);
        noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
        if (passed) {
          outputTableRows[numPassed] = outputTableRows[i];
          rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
        }
      }
      if (resultIter_->atEnd()) {
        noMatchDetector_.finish(addMiss);
      }
    }
  } else if (isLeftSemiFilterJoin(joinType_)) {
    auto addLastMatch = [&](auto row) {
      outputTableRows[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        leftSemiFilterJoinTracker_.advance(
            rawOutputProbeRowMapping[i], addLastMatch);
      }
    }
    if (resultIter_->atEnd()) {
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
          outputTableRows[numPassed] =
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
      if (resultIter_->atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    } else {
      auto addLast = [&](auto row, std::optional<bool> passed) {
        outputTableRows[numPassed] =
            passed.value() ? const_cast<char*>(kPassed) : nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        leftSemiProjectJoinTracker_.advance(
            rawOutputProbeRowMapping[i], filterPassed(i), addLast);
      }
      if (resultIter_->atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    }
  } else if (isAntiJoin(joinType_)) {
    auto addMiss = [&](auto row) {
      outputTableRows[numPassed] = nullptr;
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
    if (resultIter_->atEnd()) {
      noMatchDetector_.finish(addMiss);
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        outputTableRows[numPassed] = outputTableRows[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }
  }
  VELOX_CHECK_LE(numPassed, outputTableRowsCapacity_);
  return numPassed;
}

void HashProbe::ensureLoadedIfNotAtEnd(column_index_t channel) {
  if (resultIter_->atEnd()) {
    return;
  }

  ensureLoaded(channel);
}

void HashProbe::ensureLoaded(column_index_t channel) {
  if (!filter_ &&
      (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
       isAntiJoin(joinType_))) {
    return;
  }

  if (!passingInputRowsInitialized_) {
    passingInputRowsInitialized_ = true;
    passingInputRows_.resize(input_->size());
    if (joinIncludesMissesFromLeft(joinType_)) {
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

      passingInputRows_.updateBounds();
    }
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
    VELOX_CHECK_NOT_NULL(inputSpiller_);
    VELOX_CHECK_EQ(
        spillInputPartitionIds_.size(),
        inputSpiller_->spilledPartitionSet().size());
    inputSpiller_->finishSpill(spillPartitionSet_);
    VELOX_CHECK_EQ(spillStats_.rlock()->spillSortTimeNanos, 0);
  }

  const bool hasSpillEnabled = canSpill();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  // The last operator to finish processing inputs is responsible for
  // producing build-side rows based on the join.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(),
          operatorCtx_->driver(),
          hasSpillEnabled ? &future_ : nullptr,
          hasSpillEnabled ? promises_ : promises,
          peers)) {
    if (hasSpillEnabled) {
      VELOX_CHECK(future_.valid());
      setState(ProbeOperatorState::kWaitForPeers);
      VELOX_DCHECK(promises_.empty());
    } else {
      VELOX_DCHECK(promises.empty());
    }
    return;
  }

  VELOX_CHECK(promises.empty());
  // NOTE: if 'hasSpillEnabled' is false, then a hash probe operator doesn't
  // need to wait for all the other peers to finish probe processing.
  // Otherwise, it needs to wait and might expect spill gets triggered by the
  // other probe operators, or there is previously spilled table partition(s)
  // that needs to restore.
  VELOX_CHECK(hasSpillEnabled || peers.empty());
  lastProber_ = true;
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

bool HashProbe::nonReclaimableState() const {
  return (state_ != ProbeOperatorState::kRunning) || nonReclaimableSection_ ||
      (inputSpiller_ != nullptr) || (table_ == nullptr) ||
      (table_->numDistinct() == 0);
}

void HashProbe::ensureOutputFits() {
  if (!canReclaim()) {
    // Don't reserve memory if we can't reclaim from this hash probe operator.
    return;
  }

  // We only need to reserve memory for output if need.
  if (input_ == nullptr &&
      (hasMoreInput() || !(needLastProbe() && lastProber_))) {
    return;
  }

  if (testingTriggerSpill(pool()->name())) {
    Operator::ReclaimableSectionGuard guard(this);
    memory::testingRunArbitration(pool());
  }

  const uint64_t bytesToReserve =
      operatorCtx_->driverCtx()->queryConfig().preferredOutputBatchBytes() *
      1.2;
  if (pool()->availableReservation() >= bytesToReserve) {
    return;
  }
  {
    Operator::ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(bytesToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(bytesToReserve)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->usedBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

bool HashProbe::canReclaim() const {
  return canSpill() && !exceededMaxSpillLevelLimit_;
}

void HashProbe::reclaim(
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  TestValue::adjust("facebook::velox::exec::HashProbe::reclaim", this);
  VELOX_CHECK(canSpill());
  auto* driver = operatorCtx_->driver();
  VELOX_CHECK_NOT_NULL(driver);
  VELOX_CHECK(!nonReclaimableSection_);

  if (UNLIKELY(exceededMaxSpillLevelLimit_)) {
    // 'canReclaim()' already checks the spill limit is not exceeding max, there
    // is only a small chance from the time 'canReclaim()' is checked to the
    // actual reclaim happens that the operator has spilled such that the spill
    // level exceeds max.
    const auto* config = spillConfig();
    VELOX_CHECK_NOT_NULL(config);
    LOG(WARNING)
        << "Can't reclaim from hash probe operator, exceeded maximum spill "
           "level of "
        << config->maxSpillLevel << ", " << pool()->name() << ", usage "
        << succinctBytes(pool()->usedBytes());
    return;
  }

  if (nonReclaimableState()) {
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    ++stats.numNonReclaimableAttempts;
    LOG(WARNING) << "Can't reclaim from hash probe operator, state_["
                 << ProbeOperatorState(state_) << "], nonReclaimableSection_["
                 << nonReclaimableSection_ << "], inputSpiller_["
                 << (inputSpiller_ == nullptr ? "nullptr" : "initialized")
                 << "], table_["
                 << (table_ == nullptr ? "nullptr" : "initialized")
                 << "], table_ numDistinct["
                 << (table_ == nullptr ? "nullptr"
                                       : std::to_string(table_->numDistinct()))
                 << "], " << pool()->name()
                 << ", usage: " << succinctBytes(pool()->usedBytes())
                 << ", node pool reservation: "
                 << succinctBytes(pool()->parent()->reservedBytes());
    return;
  }

  const auto& task = driver->task();
  VELOX_CHECK(task->pauseRequested());
  const std::vector<HashProbe*> probeOps = findPeerOperators();
  bool hasMoreProbeInput{false};
  for (auto* probeOp : probeOps) {
    VELOX_CHECK_NOT_NULL(probeOp);
    VELOX_CHECK(probeOp->canSpill());
    if (probeOp->nonReclaimableState()) {
      RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
      ++stats.numNonReclaimableAttempts;
      const auto* peerPool = probeOp->pool();
      LOG(WARNING) << "Can't reclaim from hash probe operator, state_["
                   << ProbeOperatorState(probeOp->state_)
                   << "], nonReclaimableSection_["
                   << probeOp->nonReclaimableSection_ << "], inputSpiller_["
                   << (probeOp->inputSpiller_ == nullptr ? "nullptr"
                                                         : "initialized")
                   << "], table_["
                   << (probeOp->table_ == nullptr ? "nullptr" : "initialized")
                   << "], table_ numDistinct["
                   << (probeOp->table_ == nullptr
                           ? "nullptr"
                           : std::to_string(probeOp->table_->numDistinct()))
                   << "], " << peerPool->name()
                   << ", usage: " << succinctBytes(peerPool->usedBytes())
                   << ", node pool reservation: "
                   << succinctBytes(peerPool->parent()->reservedBytes());
      return;
    }
    hasMoreProbeInput |= !probeOp->noMoreSpillInput_;
  }

  spillOutput(probeOps);

  SpillPartitionSet spillPartitionSet;
  if (hasMoreProbeInput) {
    // Only spill hash table if any hash probe operators still has input probe
    // data, otherwise we skip this step.
    spillPartitionSet = spillTable();
    VELOX_CHECK(!spillPartitionSet.empty());
  }
  const auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);

  for (auto* probeOp : probeOps) {
    VELOX_CHECK_NOT_NULL(probeOp);
    probeOp->clearBuffers();
    // Setup all the probe operators to spill the rest of probe inputs if the
    // table has been spilled.
    if (!spillPartitionSet.empty()) {
      VELOX_CHECK(hasMoreProbeInput);
      probeOp->maybeSetupInputSpiller(spillPartitionIdSet);
    }
    probeOp->pool()->release();
  }

  // Clears memory resources held by the built hash table.
  table_->clear(true);
  // Sets the spilled hash table in the join bridge.
  if (!spillPartitionIdSet.empty()) {
    joinBridge_->setSpilledHashTable(std::move(spillPartitionSet));
  }
}

void HashProbe::spillOutput(const std::vector<HashProbe*>& operators) {
  struct SpillResult {
    const std::exception_ptr error{nullptr};

    explicit SpillResult(std::exception_ptr _error)
        : error(std::move(_error)) {}
  };

  std::vector<std::shared_ptr<AsyncSource<SpillResult>>> spillTasks;
  auto* spillExecutor = spillConfig()->executor;
  for (auto* op : operators) {
    HashProbe* probeOp = static_cast<HashProbe*>(op);
    spillTasks.push_back(
        memory::createAsyncMemoryReclaimTask<SpillResult>([probeOp]() {
          try {
            probeOp->spillOutput();
            return std::make_unique<SpillResult>(nullptr);
          } catch (const std::exception& e) {
            LOG(ERROR) << "Spill output from hash probe pool "
                       << probeOp->pool()->name() << " failed: " << e.what();
            // The exception is captured and thrown by the caller.
            return std::make_unique<SpillResult>(std::current_exception());
          }
        }));
    if ((spillTasks.size() > 1) && (spillExecutor != nullptr)) {
      spillExecutor->add([source = spillTasks.back()]() { source->prepare(); });
    }
  }

  SCOPE_EXIT {
    for (auto& spillTask : spillTasks) {
      // We consume the result for the pending tasks. This is a cleanup in the
      // guard and must not throw. The first error is already captured before
      // this runs.
      try {
        spillTask->move();
      } catch (const std::exception&) {
      }
    }
  };

  for (auto& spillTask : spillTasks) {
    const auto result = spillTask->move();
    if (result->error) {
      std::rethrow_exception(result->error);
    }
  }
}

void HashProbe::spillOutput() {
  // Checks if there is any output to spill or not.
  if (input_ == nullptr && !needLastProbe()) {
    return;
  }
  // We spill all the outputs produced from 'input_' into a single partition.
  auto outputSpiller = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinProbe,
      outputType_,
      HashBitRange{},
      spillConfig(),
      &spillStats_);
  outputSpiller->setPartitionsSpilled({0});

  RowVectorPtr output{nullptr};
  for (;;) {
    output = getOutputInternal(/*toSpillOutput=*/true);
    if (output != nullptr) {
      // Ensure vector are lazy loaded before spilling.
      for (int32_t i = 0; i < output->childrenSize(); ++i) {
        output->childAt(i)->loadedVector();
      }
      outputSpiller->spill(0, output);
      continue;
    }
    // NOTE: for right semi join types, we need to check if 'input_' has been
    // cleared or not instead of checking on output. The right semi joins only
    // producing the output after processing all the probe inputs.
    if (input_ == nullptr) {
      break;
    }
    VELOX_CHECK(
        isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_));
    VELOX_CHECK((output == nullptr) && (input_ != nullptr));
  }
  VELOX_CHECK_LE(outputSpiller->spilledPartitionSet().size(), 1);

  VELOX_CHECK(spillOutputPartitionSet_.empty());
  outputSpiller->finishSpill(spillOutputPartitionSet_);
  VELOX_CHECK_EQ(spillOutputPartitionSet_.size(), 1);

  removeEmptyPartitions(spillOutputPartitionSet_);
}

void HashProbe::maybeSetupSpillOutputReader() {
  if (spillOutputPartitionSet_.empty()) {
    return;
  }
  VELOX_CHECK_EQ(spillOutputPartitionSet_.size(), 1);
  VELOX_CHECK_NULL(spillOutputReader_);

  spillOutputReader_ =
      spillOutputPartitionSet_.begin()->second->createUnorderedReader(
          spillConfig_->readBufferSize, pool(), &spillStats_);
  spillOutputPartitionSet_.clear();
}

SpillPartitionSet HashProbe::spillTable() {
  struct SpillResult {
    std::unique_ptr<Spiller> spiller{nullptr};
    const std::exception_ptr error{nullptr};

    explicit SpillResult(std::exception_ptr _error) : error(_error) {}
    explicit SpillResult(std::unique_ptr<Spiller> _spiller)
        : spiller(std::move(_spiller)) {}
  };

  const std::vector<RowContainer*> rowContainers = table_->allRows();
  std::vector<std::shared_ptr<AsyncSource<SpillResult>>> spillTasks;
  auto* spillExecutor = spillConfig()->executor;
  for (auto* rowContainer : rowContainers) {
    if (rowContainer->numRows() == 0) {
      continue;
    }
    spillTasks.push_back(memory::createAsyncMemoryReclaimTask<SpillResult>(
        [this, rowContainer]() {
          try {
            return std::make_unique<SpillResult>(spillTable(rowContainer));
          } catch (const std::exception& e) {
            LOG(ERROR) << "Spill sub-table from hash probe pool "
                       << pool()->name() << " failed: " << e.what();
            // The exception is captured and thrown by the caller.
            return std::make_unique<SpillResult>(std::current_exception());
          }
        }));
    if ((spillTasks.size() > 1) && (spillExecutor != nullptr)) {
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

  SpillPartitionSet spillPartitions;
  for (auto& spillTask : spillTasks) {
    const auto result = spillTask->move();
    if (result->error) {
      std::rethrow_exception(result->error);
    }
    result->spiller->finishSpill(spillPartitions);
  }

  // Remove the spilled partitions which are empty so as we don't need to
  // trigger unnecessary spilling at hash probe side.
  removeEmptyPartitions(spillPartitions);
  return spillPartitions;
}

std::unique_ptr<Spiller> HashProbe::spillTable(RowContainer* subTableRows) {
  VELOX_CHECK_NOT_NULL(tableSpillType_);

  auto tableSpiller = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinBuild,
      joinType_,
      subTableRows,
      tableSpillType_,
      std::move(tableSpillHashBits_),
      spillConfig(),
      &spillStats_);
  tableSpiller->spill();
  return tableSpiller;
}

void HashProbe::prepareTableSpill(
    const std::optional<SpillPartitionId>& restoredPartitionId) {
  if (!canSpill()) {
    return;
  }

  const auto* config = spillConfig();
  uint8_t startPartitionBit = config->startPartitionBit;
  if (restoredPartitionId.has_value()) {
    startPartitionBit =
        restoredPartitionId->partitionBitOffset() + config->numPartitionBits;
    // Disable spilling if exceeding the max spill level and the query might
    // run out of memory if the restored partition still can't fit in memory.
    if (config->exceedSpillLevelLimit(startPartitionBit)) {
      RECORD_METRIC_VALUE(kMetricMaxSpillLevelExceededCount);
      FB_LOG_EVERY_MS(WARNING, 1'000)
          << "Exceeded spill level limit: " << config->maxSpillLevel
          << ", and disable spilling for memory pool: " << pool()->name();
      exceededMaxSpillLevelLimit_ = true;
      ++spillStats_.wlock()->spillMaxLevelExceededCount;
      return;
    }
  }
  exceededMaxSpillLevelLimit_ = false;

  tableSpillHashBits_ = HashBitRange(
      startPartitionBit, startPartitionBit + config->numPartitionBits);

  // NOTE: we only need to init 'tableSpillType_' once.
  if (tableSpillType_ != nullptr) {
    return;
  }

  const auto& tableInputType = joinNode_->sources()[1]->outputType();
  std::vector<std::string> names;
  names.reserve(tableInputType->size());
  std::vector<TypePtr> types;
  types.reserve(tableInputType->size());
  const auto numKeys = joinNode_->rightKeys().size();

  // Identify the non-key build side columns.
  folly::F14FastMap<column_index_t, column_index_t> keyChannelMap;
  for (int i = 0; i < numKeys; ++i) {
    const auto& key = joinNode_->rightKeys()[i];
    const auto channel = exprToChannel(key.get(), tableInputType);
    keyChannelMap[channel] = i;
    names.emplace_back(tableInputType->nameOf(channel));
    types.emplace_back(tableInputType->childAt(channel));
  }
  const auto numDependents = tableInputType->size() - numKeys;
  for (auto i = 0; i < tableInputType->size(); ++i) {
    if (keyChannelMap.find(i) == keyChannelMap.end()) {
      names.emplace_back(tableInputType->nameOf(i));
      types.emplace_back(tableInputType->childAt(i));
    }
  }
  tableSpillType_ = hashJoinTableSpillType(
      ROW(std::move(names), std::move(types)), joinType_);
}

void HashProbe::close() {
  Operator::close();

  // Free up major memory usage.
  joinBridge_.reset();
  inputSpiller_.reset();
  table_.reset();
  spillInputReader_.reset();
  spillOutputPartitionSet_.clear();
  spillOutputReader_.reset();
  clearBuffers();
}

void HashProbe::clearBuffers() {
  outputRowMapping_.reset();
  tempOutputRowMapping_.reset();
  outputTableRows_.reset();
  tempOutputTableRows_.reset();
  output_.reset();
  nonSpillInputIndicesBuffer_.reset();
  spillInputIndicesBuffers_.clear();
}

} // namespace facebook::velox::exec
