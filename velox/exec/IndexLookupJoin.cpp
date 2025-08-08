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
#include "velox/exec/IndexLookupJoin.h"

#include "velox/buffer/Buffer.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {
namespace {
void duplicateJoinKeyCheck(
    const std::vector<core::FieldAccessTypedExprPtr>& keys) {
  folly::F14FastSet<std::string> lookupKeyNames;
  for (const auto& key : keys) {
    lookupKeyNames.insert(key->name());
  }
  VELOX_USER_CHECK_EQ(lookupKeyNames.size(), keys.size());
}

std::string getColumnName(const core::TypedExprPtr& typeExpr) {
  const auto field = core::TypedExprs::asFieldAccess(typeExpr);
  VELOX_CHECK_NOT_NULL(field);
  VELOX_CHECK(field->isInputColumn());
  return field->name();
}

// Adds a probe input column to lookup input channels and type if the probe
// column is used in a join condition, The lookup input is projected from the
// probe input and feeds into index source for lookup.
void addLookupInputColumn(
    const std::string& columnName,
    const TypePtr& columnType,
    column_index_t columnChannel,
    std::vector<std::string>& lookupInputNames,
    std::vector<TypePtr>& lookupInputTypes,
    std::vector<column_index_t>& lookupInputChannels,
    folly::F14FastSet<std::string>& lookupInputNameSet) {
  if (lookupInputNameSet.count(columnName) != 0) {
    return;
  }
  lookupInputNames.emplace_back(columnName);
  lookupInputTypes.emplace_back(columnType);
  lookupInputChannels.emplace_back(columnChannel);
  lookupInputNameSet.insert(columnName);
}

// Validates one of between bound, and update the lookup input channels and type
// to include the corresponding probe input column if the bound is not constant.
bool addBetweenConditionBound(
    const core::TypedExprPtr& typeExpr,
    const RowTypePtr& inputType,
    const TypePtr& indexKeyType,
    std::vector<std::string>& lookupInputNames,
    std::vector<TypePtr>& lookupInputTypes,
    std::vector<column_index_t>& lookupInputChannels,
    folly::F14FastSet<std::string>& lookupInputNameSet) {
  const bool isConstant = core::TypedExprs::isConstant(typeExpr);
  if (!isConstant) {
    const auto conditionColumnName = getColumnName(typeExpr);
    const auto conditionColumnChannel =
        inputType->getChildIdx(conditionColumnName);
    const auto conditionColumnType = inputType->childAt(conditionColumnChannel);
    VELOX_USER_CHECK(conditionColumnType->equivalent(*indexKeyType));
    addLookupInputColumn(
        conditionColumnName,
        conditionColumnType,
        conditionColumnChannel,
        lookupInputNames,
        lookupInputTypes,
        lookupInputChannels,
        lookupInputNameSet);
  } else {
    VELOX_USER_CHECK(core::TypedExprs::asConstant(typeExpr)->type()->equivalent(
        *indexKeyType));
  }
  return isConstant;
}

// Process a between join condition by validating the lower and upper bound
// types, and updating the lookup input channels and type to include the probe
// input columns which contain the between condition bounds.
void addBetweenCondition(
    const core::BetweenIndexLookupConditionPtr& betweenCondition,
    const RowTypePtr& inputType,
    const TypePtr& indexKeyType,
    std::vector<std::string>& lookupInputNames,
    std::vector<TypePtr>& lookupInputTypes,
    std::vector<column_index_t>& lookupInputChannels,
    folly::F14FastSet<std::string>& lookupInputNameSet) {
  size_t numConstants{0};
  numConstants += !!addBetweenConditionBound(
      betweenCondition->lower,
      inputType,
      indexKeyType,
      lookupInputNames,
      lookupInputTypes,
      lookupInputChannels,
      lookupInputNameSet);
  numConstants += !!addBetweenConditionBound(
      betweenCondition->upper,
      inputType,
      indexKeyType,
      lookupInputNames,
      lookupInputTypes,
      lookupInputChannels,
      lookupInputNameSet);

  VELOX_USER_CHECK_LT(
      numConstants,
      2,
      "At least one of the between condition bounds needs to be not constant: {}",
      betweenCondition->toString());
}
} // namespace

IndexLookupJoin::IndexLookupJoin(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::IndexLookupJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "IndexLookupJoin"),
      // TODO: support to update output batch size with output size stats during
      // the lookup processing.
      outputBatchSize_{
          driverCtx->queryConfig().indexLookupJoinSplitOutput()
              ? outputBatchRows()
              : std::numeric_limits<vector_size_t>::max()},
      joinType_{joinNode->joinType()},
      includeMatchColumn_(joinNode->includeMatchColumn()),
      numKeys_{joinNode->leftKeys().size()},
      probeType_{joinNode->sources()[0]->outputType()},
      lookupType_{joinNode->lookupSource()->outputType()},
      lookupTableHandle_{joinNode->lookupSource()->tableHandle()},
      lookupConditions_{joinNode->joinConditions()},
      lookupColumnHandles_(joinNode->lookupSource()->assignments()),
      connectorQueryCtx_{operatorCtx_->createConnectorQueryCtx(
          lookupTableHandle_->connectorId(),
          planNodeId(),
          driverCtx->task->addConnectorPoolLocked(
              planNodeId(),
              driverCtx->pipelineId,
              driverCtx->driverId,
              operatorType(),
              lookupTableHandle_->connectorId()),
          spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr)},
      connector_(connector::getConnector(lookupTableHandle_->connectorId())),
      maxNumInputBatches_(
          1 + driverCtx->queryConfig().indexLookupJoinMaxPrefetchBatches()),
      joinNode_{joinNode} {
  duplicateJoinKeyCheck(joinNode_->leftKeys());
  duplicateJoinKeyCheck(joinNode_->rightKeys());
}

void IndexLookupJoin::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(joinNode_);
  VELOX_CHECK_NULL(indexSource_);

  SCOPE_EXIT {
    joinNode_.reset();
  };

  initInputBatches();
  initLookupInput();
  initLookupOutput();
  initOutputProjections();

  indexSource_ = connector_->createIndexSource(
      lookupInputType_,
      numKeys_,
      lookupConditions_,
      lookupOutputType_,
      lookupTableHandle_,
      lookupColumnHandles_,
      connectorQueryCtx_.get());
}

void IndexLookupJoin::ensureInputLoaded(const InputBatchState& batch) {
  VELOX_CHECK_GT(numInputBatches(), 0);
  // Ensure each input vector are lazy loaded before process next batch. This is
  // to ensure the ordered lazy materialization in the source readers.
  auto& input = batch.input;
  for (auto i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }
}

void IndexLookupJoin::initInputBatches() {
  VELOX_CHECK(inputBatches_.empty());

  inputBatches_.resize(maxNumInputBatches_);
  startBatchIndex_ = 0;
  endBatchIndex_ = 0;
}

void IndexLookupJoin::initLookupInput() {
  VELOX_CHECK_NULL(lookupInputType_);
  VELOX_CHECK(lookupInputChannels_.empty());

  std::vector<std::string> lookupInputNames;
  lookupInputNames.reserve(numKeys_ + lookupConditions_.size());
  std::vector<TypePtr> lookupInputTypes;
  lookupInputTypes.reserve(numKeys_ + lookupConditions_.size());
  lookupInputChannels_.reserve(numKeys_ + lookupConditions_.size());

  SCOPE_EXIT {
    VELOX_CHECK_GE(
        lookupInputNames.size(), numKeys_ + lookupConditions_.size());
    VELOX_CHECK_EQ(lookupInputNames.size(), lookupInputChannels_.size());
    lookupInputType_ =
        ROW(std::move(lookupInputNames), std::move(lookupInputTypes));
    VELOX_CHECK_EQ(lookupInputType_->size(), lookupInputChannels_.size());
  };

  folly::F14FastSet<std::string> lookupInputColumnSet;
  folly::F14FastSet<std::string> lookupIndexColumnSet;
  // List probe columns used in join-equi caluse first.
  for (auto keyIdx = 0; keyIdx < numKeys_; ++keyIdx) {
    const auto probeKeyName = joinNode_->leftKeys()[keyIdx]->name();
    const auto indexKeyName = joinNode_->rightKeys()[keyIdx]->name();
    VELOX_USER_CHECK_EQ(lookupIndexColumnSet.count(indexKeyName), 0);
    lookupIndexColumnSet.insert(indexKeyName);
    const auto probeKeyChannel = probeType_->getChildIdx(probeKeyName);
    const auto probeKeyType = probeType_->childAt(probeKeyChannel);
    VELOX_USER_CHECK(
        lookupType_->findChild(indexKeyName)->equivalent(*probeKeyType));
    addLookupInputColumn(
        indexKeyName,
        probeKeyType,
        probeKeyChannel,
        lookupInputNames,
        lookupInputTypes,
        lookupInputChannels_,
        lookupInputColumnSet);
  }

  SCOPE_EXIT {
    VELOX_CHECK(lookupKeyOrConditionHashers_.empty());
    VELOX_CHECK(!lookupInputChannels_.empty());
    lookupKeyOrConditionHashers_ =
        createVectorHashers(probeType_, lookupInputChannels_);
  };
  if (lookupConditions_.empty()) {
    return;
  }

  for (const auto& lookupCondition : lookupConditions_) {
    const auto indexKeyName = getColumnName(lookupCondition->key);
    VELOX_USER_CHECK_EQ(lookupIndexColumnSet.count(indexKeyName), 0);
    lookupIndexColumnSet.insert(indexKeyName);
    const auto indexKeyType = lookupType_->findChild(indexKeyName);

    if (const auto inCondition =
            std::dynamic_pointer_cast<const core::InIndexLookupCondition>(
                lookupCondition)) {
      const auto conditionInputName = getColumnName(inCondition->list);
      const auto conditionInputChannel =
          probeType_->getChildIdx(conditionInputName);
      const auto conditionInputType =
          probeType_->childAt(conditionInputChannel);
      const auto expectedConditionInputType = ARRAY(indexKeyType);
      VELOX_USER_CHECK(
          conditionInputType->equivalent(*expectedConditionInputType));
      addLookupInputColumn(
          conditionInputName,
          conditionInputType,
          conditionInputChannel,
          lookupInputNames,
          lookupInputTypes,
          lookupInputChannels_,
          lookupInputColumnSet);
    }

    if (const auto betweenCondition =
            std::dynamic_pointer_cast<core::BetweenIndexLookupCondition>(
                lookupCondition)) {
      addBetweenCondition(
          betweenCondition,
          probeType_,
          indexKeyType,
          lookupInputNames,
          lookupInputTypes,
          lookupInputChannels_,
          lookupInputColumnSet);
    }
  }
}

void IndexLookupJoin::initLookupOutput() {
  VELOX_CHECK_NULL(lookupOutputType_);

  std::vector<std::string> lookupOutputNames;
  std::vector<TypePtr> lookupOutputTypes;
  const auto& lookupSourceOutputType = joinNode_->lookupSource()->outputType();
  for (auto i = 0; i < outputType_->size(); ++i) {
    const auto& name = outputType_->nameOf(i);
    const auto lookupChannelOpt =
        lookupSourceOutputType->getChildIdxIfExists(name);
    if (!lookupChannelOpt.has_value()) {
      continue;
    }
    lookupOutputNames.push_back(name);
    lookupOutputTypes.push_back(
        lookupSourceOutputType->childAt(lookupChannelOpt.value()));
    VELOX_CHECK(outputType_->childAt(i)->equivalent(*lookupOutputTypes.back()));
  }
  // TODO: support index lookup without output value columns.
  VELOX_CHECK(
      !lookupOutputNames.empty(),
      "Must read at least one value column from index lookup table");
  lookupOutputType_ =
      ROW(std::move(lookupOutputNames), std::move(lookupOutputTypes));
}

void IndexLookupJoin::initOutputProjections() {
  for (auto i = 0; i < probeType_->size(); ++i) {
    const auto name = probeType_->nameOf(i);
    const auto outputChannelOpt = outputType_->getChildIdxIfExists(name);
    if (!outputChannelOpt.has_value()) {
      continue;
    }
    probeOutputProjections_.emplace_back(i, outputChannelOpt.value());
  }
  if (joinType_ == core::JoinType::kLeft) {
    VELOX_USER_CHECK(
        !probeOutputProjections_.empty(),
        "Lookup join with left join type must read at least one column from probe side");
  }

  for (auto i = 0; i < lookupOutputType_->size(); ++i) {
    const auto& name = lookupOutputType_->nameOf(i);
    VELOX_USER_CHECK_EQ(
        lookupColumnHandles_.count(name),
        1,
        "Lookup output column {} is not found in lookup table handle",
        name);
    const auto outputChannelOpt = outputType_->getChildIdxIfExists(name);
    if (!outputChannelOpt.has_value()) {
      continue;
    }
    lookupOutputProjections_.emplace_back(i, outputChannelOpt.value());
  }
  if (includeMatchColumn_) {
    matchOutputChannel_ = outputType_->size() - 1;
  }
  VELOX_USER_CHECK_EQ(
      probeOutputProjections_.size() + lookupOutputProjections_.size() +
          !!matchOutputChannel_.has_value(),
      outputType_->size());
}

bool IndexLookupJoin::startDrain() {
  return numInputBatches() != 0;
}

bool IndexLookupJoin::needsInput() const {
  if (noMoreInput_ || isDraining()) {
    return false;
  }
  if (numInputBatches() >= maxNumInputBatches_) {
    return false;
  }
  if (numInputBatches() == 0) {
    return true;
  }
  const auto& batch = currentInputBatch();
  if (!batch.lookupFuture.valid() || batch.lookupFuture.isReady()) {
    return false;
  }
  return true;
}

BlockingReason IndexLookupJoin::isBlocked(ContinueFuture* future) {
  auto& batch = currentInputBatch();
  if (!batch.lookupFuture.valid()) {
    endLookupBlockWait();
    return BlockingReason::kNotBlocked;
  }
  if (lookupPrefetchEnabled() && (numInputBatches() < maxNumInputBatches_) &&
      !noMoreInput_) {
    return BlockingReason::kNotBlocked;
  }
  *future = std::move(batch.lookupFuture);
  VELOX_CHECK(!batch.lookupFuture.valid());
  startLookupBlockWait();
  return BlockingReason::kWaitForIndexLookup;
}

void IndexLookupJoin::startLookupBlockWait() {
  VELOX_CHECK(!blockWaitStartNs_.has_value());
  blockWaitStartNs_ = getCurrentTimeNano();
}

void IndexLookupJoin::endLookupBlockWait() {
  if (!blockWaitStartNs_.has_value()) {
    return;
  }
  SCOPE_EXIT {
    blockWaitStartNs_ = std::nullopt;
  };
  const auto blockWaitEndNs = getCurrentTimeNano();
  VELOX_CHECK_GE(blockWaitEndNs, blockWaitStartNs_.value());
  const auto blockWaitNs = blockWaitEndNs - blockWaitStartNs_.value();
  RECORD_HISTOGRAM_METRIC_VALUE(
      velox::kMetricIndexLookupBlockedWaitTimeMs, blockWaitNs / 1'000'000);
}

void IndexLookupJoin::addInput(RowVectorPtr input) {
  VELOX_CHECK_GT(input->size(), 0);
  auto& batch = nextInputBatch();
  VELOX_CHECK_LE(numInputBatches(), maxNumInputBatches_);
  batch.input = std::move(input);
  ensureInputLoaded(batch);
  decodeAndDetectNonNullKeys(batch);
  prepareLookup(batch);
  startLookup(batch);
}

RowVectorPtr IndexLookupJoin::getOutput() {
  SCOPE_EXIT {
    if (numInputBatches() == 0 && isDraining()) {
      finishDrain();
    }
  };

  auto& batch = currentInputBatch();
  if (batch.empty()) {
    return nullptr;
  }
  if (batch.lookupFuture.valid() && !batch.lookupFuture.isReady()) {
    VELOX_CHECK_NULL(batch.lookupResult);
    return nullptr;
  }
  auto output = getOutputFromLookupResult(batch);
  if (output == nullptr) {
    return nullptr;
  }
  if (output->size() == 0) {
    return nullptr;
  }
  return output;
}

void IndexLookupJoin::prepareLookup(InputBatchState& batch) {
  VELOX_CHECK_GT(numInputBatches(), 0);
  VELOX_CHECK_NOT_NULL(batch.input);
  const size_t numLookupRows = batch.lookupInputHasNullKeys
      ? batch.nonNullInputRows.countSelected()
      : batch.input->size();
  if (batch.lookupInput == nullptr) {
    batch.lookupInput =
        BaseVector::create<RowVector>(lookupInputType_, numLookupRows, pool());
  } else {
    VectorPtr lookupInputVector = std::move(batch.lookupInput);
    BaseVector::prepareForReuse(lookupInputVector, numLookupRows);
    batch.lookupInput = std::static_pointer_cast<RowVector>(lookupInputVector);
  }

  if (!batch.lookupInputHasNullKeys) {
    for (auto i = 0; i < lookupInputType_->size(); ++i) {
      batch.input->childAt(lookupInputChannels_[i])->loadedVector();
      batch.lookupInput->childAt(i) =
          batch.input->childAt(lookupInputChannels_[i]);
    }
    return;
  }

  if (batch.lookupInput->size() == 0) {
    return;
  }

  const auto mappingByteSize = numLookupRows * sizeof(vector_size_t);
  if ((batch.nonNullInputMappings == nullptr) ||
      !batch.nonNullInputMappings->unique() ||
      (batch.nonNullInputMappings->capacity() < mappingByteSize)) {
    batch.nonNullInputMappings = allocateIndices(numLookupRows, pool());
    batch.rawNonNullInputMappings =
        batch.nonNullInputMappings->asMutable<vector_size_t>();
  }
  batch.nonNullInputMappings->setSize(mappingByteSize);

  size_t lookupRow = 0;
  batch.nonNullInputRows.applyToSelected(
      [&](auto row) { batch.rawNonNullInputMappings[lookupRow++] = row; });
  VELOX_CHECK_EQ(lookupRow, numLookupRows);

  for (auto i = 0; i < lookupInputType_->size(); ++i) {
    batch.input->childAt(lookupInputChannels_[i])->loadedVector();
    batch.lookupInput->childAt(i) = BaseVector::wrapInDictionary(
        nullptr,
        batch.nonNullInputMappings,
        numLookupRows,
        batch.input->childAt(lookupInputChannels_[i]));
  }
}

void IndexLookupJoin::decodeAndDetectNonNullKeys(InputBatchState& batch) {
  const auto numRows = batch.input->size();
  batch.nonNullInputRows.resize(numRows);
  batch.nonNullInputRows.setAll();

  for (auto i = 0; i < lookupKeyOrConditionHashers_.size(); ++i) {
    const auto* key =
        batch.input->childAt(lookupKeyOrConditionHashers_[i]->channel())
            ->loadedVector();
    lookupKeyOrConditionHashers_[i]->decode(*key, batch.nonNullInputRows);
  }
  deselectRowsWithNulls(lookupKeyOrConditionHashers_, batch.nonNullInputRows);
  if (batch.nonNullInputRows.countSelected() < numRows) {
    batch.lookupInputHasNullKeys = true;
  }
}

void IndexLookupJoin::startLookup(InputBatchState& batch) {
  VELOX_CHECK_GT(numInputBatches(), 0);
  VELOX_CHECK_NOT_NULL(batch.input);
  VELOX_CHECK_NOT_NULL(batch.lookupInput);
  if (batch.lookupInputHasNullKeys) {
    VELOX_CHECK_LT(batch.lookupInput->size(), batch.input->size());
  } else {
    VELOX_CHECK_EQ(batch.lookupInput->size(), batch.input->size());
  }
  VELOX_CHECK_NULL(batch.lookupResultIter);
  VELOX_CHECK_NULL(batch.lookupResult);
  VELOX_CHECK(!batch.lookupFuture.valid());

  if (batch.lookupInput->size() == 0) {
    // No need to start lookup for empty lookup input.
    return;
  }

  batch.lookupResultIter = indexSource_->lookup(
      connector::IndexSource::LookupRequest{batch.lookupInput});
  auto lookupResultOr =
      batch.lookupResultIter->next(outputBatchSize_, batch.lookupFuture);
  if (!lookupResultOr.has_value()) {
    VELOX_CHECK(batch.lookupFuture.valid());
    return;
  }
  VELOX_CHECK(!batch.lookupFuture.valid());
  batch.lookupResult = std::move(lookupResultOr).value();
}

RowVectorPtr IndexLookupJoin::getOutputFromLookupResult(
    InputBatchState& batch) {
  VELOX_CHECK(!batch.empty());
  VELOX_CHECK(!batch.lookupFuture.valid() || batch.lookupFuture.isReady());
  batch.lookupFuture = ContinueFuture::makeEmpty();

  if (batch.lookupInput->size() == 0) {
    if (hasRemainingOutputForLeftJoin(batch)) {
      return produceRemainingOutputForLeftJoin(batch);
    }
    finishInput(batch);
    return nullptr;
  }

  VELOX_CHECK_NOT_NULL(batch.lookupResultIter);

  if (batch.lookupResult == nullptr) {
    auto resultOptional =
        batch.lookupResultIter->next(outputBatchSize_, batch.lookupFuture);
    if (!resultOptional.has_value()) {
      VELOX_CHECK(batch.lookupFuture.valid());
      return nullptr;
    }
    VELOX_CHECK(!batch.lookupFuture.valid());

    batch.lookupResult = std::move(resultOptional).value();
    if (batch.lookupResult == nullptr) {
      if (hasRemainingOutputForLeftJoin(batch)) {
        return produceRemainingOutputForLeftJoin(batch);
      }
      finishInput(batch);
      return nullptr;
    }
  }
  prepareLookupResult(batch);
  VELOX_CHECK_NOT_NULL(batch.lookupResult);

  SCOPE_EXIT {
    maybeFinishLookupResult(batch);
  };
  if (joinType_ == core::JoinType::kInner) {
    return produceOutputForInnerJoin(batch);
  }
  return produceOutputForLeftJoin(batch);
}

void IndexLookupJoin::prepareLookupResult(InputBatchState& batch) {
  VELOX_CHECK_NOT_NULL(batch.lookupResult);
  if (rawLookupInputHitIndices_ != nullptr) {
    return;
  }

  if (!batch.lookupInputHasNullKeys) {
    rawLookupInputHitIndices_ =
        batch.lookupResult->inputHits->as<const vector_size_t>();
    return;
  }
  VELOX_CHECK_NOT_NULL(batch.nonNullInputMappings);
  vector_size_t* rawLookupInputHitIndices{nullptr};
  if (batch.lookupResult->inputHits->isMutable()) {
    rawLookupInputHitIndices =
        batch.lookupResult->inputHits->asMutable<vector_size_t>();
  } else {
    const auto indicesByteSize =
        batch.lookupResult->size() * sizeof(vector_size_t);
    if ((batch.resultInputHitIndices == nullptr) ||
        !batch.resultInputHitIndices->unique() ||
        (batch.resultInputHitIndices->capacity() < indicesByteSize)) {
      batch.resultInputHitIndices = allocateIndices(indicesByteSize, pool());
    } else {
      batch.resultInputHitIndices->setSize(indicesByteSize);
    }
    rawLookupInputHitIndices =
        batch.resultInputHitIndices->asMutable<vector_size_t>();
    std::memcpy(
        rawLookupInputHitIndices,
        batch.lookupResult->inputHits->as<const vector_size_t>(),
        indicesByteSize);
    batch.lookupResult->inputHits = batch.resultInputHitIndices;
  }
  for (auto i = 0; i < batch.lookupResult->size(); ++i) {
    rawLookupInputHitIndices[i] =
        batch.rawNonNullInputMappings[rawLookupInputHitIndices[i]];
#ifdef NDEBUG
    if (i > 0) {
      VELOX_DCHECK_LE(
          rawLookupInputHitIndices[i - 1], rawLookupInputHitIndices[i]);
    }
#endif
  }
  rawLookupInputHitIndices_ = rawLookupInputHitIndices;
}

void IndexLookupJoin::maybeFinishLookupResult(InputBatchState& batch) {
  VELOX_CHECK_NOT_NULL(batch.lookupResult);
  if (nextOutputResultRow_ == batch.lookupResult->size()) {
    batch.lookupResult = nullptr;
    nextOutputResultRow_ = 0;
    rawLookupInputHitIndices_ = nullptr;
  }
}

bool IndexLookupJoin::hasRemainingOutputForLeftJoin(
    const InputBatchState& batch) const {
  if (joinType_ != core::JoinType::kLeft) {
    return false;
  }
  if ((lastProcessedInputRow_.value_or(-1) + 1) >= batch.input->size()) {
    return false;
  }
  return true;
}

void IndexLookupJoin::finishInput(InputBatchState& batch) {
  VELOX_CHECK_NOT_NULL(batch.input);
  VELOX_CHECK_EQ(
      batch.lookupInput->size() == 0, batch.lookupResultIter == nullptr);
  VELOX_CHECK(!batch.lookupFuture.valid());

  batch.input = nullptr;
  batch.lookupResultIter = nullptr;
  batch.lookupResult = nullptr;
  batch.lookupInputHasNullKeys = false;
  lastProcessedInputRow_ = std::nullopt;
  nextOutputResultRow_ = 0;
  ++startBatchIndex_;

  if (numInputBatches() != 0) {
    auto& nextBatch = currentInputBatch();
    VELOX_CHECK(!nextBatch.empty());
    if (nextBatch.lookupResult != nullptr) {
      VELOX_CHECK(!nextBatch.lookupFuture.valid());
    } else {
      VELOX_CHECK_EQ(
          nextBatch.lookupInput->size() != 0, nextBatch.lookupFuture.valid());
    }
  }
}

void IndexLookupJoin::prepareOutput(vector_size_t numOutputRows) {
  if (output_ == nullptr) {
    output_ = BaseVector::create<RowVector>(outputType_, numOutputRows, pool());
  } else {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, numOutputRows);
    output_ = std::static_pointer_cast<RowVector>(output);
  }
}

RowVectorPtr IndexLookupJoin::produceOutputForInnerJoin(
    const InputBatchState& batch) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kInner);
  VELOX_CHECK_NOT_NULL(batch.lookupResult);
  VELOX_CHECK_LE(nextOutputResultRow_, batch.lookupResult->size());

  const size_t numOutputRows = std::min<size_t>(
      batch.lookupResult->size() - nextOutputResultRow_, outputBatchSize_);
  prepareOutput(numOutputRows);
  if (numOutputRows == batch.lookupResult->size()) {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
          nullptr,
          batch.lookupResult->inputHits,
          numOutputRows,
          batch.input->childAt(projection.inputChannel));
    }
    for (const auto& projection : lookupOutputProjections_) {
      output_->childAt(projection.outputChannel) =
          batch.lookupResult->output->childAt(projection.inputChannel);
    }
  } else {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
          nullptr,
          Buffer::slice<vector_size_t>(
              batch.lookupResult->inputHits,
              nextOutputResultRow_,
              numOutputRows,
              pool()),
          numOutputRows,
          batch.input->childAt(projection.inputChannel));
    }
    for (const auto& projection : lookupOutputProjections_) {
      output_->childAt(projection.outputChannel) =
          batch.lookupResult->output->childAt(projection.inputChannel)
              ->slice(nextOutputResultRow_, numOutputRows);
    }
  }
  nextOutputResultRow_ += numOutputRows;
  VELOX_CHECK_LE(nextOutputResultRow_, batch.lookupResult->size());
  return output_;
}

void IndexLookupJoin::fillOutputMatchRows(
    vector_size_t offset,
    vector_size_t size,
    bool match) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  bits::fillBits(
      rawLookupOutputNulls_,
      offset,
      offset + size,
      match ? bits::kNotNull : bits::kNull);
  if (!includeMatchColumn_) {
    return;
  }
  VELOX_CHECK_NOT_NULL(rawMatchValues_);
  bits::fillBits(rawMatchValues_, offset, size, match);
}

RowVectorPtr IndexLookupJoin::produceOutputForLeftJoin(
    const InputBatchState& batch) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  VELOX_CHECK_NOT_NULL(batch.lookupResult);
  VELOX_CHECK_LE(nextOutputResultRow_, batch.lookupResult->size());
  VELOX_CHECK_NOT_NULL(rawLookupInputHitIndices_);
  VELOX_CHECK_NOT_NULL(batch.input);

  const auto startOutputRow = nextOutputResultRow_;
  int32_t lastProcessedInputRow = lastProcessedInputRow_.value_or(-1);
  const auto startProcessInputRow = lastProcessedInputRow + 1;
  // Set 'maxOutputRows' to max number of output rows that can be produced
  // considering the possible missed input rows.
  const size_t maxOutputRows = std::min<size_t>(
      outputBatchSize_,
      batch.lookupResult->size() - nextOutputResultRow_ + batch.input->size() -
          startProcessInputRow);
  prepareOutputRowMappings(maxOutputRows);
  VELOX_CHECK_NOT_NULL(rawLookupOutputNulls_);
  size_t numOutputRows{0};
  size_t totalMissedInputRows{0};
  for (; numOutputRows < maxOutputRows &&
       nextOutputResultRow_ < batch.lookupResult->size();) {
    VELOX_CHECK_GE(
        rawLookupInputHitIndices_[nextOutputResultRow_], lastProcessedInputRow);
    const vector_size_t numMissedInputRows =
        rawLookupInputHitIndices_[nextOutputResultRow_] -
        lastProcessedInputRow - 1;
    VELOX_CHECK_GE(numMissedInputRows, -1);
    if (numMissedInputRows > 0) {
      if (totalMissedInputRows == 0) {
        ensureMatchColumn(maxOutputRows);
        fillOutputMatchRows(0, maxOutputRows, true);
      }
      const auto numOutputMissedInputRows = std::min<vector_size_t>(
          numMissedInputRows, maxOutputRows - numOutputRows);
      fillOutputMatchRows(numOutputRows, numOutputMissedInputRows, false);
      for (auto i = 0; i < numOutputMissedInputRows; ++i) {
        rawProbeOutputRowIndices_[numOutputRows++] = ++lastProcessedInputRow;
      }
      totalMissedInputRows += numOutputMissedInputRows;
      continue;
    }

    rawProbeOutputRowIndices_[numOutputRows] =
        rawLookupInputHitIndices_[nextOutputResultRow_];
    rawLookupOutputRowIndices_[numOutputRows] = nextOutputResultRow_;
    lastProcessedInputRow = rawLookupInputHitIndices_[nextOutputResultRow_];
    ++nextOutputResultRow_;
    ++numOutputRows;
  }
  VELOX_CHECK(
      numOutputRows == maxOutputRows ||
      nextOutputResultRow_ == batch.lookupResult->size());
  VELOX_CHECK_LE(nextOutputResultRow_, batch.lookupResult->size());
  lastProcessedInputRow_ = lastProcessedInputRow;

  if (totalMissedInputRows > 0) {
    lookupOutputNulls_->setSize(bits::nbytes(numOutputRows));
    setMatchColumnSize(numOutputRows);
  }
  probeOutputRowMapping_->setSize(numOutputRows * sizeof(vector_size_t));
  lookupOutputRowMapping_->setSize(numOutputRows * sizeof(vector_size_t));

  if (numOutputRows == 0) {
    return nullptr;
  }

  prepareOutput(numOutputRows);
  const auto numInputRows = lastProcessedInputRow - startProcessInputRow + 1;
  if (numInputRows == numOutputRows) {
    if (startProcessInputRow == 0 && numInputRows == batch.input->size()) {
      for (const auto& projection : probeOutputProjections_) {
        output_->childAt(projection.outputChannel) =
            batch.input->childAt(projection.inputChannel);
      }
    } else {
      for (const auto& projection : probeOutputProjections_) {
        output_->childAt(projection.outputChannel) =
            batch.input->childAt(projection.inputChannel)
                ->slice(startProcessInputRow, numInputRows);
      }
    }
  } else {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
          nullptr,
          probeOutputRowMapping_,
          numOutputRows,
          batch.input->childAt(projection.inputChannel));
    }
  }

  if (totalMissedInputRows > 0) {
    for (const auto& projection : lookupOutputProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
          lookupOutputNulls_,
          lookupOutputRowMapping_,
          numOutputRows,
          batch.lookupResult->output->childAt(projection.inputChannel));
    }
    if (includeMatchColumn_) {
      output_->childAt(matchOutputChannel_.value()) = matchColumn_;
    }
  } else {
    if (startOutputRow == 0 &&
        numOutputRows == batch.lookupResult->output->size()) {
      for (const auto& projection : lookupOutputProjections_) {
        output_->childAt(projection.outputChannel) =
            batch.lookupResult->output->childAt(projection.inputChannel);
      }
    } else {
      for (const auto& projection : lookupOutputProjections_) {
        output_->childAt(projection.outputChannel) =
            batch.lookupResult->output->childAt(projection.inputChannel)
                ->slice(startOutputRow, numOutputRows);
      }
    }
    if (includeMatchColumn_) {
      output_->childAt(matchOutputChannel_.value()) =
          BaseVector::createConstant(BOOLEAN(), true, numOutputRows, pool());
    }
  }
  return output_;
}

void IndexLookupJoin::ensureMatchColumn(vector_size_t maxOutputRows) {
  if (!includeMatchColumn_) {
    return;
  }
  if (matchColumn_) {
    VectorPtr matchColumn = std::move(matchColumn_);
    BaseVector::prepareForReuse(matchColumn, maxOutputRows);
    matchColumn_ = std::dynamic_pointer_cast<FlatVector<bool>>(matchColumn);
  } else {
    matchColumn_ =
        BaseVector::create<FlatVector<bool>>(BOOLEAN(), maxOutputRows, pool());
  }
  VELOX_CHECK_NOT_NULL(matchColumn_);
  rawMatchValues_ = matchColumn_->mutableRawValues<uint64_t>();
}

void IndexLookupJoin::setMatchColumnSize(vector_size_t numOutputRows) {
  if (!includeMatchColumn_) {
    return;
  }
  VELOX_CHECK_NOT_NULL(matchColumn_);
  matchColumn_->resize(numOutputRows);
}

RowVectorPtr IndexLookupJoin::produceRemainingOutputForLeftJoin(
    const InputBatchState& batch) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  VELOX_CHECK(!batch.empty());
  VELOX_CHECK(hasRemainingOutputForLeftJoin(batch));
  VELOX_CHECK_NULL(rawLookupInputHitIndices_);

  size_t lastProcessedInputRow = lastProcessedInputRow_.value_or(-1);
  const auto startProcessInputRow = lastProcessedInputRow + 1;
  const size_t numOutputRows = std::min<size_t>(
      outputBatchSize_, batch.input->size() - startProcessInputRow);
  VELOX_CHECK_GT(numOutputRows, 0);
  VELOX_CHECK_LE(numOutputRows, batch.input->size());
  prepareOutput(numOutputRows);
  if (numOutputRows != batch.input->size()) {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) =
          batch.input->childAt(projection.inputChannel)
              ->slice(startProcessInputRow, numOutputRows);
    }
  } else {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) =
          batch.input->childAt(projection.inputChannel);
    }
  }
  for (const auto& projection : lookupOutputProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::createNullConstant(
        output_->type()->childAt(projection.outputChannel),
        numOutputRows,
        pool());
  }
  if (includeMatchColumn_) {
    output_->childAt(matchOutputChannel_.value()) =
        BaseVector::createConstant(BOOLEAN(), false, numOutputRows, pool());
  }
  lastProcessedInputRow_ = lastProcessedInputRow + numOutputRows;
  return output_;
}

void IndexLookupJoin::prepareOutputRowMappings(size_t outputBatchSize) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);

  const auto mappingByteSize = outputBatchSize * sizeof(vector_size_t);
  if ((probeOutputRowMapping_ == nullptr) ||
      !probeOutputRowMapping_->unique() ||
      (probeOutputRowMapping_->capacity() < mappingByteSize)) {
    probeOutputRowMapping_ = allocateIndices(outputBatchSize, pool());
  } else {
    probeOutputRowMapping_->setSize(outputBatchSize);
  }
  rawProbeOutputRowIndices_ =
      probeOutputRowMapping_->asMutable<vector_size_t>();

  if ((lookupOutputRowMapping_ == nullptr) ||
      !lookupOutputRowMapping_->unique() ||
      (lookupOutputRowMapping_->capacity() < mappingByteSize)) {
    lookupOutputRowMapping_ = allocateIndices(outputBatchSize, pool());
  } else {
    lookupOutputRowMapping_->setSize(outputBatchSize);
  }
  rawLookupOutputRowIndices_ =
      lookupOutputRowMapping_->asMutable<vector_size_t>();

  const auto nullByteSize = bits::nbytes(outputBatchSize);
  if (lookupOutputNulls_ == nullptr || !lookupOutputNulls_->unique() ||
      (lookupOutputNulls_->capacity() < nullByteSize)) {
    lookupOutputNulls_ = allocateNulls(outputBatchSize, pool());
  }
  rawLookupOutputNulls_ = lookupOutputNulls_->asMutable<uint64_t>();
}

void IndexLookupJoin::close() {
  recordConnectorStats();
  // TODO: add close method for index source if needed to free up resource
  // or shutdown index source gracefully.
  indexSource_.reset();
  inputBatches_.clear();
  probeOutputRowMapping_ = nullptr;
  lookupOutputRowMapping_ = nullptr;
  lookupOutputNulls_ = nullptr;

  Operator::close();
}

void IndexLookupJoin::recordConnectorStats() {
  if (indexSource_ == nullptr) {
    // NOTE: index join might fail to create index source so skip record stats
    // in that case.
    return;
  }
  auto lockedStats = stats_.wlock();
  auto connectorStats = indexSource_->runtimeStats();
  for (auto& [name, value] : connectorStats) {
    lockedStats->runtimeStats.erase(name);
    lockedStats->runtimeStats.emplace(name, std::move(value));
  }
  if (connectorStats.count(kConnectorLookupWallTime) != 0) {
    const CpuWallTiming backgroundTiming{
        static_cast<uint64_t>(connectorStats[kConnectorLookupWallTime].count),
        static_cast<uint64_t>(connectorStats[kConnectorLookupWallTime].sum),
        // NOTE: this might not be accurate as it doesn't include the time
        // spent inside the index storage client.
        static_cast<uint64_t>(connectorStats[kConnectorResultPrepareTime].sum) +
            connectorStats[kClientRequestProcessTime].sum +
            connectorStats[kClientResultProcessTime].sum};
    lockedStats->backgroundTiming.clear();
    lockedStats->backgroundTiming.add(backgroundTiming);
  }
}
} // namespace facebook::velox::exec
