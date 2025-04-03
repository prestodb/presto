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
      outputBatchSize_{outputBatchRows()},
      joinType_{joinNode->joinType()},
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
  if (!lookupPrefetchEnabled()) {
    return;
  }
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
  VELOX_USER_CHECK_EQ(
      probeOutputProjections_.size() + lookupOutputProjections_.size(),
      outputType_->size());
}

bool IndexLookupJoin::needsInput() const {
  if (noMoreInput_) {
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
  prepareLookup(batch);
  startLookup(batch);
}

RowVectorPtr IndexLookupJoin::getOutput() {
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
  if (batch.lookupInput == nullptr) {
    batch.lookupInput = BaseVector::create<RowVector>(
        lookupInputType_, batch.input->size(), pool());
  } else {
    VectorPtr lookupInputVector = std::move(batch.lookupInput);
    BaseVector::prepareForReuse(lookupInputVector, batch.input->size());
    batch.lookupInput = std::static_pointer_cast<RowVector>(lookupInputVector);
  }

  for (auto i = 0; i < lookupInputType_->size(); ++i) {
    batch.lookupInput->childAt(i) =
        batch.input->childAt(lookupInputChannels_[i]);
    batch.lookupInput->childAt(i)->loadedVector();
  }
}

void IndexLookupJoin::startLookup(InputBatchState& batch) {
  VELOX_CHECK_GT(numInputBatches(), 0);
  VELOX_CHECK_NOT_NULL(batch.input);
  VELOX_CHECK_NOT_NULL(batch.lookupInput);
  VELOX_CHECK_EQ(batch.lookupInput->size(), batch.input->size());
  VELOX_CHECK_NULL(batch.lookupResultIter);
  VELOX_CHECK_NULL(batch.lookupResult);
  VELOX_CHECK(!batch.lookupFuture.valid());

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
    rawLookupInputHitIndices_ =
        batch.lookupResult->inputHits->as<const vector_size_t>();
  } else if (rawLookupInputHitIndices_ == nullptr) {
    rawLookupInputHitIndices_ =
        batch.lookupResult->inputHits->as<const vector_size_t>();
  }
  VELOX_CHECK_NOT_NULL(batch.lookupResult);

  SCOPE_EXIT {
    maybeFinishLookupResult(batch);
  };
  if (joinType_ == core::JoinType::kInner) {
    return produceOutputForInnerJoin(batch);
  }
  return produceOutputForLeftJoin(batch);
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
  VELOX_CHECK_NOT_NULL(batch.lookupResultIter);
  VELOX_CHECK(!batch.lookupFuture.valid());

  batch.input = nullptr;
  batch.lookupResultIter = nullptr;
  batch.lookupResult = nullptr;
  lastProcessedInputRow_ = std::nullopt;
  nextOutputResultRow_ = 0;
  ++startBatchIndex_;

  if (numInputBatches() != 0) {
    auto& nextBatch = currentInputBatch();
    VELOX_CHECK(!nextBatch.empty());
    if (nextBatch.lookupResult != nullptr) {
      VELOX_CHECK(!nextBatch.lookupFuture.valid());
      rawLookupInputHitIndices_ =
          nextBatch.lookupResult->inputHits->as<const vector_size_t>();
    } else {
      VELOX_CHECK(nextBatch.lookupFuture.valid());
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

RowVectorPtr IndexLookupJoin::produceOutputForLeftJoin(
    const InputBatchState& batch) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  VELOX_CHECK_NOT_NULL(batch.lookupResult);
  VELOX_CHECK_LE(nextOutputResultRow_, batch.lookupResult->size());
  VELOX_CHECK_NOT_NULL(rawLookupInputHitIndices_);
  VELOX_CHECK_NOT_NULL(batch.input);

  prepareOutputRowMappings(outputBatchSize_);
  VELOX_CHECK_NOT_NULL(rawLookupOutputNulls_);

  size_t numOutputRows{0};
  size_t totalMissedInputRows{0};
  int32_t lastProcessedInputRow = lastProcessedInputRow_.value_or(-1);
  for (; numOutputRows < outputBatchSize_ &&
       nextOutputResultRow_ < batch.lookupResult->size();) {
    VELOX_CHECK_GE(
        rawLookupInputHitIndices_[nextOutputResultRow_], lastProcessedInputRow);
    const vector_size_t numMissedInputRows =
        rawLookupInputHitIndices_[nextOutputResultRow_] -
        lastProcessedInputRow - 1;
    VELOX_CHECK_GE(numMissedInputRows, -1);
    if (numMissedInputRows > 0) {
      if (totalMissedInputRows == 0) {
        bits::fillBits(
            rawLookupOutputNulls_, 0, outputBatchSize_, bits::kNotNull);
      }
      const auto numOutputMissedInputRows = std::min<vector_size_t>(
          numMissedInputRows, outputBatchSize_ - numOutputRows);
      bits::fillBits(
          rawLookupOutputNulls_,
          numOutputRows,
          numOutputRows + numOutputMissedInputRows,
          bits::kNull);
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
      numOutputRows == outputBatchSize_ ||
      nextOutputResultRow_ == batch.lookupResult->size());
  VELOX_CHECK_LE(nextOutputResultRow_, batch.lookupResult->size());
  lastProcessedInputRow_ = lastProcessedInputRow;

  if (totalMissedInputRows > 0) {
    lookupOutputNulls_->setSize(bits::nbytes(numOutputRows));
  }
  probeOutputRowMapping_->setSize(numOutputRows * sizeof(vector_size_t));
  lookupOutputRowMapping_->setSize(numOutputRows * sizeof(vector_size_t));

  if (numOutputRows == 0) {
    return nullptr;
  }

  prepareOutput(numOutputRows);
  for (const auto& projection : probeOutputProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
        nullptr,
        probeOutputRowMapping_,
        numOutputRows,
        batch.input->childAt(projection.inputChannel));
  }
  for (const auto& projection : lookupOutputProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
        totalMissedInputRows > 0 ? lookupOutputNulls_ : nullptr,
        lookupOutputRowMapping_,
        numOutputRows,
        batch.lookupResult->output->childAt(projection.inputChannel));
  }
  return output_;
}

RowVectorPtr IndexLookupJoin::produceRemainingOutputForLeftJoin(
    const InputBatchState& batch) {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  VELOX_CHECK(!batch.empty());
  VELOX_CHECK(hasRemainingOutputForLeftJoin(batch));
  VELOX_CHECK_NULL(rawLookupInputHitIndices_);
  prepareOutputRowMappings(outputBatchSize_);
  VELOX_CHECK_NOT_NULL(rawLookupOutputNulls_);

  size_t lastProcessedInputRow = lastProcessedInputRow_.value_or(-1);
  const size_t numOutputRows = std::min<size_t>(
      outputBatchSize_, batch.input->size() - lastProcessedInputRow - 1);
  VELOX_CHECK_GT(numOutputRows, 0);
  bits::fillBits(rawLookupOutputNulls_, 0, numOutputRows, bits::kNull);
  for (auto outputRow = 0; outputRow < numOutputRows; ++outputRow) {
    rawProbeOutputRowIndices_[outputRow] = ++lastProcessedInputRow;
  }
  lookupOutputNulls_->setSize(bits::nbytes(numOutputRows));
  probeOutputRowMapping_->setSize(numOutputRows * sizeof(vector_size_t));
  lookupOutputRowMapping_->setSize(numOutputRows * sizeof(vector_size_t));

  prepareOutput(numOutputRows);
  for (const auto& projection : probeOutputProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
        nullptr,
        probeOutputRowMapping_,
        numOutputRows,
        batch.input->childAt(projection.inputChannel));
  }
  for (const auto& projection : lookupOutputProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
        lookupOutputNulls_,
        lookupOutputRowMapping_,
        numOutputRows,
        BaseVector::createNullConstant(
            output_->type()->childAt(projection.outputChannel),
            numOutputRows,
            pool()));
  }
  lastProcessedInputRow_ = lastProcessedInputRow;
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
        // NOTE: this might not be accurate as it doesn't include the time spent
        // inside the index storage client.
        static_cast<uint64_t>(connectorStats[kConnectorResultPrepareTime].sum) +
            connectorStats[kClientRequestProcessTime].sum +
            connectorStats[kClientResultProcessTime].sum};
    lockedStats->backgroundTiming.clear();
    lockedStats->backgroundTiming.add(backgroundTiming);
  }
}
} // namespace facebook::velox::exec
