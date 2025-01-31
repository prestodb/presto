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
      expressionEvaluator_(connectorQueryCtx_->expressionEvaluator()),
      connector_(connector::getConnector(lookupTableHandle_->connectorId())),
      joinNode_{joinNode} {
  VELOX_CHECK_EQ(joinNode_->sources()[1], joinNode_->lookupSource());
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
  };

  // List probe key columns used in join-equi caluse first.
  folly::F14FastSet<std::string> probeKeyColumnNames;
  for (auto keyIdx = 0; keyIdx < numKeys_; ++keyIdx) {
    lookupInputNames.emplace_back(joinNode_->leftKeys()[keyIdx]->name());
    const auto probeKeyChannel =
        probeType_->getChildIdx(lookupInputNames.back());
    lookupInputChannels_.emplace_back(probeKeyChannel);
    lookupInputTypes.emplace_back(probeType_->childAt(probeKeyChannel));
    VELOX_CHECK_EQ(probeKeyColumnNames.count(lookupInputNames.back()), 0);
    probeKeyColumnNames.insert(lookupInputNames.back());
  }

  if (lookupConditions_.empty()) {
    return;
  }

  folly::F14FastSet<std::string> probeConditionColumnNames;
  folly::F14FastSet<std::string> lookupConditionColumnNames;
  for (const auto& lookupCondition : lookupConditions_) {
    const auto lookupConditionExprSet =
        expressionEvaluator_->compile(lookupCondition);
    const auto& lookupConditionExpr = lookupConditionExprSet->expr(0);

    int numProbeColumns{0};
    int numLookupColumns{0};
    for (auto& input : lookupConditionExpr->distinctFields()) {
      const auto& columnName = input->field();
      auto probeIndexOpt = probeType_->getChildIdxIfExists(columnName);
      if (probeIndexOpt.has_value()) {
        ++numProbeColumns;
        // There is no overlap between probe key columns and probe condition
        // columns.
        VELOX_CHECK_EQ(probeKeyColumnNames.count(columnName), 0);
        // We allow the probe column used in more than one lookup conditions.
        if (probeConditionColumnNames.count(columnName) == 0) {
          probeConditionColumnNames.insert(columnName);
          lookupInputChannels_.push_back(probeIndexOpt.value());
          lookupInputNames.push_back(columnName);
          lookupInputTypes.push_back(input->type());
        }
        continue;
      }

      ++numLookupColumns;
      auto lookupIndexOpt = lookupType_->getChildIdxIfExists(columnName);
      VELOX_CHECK(
          lookupIndexOpt.has_value(),
          "Lookup condition column {} is not found",
          columnName);
      // A lookup column can only be used in on lookup condition.
      VELOX_CHECK(
          lookupConditionColumnNames.count(columnName),
          0,
          "Lookup condition column {} from lookup table used in more than one lookup conditions",
          input->field());
      lookupConditionColumnNames.insert(input->field());
    }

    VELOX_CHECK_EQ(
        numLookupColumns,
        1,
        "Unexpected number of lookup columns in lookup condition {}",
        lookupConditionExpr->toString());
    VELOX_CHECK_GT(
        numProbeColumns,
        0,
        "No probe columns found in lookup condition {}",
        lookupConditionExpr->toString());
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

BlockingReason IndexLookupJoin::isBlocked(ContinueFuture* future) {
  if (!lookupFuture_.valid()) {
    return BlockingReason::kNotBlocked;
  }
  *future = std::move(lookupFuture_);
  return BlockingReason::kWaitForIndexLookup;
}

void IndexLookupJoin::addInput(RowVectorPtr input) {
  VELOX_CHECK_GT(input->size(), 0);
  VELOX_CHECK_NULL(input_);
  input_ = std::move(input);
}

RowVectorPtr IndexLookupJoin::getOutput() {
  if (input_ == nullptr) {
    return nullptr;
  }

  if (lookupResultIter_ == nullptr) {
    VELOX_CHECK(!lookupFuture_.valid());
    prepareLookupInput();
    lookup();
  }

  VELOX_CHECK_NOT_NULL(lookupResultIter_);
  auto output = getOutputFromLookupResult();
  if (output == nullptr) {
    return nullptr;
  }
  if (output->size() == 0) {
    return nullptr;
  }
  return output;
}

void IndexLookupJoin::prepareLookupInput() {
  VELOX_CHECK_NOT_NULL(input_);
  if (lookupInput_ == nullptr) {
    lookupInput_ =
        BaseVector::create<RowVector>(lookupInputType_, input_->size(), pool());
  } else {
    VectorPtr input = std::move(lookupInput_);
    BaseVector::prepareForReuse(input, input_->size());
    lookupInput_ = std::static_pointer_cast<RowVector>(input);
  }

  for (auto i = 0; i < lookupInputType_->size(); ++i) {
    lookupInput_->childAt(i) = input_->childAt(lookupInputChannels_[i]);
    lookupInput_->childAt(i)->loadedVector();
  }
}

void IndexLookupJoin::lookup() {
  VELOX_CHECK_NOT_NULL(indexSource_);
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(lookupInput_);
  VELOX_CHECK_NULL(lookupResultIter_);
  VELOX_CHECK_EQ(lookupInput_->size(), input_->size());

  lookupResultIter_ =
      indexSource_->lookup(connector::IndexSource::LookupRequest{lookupInput_});
}

RowVectorPtr IndexLookupJoin::getOutputFromLookupResult() {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(lookupResultIter_);

  if (lookupResult_ == nullptr) {
    auto resultOptional =
        lookupResultIter_->next(outputBatchSize_, lookupFuture_);
    if (!resultOptional.has_value()) {
      VELOX_CHECK(lookupFuture_.valid());
      return nullptr;
    }
    VELOX_CHECK(!lookupFuture_.valid());

    lookupResult_ = std::move(resultOptional).value();
    if (lookupResult_ == nullptr) {
      if (hasRemainingOutputForLeftJoin()) {
        return produceRemainingOutputForLeftJoin();
      }
      finishInput();
      return nullptr;
    }
    rawLookupInputHitIndices_ =
        lookupResult_->inputHits->as<const vector_size_t>();
  }
  VELOX_CHECK_NOT_NULL(lookupResult_);

  SCOPE_EXIT {
    maybeFinishLookupResult();
  };
  if (joinType_ == core::JoinType::kInner) {
    return produceOutputForInnerJoin();
  }
  return produceOutputForLeftJoin();
}

void IndexLookupJoin::maybeFinishLookupResult() {
  VELOX_CHECK_NOT_NULL(lookupResult_);
  if (nextOutputResultRow_ == lookupResult_->size()) {
    lookupResult_ = nullptr;
    nextOutputResultRow_ = 0;
    rawLookupInputHitIndices_ = nullptr;
  }
}

bool IndexLookupJoin::hasRemainingOutputForLeftJoin() const {
  if (joinType_ != core::JoinType::kLeft) {
    return false;
  }
  if ((lastProcessedInputRow_.value_or(-1) + 1) >= input_->size()) {
    return false;
  }
  return true;
}

void IndexLookupJoin::finishInput() {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(lookupResultIter_);
  VELOX_CHECK(!lookupFuture_.valid());

  lookupResultIter_ = nullptr;
  lookupResult_ = nullptr;
  lastProcessedInputRow_ = std::nullopt;
  nextOutputResultRow_ = 0;
  input_ = nullptr;
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

RowVectorPtr IndexLookupJoin::produceOutputForInnerJoin() {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kInner);
  VELOX_CHECK_NOT_NULL(lookupResult_);
  VELOX_CHECK_LE(nextOutputResultRow_, lookupResult_->size());

  const size_t numOutputRows = std::min<size_t>(
      lookupResult_->size() - nextOutputResultRow_, outputBatchSize_);
  prepareOutput(numOutputRows);
  if (numOutputRows == lookupResult_->size()) {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
          nullptr,
          lookupResult_->inputHits,
          numOutputRows,
          input_->childAt(projection.inputChannel));
    }
    for (const auto& projection : lookupOutputProjections_) {
      output_->childAt(projection.outputChannel) =
          lookupResult_->output->childAt(projection.inputChannel);
    }
  } else {
    for (const auto& projection : probeOutputProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
          nullptr,
          Buffer::slice<vector_size_t>(
              lookupResult_->inputHits,
              nextOutputResultRow_,
              numOutputRows,
              pool()),
          numOutputRows,
          input_->childAt(projection.inputChannel));
    }
    for (const auto& projection : lookupOutputProjections_) {
      output_->childAt(projection.outputChannel) =
          lookupResult_->output->childAt(projection.inputChannel)
              ->slice(nextOutputResultRow_, numOutputRows);
    }
  }

  nextOutputResultRow_ += numOutputRows;
  VELOX_CHECK_LE(nextOutputResultRow_, lookupResult_->size());
  return output_;
}

RowVectorPtr IndexLookupJoin::produceOutputForLeftJoin() {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  VELOX_CHECK_NOT_NULL(lookupResult_);
  VELOX_CHECK_LE(nextOutputResultRow_, lookupResult_->size());
  VELOX_CHECK_NOT_NULL(rawLookupInputHitIndices_);

  prepareOutputRowMappings(outputBatchSize_);
  VELOX_CHECK_NOT_NULL(rawLookupOutputNulls_);

  size_t numOutputRows{0};
  size_t totalMissedInputRows{0};
  int32_t lastProcessedInputRow = lastProcessedInputRow_.value_or(-1);
  for (; numOutputRows < outputBatchSize_ &&
       nextOutputResultRow_ < lookupResult_->size();) {
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
          numOutputMissedInputRows,
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
      nextOutputResultRow_ == lookupResult_->size());
  VELOX_CHECK_LE(nextOutputResultRow_, lookupResult_->size());
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
        input_->childAt(projection.inputChannel));
  }
  for (const auto& projection : lookupOutputProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
        totalMissedInputRows > 0 ? lookupOutputNulls_ : nullptr,
        lookupOutputRowMapping_,
        numOutputRows,
        lookupResult_->output->childAt(projection.inputChannel));
  }
  return output_;
}

RowVectorPtr IndexLookupJoin::produceRemainingOutputForLeftJoin() {
  VELOX_CHECK_EQ(joinType_, core::JoinType::kLeft);
  VELOX_CHECK_NULL(lookupResult_);
  VELOX_CHECK(hasRemainingOutputForLeftJoin());
  VELOX_CHECK_NULL(rawLookupInputHitIndices_);

  prepareOutputRowMappings(outputBatchSize_);
  VELOX_CHECK_NOT_NULL(rawLookupOutputNulls_);

  size_t lastProcessedInputRow = lastProcessedInputRow_.value_or(-1);
  const size_t numOutputRows = std::min<size_t>(
      outputBatchSize_, input_->size() - lastProcessedInputRow - 1);
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
        input_->childAt(projection.inputChannel));
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
  // TODO: add close method for index source if needed to free up resource
  // or shutdown index source gracefully.
  indexSource_.reset();
  lookupResultIter_ = nullptr;
  lookupInput_ = nullptr;
  lookupResult_ = nullptr;
  probeOutputRowMapping_ = nullptr;
  lookupOutputRowMapping_ = nullptr;
  lookupOutputNulls_ = nullptr;

  Operator::close();
}
} // namespace facebook::velox::exec
