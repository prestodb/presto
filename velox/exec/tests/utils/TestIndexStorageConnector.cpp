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

#include "velox/exec/tests/utils/TestIndexStorageConnector.h"

#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/exec/IndexLookupJoin.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FieldReference.h"

using facebook::velox::common::testutil::TestValue;
namespace facebook::velox::exec::test {
namespace {
core::TypedExprPtr toJoinConditionExpr(
    const std::vector<std::shared_ptr<core::IndexLookupCondition>>&
        joinConditions,
    const std::shared_ptr<TestIndexTable>& indexTable,
    const RowTypePtr& inputType,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles) {
  if (joinConditions.empty()) {
    return nullptr;
  }
  const auto& keyType = indexTable->keyType;
  std::vector<core::TypedExprPtr> conditionExprs;
  conditionExprs.reserve(joinConditions.size());
  for (const auto& condition : joinConditions) {
    auto indexColumnExpr = std::make_shared<core::FieldAccessTypedExpr>(
        keyType->findChild(condition->key->name()), condition->key->name());
    if (auto inCondition =
            std::dynamic_pointer_cast<core::InIndexLookupCondition>(
                condition)) {
      conditionExprs.push_back(std::make_shared<const core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              inCondition->list, std::move(indexColumnExpr)},
          "contains"));
      continue;
    }
    if (auto betweenCondition =
            std::dynamic_pointer_cast<core::BetweenIndexLookupCondition>(
                condition)) {
      conditionExprs.push_back(std::make_shared<const core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              std::move(indexColumnExpr),
              betweenCondition->lower,
              betweenCondition->upper},
          "between"));
      continue;
    }
    VELOX_FAIL("Invalid index join condition: {}", condition->toString());
  }
  return std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), conditionExprs, "and");
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
} // namespace

TestIndexSource::TestIndexSource(
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    size_t numEqualJoinKeys,
    const core::TypedExprPtr& joinConditionExpr,
    const std::shared_ptr<TestIndexTableHandle>& tableHandle,
    connector::ConnectorQueryCtx* connectorQueryCtx,
    folly::Executor* executor)
    : tableHandle_(tableHandle),
      inputType_(inputType),
      outputType_(outputType),
      keyType_(tableHandle_->indexTable()->keyType),
      valueType_(tableHandle_->indexTable()->dataType),
      connectorQueryCtx_(connectorQueryCtx),
      numEqualJoinKeys_(numEqualJoinKeys),
      conditionExprSet_(
          joinConditionExpr != nullptr
              ? connectorQueryCtx_->expressionEvaluator()->compile(
                    joinConditionExpr)
              : nullptr),
      pool_(connectorQueryCtx_->memoryPool()->shared_from_this()),
      executor_(executor) {
  VELOX_CHECK_NOT_NULL(executor_);
  VELOX_CHECK_LE(outputType_->size(), valueType_->size() + keyType_->size());
  VELOX_CHECK_LE(numEqualJoinKeys_, keyType_->size());
  for (int i = 0; i < numEqualJoinKeys_; ++i) {
    VELOX_CHECK(
        keyType_->childAt(i)->equivalent(*inputType_->childAt(i)),
        "{} vs {}",
        keyType_->toString(),
        inputType_->toString());
  }
  initOutputProjections();
  initConditionProjections();
}

void TestIndexSource::checkNotFailed() {
  if (!error_.empty()) {
    VELOX_FAIL("TestIndexSource failed: {}", error_);
  }
}

std::shared_ptr<connector::IndexSource::LookupResultIterator>
TestIndexSource::lookup(const LookupRequest& request) {
  checkNotFailed();
  const auto numInputRows = request.input->size();
  auto& hashTable = tableHandle_->indexTable()->table;
  auto lookup = std::make_unique<HashLookup>(hashTable->hashers(), pool_.get());
  SelectivityVector activeRows(numInputRows);
  VELOX_CHECK(activeRows.isAllSelected());
  hashTable->prepareForJoinProbe(
      *lookup, request.input, activeRows, /*decodeAndRemoveNulls=*/true);
  lookup->hits.resize(numInputRows);
  std::fill(lookup->hits.data(), lookup->hits.data() + numInputRows, nullptr);
  if (!lookup->rows.empty()) {
    hashTable->joinProbe(*lookup);
  }
  // Update lookup rows to include all input rows as it might be used by left
  // join.
  auto& rows = lookup->rows;
  rows.resize(request.input->size());
  std::iota(rows.begin(), rows.end(), 0);
  return std::make_shared<ResultIterator>(
      this->shared_from_this(),
      request,
      std::move(lookup),
      tableHandle_->asyncLookup() ? executor_ : nullptr);
}

void TestIndexSource::initConditionProjections() {
  if (conditionExprSet_ == nullptr) {
    return;
  }
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  column_index_t outputChannel{0};
  for (const auto& field : conditionExprSet_->distinctFields()) {
    names.push_back(field->name());
    types.push_back(field->type());
    if (inputType_->getChildIdxIfExists(field->name()).has_value()) {
      conditionInputProjections_.emplace_back(
          inputType_->getChildIdx(field->name()), outputChannel++);
      continue;
    }
    conditionTableProjections_.emplace_back(
        keyType_->getChildIdx(field->name()), outputChannel++);
  }
  conditionInputType_ = ROW(std::move(names), std::move(types));
}

void TestIndexSource::initOutputProjections() {
  VELOX_CHECK(lookupOutputProjections_.empty());
  lookupOutputProjections_.reserve(outputType_->size());
  for (auto outputChannel = 0; outputChannel < outputType_->size();
       ++outputChannel) {
    const auto outputName = outputType_->nameOf(outputChannel);
    if (valueType_->containsChild(outputName)) {
      const auto tableValueChannel = valueType_->getChildIdx(outputName);
      // The hash table layout is: index columns, value columns.
      lookupOutputProjections_.emplace_back(
          keyType_->size() + tableValueChannel, outputChannel);
      continue;
    }
    const auto tableKeyChannel = keyType_->getChildIdx(outputName);
    lookupOutputProjections_.emplace_back(tableKeyChannel, outputChannel);
  }
  VELOX_CHECK_EQ(lookupOutputProjections_.size(), outputType_->size());
}

void TestIndexSource::recordCpuTiming(const CpuWallTiming& timing) {
  VELOX_CHECK_EQ(timing.count, 1);
  std::lock_guard<std::mutex> l(mutex_);
  if (timing.wallNanos != 0) {
    addOperatorRuntimeStats(
        IndexLookupJoin::kConnectorLookupWallTime,
        RuntimeCounter(timing.wallNanos, RuntimeCounter::Unit::kNanos),
        runtimeStats_);
    // This is just for testing purpose to check if the runtime stats has been
    // set properly.
    addOperatorRuntimeStats(
        IndexLookupJoin::kClientLookupWaitWallTime,
        RuntimeCounter(timing.wallNanos, RuntimeCounter::Unit::kNanos),
        runtimeStats_);
  }
  if (timing.cpuNanos != 0) {
    // This is just for testing purpose to check if the runtime stats has been
    // set properly.
    addOperatorRuntimeStats(
        IndexLookupJoin::kConnectorResultPrepareTime,
        RuntimeCounter(timing.cpuNanos, RuntimeCounter::Unit::kNanos),
        runtimeStats_);
  }
}

std::unordered_map<std::string, RuntimeMetric> TestIndexSource::runtimeStats() {
  std::lock_guard<std::mutex> l(mutex_);
  return runtimeStats_;
}

TestIndexSource::ResultIterator::ResultIterator(
    std::shared_ptr<TestIndexSource> source,
    const LookupRequest& request,
    std::unique_ptr<HashLookup> lookupResult,
    folly::Executor* executor)
    : source_(std::move(source)),
      request_(request),
      lookupResult_(std::move(lookupResult)),
      executor_(executor) {
  // Initialize the lookup result iterator.
  lookupResultIter_ = std::make_unique<BaseHashTable::JoinResultIterator>(
      std::vector<vector_size_t>{}, 0, /*estimatedRowSize=*/1);
  lookupResultIter_->reset(*lookupResult_);
}

std::optional<std::unique_ptr<connector::IndexSource::LookupResult>>
TestIndexSource::ResultIterator::next(
    vector_size_t size,
    ContinueFuture& future) {
  source_->checkNotFailed();

  if (hasPendingRequest_.exchange(true)) {
    VELOX_FAIL("Only one pending request is allowed at a time");
  }

  if (executor_ && !asyncResult_.has_value()) {
    asyncLookup(size, future);
    return std::nullopt;
  }

  SCOPE_EXIT {
    hasPendingRequest_ = false;
  };
  if (asyncResult_.has_value()) {
    VELOX_CHECK_NOT_NULL(executor_);
    auto result = std::move(asyncResult_.value());
    asyncResult_.reset();
    return result;
  }
  return syncLookup(size);
}

void TestIndexSource::ResultIterator::extractLookupColumns(
    folly::Range<char* const*> rows,
    RowVectorPtr& result) {
  if (result == nullptr) {
    result = BaseVector::create<RowVector>(
        source_->outputType(), rows.size(), source_->pool());
  } else {
    VectorPtr output = std::move(result);
    BaseVector::prepareForReuse(output, rows.size());
    result = std::static_pointer_cast<RowVector>(output);
  }
  VELOX_CHECK_EQ(result->size(), rows.size());

  extractColumns(
      source_->indexTable()->table.get(),
      rows,
      source_->outputProjections(),
      source_->pool_.get(),
      source_->outputType_->children(),
      lookupOutput_->children());
}

void TestIndexSource::ResultIterator::asyncLookup(
    vector_size_t size,
    ContinueFuture& future) {
  VELOX_CHECK_NOT_NULL(executor_);
  VELOX_CHECK(!asyncResult_.has_value());
  VELOX_CHECK(hasPendingRequest_);

  auto [lookupPromise, lookupFuture] =
      makeVeloxContinuePromiseContract("ResultIterator::asyncLookup");
  future = std::move(lookupFuture);
  auto asyncPromise =
      std::make_shared<ContinuePromise>(std::move(lookupPromise));
  executor_->add([this, size, promise = std::move(asyncPromise)]() mutable {
    TestValue::adjust(
        "facebook::velox::exec::test::TestIndexSource::ResultIterator::asyncLookup",
        this);
    VELOX_CHECK(!asyncResult_.has_value());
    VELOX_CHECK(hasPendingRequest_);
    TestValue::adjust(
        "facebook::velox::exec::test::TestIndexSource::ResultIterator::asyncLookup",
        this);
    SCOPE_EXIT {
      hasPendingRequest_ = false;
      promise->setValue();
    };
    asyncResult_ = syncLookup(size);
  });
}

std::unique_ptr<connector::IndexSource::LookupResult>
TestIndexSource::ResultIterator::syncLookup(vector_size_t size) {
  VELOX_CHECK(hasPendingRequest_);
  if (lookupResultIter_->atEnd()) {
    return nullptr;
  }

  CpuWallTiming timing;
  SCOPE_EXIT {
    source_->recordCpuTiming(timing);
  };
  CpuWallTimer timer{timing};
  try {
    TestValue::adjust(
        "facebook::velox::exec::test::TestIndexSource::ResultIterator::syncLookup",
        this);
    initBuffer<char*>(size, outputRowMapping_, rawOutputRowMapping_);
    initBuffer<vector_size_t>(size, inputRowMapping_, rawInputRowMapping_);

    auto numOut = source_->indexTable()->table->listJoinResults(
        *lookupResultIter_,
        /*includeMisses=*/true,
        folly::Range(rawInputRowMapping_, size),
        folly::Range(rawOutputRowMapping_, size),
        // TODO: support max bytes output later.
        /*maxBytes=*/std::numeric_limits<uint64_t>::max());
    outputRowMapping_->setSize(numOut * sizeof(char*));
    inputRowMapping_->setSize(numOut * sizeof(vector_size_t));

    if (numOut == 0) {
      VELOX_CHECK(lookupResultIter_->atEnd());
      return nullptr;
    }

    evalJoinConditions();

    initBuffer<vector_size_t>(numOut, inputHitIndices_, rawInputHitIndices_);
    auto numHits{0};
    for (auto i = 0; i < numOut; ++i) {
      if (rawOutputRowMapping_[i] == nullptr) {
        continue;
      }
      VELOX_CHECK_LE(numHits, i);
      rawOutputRowMapping_[numHits] = rawOutputRowMapping_[i];
      rawInputHitIndices_[numHits] = rawInputRowMapping_[i];
      if (numHits > 0) {
        // Make sure the input hit indices are in ascending order.
        VELOX_CHECK_GE(
            rawInputHitIndices_[numHits], rawInputHitIndices_[numHits - 1]);
      }
      ++numHits;
    }
    outputRowMapping_->setSize(numHits * sizeof(char*));
    inputHitIndices_->setSize(numHits * sizeof(vector_size_t));
    extractLookupColumns(
        folly::Range<char* const*>(rawOutputRowMapping_, numHits),
        lookupOutput_);
    VELOX_CHECK_EQ(lookupOutput_->size(), numHits);
    VELOX_CHECK_EQ(inputHitIndices_->size() / sizeof(vector_size_t), numHits);
    return std::make_unique<LookupResult>(inputHitIndices_, lookupOutput_);
  } catch (const std::exception& e) {
    VELOX_CHECK(source_->error_.empty());
    source_->error_ = e.what();
    return nullptr;
  }
}

void TestIndexSource::ResultIterator::evalJoinConditions() {
  if (source_->conditionExprSet_ == nullptr) {
    return;
  }

  std::lock_guard<std::mutex> l(source_->mutex_);
  const auto conditionInput = createConditionInput();
  source_->connectorQueryCtx_->expressionEvaluator()->evaluate(
      source_->conditionExprSet_.get(),
      source_->conditionFilterInputRows_,
      *conditionInput,
      source_->conditionFilterResult_);
  source_->decodedConditionFilterResult_.decode(
      *source_->conditionFilterResult_, source_->conditionFilterInputRows_);

  const auto numRows = outputRowMapping_->size() / sizeof(char*);
  for (auto row = 0; row < numRows; ++row) {
    if (!joinConditionPassed(row)) {
      rawOutputRowMapping_[row] = nullptr;
    }
  }
}

RowVectorPtr TestIndexSource::ResultIterator::createConditionInput() {
  VELOX_CHECK_EQ(
      inputRowMapping_->size() / sizeof(vector_size_t),
      outputRowMapping_->size() / sizeof(char*));
  const auto numRows = outputRowMapping_->size() / sizeof(char*);
  source_->conditionFilterInputRows_.resize(numRows);
  std::vector<VectorPtr> filterColumns(source_->conditionInputType_->size());
  for (const auto& projection : source_->conditionInputProjections_) {
    request_.input->childAt(projection.inputChannel)->loadedVector();
    filterColumns[projection.outputChannel] = wrapChild(
        numRows,
        inputRowMapping_,
        request_.input->childAt(projection.inputChannel));
  }

  extractColumns(
      source_->indexTable()->table.get(),
      folly::Range<char* const*>(rawOutputRowMapping_, numRows),
      source_->conditionTableProjections_,
      source_->pool_.get(),
      source_->conditionInputType_->children(),
      filterColumns);

  return std::make_shared<RowVector>(
      source_->pool_.get(),
      source_->conditionInputType_,
      nullptr,
      numRows,
      std::move(filterColumns));
}

TestIndexConnector::TestIndexConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> /*unused*/,
    folly::Executor* executor)
    : Connector(id), executor_(executor) {}

std::shared_ptr<connector::IndexSource> TestIndexConnector::createIndexSource(
    const RowTypePtr& inputType,
    size_t numJoinKeys,
    const std::vector<core::IndexLookupConditionPtr>& joinConditions,
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    connector::ConnectorQueryCtx* connectorQueryCtx) {
  VELOX_CHECK_GE(inputType->size(), numJoinKeys + joinConditions.size());
  auto testIndexTableHandle =
      std::dynamic_pointer_cast<TestIndexTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(testIndexTableHandle);
  const auto& indexTable = testIndexTableHandle->indexTable();
  auto joinConditionExpr =
      toJoinConditionExpr(joinConditions, indexTable, inputType, columnHandles);
  return std::make_shared<TestIndexSource>(
      inputType,
      outputType,
      numJoinKeys,
      std::move(joinConditionExpr),
      std::move(testIndexTableHandle),
      connectorQueryCtx,
      executor_);
}
} // namespace facebook::velox::exec::test
