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

namespace facebook::velox::exec::test {

TestIndexSource::TestIndexSource(
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    const std::shared_ptr<TestIndexTableHandle>& tableHandle,
    connector::ConnectorQueryCtx* connectorQueryCtx,
    folly::Executor* executor)
    : tableHandle_(tableHandle),
      inputType_(inputType),
      outputType_(outputType),
      keyType_(tableHandle_->indexTable()->keyType),
      valueType_(tableHandle_->indexTable()->dataType),
      connectorQueryCtx_(connectorQueryCtx),
      pool_(connectorQueryCtx_->memoryPool()->shared_from_this()),
      executor_(executor) {
  VELOX_CHECK_NOT_NULL(executor_);
  VELOX_CHECK(inputType_->equivalent(*keyType_));
  VELOX_CHECK_LE(outputType_->size(), valueType_->size());
  // As for now, we only support to read data columns.
  for (int i = 0; i < outputType_->size(); ++i) {
    VELOX_CHECK(outputType_->childAt(i)->equivalent(
        *valueType_->findChild(outputType_->nameOf(i))));
  }
  // As for now we require the all the index columns to be specified in lookup
  // input.
  for (int i = 0; i < keyType_->size(); ++i) {
    VELOX_CHECK(keyType_->childAt(i)->equivalent(*inputType_->childAt(i)));
  }
  initOutputProjections();
}

std::unique_ptr<connector::IndexSource::LookupResultIterator>
TestIndexSource::lookup(const LookupRequest& request) {
  const auto numInputRows = request.input->size();
  auto& hashTable = tableHandle_->indexTable()->table;
  auto lookup = std::make_unique<HashLookup>(hashTable->hashers());
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
  return std::make_unique<ResultIterator>(
      this->shared_from_this(),
      request,
      std::move(lookup),
      tableHandle_->asyncLookup() ? executor_ : nullptr);
}

void TestIndexSource::initOutputProjections() {
  VELOX_CHECK(lookupOutputProjections_.empty());
  lookupOutputProjections_.reserve(outputType_->size());
  for (auto outputChannel = 0; outputChannel < outputType_->size();
       ++outputChannel) {
    auto tableValueChannel =
        valueType_->getChildIdx(outputType_->nameOf(outputChannel));
    // The hash table layout is: index columns, value columns.
    lookupOutputProjections_.emplace_back(
        keyType_->size() + tableValueChannel, outputChannel);
  }
  VELOX_CHECK_EQ(lookupOutputProjections_.size(), outputType_->size());
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
    return std::move(asyncResult_.value());
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

  for (auto projection : source_->outputProjections()) {
    const auto resultChannel = projection.outputChannel;
    auto& child = result->childAt(resultChannel);
    source_->indexTable()->table->extractColumn(
        rows, projection.inputChannel, child);
  }
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
    VELOX_CHECK(!asyncResult_.has_value());
    VELOX_CHECK(hasPendingRequest_);
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

  initBuffer<char*>(size, outputRowMapping_, rawOutputRowMapping_);
  initBuffer<vector_size_t>(size, inputRowMapping_, rawInputRowMapping_);

  const auto numOut = source_->indexTable()->table->listJoinResults(
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
      folly::Range<char* const*>(rawOutputRowMapping_, numHits), lookupOutput_);
  VELOX_CHECK_EQ(lookupOutput_->size(), numHits);
  VELOX_CHECK_EQ(inputHitIndices_->size() / sizeof(vector_size_t), numHits);
  return std::make_unique<LookupResult>(inputHitIndices_, lookupOutput_);
}

TestIndexConnector::TestIndexConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> config,
    folly::Executor* executor)
    : Connector(id), executor_(executor) {}

std::shared_ptr<connector::IndexSource> TestIndexConnector::createIndexSource(
    const RowTypePtr& inputType,
    size_t numJoinKeys,
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& joinConditions,
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    connector::ConnectorQueryCtx* connectorQueryCtx) {
  VELOX_CHECK(
      joinConditions.empty(),
      "{} doesn't support join conditions",
      kTestIndexConnectorName);
  VELOX_CHECK_EQ(inputType->size(), numJoinKeys);
  auto testIndexTableHandle =
      std::dynamic_pointer_cast<TestIndexTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(testIndexTableHandle);
  return std::make_shared<TestIndexSource>(
      inputType,
      outputType,
      std::move(testIndexTableHandle),
      connectorQueryCtx,
      executor_);
}
} // namespace facebook::velox::exec::test
