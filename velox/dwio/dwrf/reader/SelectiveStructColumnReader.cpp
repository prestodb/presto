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

#include "velox/dwio/dwrf/reader/SelectiveStructColumnReader.h"
#include "velox/dwio/dwrf/reader/ColumnLoader.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

SelectiveStructColumnReader::SelectiveStructColumnReader(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    FlatMapContext flatMapContext)
    : SelectiveColumnReader(
          dataType,
          stripe,
          scanSpec,
          dataType->type,
          std::move(flatMapContext)),
      requestedType_{requestedType},
      debugString_(getExceptionContext().message()) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  DWIO_ENSURE_EQ(encodingKey.node, dataType->id, "working on the same node");
  auto encoding = static_cast<int64_t>(stripe.getEncoding(encodingKey).kind());
  DWIO_ENSURE_EQ(
      encoding,
      proto::ColumnEncoding_Kind_DIRECT,
      "Unknown encoding for StructColumnReader");

  const auto& cs = stripe.getColumnSelector();
  auto& childSpecs = scanSpec->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i].get();
    if (childSpec->isConstant()) {
      continue;
    }
    auto childDataType = nodeType_->childByName(childSpec->fieldName());
    auto childRequestedType =
        requestedType_->childByName(childSpec->fieldName());
    VELOX_CHECK(cs.shouldReadNode(childDataType->id));
    children_.push_back(SelectiveColumnReader::build(
        childRequestedType,
        childDataType,
        stripe,
        childSpec,
        FlatMapContext{encodingKey.sequence, nullptr}));
    childSpec->setSubscript(children_.size() - 1);
  }
}

std::vector<uint32_t> SelectiveStructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const StatsContext& context) const {
  auto stridesToSkip =
      SelectiveColumnReader::filterRowGroups(rowGroupSize, context);
  for (const auto& child : children_) {
    auto childStridesToSkip = child->filterRowGroups(rowGroupSize, context);
    if (stridesToSkip.empty()) {
      stridesToSkip = std::move(childStridesToSkip);
    } else {
      std::vector<uint32_t> merged;
      merged.reserve(childStridesToSkip.size() + stridesToSkip.size());
      std::merge(
          childStridesToSkip.begin(),
          childStridesToSkip.end(),
          stridesToSkip.begin(),
          stridesToSkip.end(),
          std::back_inserter(merged));
      stridesToSkip = std::move(merged);
    }
  }
  return stridesToSkip;
}

uint64_t SelectiveStructColumnReader::skip(uint64_t numValues) {
  auto numNonNulls = SelectiveColumnReader::skip(numValues);
  // 'readOffset_' of struct child readers is aligned with
  // 'readOffset_' of the struct. The child readers may have fewer
  // values since there is no value in children where the struct is
  // null. But because struct nulls are injected as nulls in child
  // readers, it is practical to keep the row numbers in terms of the
  // enclosing struct.
  //
  // Setting the 'readOffset_' in children is recursive so that nested
  // nullable structs, where inner structs may advance less because of
  // nulls above them still end up on the same row in terms of top
  // level rows.
  for (auto& child : children_) {
    if (child) {
      child->skip(numNonNulls);
      child->setReadOffsetRecursive(child->readOffset() + numValues);
    }
  }
  return numValues;
}

void SelectiveStructColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  VELOX_CHECK(!incomingNulls, "next may only be called for the root reader.");
  if (children_.empty()) {
    // no readers
    // This can be either count(*) query or a query that select only
    // constant columns (partition keys or columns missing from an old file
    // due to schema evolution)
    result->resize(numValues);

    auto resultRowVector = std::dynamic_pointer_cast<RowVector>(result);
    auto& childSpecs = scanSpec_->children();
    for (auto& childSpec : childSpecs) {
      VELOX_CHECK(childSpec->isConstant());
      auto channel = childSpec->channel();
      resultRowVector->childAt(channel) =
          BaseVector::wrapInConstant(numValues, 0, childSpec->constantValue());
    }
    return;
  }
  auto oldSize = rows_.size();
  rows_.resize(numValues);
  if (numValues > oldSize) {
    std::iota(&rows_[oldSize], &rows_[rows_.size()], oldSize);
  }
  read(readOffset_, rows_, nullptr);
  getValues(outputRows(), &result);
}

void SelectiveStructColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  numReads_ = scanSpec_->newRead();
  prepareRead<char>(offset, rows, incomingNulls);
  RowSet activeRows = rows;
  auto& childSpecs = scanSpec_->children();
  const uint64_t* structNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  bool hasFilter = false;
  assert(!children_.empty());
  for (size_t i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (childSpec->isConstant()) {
      continue;
    }
    auto fieldIndex = childSpec->subscript();
    auto reader = children_.at(fieldIndex).get();
    if (reader->isTopLevel() && childSpec->projectOut() &&
        !childSpec->filter() && !childSpec->extractValues()) {
      // Will make a LazyVector.
      continue;
    }
    advanceFieldReader(reader, offset);
    if (childSpec->filter()) {
      hasFilter = true;
      {
        SelectivityTimer timer(childSpec->selectivity(), activeRows.size());

        reader->resetInitTimeClocks();
        reader->read(offset, activeRows, structNulls);

        // Exclude initialization time.
        timer.subtract(reader->initTimeClocks());

        activeRows = reader->outputRows();
        childSpec->selectivity().addOutput(activeRows.size());
      }
      if (activeRows.empty()) {
        break;
      }
    } else {
      reader->read(offset, activeRows, structNulls);
    }
  }
  if (hasFilter) {
    setOutputRows(activeRows);
  }
  lazyVectorReadOffset_ = offset;
  readOffset_ = offset + rows.back() + 1;
}

namespace {
//   Recursively makes empty RowVectors for positions in 'children'
//   where the corresponding child type in 'rowType' is a row. The
//   reader expects RowVector outputs to be initialized so that the
//   content corresponds to the query schema regardless of the file
//   schema. An empty RowVector can have nullptr for all its non-row
//   children.
void fillRowVectorChildren(
    memory::MemoryPool& pool,
    const RowType& rowType,
    std::vector<VectorPtr>& children) {
  for (auto i = 0; i < children.size(); ++i) {
    const auto& type = rowType.childAt(i);
    if (type->isRow()) {
      std::vector<VectorPtr> innerChildren(type->size());
      fillRowVectorChildren(pool, type->asRow(), innerChildren);
      children[i] = std::make_shared<RowVector>(
          &pool, type, nullptr, 0, std::move(innerChildren));
    }
  }
}
} // namespace

void SelectiveStructColumnReader::getValues(RowSet rows, VectorPtr* result) {
  assert(!children_.empty());
  VELOX_CHECK(
      *result != nullptr,
      "SelectiveStructColumnReader expects a non-null result");
  VELOX_CHECK(
      result->get()->type()->isRow(),
      "Struct reader expects a result of type ROW.");

  auto resultRow = static_cast<RowVector*>(result->get());
  if (!result->unique()) {
    std::vector<VectorPtr> children(resultRow->children().size());
    fillRowVectorChildren(
        *resultRow->pool(), resultRow->type()->asRow(), children);
    *result = std::make_unique<RowVector>(
        resultRow->pool(),
        resultRow->type(),
        BufferPtr(nullptr),
        0,
        std::move(children));
    resultRow = static_cast<RowVector*>(result->get());
  }
  resultRow->resize(rows.size());
  if (!rows.size()) {
    return;
  }
  if (nullsInReadRange_) {
    auto readerNulls = nullsInReadRange_->as<uint64_t>();
    auto nulls = resultRow->mutableNulls(rows.size())->asMutable<uint64_t>();
    for (size_t i = 0; i < rows.size(); ++i) {
      bits::setBit(nulls, i, bits::isBitSet(readerNulls, rows[i]));
    }
  } else {
    resultRow->clearNulls(0, rows.size());
  }
  bool lazyPrepared = false;
  auto& childSpecs = scanSpec_->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (!childSpec->projectOut()) {
      continue;
    }
    auto index = childSpec->subscript();
    auto channel = childSpec->channel();
    if (childSpec->isConstant()) {
      resultRow->childAt(channel) = BaseVector::wrapInConstant(
          rows.size(), 0, childSpec->constantValue());
    } else {
      if (!childSpec->extractValues() && !childSpec->filter() &&
          children_[index]->isTopLevel()) {
        // LazyVector result.
        if (!lazyPrepared) {
          if (rows.size() != outputRows_.size()) {
            setOutputRows(rows);
          }
          lazyPrepared = true;
        }
        resultRow->childAt(channel) = std::make_shared<LazyVector>(
            &memoryPool_,
            resultRow->type()->childAt(channel),
            rows.size(),
            std::make_unique<ColumnLoader>(
                this, children_[index].get(), numReads_));
      } else {
        children_[index]->getValues(rows, &resultRow->childAt(channel));
      }
    }
  }
}

} // namespace facebook::velox::dwrf
