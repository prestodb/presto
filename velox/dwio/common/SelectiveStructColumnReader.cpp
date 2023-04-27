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

#include "velox/dwio/common/SelectiveStructColumnReader.h"

#include "velox/dwio/common/ColumnLoader.h"

namespace facebook::velox::dwio::common {

void SelectiveStructColumnReaderBase::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    FormatData::FilterRowGroupsResult& result) const {
  SelectiveColumnReader::filterRowGroups(rowGroupSize, context, result);
  for (const auto& child : children_) {
    child->filterRowGroups(rowGroupSize, context, result);
  }
}

uint64_t SelectiveStructColumnReaderBase::skip(uint64_t numValues) {
  auto numNonNulls = formatData_->skipNulls(numValues);
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

void SelectiveStructColumnReaderBase::next(
    uint64_t numValues,
    VectorPtr& result,
    const Mutation* mutation) {
  if (children_.empty()) {
    if (mutation && mutation->deletedRows) {
      numValues -= bits::countBits(mutation->deletedRows, 0, numValues);
    }

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
  mutation_ = mutation;
  hasMutation_ = mutation && mutation->deletedRows;
  read(readOffset_, rows_, nullptr);
  getValues(outputRows(), &result);
}

void SelectiveStructColumnReaderBase::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  numReads_ = scanSpec_->newRead();
  prepareRead<char>(offset, rows, incomingNulls);
  RowSet activeRows = rows;
  if (hasMutation_) {
    // We handle the mutation after prepareRead so that output rows and format
    // specific initializations (e.g. RepDef in Parquet) are done properly.
    VELOX_DCHECK(!nullsInReadRange_, "Only top level can have mutation");
    VELOX_DCHECK_EQ(
        rows.back(), rows.size() - 1, "Top level should have a dense row set");
    bits::forEachUnsetBit(
        mutation_->deletedRows, 0, rows.back() + 1, [&](auto i) {
          addOutputRow(i);
        });
    if (outputRows_.empty()) {
      readOffset_ = offset + rows.back() + 1;
      return;
    }
    activeRows = outputRows_;
  }
  const uint64_t* structNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  // a struct reader may have a null/non-null filter
  if (scanSpec_->filter()) {
    auto kind = scanSpec_->filter()->kind();
    VELOX_CHECK(
        kind == velox::common::FilterKind::kIsNull ||
        kind == velox::common::FilterKind::kIsNotNull);
    filterNulls<int32_t>(
        activeRows, kind == velox::common::FilterKind::kIsNull, false);
    if (outputRows_.empty()) {
      recordParentNullsInChildren(offset, rows);
      return;
    }
    activeRows = outputRows_;
  }

  assert(!children_.empty());
  auto& childSpecs = scanSpec_->children();
  for (size_t i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (childSpec->isConstant()) {
      continue;
    }
    auto fieldIndex = childSpec->subscript();
    auto reader = children_.at(fieldIndex);
    if (reader->isTopLevel() && childSpec->projectOut() &&
        !childSpec->hasFilter() && !childSpec->extractValues()) {
      // Will make a LazyVector.
      continue;
    }
    advanceFieldReader(reader, offset);
    if (childSpec->hasFilter()) {
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
  // If this adds nulls, the field readers will miss a value for each null added
  // here.
  recordParentNullsInChildren(offset, rows);

  if (scanSpec_->hasFilter()) {
    setOutputRows(activeRows);
  }
  lazyVectorReadOffset_ = offset;
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveStructColumnReaderBase::recordParentNullsInChildren(
    vector_size_t offset,
    RowSet rows) {
  if (formatData_->parentNullsInLeaves()) {
    return;
  }
  auto& childSpecs = scanSpec_->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (childSpec->isConstant()) {
      continue;
    }
    auto fieldIndex = childSpec->subscript();
    auto reader = children_.at(fieldIndex);
    reader->addParentNulls(
        offset,
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr,
        rows);
  }
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

void SelectiveStructColumnReaderBase::getValues(
    RowSet rows,
    VectorPtr* result) {
  assert(!children_.empty());
  VELOX_CHECK(
      *result != nullptr,
      "SelectiveStructColumnReaderBase expects a non-null result");
  VELOX_CHECK(
      result->get()->type()->isRow(),
      "Struct reader expects a result of type ROW.");
  auto& rowType = result->get()->type()->asRow();
  if (!result->unique() || result->get()->isLazy()) {
    std::vector<VectorPtr> children(rowType.size());
    fillRowVectorChildren(*result->get()->pool(), rowType, children);
    *result = std::make_unique<RowVector>(
        result->get()->pool(),
        result->get()->type(),
        nullptr,
        0,
        std::move(children));
  }
  auto* resultRow = static_cast<RowVector*>(result->get());
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
      if (!childSpec->extractValues() && !childSpec->hasFilter() &&
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
            std::make_unique<ColumnLoader>(this, children_[index], numReads_));
      } else {
        children_[index]->getValues(rows, &resultRow->childAt(channel));
      }
    }
  }
}

} // namespace facebook::velox::dwio::common
