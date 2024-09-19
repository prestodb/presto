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

#include "velox/common/process/TraceContext.h"
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

void SelectiveStructColumnReaderBase::fillOutputRowsFromMutation(
    vector_size_t size) {
  if (mutation_->deletedRows) {
    bits::forEachUnsetBit(mutation_->deletedRows, 0, size, [&](auto i) {
      if ((mutation_->randomSkip == nullptr) ||
          mutation_->randomSkip->testOne()) {
        addOutputRow(i);
      }
    });
  } else {
    VELOX_CHECK_NOT_NULL(mutation_->randomSkip);
    vector_size_t i = 0;
    while (i < size) {
      const auto skip = mutation_->randomSkip->nextSkip();
      const auto remaining = size - i;
      if (skip >= remaining) {
        mutation_->randomSkip->consume(remaining);
        break;
      }
      i += skip;
      addOutputRow(i++);
      mutation_->randomSkip->consume(skip + 1);
    }
  }
}

namespace {

bool testFilterOnConstant(const velox::common::ScanSpec& spec) {
  if (spec.isConstant() && !spec.constantValue()->isNullAt(0)) {
    // Non-null constant is known value during split scheduling and filters on
    // them should not be handled at execution level.
    return true;
  }
  // Check filter on missing field.
  return !spec.hasFilter() || spec.testNull();
}

} // namespace

void SelectiveStructColumnReaderBase::next(
    uint64_t numValues,
    VectorPtr& result,
    const Mutation* mutation) {
  process::TraceContext trace("SelectiveStructColumnReaderBase::next");
  mutation_ = mutation;
  hasDeletion_ = common::hasDeletion(mutation);
  if (children_.empty()) {
    if (hasDeletion_) {
      if (fillMutatedOutputRows_) {
        fillOutputRowsFromMutation(numValues);
        numValues = outputRows_.size();
      } else {
        if (mutation->deletedRows) {
          numValues -= bits::countBits(mutation->deletedRows, 0, numValues);
        }
        if (mutation->randomSkip) {
          numValues *= mutation->randomSkip->sampleRate();
        }
      }
    }
    for (const auto& childSpec : scanSpec_->children()) {
      if (isChildConstant(*childSpec) && !testFilterOnConstant(*childSpec)) {
        numValues = 0;
        break;
      }
    }

    // No readers
    // This can be either count(*) query or a query that select only
    // constant columns (partition keys or columns missing from an old file
    // due to schema evolution) or row number column.
    auto resultRowVector = std::dynamic_pointer_cast<RowVector>(result);
    resultRowVector->unsafeResize(numValues);

    for (auto& childSpec : scanSpec_->children()) {
      VELOX_CHECK(childSpec->isConstant() || childSpec->isExplicitRowNumber());
      if (childSpec->projectOut() && childSpec->isConstant()) {
        const auto channel = childSpec->channel();
        resultRowVector->childAt(channel) = BaseVector::wrapInConstant(
            numValues, 0, childSpec->constantValue());
      }
    }
    return;
  }

  const auto oldSize = rows_.size();
  rows_.resize(numValues);
  if (numValues > oldSize) {
    std::iota(&rows_[oldSize], &rows_[rows_.size()], oldSize);
  }
  read(readOffset_, rows_, nullptr);
  getValues(outputRows(), &result);
}

void SelectiveStructColumnReaderBase::read(
    vector_size_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  numReads_ = scanSpec_->newRead();
  prepareRead<char>(offset, rows, incomingNulls);
  RowSet activeRows = rows;
  if (hasDeletion_) {
    // We handle the mutation after prepareRead so that output rows and format
    // specific initializations (e.g. RepDef in Parquet) are done properly.
    VELOX_DCHECK_NULL(nullsInReadRange_, "Only top level can have mutation");
    VELOX_DCHECK_EQ(
        rows.back(), rows.size() - 1, "Top level should have a dense row set");
    fillOutputRowsFromMutation(rows.size());
    if (outputRows_.empty()) {
      readOffset_ = offset + rows.back() + 1;
      return;
    }
    activeRows = outputRows_;
  }

  const uint64_t* structNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  // A struct reader may have a null/non-null filter
  if (scanSpec_->filter()) {
    const auto kind = scanSpec_->filter()->kind();
    VELOX_CHECK(
        kind == velox::common::FilterKind::kIsNull ||
        kind == velox::common::FilterKind::kIsNotNull);
    filterNulls<int32_t>(
        activeRows, kind == velox::common::FilterKind::kIsNull, false);
    if (outputRows_.empty()) {
      recordParentNullsInChildren(offset, rows);
      lazyVectorReadOffset_ = offset;
      readOffset_ = offset + rows.back() + 1;
      return;
    }
    activeRows = outputRows_;
  }

  const auto& childSpecs = scanSpec_->children();
  VELOX_CHECK(!childSpecs.empty());
  for (size_t i = 0; i < childSpecs.size(); ++i) {
    const auto& childSpec = childSpecs[i];
    VELOX_TRACE_HISTORY_PUSH("read %s", childSpec->fieldName().c_str());
    if (isChildConstant(*childSpec)) {
      if (!testFilterOnConstant(*childSpec)) {
        activeRows = {};
        break;
      }
      continue;
    }

    if (childSpec->isExplicitRowNumber()) {
      continue;
    }

    const auto fieldIndex = childSpec->subscript();
    auto* reader = children_.at(fieldIndex);
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
    const RowSet& rows) {
  if (formatData_->parentNullsInLeaves()) {
    return;
  }
  const auto& childSpecs = scanSpec_->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    const auto& childSpec = childSpecs[i];
    if (isChildConstant(*childSpec)) {
      continue;
    }
    if (childSpec->isExplicitRowNumber()) {
      continue;
    }

    const auto fieldIndex = childSpec->subscript();
    auto* reader = children_.at(fieldIndex);
    reader->addParentNulls(
        offset,
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr,
        rows);
  }
}

bool SelectiveStructColumnReaderBase::isChildConstant(
    const velox::common::ScanSpec& childSpec) const {
  // Returns true if the child has a constant set in the ScanSpec, or if the
  // file doesn't have this child (in which case it will be treated as null).
  return childSpec.isConstant() ||
      // The below check is trying to determine if this is a missing field in a
      // struct that should be constant null.
      (!isRoot_ && // If we're in the root struct channel is meaningless in this
                   // context and it will be a null constant anyway if it's
                   // missing.
       childSpec.channel() !=
           velox::common::ScanSpec::kNoChannel && // This can happen if there's
                                                  // a filter on a subfield of a
                                                  // row type that doesn't exist
                                                  // in the output.
       fileType_->type()->kind() !=
           TypeKind::MAP && // If this is the case it means this is a flat map,
                            // so it can't have "missing" fields.
       childSpec.channel() >= fileType_->size());
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

VectorPtr tryReuseResult(const VectorPtr& result) {
  if (!result.unique()) {
    return nullptr;
  }
  switch (result->encoding()) {
    case VectorEncoding::Simple::ROW:
      // Do not call prepareForReuse as it would throw away constant vectors
      // that can be reused.  Reusability of children should be checked in
      // getValues of child readers (all readers other than struct are
      // recreating the result vector on each batch currently, so no issue with
      // reusability now).
      result->reuseNulls();
      result->clearContainingLazyAndWrapped();
      return result;
    case VectorEncoding::Simple::LAZY: {
      auto& lazy = static_cast<const LazyVector&>(*result);
      if (!lazy.isLoaded()) {
        return nullptr;
      }
      return tryReuseResult(lazy.loadedVectorShared());
    }
    case VectorEncoding::Simple::DICTIONARY:
      return tryReuseResult(result->valueVector());
    default:
      return nullptr;
  }
}

void setConstantField(
    const VectorPtr& constant,
    vector_size_t size,
    VectorPtr& field) {
  if (field && field->isConstantEncoding() && field.unique() &&
      field->size() > 0 && field->equalValueAt(constant.get(), 0, 0)) {
    field->resize(size);
  } else {
    field = BaseVector::wrapInConstant(size, 0, constant);
  }
}

void setNullField(
    vector_size_t size,
    VectorPtr& field,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  if (field && field->isConstantEncoding() && field.unique() &&
      field->size() > 0 && field->isNullAt(0)) {
    field->resize(size);
  } else {
    field = BaseVector::createNullConstant(type, size, pool);
  }
}

} // namespace

void SelectiveStructColumnReaderBase::getValues(
    const RowSet& rows,
    VectorPtr* result) {
  VELOX_CHECK(!scanSpec_->children().empty());
  VELOX_CHECK_NOT_NULL(
      *result, "SelectiveStructColumnReaderBase expects a non-null result");
  VELOX_CHECK(
      result->get()->type()->isRow(),
      "Struct reader expects a result of type ROW.");
  auto& rowType = result->get()->type()->asRow();
  if (auto reused = tryReuseResult(*result)) {
    *result = std::move(reused);
  } else {
    VLOG(1) << "Reallocating result row vector with " << rowType.size()
            << " children";
    std::vector<VectorPtr> children(rowType.size());
    fillRowVectorChildren(*result->get()->pool(), rowType, children);
    *result = std::make_shared<RowVector>(
        result->get()->pool(),
        result->get()->type(),
        nullptr,
        0,
        std::move(children));
  }

  auto* resultRow = static_cast<RowVector*>(result->get());
  resultRow->unsafeResize(rows.size());
  if (rows.empty()) {
    return;
  }

  setComplexNulls(rows, *result);
  bool lazyPrepared = false;
  for (const auto& childSpec : scanSpec_->children()) {
    VELOX_TRACE_HISTORY_PUSH("getValues %s", childSpec->fieldName().c_str());
    if (!childSpec->projectOut()) {
      continue;
    }

    if (childSpec->isExplicitRowNumber()) {
      // Row number data is generated after, skip data loading for it.
      continue;
    }
    const auto channel = childSpec->channel();
    auto& childResult = resultRow->childAt(channel);
    if (childSpec->isConstant()) {
      setConstantField(childSpec->constantValue(), rows.size(), childResult);
      continue;
    }

    const auto index = childSpec->subscript();
    // Set missing fields to be null constant, if we're in the top level struct
    // missing columns should already be a null constant from the check above.
    if (index == kConstantChildSpecSubscript) {
      const auto& childType = rowType.childAt(channel);
      setNullField(rows.size(), childResult, childType, resultRow->pool());
      continue;
    }

    if (childSpec->extractValues() || childSpec->hasFilter() ||
        !children_[index]->isTopLevel()) {
      children_[index]->getValues(rows, &childResult);
      continue;
    }

    // LazyVector result.
    if (!lazyPrepared) {
      if (rows.size() != outputRows_.size()) {
        setOutputRows(rows);
      }
      lazyPrepared = true;
    }
    auto lazyLoader =
        std::make_unique<ColumnLoader>(this, children_[index], numReads_);
    if (childResult && childResult->isLazy() && childResult.unique()) {
      static_cast<LazyVector&>(*childResult)
          .reset(std::move(lazyLoader), rows.size());
    } else {
      childResult = std::make_shared<LazyVector>(
          memoryPool_,
          resultRow->type()->childAt(channel),
          rows.size(),
          std::move(lazyLoader),
          std::move(childResult));
    }
  }
  resultRow->updateContainsLazyNotLoaded();
}

namespace detail {

#if XSIMD_WITH_AVX2

xsimd::batch<int32_t> bitsToInt32s[256];

__attribute__((constructor)) void initBitsToInt32s() {
  for (int i = 0; i < 256; ++i) {
    int32_t data[8];
    for (int j = 0; j < 8; ++j) {
      data[j] = bits::isBitSet(&i, j);
    }
    bitsToInt32s[i] = xsimd::load_unaligned(data);
  }
}

#endif

} // namespace detail

} // namespace facebook::velox::dwio::common
