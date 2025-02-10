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

// Recursively makes empty RowVectors for positions in 'children' where the
// corresponding child type in 'rowType' is a row. The reader expects RowVector
// outputs to be initialized so that the content corresponds to the query schema
// regardless of the file schema. An empty RowVector can have nullptr for all
// its non-row children.
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
  if (result.use_count() != 1) {
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

void prepareResult(VectorPtr& result) {
  if (auto reused = tryReuseResult(result)) {
    result = std::move(reused);
    return;
  }
  auto& rowType = result->type()->asRow();
  VLOG(1) << "Reallocating result row vector with " << rowType.size()
          << " children";
  std::vector<VectorPtr> children(rowType.size());
  fillRowVectorChildren(*result->pool(), rowType, children);
  result = std::make_shared<RowVector>(
      result->pool(), result->type(), nullptr, 0, std::move(children));
}

void setConstantField(
    const VectorPtr& constant,
    vector_size_t size,
    VectorPtr& field) {
  if (field && field->isConstantEncoding() && field.use_count() == 1 &&
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
  if (field && field->isConstantEncoding() && field.use_count() == 1 &&
      field->size() > 0 && field->isNullAt(0)) {
    field->resize(size);
  } else {
    field = BaseVector::createNullConstant(type, size, pool);
  }
}

void setRowNumberField(
    int64_t offset,
    const RowSet& rows,
    memory::MemoryPool* pool,
    VectorPtr& field) {
  VELOX_CHECK_GE(offset, 0);
  if (field && BaseVector::isVectorWritable(field)) {
    VELOX_CHECK(
        *field->type() == *BIGINT(),
        "Unexpected row number type: {}",
        field->type()->toString());
    field->clearAllNulls();
    field->resize(rows.size());
  } else {
    field = std::make_shared<FlatVector<int64_t>>(
        pool,
        BIGINT(),
        nullptr,
        rows.size(),
        AlignedBuffer::allocate<int64_t>(rows.size(), pool),
        std::vector<BufferPtr>());
  }
  auto* values = field->asChecked<FlatVector<int64_t>>()->mutableRawValues();
  for (int i = 0; i < rows.size(); ++i) {
    values[i] = offset + rows[i];
  }
}

void setCompositeField(
    const velox::common::ScanSpec& spec,
    int64_t offset,
    const RowSet& rows,
    memory::MemoryPool* pool,
    VectorPtr& field) {
  VELOX_CHECK(field && field->type()->isRow());
  prepareResult(field);
  auto* rowField = field->asChecked<RowVector>();
  rowField->clearAllNulls();
  rowField->unsafeResize(rows.size());
  for (auto& child : spec.children()) {
    auto& childField = rowField->childAt(child->channel());
    switch (child->columnType()) {
      case velox::common::ScanSpec::ColumnType::kRegular:
        VELOX_CHECK(child->isConstant());
        setConstantField(child->constantValue(), rows.size(), childField);
        break;
      case velox::common::ScanSpec::ColumnType::kRowIndex:
        setRowNumberField(offset, rows, pool, childField);
        break;
      case velox::common::ScanSpec::ColumnType::kComposite:
        setCompositeField(*child, offset, rows, pool, childField);
        break;
    }
  }
}

void setLazyField(
    std::unique_ptr<VectorLoader> loader,
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool,
    VectorPtr& result) {
  if (result && result->isLazy() && result.use_count() == 1) {
    static_cast<LazyVector&>(*result).reset(std::move(loader), size);
  } else {
    result = std::make_shared<LazyVector>(
        pool, type, size, std::move(loader), std::move(result));
  }
}

} // namespace

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

void SelectiveStructColumnReaderBase::next(
    uint64_t numValues,
    VectorPtr& result,
    const Mutation* mutation) {
  process::TraceContext trace("SelectiveStructColumnReaderBase::next");
  mutation_ = mutation;
  hasDeletion_ = common::hasDeletion(mutation);
  const RowSet rows(iota(numValues, rows_), numValues);

  if (!children_.empty()) {
    read(readOffset_, rows, nullptr);
    getValues(outputRows(), &result);
    return;
  }

  // No child readers.  This can be either count(*) query or a query that select
  // only constant columns (partition keys or columns missing from an old file
  // due to schema evolution) or columns not from file (e.g. row numbers).
  inputRows_ = rows;
  outputRows_.clear();
  if (hasDeletion_) {
    fillOutputRowsFromMutation(numValues);
    numValues = outputRows_.size();
  }
  for (const auto& childSpec : scanSpec_->children()) {
    if (isChildConstant(*childSpec) && !testFilterOnConstant(*childSpec)) {
      outputRows_.clear();
      numValues = 0;
      break;
    }
  }
  prepareResult(result);
  auto* resultRowVector = result->asChecked<RowVector>();
  resultRowVector->unsafeResize(numValues);
  for (auto& childSpec : scanSpec_->children()) {
    if (!childSpec->projectOut()) {
      continue;
    }
    auto& childField = resultRowVector->childAt(childSpec->channel());
    if (childSpec->isConstant()) {
      childField =
          BaseVector::wrapInConstant(numValues, 0, childSpec->constantValue());
      if (childSpec->deltaUpdate()) {
        VELOX_CHECK(!childSpec->hasFilter());
        childSpec->deltaUpdate()->update(rows, childField);
      }
    } else if (
        childSpec->columnType() ==
        velox::common::ScanSpec::ColumnType::kRowIndex) {
      VELOX_CHECK(!childSpec->hasFilter());
      setRowNumberField(
          currentRowNumber_, outputRows(), resultRowVector->pool(), childField);
    } else if (
        childSpec->columnType() ==
        velox::common::ScanSpec::ColumnType::kComposite) {
      VELOX_CHECK(!childSpec->hasFilter());
      setCompositeField(
          *childSpec,
          currentRowNumber_,
          outputRows(),
          resultRowVector->pool(),
          childField);
    } else {
      VELOX_UNREACHABLE();
    }
  }
}

void SelectiveStructColumnReaderBase::read(
    int64_t offset,
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

    if (childSpec->deltaUpdate()) {
      // Will make LazyVector.
      continue;
    }

    if (isChildConstant(*childSpec)) {
      if (!testFilterOnConstant(*childSpec)) {
        activeRows = {};
        break;
      }
      continue;
    }

    if (!childSpec->readFromFile()) {
      VELOX_CHECK(!childSpec->hasFilter());
      continue;
    }

    const auto fieldIndex = childSpec->subscript();
    auto* reader = children_.at(fieldIndex);
    if (reader->isTopLevel() && childSpec->projectOut() &&
        !childSpec->hasFilter()) {
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
    int64_t offset,
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
    if (!childSpec->readFromFile()) {
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

bool SelectiveStructColumnReaderBase::isChildMissing(
    const velox::common::ScanSpec& childSpec) const {
  return
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
  prepareResult(*result);
  auto* resultRow = static_cast<RowVector*>(result->get());
  resultRow->unsafeResize(rows.size());
  if (rows.empty()) {
    return;
  }

  setComplexNulls(rows, *result);
  for (const auto& childSpec : scanSpec_->children()) {
    VELOX_TRACE_HISTORY_PUSH("getValues %s", childSpec->fieldName().c_str());
    if (!childSpec->keepValues()) {
      continue;
    }

    const auto channel = childSpec->channel();
    const auto index = childSpec->subscript();
    auto& childResult = resultRow->childAt(channel);

    if (childSpec->deltaUpdate()) {
      VELOX_CHECK(
          childSpec->columnType() ==
          velox::common::ScanSpec::ColumnType::kRegular);
      VELOX_CHECK(!childSpec->hasFilter());
      if (childSpec->isConstant()) {
        setConstantField(childSpec->constantValue(), rows.size(), childResult);
        childSpec->deltaUpdate()->update(rows, childResult);
      } else {
        setOutputRowsForLazy(rows);
        setLazyField(
            std::make_unique<DeltaUpdateColumnLoader>(
                this, children_[index], numReads_),
            resultRow->type()->childAt(channel),
            rows.size(),
            memoryPool_,
            childResult);
      }
      continue;
    }

    if (childSpec->isConstant()) {
      setConstantField(childSpec->constantValue(), rows.size(), childResult);
      continue;
    }

    if (childSpec->columnType() ==
        velox::common::ScanSpec::ColumnType::kRowIndex) {
      setRowNumberField(
          currentRowNumber_, rows, resultRow->pool(), childResult);
      continue;
    }

    if (childSpec->columnType() ==
        velox::common::ScanSpec::ColumnType::kComposite) {
      setCompositeField(
          *childSpec, currentRowNumber_, rows, resultRow->pool(), childResult);
      continue;
    }

    // Set missing fields to be null constant, if we're in the top level struct
    // missing columns should already be a null constant from the check above.
    if (index == kConstantChildSpecSubscript) {
      const auto& childType = rowType.childAt(channel);
      setNullField(rows.size(), childResult, childType, resultRow->pool());
      continue;
    }

    if (childSpec->hasFilter() || !children_[index]->isTopLevel()) {
      children_[index]->getValues(rows, &childResult);
      continue;
    }

    // LazyVector result.
    setOutputRowsForLazy(rows);
    setLazyField(
        std::make_unique<ColumnLoader>(this, children_[index], numReads_),
        resultRow->type()->childAt(channel),
        rows.size(),
        memoryPool_,
        childResult);
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
