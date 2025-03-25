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

#include "velox/dwio/common/ColumnLoader.h"

#include "velox/common/process/TraceContext.h"

namespace facebook::velox::dwio::common {

namespace {

// Call the field reader `read` with the correct row set based on whether the
// row set has been filtered or not after its parent is read.
RowSet read(
    SelectiveStructColumnReaderBase* structReader,
    SelectiveColumnReader* fieldReader,
    uint64_t version,
    RowSet rows,
    raw_vector<vector_size_t>& selectedRows,
    ValueHook* hook) {
  VELOX_CHECK_EQ(
      version,
      structReader->numReads(),
      "Loading LazyVector after the enclosing reader has moved");
  const auto offset = structReader->lazyVectorReadOffset();
  const auto* incomingNulls = structReader->nulls();
  const auto outputRows = structReader->outputRows();
  RowSet effectiveRows;

  if (rows.size() == outputRows.size()) {
    // All the rows planned at creation are accessed.
    effectiveRows = outputRows;
  } else {
    // rows is a set of indices into outputRows. There has been a
    // selection between creation and loading.
    selectedRows.resize(rows.size());
    VELOX_DCHECK(!selectedRows.empty());
    for (auto i = 0; i < rows.size(); ++i) {
      selectedRows[i] = outputRows[rows[i]];
    }
    effectiveRows = selectedRows;
  }

  structReader->advanceFieldReader(fieldReader, offset);
  fieldReader->scanSpec()->setValueHook(hook);
  fieldReader->read(offset, effectiveRows, incomingNulls);
  if (fieldReader->fileType().type()->isRow() ||
      fieldReader->scanSpec()->isFlatMapAsStruct()) {
    // 'fieldReader_' may itself produce LazyVectors. For this it must have its
    // result row numbers set.
    static_cast<SelectiveStructColumnReaderBase*>(fieldReader)
        ->setLoadableRows(effectiveRows);
  }
  return effectiveRows;
}

// Wraps '*result' in a dictionary to make the contiguous values
// appear at the indices i 'rows'. Used when loading a LazyVector for
// a sparse set of rows in conditional exprs.
void scatter(RowSet rows, vector_size_t resultSize, VectorPtr* result) {
  VELOX_CHECK_GE(resultSize, rows.back() + 1);

  // Initialize the indices to 0 to make the dictionary safely
  // readable also for uninitialized positions.
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(resultSize, (*result)->pool(), 0);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (int32_t i = 0; i < rows.size(); ++i) {
    rawIndices[rows[i]] = i;
  }
  // Disable dictionary values caching in expression eval so that we don't need
  // to reallocate the result for every batch.
  result->get()->disableMemo();
  *result = BaseVector::wrapInDictionary(nullptr, indices, resultSize, *result);
}

template <typename T>
void addToHookImpl(
    const DecodedVector& decoded,
    const RowSet& rows,
    ValueHook& hook) {
  if (decoded.isIdentityMapping()) {
    auto* values = decoded.data<T>();
    hook.addValues(rows.data(), values, rows.size());
    return;
  }
  for (auto i : rows) {
    if (!decoded.isNullAt(i)) {
      hook.addValueTyped(i, decoded.valueAt<T>(i));
    } else if (hook.acceptsNulls()) {
      hook.addNull(i);
    }
  }
}

void addToHook(
    const DecodedVector& decoded,
    const RowSet& rows,
    ValueHook& hook) {
  switch (decoded.base()->typeKind()) {
    case TypeKind::BOOLEAN:
      addToHookImpl<bool>(decoded, rows, hook);
      break;
    case TypeKind::TINYINT:
      addToHookImpl<int8_t>(decoded, rows, hook);
      break;
    case TypeKind::SMALLINT:
      addToHookImpl<int16_t>(decoded, rows, hook);
      break;
    case TypeKind::INTEGER:
      addToHookImpl<int32_t>(decoded, rows, hook);
      break;
    case TypeKind::BIGINT:
      addToHookImpl<int64_t>(decoded, rows, hook);
      break;
    case TypeKind::REAL:
      addToHookImpl<float>(decoded, rows, hook);
      break;
    case TypeKind::DOUBLE:
      addToHookImpl<double>(decoded, rows, hook);
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      addToHookImpl<StringView>(decoded, rows, hook);
      break;
    default:
      VELOX_FAIL(
          "Unsupported type kind for hook: {}", decoded.base()->typeKind());
  }
}

} // namespace

void ColumnLoader::loadInternal(
    RowSet rows,
    ValueHook* hook,
    vector_size_t resultSize,
    VectorPtr* result) {
  process::TraceContext trace("ColumnLoader::loadInternal");
  ExceptionContextSetter exceptionContext(
      {[](VeloxException::Type /*exceptionType*/, auto* reader) {
         return static_cast<SelectiveStructColumnReaderBase*>(reader)
             ->debugString();
       },
       structReader_});
  raw_vector<vector_size_t> selectedRows;
  auto effectiveRows =
      read(structReader_, fieldReader_, version_, rows, selectedRows, hook);
  if (!hook) {
    fieldReader_->getValues(effectiveRows, result);
    if (((rows.back() + 1) < resultSize) ||
        rows.size() != structReader_->outputRows().size()) {
      // We read sparsely. The values that were read should appear
      // at the indices in the result vector that were given by
      // 'rows'.
      scatter(rows, resultSize, result);
    }
  }
}

void DeltaUpdateColumnLoader::loadInternal(
    RowSet rows,
    ValueHook* hook,
    vector_size_t resultSize,
    VectorPtr* result) {
  process::TraceContext trace("DeltaUpdateColumnLoader::loadInternal");
  ExceptionContextSetter exceptionContext(
      {[](VeloxException::Type /*exceptionType*/, auto* reader) {
         return static_cast<SelectiveStructColumnReaderBase*>(reader)
             ->debugString();
       },
       structReader_});
  auto* scanSpec = fieldReader_->scanSpec();
  VELOX_CHECK(scanSpec->readFromFile());
  // Filters on delta updated columns need to be disabled and applied after this
  // method return.
  VELOX_CHECK(!scanSpec->hasFilter());
  scanSpec->setValueHook(nullptr);
  raw_vector<vector_size_t> selectedRows;
  RowSet effectiveRows;
  effectiveRows =
      read(structReader_, fieldReader_, version_, rows, selectedRows, nullptr);
  fieldReader_->getValues(effectiveRows, result);
  scanSpec->deltaUpdate()->update(effectiveRows, *result);
  if (hook) {
    DecodedVector decoded(**result);
    addToHook(decoded, effectiveRows, *hook);
  } else if (
      rows.back() + 1 < resultSize ||
      rows.size() != structReader_->outputRows().size()) {
    scatter(rows, resultSize, result);
  }
}

} // namespace facebook::velox::dwio::common
