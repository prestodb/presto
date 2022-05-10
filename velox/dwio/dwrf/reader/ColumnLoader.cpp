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

#include "velox/dwio/dwrf/reader/ColumnLoader.h"

namespace facebook::velox::dwrf {

// Wraps '*result' in a dictionary to make the contiguous values
// appear at the indices i 'rows'. Used when loading a LazyVector for
// a sparse set of rows in conditional exprs.
namespace {
static void scatter(RowSet rows, VectorPtr* result) {
  auto end = rows.back() + 1;
  // Initialize the indices to 0 to make the dictionary safely
  // readable also for uninitialized positions.
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(end, (*result)->pool(), 0);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (int32_t i = 0; i < rows.size(); ++i) {
    rawIndices[rows[i]] = i;
  }
  *result =
      BaseVector::wrapInDictionary(BufferPtr(nullptr), indices, end, *result);
}
} // namespace

void ColumnLoader::loadInternal(
    RowSet rows,
    ValueHook* hook,
    VectorPtr* result) {
  VELOX_CHECK_EQ(
      version_,
      structReader_->numReads(),
      "Loading LazyVector after the enclosing reader has moved");
  auto offset = structReader_->lazyVectorReadOffset();
  auto incomingNulls = structReader_->nulls();
  auto outputRows = structReader_->outputRows();
  raw_vector<vector_size_t> selectedRows;
  RowSet effectiveRows;
  ExceptionContextSetter exceptionContext(
      {[](auto* reader) {
         return static_cast<SelectiveStructColumnReader*>(reader)
             ->debugString();
       },
       this});

  if (rows.size() == outputRows.size()) {
    // All the rows planned at creation are accessed.
    effectiveRows = outputRows;
  } else {
    // rows is a set of indices into outputRows. There has been a
    // selection between creation and loading.
    selectedRows.resize(rows.size());
    assert(!selectedRows.empty());
    for (auto i = 0; i < rows.size(); ++i) {
      selectedRows[i] = outputRows[rows[i]];
    }
    effectiveRows = RowSet(selectedRows);
  }

  structReader_->advanceFieldReader(fieldReader_, offset);
  fieldReader_->scanSpec()->setValueHook(hook);
  fieldReader_->read(offset, effectiveRows, incomingNulls);
  if (fieldReader_->type()->kind() == TypeKind::ROW) {
    // 'fieldReader_' may itself produce LazyVectors. For this it must have its
    // result row numbers set.
    reinterpret_cast<SelectiveStructColumnReader*>(fieldReader_)
        ->setLoadableRows(effectiveRows);
  }
  if (!hook) {
    fieldReader_->getValues(effectiveRows, result);
    if (rows.size() != outputRows.size()) {
      // We read sparsely. The values that were read should appear
      // at the indices in the result vector that were given by
      // 'rows'.
      scatter(rows, result);
    }
  }
}

} // namespace facebook::velox::dwrf
