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
#pragma once

#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class VectorHasher;

// Deselects rows from 'rows' where any of the vectors managed by the 'hashers'
// has a null.
void deselectRowsWithNulls(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    SelectivityVector& rows);

// Reusable memory needed for processing filter results.
struct FilterEvalCtx {
  DecodedVector decodedResult;
  BufferPtr selectedIndices;
  BufferPtr selectedBits;

  // Make sure selectedBits has enough capacity to hold 'size' bits and return
  // raw pointer to the underlying buffer.
  uint64_t* getRawSelectedBits(vector_size_t size, memory::MemoryPool* pool);

  // Make sure selectedIndices buffer has enough capacity to hold 'size'
  // indices and return raw pointer to the underlying buffer.
  vector_size_t* getRawSelectedIndices(
      vector_size_t size,
      memory::MemoryPool* pool);
};

// Convert the results of filter evaluation as a vector of booleans into indices
// of the passing rows. Return number of rows that passed the filter. Populate
// filterEvalCtx.selectedBits and selectedIndices with the indices of the
// passing rows if only some rows pass the filter. If all or no rows passed the
// filter filterEvalCtx.selectedBits and selectedIndices are not updated.
//
// filterEvalCtx.filterResult is expected to be the vector of booleans
// representing the results of evaluating a filter.
//
// filterResult.rows are the rows in filterResult to process
vector_size_t processFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool);

// Wraps the specified vector into a dictionary using the specified mapping.
// Returns vector as-is if mapping is null. An optional nulls buffer can be
// provided to introduce additional nulls.
VectorPtr wrapChild(
    vector_size_t size,
    BufferPtr mapping,
    const VectorPtr& child,
    BufferPtr nulls = nullptr);

// Wraps all children of the specified row vector into a dictionary using
// specified mapping. Returns vector as-is if mapping is null.
RowVectorPtr
wrap(vector_size_t size, BufferPtr mapping, const RowVectorPtr& vector);

// Ensures that all LazyVectors reachable from 'input' are loaded for all rows.
void loadColumns(const RowVectorPtr& input, core::ExecCtx& execCtx);

} // namespace facebook::velox::exec
