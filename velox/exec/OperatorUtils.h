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
#include "velox/exec/Spiller.h"

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

/// Wraps all children of the specified row vector into a dictionary using
/// specified mapping. Returns vector as-is if mapping is null.
RowVectorPtr
wrap(vector_size_t size, BufferPtr mapping, const RowVectorPtr& vector);

/// Wraps 'childVectors' into a dictionary with 'rowType' using specified
/// mapping. Returns an empty vector if mapping is null. This is different than
/// 'wrap' which takes an input vector. The latter returns the input vector if
/// mapping is null.
RowVectorPtr wrap(
    vector_size_t size,
    BufferPtr mapping,
    const RowTypePtr& rowType,
    const std::vector<VectorPtr>& childVectors,
    memory::MemoryPool* pool);

// Ensures that all LazyVectors reachable from 'input' are loaded for all rows.
void loadColumns(const RowVectorPtr& input, core::ExecCtx& execCtx);

/// Scatter copy from multiple source row vectors into the target row vector.
/// 'targetIndex' is first row in 'target' to copy to. 'count' specifies how
/// many rows to copy from the sources. 'sources' and 'sourceIndices' specify
/// the source rows to copy from. If 'columnMap' is not empty, it provides the
/// column channel mappings between target row vector and source row vectors.
///
/// NOTE: all the source row vectors must have the same data type.
void gatherCopy(
    RowVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    const std::vector<IdentityProjection>& columnMap = {});

/// Generates the system-wide unique disk spill file path for an operator. It
/// will be the directory on fs with namespace support or common file prefix if
/// not. It is assumed that the disk spilling file hierarchy for an operator is
/// flat.
std::string makeOperatorSpillPath(
    const std::string& spillDir,
    int pipelineId,
    int driverId,
    int32_t operatorId);

/// Add a named runtime metric to operator 'stats'.
void addOperatorRuntimeStats(
    const std::string& name,
    const RuntimeCounter& value,
    std::unordered_map<std::string, RuntimeMetric>& stats);

/// Aggregates runtime metrics we want to see per operator rather than per
/// event.
void aggregateOperatorRuntimeStats(
    std::unordered_map<std::string, RuntimeMetric>& stats);

/// Allocates 'mapping' to fit at least 'size' indices and initializes them to
/// zero if 'mapping' is either: nullptr, not unique or cannot fit 'size'.
/// Returns 'mapping' as folly::Range<vector_size_t*>. Can be used by operator
/// to initialize / resize reusable state across batches of processing.
folly::Range<vector_size_t*> initializeRowNumberMapping(
    BufferPtr& mapping,
    vector_size_t size,
    memory::MemoryPool* pool);

/// Projects children of 'src' row vector according to 'projections'. Optionally
/// takes a 'mapping' and 'size' that represent the indices and size,
/// respectively, of a dictionary wrapping that should be applied to the
/// projections. The output param 'projectedChildren' will contain all the final
/// projections at the expected channel index. Indices not specified in
/// 'projections' will be left untouched in 'projectedChildren'.
void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const RowVectorPtr& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping);

/// Overload of the above function that takes reference to const vector of
/// VectorPtr as 'src' argument, instead of row vector.
void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const std::vector<VectorPtr>& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping);

} // namespace facebook::velox::exec
