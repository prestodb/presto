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
  uint64_t* FOLLY_NONNULL getRawSelectedBits(
      vector_size_t size,
      memory::MemoryPool* FOLLY_NONNULL pool);

  // Make sure selectedIndices buffer has enough capacity to hold 'size'
  // indices and return raw pointer to the underlying buffer.
  vector_size_t* FOLLY_NONNULL getRawSelectedIndices(
      vector_size_t size,
      memory::MemoryPool* FOLLY_NONNULL pool);
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
    memory::MemoryPool* FOLLY_NONNULL pool);

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
    memory::MemoryPool* FOLLY_NONNULL pool);

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
    RowVector* FOLLY_NONNULL target,
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
    const std::string& spillPath,
    const std::string& taskId,
    int driverId,
    int32_t operatorId);

/// Generates the spiller config for a given operator if the disk spilling is
/// enabled, otherwise returns null.
std::optional<Spiller::Config> makeOperatorSpillConfig(
    const core::QueryCtx& queryCtx,
    const OperatorCtx& operatorCtx,
    const char* spillConfigPropertyName,
    int32_t operatorId);

} // namespace facebook::velox::exec
