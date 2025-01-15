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

/// Deselects rows from 'rows' where any of the vectors managed by the 'hashers'
/// has a null.
void deselectRowsWithNulls(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    SelectivityVector& rows);

/// Reusable memory needed for processing filter results.
struct FilterEvalCtx {
  DecodedVector decodedResult;
  BufferPtr selectedIndices;
  BufferPtr selectedBits;

  /// Make sure selectedBits has enough capacity to hold 'size' bits and return
  /// raw pointer to the underlying buffer.
  uint64_t* getRawSelectedBits(vector_size_t size, memory::MemoryPool* pool);

  /// Make sure selectedIndices buffer has enough capacity to hold 'size'
  /// indices and return raw pointer to the underlying buffer.
  vector_size_t* getRawSelectedIndices(
      vector_size_t size,
      memory::MemoryPool* pool);
};

/// Convert the results of filter evaluation as a vector of booleans into
/// indices of the passing rows. Return number of rows that passed the filter.
/// Populate filterEvalCtx.selectedBits and selectedIndices with the indices of
/// the passing rows if only some rows pass the filter. If all or no rows passed
/// the filter filterEvalCtx.selectedBits and selectedIndices are not updated.
///
/// filterEvalCtx.filterResult is expected to be the vector of booleans
/// representing the results of evaluating a filter.
///
/// filterResult.rows are the rows in filterResult to process
vector_size_t processFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool);

/// Wraps the specified vector into a dictionary using the specified mapping.
/// Returns vector as-is if mapping is null. An optional nulls buffer can be
/// provided to introduce additional nulls.
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

/// Represents unique dictionary wrappers over a set of vectors when
/// wrapping these inside another dictionary. When multiple wrapped
/// vectors with the same wrapping get re-wrapped, we replace the
/// wrapper with a composition of the two dictionaries. This needs to
/// be done once per distinct wrapper in the input. WrapState records
/// the compositions that are already made.
struct WrapState {
  // Records wrap nulls added in wrapping. If wrap nulls are added, the same
  // wrap nulls must be applied to all columns.
  Buffer* nulls;

  // Set of distinct wrappers in input, each mapped to the wrap
  // indices combining the former with the new wrap.

  folly::F14FastMap<Buffer*, BufferPtr> transposeResults;
};

/// Wraps 'inputVector' with 'wrapIndices' and
/// 'wrapNulls'. 'wrapSize' is the size of of 'wrapIndices' and of
/// the resulting vector. Dictionary combining is deduplicated using
/// 'wrapState'. If the same indices are added on top of dictionary
/// encoded vectors sharing the same wrapping, the resulting vectors
/// will share the same composition of the original wrap and
/// 'wrapIndices'.
VectorPtr wrapOne(
    vector_size_t wrapSize,
    BufferPtr wrapIndices,
    const VectorPtr& inputVector,
    BufferPtr wrapNulls,
    WrapState& wrapState);

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

/// Projects children of 'src' row vector to 'dest' row vector
/// according to 'projections' and 'mapping'. 'size' specifies number
/// of projected rows in 'dest'. 'state'  is used to
/// deduplicate dictionary merging when applying the same dictionary
/// over more than one identical set of indices.
void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const RowVectorPtr& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping,
    WrapState* state);

/// Overload of the above function that takes reference to const vector of
/// VectorPtr as 'src' argument, instead of row vector.
void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const std::vector<VectorPtr>& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping,
    WrapState* state);

using BlockedOperatorCb = std::function<BlockingReason(ContinueFuture* future)>;

/// An operator that blocks until the blockedCb tells it not. blockedCb is
/// responsible for notifying the unblock through the 'promise' parameter and
/// setting subsequent blocks through 'shouldBlock' parameter.
class BlockedOperator : public Operator {
 public:
  BlockedOperator(
      DriverCtx* ctx,
      int32_t id,
      core::PlanNodePtr node,
      BlockedOperatorCb&& blockedCb)
      : Operator(ctx, node->outputType(), id, node->id(), "BlockedOperator"),
        blockedCb_(std::move(blockedCb)) {}

  BlockingReason isBlocked(ContinueFuture* future) override {
    return blockedCb_(future);
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    return std::move(input_);
  }

  void noMoreInput() override {
    Operator::noMoreInput();
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

  void close() override {
    Operator::close();
  }

  bool canReclaim() const override {
    return false;
  }

 private:
  BlockedOperatorCb blockedCb_;
};

class BlockedNode : public core::PlanNode {
 public:
  BlockedNode(const core::PlanNodeId& id, core::PlanNodePtr input)
      : PlanNode(id), sources_{std::move(input)} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "BlockedNode";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

class BlockedOperatorFactory : public Operator::PlanNodeTranslator {
 public:
  BlockedOperatorFactory() = default;

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override;

  void setBlockedCb(BlockedOperatorCb blockedCb) {
    blockedCb_ = std::move(blockedCb);
  }

 private:
  BlockedOperatorCb blockedCb_{nullptr};
};
} // namespace facebook::velox::exec
