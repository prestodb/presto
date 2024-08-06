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

#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/Operator.h"
#include "velox/exec/ProbeOperatorState.h"

namespace facebook::velox::exec {

/// Implements a Nested Loop Join (NLJ) between records from the probe (input_)
/// and build (NestedLoopJoinBridge) sides. It supports inner, left, right and
/// full outer joins.
///
/// This class is generally useful to evaluate non-equi-joins (e.g. "k1 >= k2"),
/// when join conditions may need to be evaluated against a full cross product
/// of the input.
///
/// The output follows the order of the probe side rows (for inner and left
/// joins). All build vectors are materialized upfront (check buildVectors_),
/// but probe batches are processed one-by-one as a stream.
///
/// To produce output, the operator processes each probe record from probe
/// input, using the following steps:
///
/// 1. Materialize a cross product by wrapping each probe record (as a constant)
///    to each build vector.
/// 2. Evaluate the join condition.
/// 3. Add key matches to the output.
/// 4. Once all build vectors are processed for a particular probe row, check if
///    a probe mismatch is needed (only for left and full outer joins).
/// 5. Once all probe and build inputs are processed, check if build mismatches
///    are needed (only for right and full outer joins).
/// 6. If so, signal other peer operators; only a single operator instance will
///    collect all build matches at the end, and emit any records that haven't
///    been matched by any of the peers.
///
/// The output always contains dictionaries wrapped around probe columns, and
/// copies for build columns. The buid-side copies are done lazily; it first
/// accumulates the ranges to be copied, then performs the copies in batch,
/// column-by-column. It produces at most `outputBatchSize_` records, but it may
/// produce fewer since the output needs to follow the probe vector boundaries.
class NestedLoopJoinProbe : public Operator {
 public:
  NestedLoopJoinProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::NestedLoopJoinNode>& joinNode);

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return state_ == ProbeOperatorState::kRunning && input_ == nullptr &&
        !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override {
    return state_ == ProbeOperatorState::kFinish;
  }

  void close() override;

 private:
  // TODO: maybe consolidate initializeFilter routine across operators like
  // HashProbe and MergeJoin.
  void initializeFilter(
      const core::TypedExprPtr& filter,
      const RowTypePtr& leftType,
      const RowTypePtr& rightType);

  // Materializes build data from nested loop join bridge into `buildVectors_`.
  // Returns whether the data has been materialized and is ready for use. Nested
  // loop join requires all build data to be materialized and available in
  // `buildVectors_` before it can produce output.
  bool getBuildData(ContinueFuture* future);

  // Generates output from join matches between probe and build sides, as well
  // as probe mismatches (for left and full outer joins). As much as possible,
  // generates outputs `outputBatchSize_` records at a time, but batches may be
  // smaller in some cases - outputs follow the probe side buffer boundaries.
  RowVectorPtr generateOutput();

  // Fill in joined output to `output_` by matching the current probeRow_ and
  // successive build vectors (using getNextCrossProductBatch()). Stops when
  // either all build vectors were matched for the current probeRow (returns
  // true), or if the output is full (returns false). If it returns false, a
  // valid vector with more than zero records will be available at `output_`; if
  // it returns true, either nullptr or zero records may be placed at `output_`.
  //
  // Also updates `buildMatched_` if the build records that received a match, so
  // that they can be used to implement right and full outer join semantic once
  // all probe data has been processed.
  bool addToOutput();

  // Advances 'probeRow_' and resets required state information. Returns true
  // if there is not more probe data to be processed in the current `input_`
  // (and hence a new probe input is required). False otherwise.
  bool advanceProbeRow();

  // Ensures a new batch of records is available at `output_` and ready to
  // receive rows. Batches have space for `outputBatchSize_`.
  void prepareOutput();

  // Evaluates the joinCondition for a given build vector. This method sets
  // `filterOutput_` and `decodedFilterResult_`, which will be ready to be used
  // by `isJoinConditionMatch(buildRow)` below.
  void evaluateJoinFilter(const RowVectorPtr& buildVector);

  // Checks if the join condition matched for a particular row.
  bool isJoinConditionMatch(vector_size_t i) const {
    return (
        !decodedFilterResult_.isNullAt(i) &&
        decodedFilterResult_.valueAt<bool>(i));
  }

  // Generates the next batch of a cross product between probe and build. It
  // uses the current probe record being processed (`probeRow_` from `intput_`)
  // for probe projections, and the columns from buildVector for build
  // projections.
  //
  // Output projections can be specified so that this function can be used to
  // generate both filter input and actual output (in case there is no join
  // filter - cross join).
  RowVectorPtr getNextCrossProductBatch(
      const RowVectorPtr& buildVector,
      const RowTypePtr& outputType,
      const std::vector<IdentityProjection>& probeProjections,
      const std::vector<IdentityProjection>& buildProjections);

  // Add a single record to `output_` based on buildRow from buildVector, and
  // the current probeRow and probe vector (input_). Probe side projections are
  // zero-copy (dictionary indices), and build side projections are marked to be
  // copied using `buildCopyRanges_`; they will be copied later on by
  // `copyBuildValues()`.
  void addOutputRow(vector_size_t buildRow);

  // Checks if it is required to add a probe mismatch row, and does it if
  // needed. The caller needs to ensure there is available space in `output_`
  // for the new record, which has nulled out build projections.
  void checkProbeMismatchRow();

  // Add a probe mismatch (only for left/full outer joins). The record is based
  // on the current probeRow and vector (input_) and build projections are null.
  void addProbeMismatchRow();

  // Copies the ranges from buildVector specified by `buildCopyRanges_` to
  // `output_`, one projected column at a time. Clears buildCopyRanges_.
  void copyBuildValues(const RowVectorPtr& buildVector);

  // Called when we are done processing the current probe batch, to signal we
  // are ready for the next one.
  //
  // If this is the last probe batch (and this is a right or full outer join),
  // change the operator state to signal peers.
  void finishProbeInput();

  // When doing right/full joins, all but the last probe operator that finished
  // matching probe-side input will turn into kFinish state.
  // The last finishing operator will gather buildMatched from all the other
  // probe operators to emit output for mismatched build side rows.
  void beginBuildMismatch();

  // If this is the operator producing build mismatches (only after producing
  // all matches and probe mismatches).
  bool processingBuildMismatch() const {
    return state_ == ProbeOperatorState::kRunning && input_ == nullptr &&
        noMoreInput_;
  }

  // Whether we have processed all build data for the current probe row (based
  // on buildIndex_'s value).
  bool hasProbedAllBuildData() const {
    return (buildIndex_ >= buildVectors_.value().size());
  }

  // Wraps rows of 'data' that are not selected in 'matched' and projects
  // to the output according to 'projections'. 'nullProjections' is used to
  // create null column vectors in output for outer join. 'unmatchedMapping' is
  // the reusable buffer to record the mismatched row numbers for output
  // projections.
  RowVectorPtr getBuildMismatchedOutput(
      const RowVectorPtr& data,
      const SelectivityVector& matched,
      BufferPtr& unmatchedMapping,
      const std::vector<IdentityProjection>& projections,
      const std::vector<IdentityProjection>& nullProjections);

  // TODO: Add state transition check.
  void setState(ProbeOperatorState state) {
    state_ = state;
  }

 private:
  // Output buffer members.

  // Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  // The current output batch being populated.
  RowVectorPtr output_;

  // Number of output rows in the current output batch.
  vector_size_t numOutputRows_{0};

  // Dictionary indices for probe columns.
  BufferPtr probeIndices_;
  vector_size_t* rawProbeIndices_;

  // Join condition expression.

  // May be nullptr for a cross join.
  std::unique_ptr<ExprSet> joinCondition_;

  // Input type for the join condition expression.
  RowTypePtr filterInputType_;

  // Join condition evaluation state that need to persisted across the
  // generation of successive output buffers.
  SelectivityVector filterInputRows_;
  VectorPtr filterOutput_;
  DecodedVector decodedFilterResult_;

  // Join metadata and state.
  std::shared_ptr<const core::NestedLoopJoinNode> joinNode_;
  const core::JoinType joinType_;

  ProbeOperatorState state_{ProbeOperatorState::kWaitForBuild};
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  // Probe side state.

  // Probe row being currently processed (related to `input_`).
  vector_size_t probeRow_{0};

  // Whether the current probeRow_ has produces a match. Used for left and full
  // outer joins.
  bool probeRowHasMatch_{false};

  // Controls if this is the operator gathering and producing right/full outer
  // join mismatches. This is only set after all probe and build data has been
  // processed, only for right/full outer joins, and only executed in one single
  // operator (need to wait until all peers are finished).
  bool lastProbe_{false};

  // Indicate if the probe side has empty input or not. For the last probe,
  // this indicates if all the probe sides are empty or not. This flag is used
  // for mismatched output producing.
  bool probeSideEmpty_{true};

  // Build side state.

  // Stores the data for build vectors (right side of the join).
  std::optional<std::vector<RowVectorPtr>> buildVectors_;
  bool buildSideEmpty_{false};

  // Index into `buildVectors_` for the build vector being currently processed.
  size_t buildIndex_{0};

  // Row being currently processed from `buildVectors_[buildIndex_]`.
  vector_size_t buildRow_{0};

  // Keep track of the build rows that had matches (only used for right or full
  // outer joins).
  std::vector<SelectivityVector> buildMatched_;

  // Stores the ranges of build values to be copied to the output vector (we
  // batch them and copy once, instead of copying them row-by-row).
  std::vector<BaseVector::CopyRange> buildCopyRanges_;

  // List of output projections from the build side. Note that the list of
  // projections from the probe side is available at `identityProjections_`.
  std::vector<IdentityProjection> buildProjections_;

  // Projections needed as input to the filter to evaluation join filter
  // conditions. Note that if this is a cross-join, filter projections are the
  // same as output projections.
  std::vector<IdentityProjection> filterProbeProjections_;
  std::vector<IdentityProjection> filterBuildProjections_;

  BufferPtr buildOutMapping_;
};

} // namespace facebook::velox::exec
