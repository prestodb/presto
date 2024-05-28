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

  bool getBuildData(ContinueFuture* future);

  // Calculates the number of probe rows to match with the build side vectors
  // given the output batch size limit.
  vector_size_t getNumProbeRows() const;

  // Generates cross product of next 'probeCnt' rows of input_, and all rows of
  // build side vector at 'buildIndex_' in 'buildData_'.
  // 'outputType' specifies the type of output.
  // Projections from input_ and buildData_ to the output are specified by
  // 'probeProjections' and 'buildProjections' respectively. Caller is
  // responsible for ensuring all columns in outputType is contained in either
  // projections.
  // TODO: consider consolidate the routine of producing cartesian product that
  // can be reused at MergeJoin::addToOutput
  RowVectorPtr getCrossProduct(
      vector_size_t probeCnt,
      const RowTypePtr& outputType,
      const std::vector<IdentityProjection>& probeProjections,
      const std::vector<IdentityProjection>& buildProjections);

  // Evaluates joinCondition against the output of getCrossProduct(probeCnt),
  // returns the result that passed joinCondition, updates probeMatched_,
  // buildMatched_ accordingly.
  RowVectorPtr doMatch(vector_size_t probeCnt);

  // Updates 'probeRow_' and 'buildIndex_' by advancing 'probeRow_' by probeCnt.
  // Returns true if 'buildIndex_' points to the end of 'buildData_'.
  bool advanceProbeRows(vector_size_t probeCnt);

  bool hasProbedAllBuildData() const {
    return (buildIndex_ == buildVectors_.value().size());
  }

  // Wraps rows of 'data' that are not selected in 'matched' and projects
  // to the output according to 'projections'. 'nullProjections' is used to
  // create null column vectors in output for outer join. 'unmatchedMapping' is
  // the reusable buffer to record the mismatched row numbers for output
  // projections.
  RowVectorPtr getMismatchedOutput(
      const RowVectorPtr& data,
      const SelectivityVector& matched,
      BufferPtr& unmatchedMapping,
      const std::vector<IdentityProjection>& projections,
      const std::vector<IdentityProjection>& nullProjections);

  void finishProbeInput();

  // When doing right/full joins, all but the last probe operators that finished
  // matching and probe-side mismatch output, will turn into kFinish state.
  // The last finishing operator will gather buildMatched from all the other
  // probe operators to emit output for mismatched build side rows.
  void beginBuildMismatch();

  bool processingBuildMismatch() const {
    return state_ == ProbeOperatorState::kRunning && input_ == nullptr &&
        noMoreInput_;
  }

  // TODO: Add state transition check.
  void setState(ProbeOperatorState state) {
    state_ = state;
  }

 private:
  // Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;
  std::shared_ptr<const core::NestedLoopJoinNode> joinNode_;
  const core::JoinType joinType_;

  ProbeOperatorState state_{ProbeOperatorState::kWaitForBuild};
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  // Join condition-related state
  std::unique_ptr<ExprSet> joinCondition_;
  RowTypePtr filterInputType_;
  SelectivityVector filterInputRows_;

  // Probe side state
  // Input row to process on next call to getOutput().
  vector_size_t probeRow_{0};
  // Records the number of probed rows in last getCrossProduct() call. It is
  // reset upon each 'buildIndex_' update.
  vector_size_t numPrevProbedRows_{0};
  bool lastProbe_{false};
  // Represents whether probe side rows have been matched.
  SelectivityVector probeMatched_;
  std::vector<IdentityProjection> filterProbeProjections_;
  BufferPtr probeOutMapping_;
  BufferPtr probeIndices_;
  // Indicate if the probe side has empty input or not. For the last prober,
  // this indicates if all the probe sides are empty or not. This flag is used
  // for mismatched output producing.
  bool probeSideEmpty_{true};

  // Build side state
  std::optional<std::vector<RowVectorPtr>> buildVectors_;
  bool buildSideEmpty_{false};
  // Index into buildData_ for the build side vector to process on next call to
  // getOutput().
  size_t buildIndex_{0};
  std::vector<IdentityProjection> buildProjections_;
  BufferPtr buildIndices_;

  // Represents whether probe build rows have been matched.
  std::vector<SelectivityVector> buildMatched_;
  std::vector<IdentityProjection> filterBuildProjections_;
  BufferPtr buildOutMapping_;
};

} // namespace facebook::velox::exec
