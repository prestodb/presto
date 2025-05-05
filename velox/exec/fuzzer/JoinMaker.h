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

#include <string>
#include <vector>

#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/IExpr.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {
/// Represents a single input "table" to a join plan created using JoinMaker.
/// This can be converted to a ValuesNode or a TableScanNode in the resulting
/// plan.
struct JoinSource {
 public:
  /// @param outputType The RowType the source node will eventually produce,
  /// this should match the Types of the Vectors in `batches`.
  /// @param keys The names of the key fields in `outputType`.
  /// @param batches The data this source will produce.
  /// @param projections Projections to be applied to batches after they are
  /// output.
  /// @param splitsDir A temporary directory to write files containing `batches`
  /// into when generating splits for a TableScan.
  /// @param writerPool A MemoryPool that can be used by the Writers when
  /// writing out splits.
  JoinSource(
      const RowTypePtr& outputType,
      const std::vector<std::string>& keys,
      const std::vector<RowVectorPtr>& batches,
      const std::vector<core::ExprPtr>& projections,
      const std::string& splitsDir,
      const std::shared_ptr<memory::MemoryPool> writerPool)
      : outputType_(outputType),
        keys_(keys),
        batches_(batches),
        projections_(projections),
        splitsDir_(splitsDir),
        writerPool_(writerPool) {}

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  const std::vector<std::string>& keys() const {
    return keys_;
  }

  const std::vector<RowVectorPtr>& batches() const {
    return batches_;
  }

  /// Get `batches` as flat Vectors.
  const std::vector<RowVectorPtr>& flatBatches() const;

  /// Get `batches` as splits.
  const std::vector<Split>& splits() const;

  /// Get `batches` as splits grouped into `numGroups` groups.
  const std::vector<Split>& groupedSplits(const int32_t numGroups) const;

  const std::vector<core::ExprPtr>& projections() const {
    return projections_;
  }

 private:
  std::vector<RowVectorPtr> flatten(
      const std::vector<RowVectorPtr>& vectors) const;

  std::vector<std::vector<RowVectorPtr>> splitInputByGroup(
      int32_t numGroups) const;

  const RowTypePtr outputType_;
  const std::vector<std::string> keys_;
  const std::vector<RowVectorPtr> batches_;
  const std::vector<core::ExprPtr> projections_;
  const std::string splitsDir_;
  const std::shared_ptr<memory::MemoryPool> writerPool_;

  // The following are used to cache `batches_` when it is converted to other
  // forms.
  mutable std::optional<std::vector<RowVectorPtr>> flatBatches_;
  mutable std::optional<std::vector<Split>> splits_;
  mutable std::unordered_map<int32_t, std::vector<Split>> groupedSplits_;
};

/// Can be used to construct a variety of join plans that should produce
/// equivalent results.
class JoinMaker {
 public:
  /// @param joinType The type of join: inner, left, full outer, etc.
  /// @param nullAware Whether the join is null aware, only applies to join
  /// types that support it (semi and anti).
  /// @param sources Specifies the inputs to the join, currently we only
  /// support exactly 2 sources.
  /// @param outputColumns The names of the columns to extract from the input
  /// sources and return as the result of the join.
  /// @param filter A SQL filter to apply as part of the join, it will be parsed
  /// using DuckDB. This can be empty.
  JoinMaker(
      const core::JoinType joinType,
      const bool nullAware,
      const std::vector<std::shared_ptr<JoinSource>>& sources,
      const std::vector<std::string>& outputColumns,
      const std::string& filter)
      : joinType_(joinType),
        nullAware_(nullAware),
        sources_(sources),
        outputColumns_(outputColumns),
        filter_(filter) {
    VELOX_CHECK(
        sources.size() == 2, "For now we only support joining 2 tables");
  }

  /// What sort of input to use in the join, either the original encoded
  /// Vectors, or flattened copies.
  enum class InputType { ENCODED, FLAT };

  /// What PartitionStrategy to use, only applies to HashJoin.
  /// `NONE` means the data is read in a single partition.
  /// `HASH` and `ROUND_ROBIN` cause a LocalPartition step to be added to both
  /// sides of the join using the specified algorithm.
  enum class PartitionStrategy { NONE, HASH, ROUND_ROBIN };

  /// Specifies what order to include the sources in the join.
  /// `NATURAL` uses the order as specified in the JoinMaker constructor.
  /// `FLIPPED` reversed that order.
  enum class JoinOrder { NATURAL, FLIPPED };

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    std::unordered_map<core::PlanNodeId, std::vector<velox::exec::Split>>
        splits;
    core::ExecutionStrategy executionStrategy{
        core::ExecutionStrategy::kUngrouped};
    bool mixedGroupedExecution;
    int32_t numGroups;

    explicit PlanWithSplits(
        const core::PlanNodePtr& _plan,
        const core::PlanNodeId& _probeScanId = "",
        const core::PlanNodeId& _buildScanId = "",
        const std::unordered_map<
            core::PlanNodeId,
            std::vector<velox::exec::Split>>& _splits = {},
        core::ExecutionStrategy _executionStrategy =
            core::ExecutionStrategy::kUngrouped,
        int32_t _numGroups = 0,
        bool _mixedGroupedExecution = false)
        : plan(_plan),
          probeScanId(_probeScanId),
          buildScanId(_buildScanId),
          splits(_splits),
          executionStrategy(_executionStrategy),
          mixedGroupedExecution(_mixedGroupedExecution),
          numGroups(_numGroups) {}
  };

  PlanWithSplits makeHashJoin(
      const InputType inputType,
      const PartitionStrategy partitionStrategy,
      const JoinOrder joinOrder) const;

  PlanWithSplits makeHashJoinWithTableScan(
      std::optional<int32_t> numGroups,
      bool mixedGroupedExecution,
      JoinOrder joinOrder) const;

  PlanWithSplits makeMergeJoin(
      const InputType inputType,
      const JoinOrder joinOrder) const;

  PlanWithSplits makeMergeJoinWithTableScan(const JoinOrder joinOrder) const;

  PlanWithSplits makeNestedLoopJoin(
      const InputType inputType,
      const JoinOrder joinOrder) const;

  PlanWithSplits makeNestedLoopJoinWithTableScan(
      const JoinOrder joinOrder) const;

  /// Returns whether or not the join type supports reversing the order of the
  /// inputs in a hash join.
  bool supportsFlippingHashJoin() const;

  /// Returns whether or not the join type supports reversing the order of the
  /// inputs in a merge join.
  bool supportsFlippingMergeJoin() const;

  /// Returns whether or not the join type supports reversing the order of the
  /// inputs in a nested loop join.
  bool supportsFlippingNestedLoopJoin() const;

  bool supportsMergeJoin() const {
    return core::MergeJoinNode::isSupported(joinType_);
  }

  bool supportsNestedLoopJoin() const {
    return core::NestedLoopJoinNode::isSupported(joinType_);
  }

  /// Returns whether or not the types of the sources allow them to be read as
  /// splits using a TableScan.
  bool supportsTableScan() const;

 private:
  // Get the sources in the order they should be read according to `joinOrder`.
  std::vector<std::shared_ptr<JoinSource>> getJoinSources(
      const JoinOrder joinOrder) const;

  // Get the JoinType to use given `joinOrder`. Throws if the joinType does not
  // support the given JoinOrder.
  core::JoinType getJoinType(const JoinOrder joinOrder) const;

  std::string makeNestedLoopJoinFilter(
      const std::shared_ptr<JoinSource>& left,
      const std::shared_ptr<JoinSource>& righ) const;

  core::PlanNodePtr makeHashJoinPlan(
      test::PlanBuilder& probePlan,
      test::PlanBuilder& buildPlan,
      const std::shared_ptr<JoinSource>& probeSource,
      const std::shared_ptr<JoinSource>& buildSource,
      const core::JoinType joinType) const;

  core::PlanNodePtr makeMergeJoinPlan(
      test::PlanBuilder& probePlan,
      test::PlanBuilder& buildPlan,
      const std::shared_ptr<JoinSource>& probeSource,
      const std::shared_ptr<JoinSource>& buildSource,
      const core::JoinType joinType) const;

  core::PlanNodePtr makeNestedLoopJoinPlan(
      test::PlanBuilder& probePlan,
      test::PlanBuilder& buildPlan,
      const std::shared_ptr<JoinSource>& probeSource,
      const std::shared_ptr<JoinSource>& buildSource,
      const core::JoinType joinType) const;

  const core::JoinType joinType_;
  const bool nullAware_;
  const std::vector<std::shared_ptr<JoinSource>> sources_;
  const std::vector<std::string> outputColumns_;
  const std::string filter_;
};

// Returns the reversed JoinType if the JoinType can be reversed, otherwise
// throws an exception.
core::JoinType flipJoinType(core::JoinType joinType);

} // namespace facebook::velox::exec
