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
#include <memory>
#include <unordered_set>
#include <vector>
#include "velox/core/PlanNode.h"

namespace facebook::velox::core {

class QueryConfig;

/// Gives hints on how to execute the fragment of a plan.
enum class ExecutionStrategy {
  /// Process splits as they come in any available driver.
  kUngrouped,
  /// Process splits from each split group only in one driver.
  /// It is used when split groups represent separate partitions of the data on
  /// the grouping keys or join keys. In that case it is sufficient to keep only
  /// the keys from a single split group in a hash table used by group-by or
  /// join.
  kGrouped,
};

/// Contains some information on how to execute the fragment of a plan.
/// Used to construct Task.
struct PlanFragment {
  /// Top level (root) Plan Node.
  std::shared_ptr<const core::PlanNode> planNode;
  ExecutionStrategy executionStrategy{ExecutionStrategy::kUngrouped};
  int numSplitGroups{0};

  /// Contains leaf plan nodes that need to be executed in the grouped mode.
  std::unordered_set<PlanNodeId> groupedExecutionLeafNodeIds;

  /// Returns true if the fragment uses grouped execution strategy meaning that
  /// at least one pipeline has a leaf node that should run grouped execution.
  /// Note that it does not mean that all pipelines run grouped execution -
  /// some leaf nodes might still run ungrouped execution.
  inline bool isGroupedExecution() const {
    return executionStrategy == ExecutionStrategy::kGrouped;
  }

  /// Returns true for leaf nodes that use grouped execution, false otherwise.
  inline bool leafNodeRunsGroupedExecution(const PlanNodeId& planNodeId) const {
    return groupedExecutionLeafNodeIds.find(planNodeId) !=
        groupedExecutionLeafNodeIds.end();
  }

  PlanFragment() = default;

  explicit PlanFragment(std::shared_ptr<const core::PlanNode> topNode)
      : planNode(std::move(topNode)) {}

  PlanFragment(
      std::shared_ptr<const core::PlanNode> topNode,
      ExecutionStrategy strategy,
      int numberOfSplitGroups,
      const std::unordered_set<PlanNodeId>& groupedExecLeafNodeIds)
      : planNode(std::move(topNode)),
        executionStrategy(strategy),
        numSplitGroups(numberOfSplitGroups),
        groupedExecutionLeafNodeIds(groupedExecLeafNodeIds) {}

  /// Returns true if the spilling is enabled and there is at least one node in
  /// the plan, whose operator can spill. Returns false otherwise.
  bool canSpill(const QueryConfig& queryConfig) const;
};

} // namespace facebook::velox::core

template <>
struct fmt::formatter<facebook::velox::core::ExecutionStrategy>
    : formatter<int> {
  auto format(
      const facebook::velox::core::ExecutionStrategy& s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
