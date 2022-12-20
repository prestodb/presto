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

  inline bool isGroupedExecution() const {
    return executionStrategy == ExecutionStrategy::kGrouped;
  }

  PlanFragment() = default;

  explicit PlanFragment(std::shared_ptr<const core::PlanNode> topNode)
      : planNode(std::move(topNode)) {}

  PlanFragment(
      std::shared_ptr<const core::PlanNode> topNode,
      ExecutionStrategy strategy,
      int numberOfSplitGroups)
      : planNode(std::move(topNode)),
        executionStrategy(strategy),
        numSplitGroups(numberOfSplitGroups) {}

  /// Returns true if the spilling is enabled and there is at least one node in
  /// the plan, whose operator can spill. Returns false otherwise.
  bool canSpill(const QueryConfig& queryConfig) const;
};

} // namespace facebook::velox::core
