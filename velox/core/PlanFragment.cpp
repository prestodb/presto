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
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryConfig.h"

namespace facebook::velox::core {

bool PlanFragment::canSpill(const QueryConfig& queryConfig) const {
  if (not queryConfig.spillEnabled()) {
    return false;
  }
  return PlanNode::findFirstNode(
             planNode.get(), [&](const core::PlanNode* node) {
               return node->canSpill(queryConfig);
             }) != nullptr;
}

std::string executionStrategyToString(ExecutionStrategy strategy) {
  switch (strategy) {
    case ExecutionStrategy::kGrouped:
      return "GROUPED";
    case ExecutionStrategy::kUngrouped:
      return "UNGROUPED";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(strategy));
  }
}
} // namespace facebook::velox::core
