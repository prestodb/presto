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

#include "velox/tool/trace/AggregationReplayer.h"
#include "velox/exec/QueryDataReader.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace {
core::PlanNodePtr AggregationReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  const auto* aggregationNode =
      dynamic_cast<const core::AggregationNode*>(node);
  return std::make_shared<core::AggregationNode>(
      nodeId,
      aggregationNode->step(),
      aggregationNode->groupingKeys(),
      aggregationNode->preGroupedKeys(),
      aggregationNode->aggregateNames(),
      aggregationNode->aggregates(),
      aggregationNode->globalGroupingSets(),
      aggregationNode->groupId(),
      aggregationNode->ignoreNullKeys(),
      source);
}
} // namespace facebook::velox::tool::trace
