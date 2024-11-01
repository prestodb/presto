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

#include "velox/tool/trace/FilterProjectReplayer.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace {
core::PlanNodePtr FilterProjectReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  if (node->name() == "Filter") {
    const auto* filterNode = dynamic_cast<const core::FilterNode*>(node);
    VELOX_CHECK(
        !isFilterProject(dynamic_cast<const core::FilterNode*>(node)),
        "If the target node is a FilterNode, it must be a standalone FilterNode");

    // A standalone FilterNode.
    return std::make_shared<core::FilterNode>(
        nodeId, filterNode->filter(), source);
  }

  const auto* projectNode = dynamic_cast<const core::ProjectNode*>(node);

  // A standalone ProjectNode.
  if (node->sources().empty() || node->sources().front()->name() != "Filter") {
    return std::make_shared<core::ProjectNode>(
        nodeId, projectNode->names(), projectNode->projections(), source);
  }

  // A ProjectNode with a FilterNode as its source.
  // -- ProjectNode [nodeId]
  //   -- FilterNode [nodeId - 1]
  const auto originalFilterNode =
      std::dynamic_pointer_cast<const core::FilterNode>(
          node->sources().front());
  const auto filterNode = std::make_shared<core::FilterNode>(
      nodeId, originalFilterNode->filter(), source);
  const auto projectNodeId = planNodeIdGenerator_->next();
  return std::make_shared<core::ProjectNode>(
      projectNodeId,
      projectNode->names(),
      projectNode->projections(),
      filterNode);
}

bool FilterProjectReplayer::isFilterProject(
    const core::PlanNode* filterNode) const {
  const auto* projectNode =
      dynamic_cast<const core::ProjectNode*>(core::PlanNode::findFirstNode(
          planFragment_.get(), [this](const core::PlanNode* node) {
            return node->id() == std::to_string(std::stoull(nodeId_) + 1);
          }));
  return projectNode != nullptr && projectNode->name() == "Project" &&
      projectNode->sources().size() == 1 &&
      projectNode->sources().front()->id() == nodeId_;
}
} // namespace facebook::velox::tool::trace
