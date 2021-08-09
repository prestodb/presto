/*
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

#include "velox/experimental/codegen/transform/PlanNodeTransform.h"
#include "velox/experimental/codegen/transform/utils/adapters.h"
#include "velox/experimental/codegen/transform/utils/ranges_utils.h"
#include "velox/experimental/codegen/transform/utils/utils.h"

#include "velox/core/PlanNode.h"

namespace facebook::velox::codegen {
namespace {
struct ValueNodeReplacerVisitor {
  explicit ValueNodeReplacerVisitor(std::vector<RowVectorPtr>&& inputVector)
      : inputVector_(std::move(inputVector)) {}
  explicit ValueNodeReplacerVisitor(
      const std::vector<RowVectorPtr>& inputVector)
      : inputVector_(inputVector) {}
  std::vector<RowVectorPtr> inputVector_;

  template <typename Children>
  std::shared_ptr<core::PlanNode> visit(
      const core::PlanNode& planNode,
      const Children& children) {
    if (auto projectNode = dynamic_cast<const core::ProjectNode*>(&planNode)) {
      return transform::utils::adapter::ProjectCopy::copyWith(
          *projectNode,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          *ranges::begin(children));
    };
    if (auto filterNode = dynamic_cast<const core::FilterNode*>(&planNode)) {
      return transform::utils::adapter::FilterCopy::copyWith(
          *filterNode,
          std::placeholders::_1,
          std::placeholders::_1,
          *ranges::begin(children));
    };
    if (auto valueNode = dynamic_cast<const core::ValuesNode*>(&planNode)) {
      return std::make_shared<core::ValuesNode>(valueNode->id(), inputVector_);
    }
    throw std::logic_error("Unknown node type");
  }
};
} // namespace

struct ValueNodeReplacerTransform final : transform::PlanNodeTransform {
  explicit ValueNodeReplacerTransform(std::vector<RowVectorPtr>&& inputVector)
      : visitor_(std::move(inputVector)) {}
  explicit ValueNodeReplacerTransform(
      const std::vector<RowVectorPtr>& inputVector)
      : visitor_(inputVector) {}

  std::shared_ptr<core::PlanNode> transform(
      const core::PlanNode& plan) override {
    using facebook::velox::transform::utils::isomorphicTreeTransform;
    auto nodeTransformer = [this](auto& node, const auto& transformedChildren) {
      return visitor_.visit(node, transformedChildren);
    };
    auto [treeRoot, nodeMap] = isomorphicTreeTransform(plan, nodeTransformer);
    return treeRoot;
  }

  ValueNodeReplacerVisitor visitor_;
};
} // namespace facebook::velox::codegen
