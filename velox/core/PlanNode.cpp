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
#include "velox/core/PlanNode.h"

namespace facebook::velox::core {

namespace {
const std::vector<std::shared_ptr<const PlanNode>> EMPTY_SOURCES;
}

AggregationNode::AggregationNode(
    const PlanNodeId& id,
    Step step,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
        groupingKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<std::shared_ptr<const CallTypedExpr>>& aggregates,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
        aggregateMasks,
    bool ignoreNullKeys,
    std::shared_ptr<const PlanNode> source)
    : PlanNode(id),
      step_(step),
      groupingKeys_(groupingKeys),
      aggregateNames_(aggregateNames),
      aggregates_(aggregates),
      aggregateMasks_(aggregateMasks),
      ignoreNullKeys_(ignoreNullKeys),
      sources_{source},
      outputType_(getOutputType(groupingKeys_, aggregateNames_, aggregates_)) {
  // Empty grouping keys are used in global aggregation:
  //    SELECT sum(c) FROM t
  // Empty aggregates are used in distinct:
  //    SELECT distinct(b, c) FROM t GROUP BY a
  VELOX_CHECK(
      !groupingKeys_.empty() || !aggregates_.empty(),
      "Aggregation must specify either grouping keys or aggregates");
}

const std::vector<std::shared_ptr<const PlanNode>>& ValuesNode::sources()
    const {
  return EMPTY_SOURCES;
}

const std::vector<std::shared_ptr<const PlanNode>>& TableScanNode::sources()
    const {
  return EMPTY_SOURCES;
}

const std::vector<std::shared_ptr<const PlanNode>>& ExchangeNode::sources()
    const {
  return EMPTY_SOURCES;
}

UnnestNode::UnnestNode(
    const PlanNodeId& id,
    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> replicateVariables,
    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> unnestVariables,
    const std::vector<std::string>& unnestNames,
    const std::optional<std::string>& ordinalityName,
    const std::shared_ptr<const PlanNode>& source)
    : PlanNode(id),
      replicateVariables_{std::move(replicateVariables)},
      unnestVariables_{std::move(unnestVariables)},
      withOrdinality_{ordinalityName.has_value()},
      sources_{source} {
  // Calculate output type. First come "replicate" columns, followed by
  // "unnest" columns, followed by an optional ordinality column.
  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (const auto& variable : replicateVariables_) {
    names.emplace_back(variable->name());
    types.emplace_back(variable->type());
  }

  int unnestIndex = 0;
  for (const auto& variable : unnestVariables_) {
    if (variable->type()->isArray()) {
      names.emplace_back(unnestNames[unnestIndex++]);
      types.emplace_back(variable->type()->asArray().elementType());
    } else if (variable->type()->isMap()) {
      const auto& mapType = variable->type()->asMap();

      names.emplace_back(unnestNames[unnestIndex++]);
      types.emplace_back(mapType.keyType());

      names.emplace_back(unnestNames[unnestIndex++]);
      types.emplace_back(mapType.valueType());
    } else {
      VELOX_FAIL(
          "Unexpected type of unnest variable. Expected ARRAY or MAP, but got {}.",
          variable->type()->toString());
    }
  }

  if (ordinalityName.has_value()) {
    names.emplace_back(ordinalityName.value());
    types.emplace_back(BIGINT());
  }
  outputType_ = ROW(std::move(names), std::move(types));
}

AbstractJoinNode::AbstractJoinNode(
    const PlanNodeId& id,
    JoinType joinType,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& leftKeys,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& rightKeys,
    std::shared_ptr<const ITypedExpr> filter,
    std::shared_ptr<const PlanNode> left,
    std::shared_ptr<const PlanNode> right,
    const RowTypePtr outputType)
    : PlanNode(id),
      joinType_(joinType),
      leftKeys_(leftKeys),
      rightKeys_(rightKeys),
      filter_(std::move(filter)),
      sources_({std::move(left), std::move(right)}),
      outputType_(outputType) {
  VELOX_CHECK(
      !leftKeys_.empty(), "HashJoinNode requires at least one join key");
  VELOX_CHECK_EQ(
      leftKeys_.size(),
      rightKeys_.size(),
      "HashJoinNode requires same number of join keys on probe and build sides");
  auto leftType = sources_[0]->outputType();
  for (auto key : leftKeys_) {
    VELOX_CHECK(
        leftType->containsChild(key->name()),
        "Probe side join key not found in probe side output: {}",
        key->name());
  }
  auto rightType = sources_[1]->outputType();
  for (auto key : rightKeys_) {
    VELOX_CHECK(
        rightType->containsChild(key->name()),
        "Build side join key not found in build side output: {}",
        key->name());
  }
  for (auto i = 0; i < outputType_->size(); ++i) {
    auto name = outputType_->nameOf(i);
    if (leftType->containsChild(name)) {
      VELOX_CHECK(
          !rightType->containsChild(name),
          "Duplicate column name found on join's build and probe sides: {}",
          name);
    } else if (rightType->containsChild(name)) {
      VELOX_CHECK(
          !leftType->containsChild(name),
          "Duplicate column name found on join's build and probe sides: {}",
          name);
    } else {
      VELOX_FAIL(
          "Join's output column not found in either probe or build sides: {}",
          name);
    }
  }
}

CrossJoinNode::CrossJoinNode(
    const PlanNodeId& id,
    std::shared_ptr<const PlanNode> left,
    std::shared_ptr<const PlanNode> right,
    RowTypePtr outputType)
    : PlanNode(id),
      sources_({std::move(left), std::move(right)}),
      outputType_(std::move(outputType)) {}

AssignUniqueIdNode::AssignUniqueIdNode(
    const PlanNodeId& id,
    const std::string& idName,
    const int32_t taskUniqueId,
    std::shared_ptr<const PlanNode> source)
    : PlanNode(id), taskUniqueId_(taskUniqueId), sources_{std::move(source)} {
  std::vector<std::string> names(sources_[0]->outputType()->names());
  std::vector<TypePtr> types(sources_[0]->outputType()->children());

  names.emplace_back(idName);
  types.emplace_back(BIGINT());
  outputType_ = ROW(std::move(names), std::move(types));
}

} // namespace facebook::velox::core
