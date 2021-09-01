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
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& aggrMasks,
    bool ignoreNullKeys,
    std::shared_ptr<const PlanNode> source)
    : PlanNode(id),
      step_(step),
      groupingKeys_(groupingKeys),
      aggregateNames_(aggregateNames),
      aggregates_(aggregates),
      aggrMasks_(aggrMasks),
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

} // namespace facebook::velox::core
