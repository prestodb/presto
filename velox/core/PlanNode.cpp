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

const SortOrder kAscNullsFirst(true, true);
const SortOrder kAscNullsLast(true, false);
const SortOrder kDescNullsFirst(false, true);
const SortOrder kDescNullsLast(false, false);

namespace {
const std::vector<PlanNodePtr> kEmptySources;

std::shared_ptr<RowType> getAggregationOutputType(
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
        groupingKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<std::shared_ptr<const CallTypedExpr>>& aggregates) {
  VELOX_CHECK_EQ(
      aggregateNames.size(),
      aggregates.size(),
      "Number of aggregate names must be equal to number of aggregates");

  std::vector<std::string> names;
  std::vector<std::shared_ptr<const Type>> types;

  for (auto& key : groupingKeys) {
    auto field =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(key);
    VELOX_CHECK(field, "Grouping key must be a field reference");
    names.push_back(field->name());
    types.push_back(field->type());
  }

  for (int32_t i = 0; i < aggregateNames.size(); i++) {
    names.push_back(aggregateNames[i]);
    types.push_back(aggregates[i]->type());
  }

  return std::make_shared<RowType>(std::move(names), std::move(types));
}
} // namespace

AggregationNode::AggregationNode(
    const PlanNodeId& id,
    Step step,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
        groupingKeys,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
        preGroupedKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<std::shared_ptr<const CallTypedExpr>>& aggregates,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
        aggregateMasks,
    bool ignoreNullKeys,
    PlanNodePtr source)
    : PlanNode(id),
      step_(step),
      groupingKeys_(groupingKeys),
      preGroupedKeys_(preGroupedKeys),
      aggregateNames_(aggregateNames),
      aggregates_(aggregates),
      aggregateMasks_(aggregateMasks),
      ignoreNullKeys_(ignoreNullKeys),
      sources_{source},
      outputType_(getAggregationOutputType(
          groupingKeys_,
          aggregateNames_,
          aggregates_)) {
  // Empty grouping keys are used in global aggregation:
  //    SELECT sum(c) FROM t
  // Empty aggregates are used in distinct:
  //    SELECT distinct(b, c) FROM t GROUP BY a
  VELOX_CHECK(
      !groupingKeys_.empty() || !aggregates_.empty(),
      "Aggregation must specify either grouping keys or aggregates");

  std::unordered_set<std::string> groupingKeyNames;
  groupingKeyNames.reserve(groupingKeys.size());
  for (const auto& key : groupingKeys) {
    groupingKeyNames.insert(key->name());
  }

  for (const auto& key : preGroupedKeys) {
    VELOX_CHECK_EQ(
        1,
        groupingKeyNames.count(key->name()),
        "Pre-grouped key must be one of the grouping keys: {}.",
        key->name());
  }
}

namespace {
void addKeys(
    std::stringstream& stream,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& keys) {
  for (auto i = 0; i < keys.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << keys[i]->name();
  }
}
} // namespace

void AggregationNode::addDetails(std::stringstream& stream) const {
  stream << stepName(step_) << " ";

  if (!groupingKeys_.empty()) {
    stream << "[";
    addKeys(stream, groupingKeys_);
    stream << "] ";
  }

  for (auto i = 0; i < aggregateNames_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << aggregateNames_[i] << " := " << aggregates_[i]->toString();
  }
}

const std::vector<PlanNodePtr>& ValuesNode::sources() const {
  return kEmptySources;
}

void ValuesNode::addDetails(std::stringstream& stream) const {
  vector_size_t totalCount = 0;
  for (const auto& vector : values_) {
    totalCount += vector->size();
  }
  stream << totalCount << " rows in " << values_.size() << " vectors";
}

void ProjectNode::addDetails(std::stringstream& stream) const {
  stream << "expressions: ";
  for (auto i = 0; i < projections_.size(); i++) {
    auto& projection = projections_[i];
    if (i > 0) {
      stream << ", ";
    }
    stream << "(" << names_[i] << ":" << projection->type()->toString() << ", "
           << projection->toString() << ")";
  }
}

const std::vector<PlanNodePtr>& TableScanNode::sources() const {
  return kEmptySources;
}

void TableScanNode::addDetails(std::stringstream& stream) const {
  stream << tableHandle_->toString();
}

const std::vector<PlanNodePtr>& ExchangeNode::sources() const {
  return kEmptySources;
}

void ExchangeNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

UnnestNode::UnnestNode(
    const PlanNodeId& id,
    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> replicateVariables,
    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> unnestVariables,
    const std::vector<std::string>& unnestNames,
    const std::optional<std::string>& ordinalityName,
    const PlanNodePtr& source)
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

void UnnestNode::addDetails(std::stringstream& stream) const {
  addKeys(stream, unnestVariables_);
}

AbstractJoinNode::AbstractJoinNode(
    const PlanNodeId& id,
    JoinType joinType,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& leftKeys,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& rightKeys,
    TypedExprPtr filter,
    PlanNodePtr left,
    PlanNodePtr right,
    const RowTypePtr outputType)
    : PlanNode(id),
      joinType_(joinType),
      leftKeys_(leftKeys),
      rightKeys_(rightKeys),
      filter_(std::move(filter)),
      sources_({std::move(left), std::move(right)}),
      outputType_(outputType) {
  VELOX_CHECK(!leftKeys_.empty(), "JoinNode requires at least one join key");
  VELOX_CHECK_EQ(
      leftKeys_.size(),
      rightKeys_.size(),
      "JoinNode requires same number of join keys on left and right sides");
  if (isSemiJoin() || isAntiJoin()) {
    VELOX_CHECK_NULL(filter, "Semi and anti join does not support filter");
  }
  auto leftType = sources_[0]->outputType();
  for (auto key : leftKeys_) {
    VELOX_CHECK(
        leftType->containsChild(key->name()),
        "Left side join key not found in left side output: {}",
        key->name());
  }
  auto rightType = sources_[1]->outputType();
  for (auto key : rightKeys_) {
    VELOX_CHECK(
        rightType->containsChild(key->name()),
        "Right side join key not found in right side output: {}",
        key->name());
  }
  for (auto i = 0; i < leftKeys_.size(); ++i) {
    VELOX_CHECK_EQ(
        leftKeys_[i]->type()->kind(),
        rightKeys_[i]->type()->kind(),
        "Join key types on the left and right sides must match");
  }
  for (auto i = 0; i < outputType_->size(); ++i) {
    auto name = outputType_->nameOf(i);
    if (leftType->containsChild(name)) {
      VELOX_CHECK(
          !rightType->containsChild(name),
          "Duplicate column name found on join's left and right sides: {}",
          name);
    } else if (rightType->containsChild(name)) {
      VELOX_CHECK(
          !leftType->containsChild(name),
          "Duplicate column name found on join's left and right sides: {}",
          name);
    } else {
      VELOX_FAIL(
          "Join's output column not found in either left or right sides: {}",
          name);
    }
  }
}

void AbstractJoinNode::addDetails(std::stringstream& stream) const {
  stream << joinTypeName(joinType_) << " ";

  for (auto i = 0; i < leftKeys_.size(); ++i) {
    if (i > 0) {
      stream << " AND ";
    }
    stream << leftKeys_[i]->name() << "=" << rightKeys_[i]->name();
  }

  if (filter_) {
    stream << ", filter: " << filter_->toString();
  }
}

CrossJoinNode::CrossJoinNode(
    const PlanNodeId& id,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : PlanNode(id),
      sources_({std::move(left), std::move(right)}),
      outputType_(std::move(outputType)) {}

void CrossJoinNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

AssignUniqueIdNode::AssignUniqueIdNode(
    const PlanNodeId& id,
    const std::string& idName,
    const int32_t taskUniqueId,
    PlanNodePtr source)
    : PlanNode(id), taskUniqueId_(taskUniqueId), sources_{std::move(source)} {
  std::vector<std::string> names(sources_[0]->outputType()->names());
  std::vector<TypePtr> types(sources_[0]->outputType()->children());

  names.emplace_back(idName);
  types.emplace_back(BIGINT());
  outputType_ = ROW(std::move(names), std::move(types));
  uniqueIdCounter_ = std::make_shared<std::atomic_int64_t>();
}

void AssignUniqueIdNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

namespace {
void addSortingKeys(
    std::stringstream& stream,
    const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& sortingKeys,
    const std::vector<SortOrder>& sortingOrders) {
  for (auto i = 0; i < sortingKeys.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
  }
}
} // namespace

void LocalMergeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
}

void TableWriteNode::addDetails(std::stringstream& /* stream */) const {
  // TODO Add connector details.
}

void MergeExchangeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
}

void LocalPartitionNode::addDetails(std::stringstream& stream) const {
  // Nothing to add.
  switch (type_) {
    case Type::kGather:
      stream << "GATHER";
      break;
    case Type::kRepartition:
      stream << "REPARTITION";
      break;
  }
}

void EnforceSingleRowNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

void PartitionedOutputNode::addDetails(std::stringstream& stream) const {
  if (broadcast_) {
    stream << "BROADCAST";
  } else if (numPartitions_ == 1) {
    stream << "SINGLE";
  } else {
    stream << "HASH(";
    addKeys(stream, keys_);
    stream << ") " << numPartitions_;
  }

  if (replicateNullsAndAny_) {
    stream << " replicate nulls and any";
  }
}

void TopNNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  stream << count_ << " ";

  addSortingKeys(stream, sortingKeys_, sortingOrders_);
}

void LimitNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  stream << count_;
  if (offset_) {
    stream << " offset " << offset_;
  }
}

void OrderByNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
}

void PlanNode::toString(
    std::stringstream& stream,
    bool detailed,
    bool recursive,
    size_t indentationSize,
    std::function<void(
        const PlanNodeId& planNodeId,
        const std::string& indentation,
        std::stringstream& stream)> addContext) const {
  const std::string indentation(indentationSize, ' ');

  stream << indentation << "-- " << name();

  if (detailed) {
    stream << "[";
    addDetails(stream);
    stream << "]";
    stream << " -> ";
    outputType()->printChildren(stream, ", ");
  }
  stream << std::endl;

  if (addContext) {
    auto contextIndentation = indentation + "   ";
    stream << contextIndentation;
    addContext(id_, contextIndentation, stream);
    stream << std::endl;
  }

  if (recursive) {
    for (auto& source : sources()) {
      source->toString(stream, detailed, true, indentationSize + 2, addContext);
    }
  }
}

namespace {
void collectLeafPlanNodeIds(
    const core::PlanNode& planNode,
    std::unordered_set<core::PlanNodeId>& leafIds) {
  if (planNode.sources().empty()) {
    leafIds.insert(planNode.id());
    return;
  }

  for (const auto& child : planNode.sources()) {
    collectLeafPlanNodeIds(*child, leafIds);
  }
}
} // namespace

std::unordered_set<core::PlanNodeId> PlanNode::leafPlanNodeIds() const {
  std::unordered_set<core::PlanNodeId> leafIds;
  collectLeafPlanNodeIds(*this, leafIds);
  return leafIds;
}

} // namespace facebook::velox::core
