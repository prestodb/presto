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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include <velox/exec/Aggregate.h>
#include <velox/exec/HashPartitionFunction.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/parse/Expressions.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;

namespace facebook::velox::exec::test {

namespace {

std::shared_ptr<const core::ITypedExpr> parseExpr(
    const std::string& text,
    RowTypePtr rowType,
    memory::MemoryPool* pool) {
  auto untyped = duckdb::parseExpr(text);
  return core::Expressions::inferTypes(untyped, rowType, pool);
}
} // namespace

PlanBuilder& PlanBuilder::tableScan(const RowTypePtr& outputType) {
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (uint32_t i = 0; i < outputType->size(); ++i) {
    const auto& name = outputType->nameOf(i);
    const auto& type = outputType->childAt(i);
    assignments.insert(
        {name,
         std::make_shared<HiveColumnHandle>(
             name, HiveColumnHandle::ColumnType::kRegular, type)});
  }

  auto tableHandle =
      std::make_shared<HiveTableHandle>(true, SubfieldFilters{}, nullptr);
  return tableScan(outputType, tableHandle, assignments);
}

PlanBuilder& PlanBuilder::tableScan(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& assignments) {
  planNode_ = std::make_shared<core::TableScanNode>(
      nextPlanNodeId(), outputType, tableHandle, assignments);
  return *this;
}

PlanBuilder& PlanBuilder::values(
    const std::vector<RowVectorPtr>& values,
    bool parallelizable) {
  auto valuesCopy = values;
  planNode_ = std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::move(valuesCopy), parallelizable);
  return *this;
}

PlanBuilder& PlanBuilder::exchange(const RowTypePtr& outputType) {
  planNode_ =
      std::make_shared<core::ExchangeNode>(nextPlanNodeId(), outputType);
  return *this;
}

PlanBuilder& PlanBuilder::mergeExchange(
    const RowTypePtr& outputType,
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<core::SortOrder>& sortOrder) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (int i = 0; i < keyIndices.size(); i++) {
    sortingKeys.push_back(field(outputType, keyIndices[i]));
    sortingOrders.push_back(sortOrder[i]);
  }
  planNode_ = std::make_shared<core::MergeExchangeNode>(
      nextPlanNodeId(), outputType, sortingKeys, sortingOrders);

  return *this;
}

PlanBuilder& PlanBuilder::project(const std::vector<std::string>& projections) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  std::vector<std::string> projectNames;
  for (auto i = 0; i < projections.size(); ++i) {
    auto untypedExpr = duckdb::parseExpr(projections[i]);
    expressions.push_back(inferTypes(untypedExpr));
    if (untypedExpr->alias().has_value()) {
      projectNames.push_back(untypedExpr->alias().value());
    } else if (
        auto fieldExpr =
            dynamic_cast<const core::FieldAccessExpr*>(untypedExpr.get())) {
      projectNames.push_back(fieldExpr->getFieldName());
    } else {
      projectNames.push_back(fmt::format("p{}", i));
    }
  }
  planNode_ = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(projectNames),
      std::move(expressions),
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::filter(const std::string& filter) {
  planNode_ = std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      parseExpr(filter, planNode_->outputType(), pool_),
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::tableWrite(
    const std::vector<std::string>& columnNames,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    const std::string& rowCountColumnName) {
  return tableWrite(
      planNode_->outputType(), columnNames, insertHandle, rowCountColumnName);
}

PlanBuilder& PlanBuilder::tableWrite(
    const RowTypePtr& columns,
    const std::vector<std::string>& columnNames,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    const std::string& rowCountColumnName) {
  auto outputType =
      ROW({rowCountColumnName, "fragments", "commitcontext"},
          {BIGINT(), VARBINARY(), VARBINARY()});
  planNode_ = std::make_shared<core::TableWriteNode>(
      nextPlanNodeId(),
      columns,
      columnNames,
      insertHandle,
      outputType,
      planNode_);
  return *this;
}

namespace {

template <TypeKind Kind>
TypePtr nameToType() {
  using T = typename TypeTraits<Kind>::NativeType;
  return CppToType<T>::create();
}

TypePtr resolveAggregateType(
    const std::string& aggregateName,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& rawInputTypes) {
  if (auto signatures = exec::getAggregateFunctionSignatures(aggregateName)) {
    for (const auto& signature : signatures.value()) {
      exec::SignatureBinder binder(*signature, rawInputTypes);
      if (binder.tryBind()) {
        return binder.tryResolveType(
            exec::isPartialOutput(step) ? signature->intermediateType()
                                        : signature->returnType());
      }
    }
  }

  return nullptr;
}

class AggregateTypeResolver {
 public:
  explicit AggregateTypeResolver(core::AggregationNode::Step step)
      : step_(step), previousHook_(core::Expressions::getResolverHook()) {
    core::Expressions::setTypeResolverHook(
        [&](const auto& inputs, const auto& expr) {
          return resolveType(inputs, expr);
        });
  }

  ~AggregateTypeResolver() {
    core::Expressions::setTypeResolverHook(previousHook_);
  }

  void setResultType(const TypePtr& type) {
    resultType_ = type;
  }

 private:
  std::shared_ptr<const Type> resolveType(
      const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs,
      const std::shared_ptr<const core::CallExpr>& expr) const {
    if (resultType_) {
      return resultType_;
    }

    std::vector<TypePtr> types;
    for (auto& input : inputs) {
      types.push_back(input->type());
    }

    auto functionName = expr->getFunctionName();

    // Use raw input types (if available) to resolve intermediate and final
    // result types.
    if (exec::isRawInput(step_)) {
      if (auto type = resolveAggregateType(functionName, step_, types)) {
        return type;
      }
    }

    return nullptr;
  }

  const core::AggregationNode::Step step_;
  const core::Expressions::TypeResolverHook previousHook_;
  TypePtr resultType_;
};

} // namespace

std::shared_ptr<core::PlanNode>
PlanBuilder::createIntermediateOrFinalAggregation(
    core::AggregationNode::Step step,
    const core::AggregationNode* partialAggNode) {
  // Create intermediate or final aggregation using same grouping keys and same
  // aggregate function names.
  const auto& partialAggregates = partialAggNode->aggregates();
  const auto& groupingKeys = partialAggNode->groupingKeys();

  auto numAggregates = partialAggregates.size();
  auto numGroupingKeys = groupingKeys.size();

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> masks(
      numAggregates);

  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregates;
  aggregates.reserve(numAggregates);
  for (auto i = 0; i < numAggregates; i++) {
    // Resolve final or intermediate aggregation result type using raw input
    // types for the partial aggregation.
    auto name = partialAggregates[i]->name();
    auto rawInputs = partialAggregates[i]->inputs();

    std::vector<TypePtr> rawInputTypes;
    for (auto& rawInput : rawInputs) {
      rawInputTypes.push_back(rawInput->type());
    }

    auto type = resolveAggregateType(name, step, rawInputTypes);
    VELOX_CHECK_NOT_NULL(
        type, "Failed to resolve result type for aggregate function {}", name);
    std::vector<std::shared_ptr<const core::ITypedExpr>> inputs = {
        field(numGroupingKeys + i)};
    aggregates.emplace_back(
        std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name));
  }

  return std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      groupingKeys,
      partialAggNode->preGroupedKeys(),
      partialAggNode->aggregateNames(),
      aggregates,
      masks,
      partialAggNode->ignoreNullKeys(),
      planNode_);
}

PlanBuilder& PlanBuilder::intermediateAggregation() {
  // Current plan node must be a partial aggregation.
  auto* aggNode = dynamic_cast<core::AggregationNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(
      aggNode, "Current plan node must be a partial aggregation.");

  VELOX_CHECK(exec::isPartialOutput(aggNode->step()));
  VELOX_CHECK(exec::isRawInput(aggNode->step()));

  auto step = core::AggregationNode::Step::kIntermediate;

  planNode_ = createIntermediateOrFinalAggregation(step, aggNode);
  return *this;
}

PlanBuilder& PlanBuilder::finalAggregation() {
  // Current plan node must be a partial or intermediate aggregation.
  const auto* aggNode = dynamic_cast<core::AggregationNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(
      aggNode,
      "Current plan node must be a partial or intermediate aggregation.");

  VELOX_CHECK(exec::isPartialOutput(aggNode->step()));
  if (!exec::isRawInput(aggNode->step())) {
    // Check the source node.
    aggNode =
        dynamic_cast<const core::AggregationNode*>(aggNode->sources()[0].get());
    VELOX_CHECK_NOT_NULL(
        aggNode,
        "Plan node before current plan node must be a partial aggregation.");
    VELOX_CHECK(exec::isRawInput(aggNode->step()));
    VELOX_CHECK(exec::isPartialOutput(aggNode->step()));
  }

  auto step = core::AggregationNode::Step::kFinal;

  planNode_ = createIntermediateOrFinalAggregation(step, aggNode);
  return *this;
}

PlanBuilder::AggregateExpressionsAndNames
PlanBuilder::createAggregateExpressionsAndNames(
    const std::vector<std::string>& aggregates,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& resultTypes) {
  AggregateTypeResolver resolver(step);
  std::vector<std::shared_ptr<const core::CallTypedExpr>> exprs;
  std::vector<std::string> names;
  exprs.reserve(aggregates.size());
  names.reserve(aggregates.size());
  for (auto i = 0; i < aggregates.size(); i++) {
    auto& agg = aggregates[i];
    if (i < resultTypes.size()) {
      resolver.setResultType(resultTypes[i]);
    }

    auto untypedExpr = duckdb::parseExpr(agg);

    auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        inferTypes(untypedExpr));
    exprs.emplace_back(expr);

    if (untypedExpr->alias().has_value()) {
      names.push_back(untypedExpr->alias().value());
    } else {
      names.push_back(fmt::format("a{}", i));
    }
  }

  return {exprs, names};
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::createAggregateMasks(
    size_t numAggregates,
    const std::vector<std::string>& masks) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> maskExprs(
      numAggregates);
  if (masks.empty()) {
    return maskExprs;
  }

  VELOX_CHECK_EQ(numAggregates, masks.size());
  for (auto i = 0; i < numAggregates; ++i) {
    if (!masks[i].empty()) {
      maskExprs[i] = field(masks[i]);
    }
  }

  return maskExprs;
}

PlanBuilder& PlanBuilder::aggregation(
    const std::vector<ChannelIndex>& groupingKeys,
    const std::vector<ChannelIndex>& preGroupedKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys,
    const std::vector<TypePtr>& resultTypes) {
  auto numAggregates = aggregates.size();
  auto aggregatesAndNames =
      createAggregateExpressionsAndNames(aggregates, step, resultTypes);
  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      fields(groupingKeys),
      fields(preGroupedKeys),
      aggregatesAndNames.names,
      aggregatesAndNames.aggregates,
      createAggregateMasks(numAggregates, masks),
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::streamingAggregation(
    const std::vector<ChannelIndex>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys,
    const std::vector<TypePtr>& resultTypes) {
  auto numAggregates = aggregates.size();
  auto aggregatesAndNames =
      createAggregateExpressionsAndNames(aggregates, step, resultTypes);
  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      fields(groupingKeys),
      fields(groupingKeys),
      aggregatesAndNames.names,
      aggregatesAndNames.aggregates,
      createAggregateMasks(numAggregates, masks),
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::localMerge(
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<core::SortOrder>& sortOrder,
    std::vector<std::shared_ptr<const core::PlanNode>> sources) {
  VELOX_CHECK_NULL(planNode_, "localMerge() must be the first call");
  VELOX_CHECK_GE(
      sources.size(), 1, "localMerge() requires at least one source");
  const auto& inputType = sources[0]->outputType();
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (int i = 0; i < keyIndices.size(); i++) {
    sortingKeys.push_back(field(inputType, keyIndices[i]));
    sortingOrders.push_back(sortOrder[i]);
  }
  planNode_ = std::make_shared<core::LocalMergeNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, std::move(sources));

  return *this;
}

PlanBuilder& PlanBuilder::orderBy(
    const std::vector<std::string>& keys,
    bool isPartial) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (auto i = 0; i < keys.size(); ++i) {
    auto [untypedExpr, sortOrder] = duckdb::parseOrderByExpr(keys[i]);
    auto typedExpr = inferTypes(untypedExpr);

    auto sortingKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    VELOX_CHECK_NOT_NULL(
        sortingKey,
        "ORDER BY clause must use column names, not expressions: {}",
        keys[i]);
    sortingKeys.emplace_back(sortingKey);
    sortingOrders.emplace_back(sortOrder);
  }

  planNode_ = std::make_shared<core::OrderByNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, isPartial, planNode_);

  return *this;
}

PlanBuilder& PlanBuilder::topN(
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<core::SortOrder>& sortOrder,
    int32_t count,
    bool isPartial) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (int i = 0; i < keyIndices.size(); i++) {
    sortingKeys.push_back(field(keyIndices[i]));
    sortingOrders.push_back(sortOrder[i]);
  }
  planNode_ = std::make_shared<core::TopNNode>(
      nextPlanNodeId(),
      sortingKeys,
      sortingOrders,
      count,
      isPartial,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::limit(int32_t offset, int32_t count, bool isPartial) {
  planNode_ = std::make_shared<core::LimitNode>(
      nextPlanNodeId(), offset, count, isPartial, planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::enforceSingleRow() {
  planNode_ =
      std::make_shared<core::EnforceSingleRowNode>(nextPlanNodeId(), planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::assignUniqueId(
    const std::string& idName,
    const int32_t taskUniqueId) {
  planNode_ = std::make_shared<core::AssignUniqueIdNode>(
      nextPlanNodeId(), idName, taskUniqueId, planNode_);
  return *this;
}

namespace {
RowTypePtr toRowType(
    RowTypePtr inputType,
    const std::vector<ChannelIndex>& outputLayout) {
  if (outputLayout.empty()) {
    return inputType;
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (auto index : outputLayout) {
    names.push_back(inputType->nameOf(index));
    types.push_back(inputType->childAt(index));
  }
  return ROW(std::move(names), std::move(types));
}

core::PartitionFunctionFactory createPartitionFunctionFactory(
    const RowTypePtr& inputType,
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        keys) {
  if (keys.empty()) {
    return
        [](auto /*numPartitions*/) -> std::unique_ptr<core::PartitionFunction> {
          VELOX_UNREACHABLE();
        };
  } else {
    return [inputType, keyIndices](
               auto numPartitions) -> std::unique_ptr<core::PartitionFunction> {
      return std::make_unique<exec::HashPartitionFunction>(
          numPartitions, inputType, keyIndices);
    };
  }
}

} // namespace

PlanBuilder& PlanBuilder::partitionedOutput(
    const std::vector<ChannelIndex>& keyIndices,
    int numPartitions,
    const std::vector<ChannelIndex>& outputLayout) {
  return partitionedOutput(keyIndices, numPartitions, false, outputLayout);
}

PlanBuilder& PlanBuilder::partitionedOutput(
    const std::vector<ChannelIndex>& keyIndices,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<ChannelIndex>& outputLayout) {
  auto outputType = toRowType(planNode_->outputType(), outputLayout);
  auto keys = fields(keyIndices);
  auto partitionFunctionFactory =
      createPartitionFunctionFactory(planNode_->outputType(), keyIndices, keys);
  planNode_ = std::make_shared<core::PartitionedOutputNode>(
      nextPlanNodeId(),
      keys,
      numPartitions,
      false,
      replicateNullsAndAny,
      std::move(partitionFunctionFactory),
      outputType,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::partitionedOutputBroadcast(
    const std::vector<ChannelIndex>& outputLayout) {
  auto outputType = toRowType(planNode_->outputType(), outputLayout);
  planNode_ = core::PartitionedOutputNode::broadcast(
      nextPlanNodeId(), 1, outputType, planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::localPartition(
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
    const std::vector<ChannelIndex>& outputLayout) {
  VELOX_CHECK_NULL(planNode_, "localPartition() must be the first call");
  auto inputType = sources[0]->outputType();
  auto outputType = toRowType(inputType, outputLayout);
  auto keys = fields(inputType, keyIndices);
  auto partitionFunctionFactory =
      createPartitionFunctionFactory(inputType, keyIndices, keys);
  planNode_ = std::make_shared<core::LocalPartitionNode>(
      nextPlanNodeId(), partitionFunctionFactory, outputType, sources);
  return *this;
}

namespace {
RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<TypePtr> types = a->children();
  names.insert(names.end(), b->names().begin(), b->names().end());
  types.insert(types.end(), b->children().begin(), b->children().end());
  return ROW(std::move(names), std::move(types));
}

RowTypePtr extract(
    const RowTypePtr& type,
    const std::vector<std::string>& childNames) {
  std::vector<std::string> names = childNames;

  std::vector<TypePtr> types;
  types.reserve(childNames.size());
  for (const auto& name : childNames) {
    types.emplace_back(type->findChild(name));
  }
  return ROW(std::move(names), std::move(types));
}
} // namespace

PlanBuilder& PlanBuilder::hashJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::shared_ptr<facebook::velox::core::PlanNode>& build,
    const std::string& filterText,
    const std::vector<std::string>& output,
    core::JoinType joinType) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  std::shared_ptr<const core::ITypedExpr> filterExpr;
  if (!filterText.empty()) {
    filterExpr = parseExpr(filterText, resultType, pool_);
  }
  auto outputType = extract(resultType, output);
  auto leftKeyFields = fields(leftType, leftKeys);
  auto rightKeyFields = fields(rightType, rightKeys);

  planNode_ = std::make_shared<core::HashJoinNode>(
      nextPlanNodeId(),
      joinType,
      leftKeyFields,
      rightKeyFields,
      std::move(filterExpr),
      std::move(planNode_),
      build,
      outputType);
  return *this;
}

PlanBuilder& PlanBuilder::mergeJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::shared_ptr<facebook::velox::core::PlanNode>& build,
    const std::string& filterText,
    const std::vector<std::string>& output,
    core::JoinType joinType) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  std::shared_ptr<const core::ITypedExpr> filterExpr;
  if (!filterText.empty()) {
    filterExpr = parseExpr(filterText, resultType, pool_);
  }
  auto outputType = extract(resultType, output);
  auto leftKeyFields = fields(leftType, leftKeys);
  auto rightKeyFields = fields(rightType, rightKeys);

  planNode_ = std::make_shared<core::MergeJoinNode>(
      nextPlanNodeId(),
      joinType,
      leftKeyFields,
      rightKeyFields,
      std::move(filterExpr),
      std::move(planNode_),
      build,
      outputType);
  return *this;
}

PlanBuilder& PlanBuilder::crossJoin(
    const std::shared_ptr<core::PlanNode>& build,
    const std::vector<std::string>& output) {
  auto resultType = concat(planNode_->outputType(), build->outputType());
  auto outputType = extract(resultType, output);

  planNode_ = std::make_shared<core::CrossJoinNode>(
      nextPlanNodeId(), std::move(planNode_), build, outputType);
  return *this;
}

PlanBuilder& PlanBuilder::unnest(
    const std::vector<std::string>& replicateColumns,
    const std::vector<std::string>& unnestColumns,
    const std::optional<std::string>& ordinalColumn) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      replicateFields;
  replicateFields.reserve(replicateColumns.size());
  for (const auto& name : replicateColumns) {
    replicateFields.emplace_back(field(name));
  }

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> unnestFields;
  unnestFields.reserve(unnestColumns.size());
  for (const auto& name : unnestColumns) {
    unnestFields.emplace_back(field(name));
  }

  std::vector<std::string> unnestNames;
  for (const auto& name : unnestColumns) {
    auto input = planNode_->outputType()->findChild(name);
    if (input->isArray()) {
      unnestNames.push_back(name + "_e");
    } else if (input->isMap()) {
      unnestNames.push_back(name + "_k");
      unnestNames.push_back(name + "_v");
    } else {
      VELOX_NYI(
          "Unsupported type of unnest variable. Expected ARRAY or MAP, but got {}.",
          input->toString());
    }
  }

  planNode_ = std::make_shared<core::UnnestNode>(
      nextPlanNodeId(),
      replicateFields,
      unnestFields,
      unnestNames,
      ordinalColumn,
      planNode_);
  return *this;
}

std::string PlanBuilder::nextPlanNodeId() {
  return fmt::format("{}", planNodeIdGenerator_->next());
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    int index) {
  return field(planNode_->outputType(), index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const std::string& name) {
  return field(planNode_->outputType(), name);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const RowTypePtr& inputType,
    const std::string& name) {
  auto index = inputType->getChildIdx(name);
  return field(inputType, index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const RowTypePtr& inputType,
    int index) {
  auto name = inputType->names()[index];
  auto type = inputType->childAt(index);
  return std::make_shared<core::FieldAccessTypedExpr>(type, name);
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(const std::vector<std::string>& names) {
  return fields(planNode_->outputType(), names);
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(
    const RowTypePtr& inputType,
    const std::vector<std::string>& names) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
  for (const auto& name : names) {
    fields.push_back(field(inputType, name));
  }
  return fields;
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(
    const RowTypePtr& inputType,
    const std::vector<ChannelIndex>& indices) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
  for (auto& index : indices) {
    fields.push_back(field(inputType, index));
  }
  return fields;
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(const std::vector<ChannelIndex>& indices) {
  return fields(planNode_->outputType(), indices);
}

std::shared_ptr<const core::ITypedExpr> PlanBuilder::inferTypes(
    const std::shared_ptr<const core::IExpr>& untypedExpr) {
  return core::Expressions::inferTypes(
      untypedExpr, planNode_->outputType(), pool_);
}
} // namespace facebook::velox::exec::test
