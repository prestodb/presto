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
#include "velox/exec/tests/PlanBuilder.h"
#include <velox/exec/Aggregate.h>
#include <velox/exec/HashPartitionFunction.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;

namespace facebook::velox::exec::test {

namespace {

std::vector<std::string> makeNames(const std::string& prefix, int size) {
  std::vector<std::string> names;
  for (int i = 0; i < size; i++) {
    names.push_back(fmt::format("{}{}", prefix, i));
  }
  return names;
}

std::shared_ptr<const core::ITypedExpr> parseExpr(
    const std::string& text,
    std::shared_ptr<const RowType> rowType) {
  auto untyped = parse::parseExpr(text);
  return core::Expressions::inferTypes(untyped, rowType, nullptr);
}
} // namespace

PlanBuilder& PlanBuilder::tableScan(
    const std::shared_ptr<const RowType>& outputType) {
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (auto& name : outputType->names()) {
    assignments.insert(
        {name,
         std::make_shared<HiveColumnHandle>(
             name, HiveColumnHandle::ColumnType::kRegular)});
  }

  auto tableHandle =
      std::make_shared<HiveTableHandle>(true, SubfieldFilters{}, nullptr);
  return tableScan(outputType, tableHandle, assignments);
}

PlanBuilder& PlanBuilder::tableScan(
    const std::shared_ptr<const RowType>& outputType,
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

PlanBuilder& PlanBuilder::exchange(
    const std::shared_ptr<const RowType>& outputType) {
  planNode_ =
      std::make_shared<core::ExchangeNode>(nextPlanNodeId(), outputType);
  return *this;
}

PlanBuilder& PlanBuilder::mergeExchange(
    const std::shared_ptr<const RowType>& outputType,
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

PlanBuilder& PlanBuilder::project(
    const std::vector<std::string>& projections,
    const std::vector<std::string>& names) {
  std::vector<std::string> projectNames;
  if (names.empty()) {
    projectNames = makeNames("p", projections.size());
  } else {
    VELOX_CHECK_EQ(names.size(), projections.size());
    projectNames = names;
  }
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  for (auto& projection : projections) {
    expressions.push_back(parseExpr(projection, planNode_->outputType()));
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
      nextPlanNodeId(), parseExpr(filter, planNode_->outputType()), planNode_);
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
    const std::shared_ptr<const RowType>& columns,
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
    auto aggregate =
        exec::Aggregate::create(functionName, step_, types, UNKNOWN());
    if (aggregate) {
      return aggregate->resultType();
    }
    return nullptr;
  }

  const core::AggregationNode::Step step_;
  const core::Expressions::TypeResolverHook previousHook_;
  TypePtr resultType_;
};

} // namespace

PlanBuilder& PlanBuilder::aggregation(
    const std::vector<ChannelIndex>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys,
    const std::vector<TypePtr>& resultTypes) {
  AggregateTypeResolver resolver(step);
  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregateExprs;
  aggregateExprs.reserve(aggregates.size());
  for (auto i = 0; i < aggregates.size(); i++) {
    auto& agg = aggregates[i];
    if (i < resultTypes.size()) {
      resolver.setResultType(resultTypes[i]);
    }

    auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        parseExpr(agg, planNode_->outputType()));
    aggregateExprs.emplace_back(expr);
  }

  auto names = makeNames("a", aggregates.size());
  auto groupingExpr = fields(groupingKeys);

  // Generate masks vector for aggregations.
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(
      aggregateExprs.size());
  if (!masks.empty()) {
    VELOX_CHECK_EQ(aggregates.size(), masks.size());
    for (auto i = 0; i < masks.size(); i++) {
      aggregateMasks[i] = field(masks[i]);
    }
  }

  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      groupingExpr,
      names,
      aggregateExprs,
      aggregateMasks,
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::localMerge(
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<core::SortOrder>& sortOrder) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (int i = 0; i < keyIndices.size(); i++) {
    sortingKeys.push_back(field(keyIndices[i]));
    sortingOrders.push_back(sortOrder[i]);
  }
  planNode_ = std::make_shared<core::LocalMergeNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, planNode_);

  return *this;
}

PlanBuilder& PlanBuilder::orderBy(
    const std::vector<ChannelIndex>& keyIndices,
    const std::vector<core::SortOrder>& sortOrder,
    bool isPartial) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (int i = 0; i < keyIndices.size(); i++) {
    sortingKeys.emplace_back(field(keyIndices[i]));
    sortingOrders.emplace_back(sortOrder[i]);
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
    const std::vector<ChannelIndex>& channels) {
  std::vector<std::string> names;
  names.reserve(channels.size());
  std::vector<TypePtr> types;
  types.reserve(channels.size());
  for (auto channel : channels) {
    names.emplace_back(type->nameOf(channel));
    types.emplace_back(type->childAt(channel));
  }
  return ROW(std::move(names), std::move(types));
}
} // namespace

PlanBuilder& PlanBuilder::hashJoin(
    const std::vector<ChannelIndex>& leftKeys,
    const std::vector<ChannelIndex>& rightKeys,
    const std::shared_ptr<facebook::velox::core::PlanNode>& build,
    const std::string& filterText,
    const std::vector<ChannelIndex>& output,
    core::JoinType joinType) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  std::shared_ptr<const core::ITypedExpr> filterExpr;
  if (!filterText.empty()) {
    filterExpr = parseExpr(filterText, resultType);
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

PlanBuilder& PlanBuilder::crossJoin(
    const std::shared_ptr<core::PlanNode>& build,
    const std::vector<ChannelIndex>& output) {
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
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    int index) {
  return field(planNode_->outputType(), index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const std::string& name) {
  auto index = planNode_->outputType()->getChildIdx(name);
  return field(planNode_->outputType(), index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const std::shared_ptr<const RowType>& inputType,
    int index) {
  auto name = inputType->names()[index];
  auto type = inputType->childAt(index);
  std::vector<std::shared_ptr<const core::ITypedExpr>> inputs = {
      std::make_shared<core::InputTypedExpr>(inputType)};
  return std::make_shared<core::FieldAccessTypedExpr>(
      type, std::move(inputs), name);
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(
    const std::shared_ptr<const RowType> inputType,
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
} // namespace facebook::velox::exec::test
