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

#include "velox/parse/QueryPlanner.h"
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/parse/DuckLogicalOperator.h"

#include <duckdb.hpp> // @manual
#include <duckdb/main/connection.hpp> // @manual
#include <duckdb/planner/expression/bound_aggregate_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_cast_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_comparison_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_constant_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_function_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_reference_expression.hpp> // @manual
#include <duckdb/planner/operator/logical_dummy_scan.hpp> // @manual

namespace facebook::velox::core {

namespace {

class ColumnNameGenerator {
 public:
  std::string next(const std::string& prefix = "_c") {
    if (names_.count(prefix)) {
      auto name = fmt::format("{}{}", prefix, nextId_++);
      names_.insert(name);
      return name;
    }

    names_.insert(prefix);
    return prefix;
  }

 private:
  std::unordered_set<std::string> names_;
  int nextId_{0};
};

struct QueryContext {
  PlanNodeIdGenerator planNodeIdGenerator;
  ColumnNameGenerator columnNameGenerator;
  const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
      inMemoryTables;
  MakeTableScan makeTableScan;
  bool isInDelimJoin{false};

  QueryContext(const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
                   _inMemoryTables)
      : inMemoryTables{_inMemoryTables} {}

  std::string nextNodeId() {
    return planNodeIdGenerator.next();
  }

  std::string nextColumnName() {
    return columnNameGenerator.next();
  }

  std::string nextColumnName(const std::string& prefix) {
    return columnNameGenerator.next(prefix);
  }
};
std::string mapScalarFunctionName(const std::string& name) {
  static const std::unordered_map<std::string, std::string> kMapping = {
      {"+", "plus"},
      {"-", "minus"},
      {"*", "multiply"},
      {"/", "divide"},
      {"%", "mod"},
      {"~~", "like"},
      {"!~~", "not_like"},
      {"list_value", "array_constructor"},
  };

  auto it = kMapping.find(name);
  if (it != kMapping.end()) {
    return it->second;
  }

  return name;
}

std::string mapAggregateFunctionName(const std::string& name) {
  static const std::unordered_map<std::string, std::string> kMapping = {
      {"count_star", "count"},
  };

  auto it = kMapping.find(name);
  if (it != kMapping.end()) {
    return it->second;
  }

  return name;
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalDummyScan& logicalDummyScan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < logicalDummyScan.types.size(); ++i) {
    names.push_back(queryContext.nextColumnName());
    types.push_back(duckdb::toVeloxType(logicalDummyScan.types[i]));
  }

  auto rowType = ROW(std::move(names), std::move(types));

  std::vector<RowVectorPtr> vectors = {std::make_shared<RowVector>(
      pool, rowType, nullptr, 1, std::vector<VectorPtr>{})};
  return std::make_shared<ValuesNode>(queryContext.nextNodeId(), vectors);
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalGet& logicalGet,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  if (logicalGet.function.name == "unnest") {
    VELOX_CHECK_EQ(1, sources.size());
    return std::make_shared<UnnestNode>(
        queryContext.nextNodeId(),
        std::vector<FieldAccessTypedExprPtr>{}, // replicateVariables
        std::vector<FieldAccessTypedExprPtr>{
            std::make_shared<FieldAccessTypedExpr>(
                sources[0]->outputType()->childAt(0),
                sources[0]->outputType()->asRow().nameOf(0))},
        std::vector<std::string>{"a"},
        std::nullopt, // ordinalityName
        std::move(sources[0]));
  }

  VELOX_CHECK_EQ(logicalGet.function.name, "seq_scan");
  VELOX_CHECK_EQ(0, sources.size());

  std::vector<std::string> columnNames;
  const auto& columnIds = logicalGet.column_ids;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  constexpr uint64_t kNone = ~0UL;
  for (auto i = 0; i < columnIds.size(); ++i) {
    if (columnIds[i] == kNone) {
      continue;
    }
    names.push_back(
        queryContext.nextColumnName(logicalGet.names[columnIds[i]]));
    types.push_back(
        duckdb::toVeloxType(logicalGet.returned_types[columnIds[i]]));
    columnNames.push_back(logicalGet.names[columnIds[i]]);
  }

  auto rowType = ROW(std::move(names), std::move(types));

  auto tableName = logicalGet.function.to_string(logicalGet.bind_data.get());
  auto it = queryContext.inMemoryTables.find(tableName);

  if (it == queryContext.inMemoryTables.end()) {
    return queryContext.makeTableScan(
        queryContext.nextNodeId(), tableName, rowType, columnNames);
  }

  std::vector<RowVectorPtr> data;
  for (auto& rowVector : it->second) {
    std::vector<VectorPtr> children;
    if (rowVector->size() > 0) {
      for (auto i = 0; i < columnIds.size(); ++i) {
        children.push_back(rowVector->childAt(columnIds[i]));
      }
    }
    data.push_back(std::make_shared<RowVector>(
        pool, rowType, nullptr, rowVector->size(), children));
  }

  return std::make_shared<ValuesNode>(queryContext.nextNodeId(), data);
}

TypedExprPtr toVeloxExpression(
    ::duckdb::Expression& expression,
    const TypePtr& inputType);

TypedExprPtr toVeloxComparisonExpression(
    const std::string& name,
    ::duckdb::Expression& expression,
    const TypePtr& inputType) {
  auto* comparison =
      dynamic_cast<::duckdb::BoundComparisonExpression*>(&expression);
  std::vector<TypedExprPtr> children{
      toVeloxExpression(*comparison->left, inputType),
      toVeloxExpression(*comparison->right, inputType)};

  return std::make_shared<CallTypedExpr>(BOOLEAN(), std::move(children), name);
}

namespace {
struct VeloxColumnProjections {
  VeloxColumnProjections(QueryContext& context) : context(context) {}

  core::FieldAccessTypedExprPtr toFieldAccess(
      ::duckdb::Expression& expression,
      const TypePtr& inputType) {
    auto expr = toVeloxExpression(expression, inputType);
    auto column = std::dynamic_pointer_cast<const FieldAccessTypedExpr>(expr);
    if (column) {
      columns.push_back(column);
      exprs.push_back(expr);
      return column;
    }

    allIdentity = false;
    exprs.push_back(expr);
    const auto name = context.nextColumnName("_p");
    auto projected =
        std::make_shared<FieldAccessTypedExpr>(exprs.back()->type(), name);
    columns.push_back(projected);
    return projected;
  }

  void addColumn(const FieldAccessTypedExprPtr& column) {
    for (auto& existingColumn : columns) {
      if (column->name() == existingColumn->name()) {
        return;
      }
    }
    exprs.push_back(column);
    columns.push_back(column);
  }

  /// Returns 'input' wrapped in the projections of 'this'. May only be called
  /// once.
  PlanNodePtr source(PlanNodePtr input) {
    if (allIdentity) {
      return input;
    }

    std::vector<std::string> names;
    names.reserve(columns.size());
    for (auto& column : columns) {
      names.push_back(column->name());
    }
    return std::make_shared<ProjectNode>(
        context.nextNodeId(), std::move(names), std::move(exprs), input);
  }

  QueryContext& context;
  bool allIdentity{true};
  std::vector<core::TypedExprPtr> exprs;
  std::vector<core::FieldAccessTypedExprPtr> columns;
};
} // namespace

TypedExprPtr toVeloxExpression(
    ::duckdb::Expression& expression,
    const TypePtr& inputType) {
  switch (expression.type) {
    case ::duckdb::ExpressionType::VALUE_CONSTANT: {
      auto* constant =
          dynamic_cast<::duckdb::BoundConstantExpression*>(&expression);
      return std::make_shared<ConstantTypedExpr>(
          duckdb::toVeloxType(constant->return_type),
          duckdb::duckValueToVariant(constant->value));
    }
    case ::duckdb::ExpressionType::COMPARE_EQUAL:
      return toVeloxComparisonExpression("eq", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_NOTEQUAL:
      return toVeloxComparisonExpression("neq", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return toVeloxComparisonExpression("gt", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return toVeloxComparisonExpression("gte", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_LESSTHAN:
      return toVeloxComparisonExpression("lt", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return toVeloxComparisonExpression("lte", expression, inputType);

    case ::duckdb::ExpressionType::OPERATOR_CAST: {
      auto* cast = dynamic_cast<::duckdb::BoundCastExpression*>(&expression);
      return std::make_shared<CastTypedExpr>(
          duckdb::toVeloxType(cast->return_type),
          std::vector<TypedExprPtr>{toVeloxExpression(*cast->child, inputType)},
          cast->try_cast);
    }
    case ::duckdb::ExpressionType::BOUND_FUNCTION: {
      auto* func =
          dynamic_cast<::duckdb::BoundFunctionExpression*>(&expression);

      std::vector<TypedExprPtr> children;
      for (auto& child : func->children) {
        children.push_back(toVeloxExpression(*child, inputType));
      }

      auto name = mapScalarFunctionName(func->function.name);
      bool negate = false;
      if (name == "not_like") {
        name = "like";
        negate = true;
      }
      auto call = std::make_shared<CallTypedExpr>(
          duckdb::toVeloxType(func->function.return_type),
          std::move(children),
          name);
      if (negate) {
        return std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{call}, "not");
      }
      return call;
    }
    case ::duckdb::ExpressionType::BOUND_REF: {
      auto* ref =
          dynamic_cast<::duckdb::BoundReferenceExpression*>(&expression);
      return std::make_shared<FieldAccessTypedExpr>(
          duckdb::toVeloxType(ref->return_type),
          inputType->asRow().nameOf(ref->index));
    }
    case ::duckdb::ExpressionType::BOUND_AGGREGATE: {
      auto* agg =
          dynamic_cast<::duckdb::BoundAggregateExpression*>(&expression);

      std::vector<TypedExprPtr> children;
      for (auto& child : agg->children) {
        children.push_back(toVeloxExpression(*child, inputType));
      }

      return std::make_shared<CallTypedExpr>(
          duckdb::toVeloxType(agg->return_type),
          std::move(children),
          mapAggregateFunctionName(agg->function.name));
    }
    default:
      VELOX_NYI(
          "Expression type {} is not supported yet: {}",
          ::duckdb::ExpressionTypeToString(expression.type),
          expression.ToString());
  }
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalFilter& logicalFilter,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  TypedExprPtr veloxFilter;
  for (auto& expr : logicalFilter.expressions) {
    auto conjunct = toVeloxExpression(*expr, sources[0]->outputType());
    if (!veloxFilter) {
      veloxFilter = conjunct;
    } else {
      veloxFilter = std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{veloxFilter, conjunct}, "and");
    }
  }
  return std::make_shared<FilterNode>(
      queryContext.nextNodeId(), std::move(veloxFilter), std::move(sources[0]));
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalProjection& logicalProjection,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<TypedExprPtr> projections;
  for (auto& expression : logicalProjection.expressions) {
    projections.push_back(
        toVeloxExpression(*expression, sources[0]->outputType()));
  }

  // TODO Figure out how to use these.
  auto columnBindings = logicalProjection.GetColumnBindings();

  std::vector<std::string> names;
  for (auto i = 0; i < projections.size(); ++i) {
    names.push_back(queryContext.nextColumnName("_p"));
  }
  return std::make_shared<ProjectNode>(
      queryContext.nextNodeId(),
      std::move(names),
      std::move(projections),
      std::move(sources[0]));
}

namespace {
std::string translateAggregateName(const std::string& name) {
  // first(x) is used to get one element of a set. The closes Velox counterpart
  // is arbitrary, which usually returns the first value it sees.
  if (name == "first") {
    return "arbitrary";
  }
  return name;
}
} // namespace

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalAggregate& logicalAggregate,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<AggregationNode::Aggregate> aggregates;

  std::vector<std::string> projectNames;
  std::vector<TypedExprPtr> projections;

  bool identityProjection = true;
  for (auto& expression : logicalAggregate.expressions) {
    auto call = std::dynamic_pointer_cast<const CallTypedExpr>(
        toVeloxExpression(*expression, sources[0]->outputType()));
    if (expression->return_type.InternalType() ==
        ::duckdb::PhysicalType::INT128) {
      call = std::make_shared<CallTypedExpr>(
          BIGINT(), call->inputs(), call->name());
    }
    std::vector<TypedExprPtr> fieldInputs;
    std::vector<TypePtr> rawInputTypes;

    for (auto& input : call->inputs()) {
      projections.push_back(input);
      rawInputTypes.push_back(input->type());

      if (auto field =
              std::dynamic_pointer_cast<const FieldAccessTypedExpr>(input)) {
        projectNames.push_back(field->name());
        fieldInputs.push_back(field);
      } else {
        identityProjection = false;
        projectNames.push_back(queryContext.nextColumnName("_p"));
        fieldInputs.push_back(std::make_shared<FieldAccessTypedExpr>(
            input->type(), projectNames.back()));
      }
    }

    auto aggName = translateAggregateName(call->name());
    aggregates.push_back({
        std::make_shared<CallTypedExpr>(call->type(), fieldInputs, aggName),
        rawInputTypes,
        nullptr, // mask
        {}, // sortingKeys
        {} // sortingOrders
    });
  }

  std::vector<FieldAccessTypedExprPtr> groupingKeys;
  for (auto& expression : logicalAggregate.groups) {
    auto groupingExpr =
        toVeloxExpression(*expression, sources[0]->outputType());
    projections.push_back(groupingExpr);
    if (auto field = std::dynamic_pointer_cast<const FieldAccessTypedExpr>(
            groupingExpr)) {
      projectNames.push_back(field->name());
      groupingKeys.push_back(field);
    } else {
      identityProjection = false;
      projectNames.push_back(queryContext.nextColumnName("_p"));
      groupingKeys.push_back(std::make_shared<FieldAccessTypedExpr>(
          groupingExpr->type(), projectNames.back()));
    }
  }

  auto source = sources[0];

  if (!identityProjection) {
    source = std::make_shared<ProjectNode>(
        queryContext.nextNodeId(),
        std::move(projectNames),
        std::move(projections),
        std::move(sources[0]));
  }

  std::vector<std::string> names;
  for (auto i = 0; i < aggregates.size(); ++i) {
    names.push_back(queryContext.nextColumnName("_a"));
  }

  return std::make_shared<AggregationNode>(
      queryContext.nextNodeId(),
      AggregationNode::Step::kSingle,
      groupingKeys,
      std::vector<FieldAccessTypedExprPtr>{}, // preGroupedKeys
      names,
      std::move(aggregates),
      false, // ignoreNullKeys
      source);
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalOrder& logicalOrder,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  VeloxColumnProjections projections(queryContext);
  std::vector<FieldAccessTypedExprPtr> keys;
  std::vector<SortOrder> sortOrder;
  auto source = sources[0];
  for (auto& order : logicalOrder.orders) {
    keys.push_back(
        projections.toFieldAccess(*order.expression, source->outputType()));
    sortOrder.push_back(SortOrder(
        order.type == ::duckdb::OrderType::ASCENDING ||
            order.type == ::duckdb::OrderType::ORDER_DEFAULT,
        order.null_order == ::duckdb::OrderByNullType::NULLS_FIRST ||
            order.null_order == ::duckdb::OrderByNullType::ORDER_DEFAULT));
  }

  return std::make_shared<OrderByNode>(
      queryContext.nextNodeId(),
      keys,
      sortOrder,
      /*isPartial=*/false,
      projections.source(source));
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalCrossProduct& logicalCrossProduct,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(2, sources.size());

  const auto& leftInputType = sources[0]->outputType()->asRow();
  const auto& rightInputType = sources[1]->outputType()->asRow();

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < leftInputType.size(); ++i) {
    names.push_back(leftInputType.nameOf(i));
    types.push_back(leftInputType.childAt(i));
  }
  for (auto i = 0; i < rightInputType.size(); ++i) {
    names.push_back(rightInputType.nameOf(i));
    types.push_back(rightInputType.childAt(i));
  }

  return std::make_shared<NestedLoopJoinNode>(
      queryContext.nextNodeId(),
      std::move(sources[0]),
      std::move(sources[1]),
      ROW(std::move(names), std::move(types)));
}

namespace {
std::vector<idx_t> columnIndices(std::vector<idx_t> map, int32_t size) {
  if (size > 0 && map.empty()) {
    std::vector<idx_t> result(size);
    std::iota(result.begin(), result.end(), 0);
    return result;
  }
  return map;
}
} // namespace

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalComparisonJoin& join,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(2, sources.size());

  auto& leftInputType = sources[0]->outputType();
  auto& rightInputType = sources[1]->outputType();

  VeloxColumnProjections leftProjection(queryContext);
  VeloxColumnProjections rightProjection(queryContext);
  JoinType joinType = JoinType::kInner;
  switch (join.join_type) {
    case ::duckdb::JoinType::INNER:
    case ::duckdb::JoinType::SINGLE:
      joinType = JoinType::kInner;
      break;
    case ::duckdb::JoinType::LEFT:
      joinType = JoinType::kLeft;
      break;
    case ::duckdb::JoinType::RIGHT:
      joinType = JoinType::kRight;
      break;
    case ::duckdb::JoinType::OUTER:
      joinType = JoinType::kFull;
      break;
    case ::duckdb::JoinType::SEMI:
      joinType = JoinType::kLeftSemiFilter;
      break;
    case ::duckdb::JoinType::ANTI:
      joinType = JoinType::kAnti;
      break;
    case ::duckdb::JoinType::MARK:
      joinType = JoinType::kLeftSemiProject;
      break;
    default:
      VELOX_NYI("Bad Duck join type {}", static_cast<int32_t>(join.join_type));
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i :
       columnIndices(join.left_projection_map, leftInputType->size())) {
    auto source = std::make_shared<core::FieldAccessTypedExpr>(
        leftInputType->childAt(i), leftInputType->nameOf(i));
    leftProjection.addColumn(source);
    names.push_back(source->name());
    types.push_back(source->type());
  }
  auto joinNodeId = queryContext.nextNodeId();
  switch (joinType) {
    case JoinType::kLeftSemiFilter:
    case JoinType::kAnti:
      break;
    case JoinType::kLeftSemiProject:
      names.push_back(fmt::format("exists{}", joinNodeId));
      types.push_back(BOOLEAN());
      break;
    default:
      for (auto i :
           columnIndices(join.right_projection_map, rightInputType->size())) {
        auto source = std::make_shared<core::FieldAccessTypedExpr>(
            rightInputType->childAt(i), rightInputType->nameOf(i));
        rightProjection.addColumn(source);
        names.push_back(source->name());
        types.push_back(source->type());
      }
  }
  auto outputType = ROW(std::move(names), std::move(types));
  TypedExprPtr filter;
  if (!join.expressions.empty()) {
    VELOX_CHECK_EQ(join.expressions.size(), 1);
    filter = toVeloxExpression(*join.expressions.front(), outputType);
  }

  std::vector<FieldAccessTypedExprPtr> leftKeys;
  std::vector<FieldAccessTypedExprPtr> rightKeys;
  for (auto& condition : join.conditions) {
    VELOX_CHECK(
        condition.comparison == ::duckdb::ExpressionType::COMPARE_EQUAL ||
        condition.comparison ==
            ::duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM);
    leftKeys.push_back(
        leftProjection.toFieldAccess(*condition.left, leftInputType));
    rightKeys.push_back(
        rightProjection.toFieldAccess(*condition.right, rightInputType));
  }

  return std::make_shared<HashJoinNode>(
      joinNodeId,
      joinType,
      false,
      std::move(leftKeys),
      std::move(rightKeys),
      filter,
      leftProjection.source(sources[0]),
      rightProjection.source(sources[1]),
      outputType);
}

int32_t getDelimCrossRightSide(
    ::duckdb::Expression& filter,
    int32_t numFromDelimGet) {
  if (filter.type != ::duckdb::ExpressionType::COMPARE_EQUAL) {
    return -1;
  }
  auto& comparison =
      reinterpret_cast<const ::duckdb::BoundComparisonExpression&>(filter);
  auto left =
      dynamic_cast<::duckdb::BoundReferenceExpression*>(comparison.left.get());
  auto right =
      dynamic_cast<::duckdb::BoundReferenceExpression*>(comparison.right.get());
  if (left && right) {
    if (left->index < numFromDelimGet && right->index >= numFromDelimGet) {
      return right->index - numFromDelimGet;
    }
    if (left->index >= numFromDelimGet && right->index < numFromDelimGet) {
      return left->index - numFromDelimGet;
    }
  }
  return -1;
}

// Processesa filter over cross join of delim get and the table in
// the correlated subq. Some of the filters are joining the delim
// get and the table in the subquery. The delim get side of the
// output must be aliases to the corresponding columns on the
// right. This comes from some of the filters. There can be other
// filters that pertain to the right side that will be left in
// place.
PlanNodePtr processDelimGetJoin(
    const ::duckdb::LogicalFilter& filter,
    memory::MemoryPool* pool,
    QueryContext& queryContext);

PlanNodePtr processDelimGetJoin(
    const ::duckdb::LogicalComparisonJoin& join,
    memory::MemoryPool* pool,
    QueryContext& queryContext);

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalOperator& plan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<PlanNodePtr> sources;

  ScopedVarSetter isDelim(
      &queryContext.isInDelimJoin, queryContext.isInDelimJoin);
  if (plan.type == ::duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    queryContext.isInDelimJoin = true;
  }
  if (queryContext.isInDelimJoin &&
      plan.type == ::duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    auto& filter = dynamic_cast<::duckdb::LogicalFilter&>(plan);
    if (filter.children[0]->type ==
            ::duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
        filter.children[0]->children[1]->type ==
            ::duckdb::LogicalOperatorType::LOGICAL_DELIM_GET) {
      return processDelimGetJoin(filter, pool, queryContext);
    }
  }
  if (plan.type == ::duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
      queryContext.isInDelimJoin &&
      plan.children[1]->type ==
          ::duckdb::LogicalOperatorType::LOGICAL_DELIM_GET) {
    return processDelimGetJoin(
        dynamic_cast<::duckdb::LogicalComparisonJoin&>(plan),
        pool,
        queryContext);
  }
  for (auto& child : plan.children) {
    sources.push_back(toVeloxPlan(*child, pool, queryContext));
    if (sources.back() == nullptr) {
      VELOX_FAIL("null plan for: {}", child->ToString());
    }
  }

  switch (plan.type) {
    case ::duckdb::LogicalOperatorType::LOGICAL_DUMMY_SCAN:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalDummyScan&>(plan), pool, queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_GET:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalGet&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_FILTER:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalFilter&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalProjection&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalAggregate&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalCrossProduct&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalOrder&>(plan),
          pool,
          std::move(sources),
          queryContext);
    }
    case ::duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
      auto& limit = dynamic_cast<const ::duckdb::LogicalLimit&>(plan);
      return std::make_shared<core::LimitNode>(
          queryContext.nextNodeId(),
          limit.offset_val,
          limit.limit_val,
          false,
          sources[0]);
    }
    case ::duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalComparisonJoin&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN: {
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalComparisonJoin&>(plan),
          pool,
          std::move(sources),
          queryContext);
    }
    case ::duckdb::LogicalOperatorType::LOGICAL_DELIM_GET:
      return nullptr;
    default:
      VELOX_NYI(
          "Plan node is not supported yet: {}",
          ::duckdb::LogicalOperatorToString(plan.type));
  }
}

PlanNodePtr processDelimGetJoin(
    const ::duckdb::LogicalFilter& filter,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  auto& cross =
      dynamic_cast<::duckdb::LogicalCrossProduct&>(*filter.children[0]);
  int32_t numFromDelimGet = cross.children[1]->types.size();
  // Column index on the left side for each delim get column.
  std::vector<int32_t> delimAlias;
  auto right =
      toVeloxPlan(*filter.children[0]->children[0], pool, queryContext);
  auto rightType = right->outputType();
  std::vector<::duckdb::Expression*> remaining;
  for (auto& conjunct : filter.expressions) {
    auto rightSide = getDelimCrossRightSide(*conjunct, numFromDelimGet);
    if (rightSide >= 0) {
      delimAlias.push_back(rightSide);
    } else {
      remaining.push_back(conjunct.get());
    }
  }
  std::vector<std::string> names;
  std::vector<TypedExprPtr> exprs;
  for (auto i = 0; i < numFromDelimGet; ++i) {
    names.push_back(queryContext.nextColumnName("_delim"));
    auto rightIndex = delimAlias[i];
    auto type = rightType->childAt(rightIndex);
    exprs.push_back(std::make_shared<FieldAccessTypedExpr>(
        type, rightType->nameOf(rightIndex)));
  }
  for (auto i = 0; i < rightType->size(); ++i) {
    names.push_back(rightType->nameOf(i));
    auto type = rightType->childAt(i);
    exprs.push_back(std::make_shared<FieldAccessTypedExpr>(type, names.back()));
  }
  auto project = std::make_shared<ProjectNode>(
      queryContext.nextNodeId(), std::move(names), std::move(exprs), right);
  if (remaining.empty()) {
    return project;
  }
  TypedExprPtr veloxFilter;
  for (auto& expr : remaining) {
    auto conjunct = toVeloxExpression(*expr, project->outputType());
    if (!veloxFilter) {
      veloxFilter = conjunct;
    } else {
      veloxFilter = std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{veloxFilter, conjunct}, "and");
    }
  }
  return std::make_shared<FilterNode>(
      queryContext.nextNodeId(), veloxFilter, project);
}

PlanNodePtr processDelimGetJoin(
    const ::duckdb::LogicalComparisonJoin& join,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  int32_t numFromDelimGet = join.children[1]->types.size();
  // Column index on the right side for each delim get column.
  std::vector<int32_t> delimAlias(numFromDelimGet);
  auto right = toVeloxPlan(*join.children[0], pool, queryContext);
  auto rightType = right->outputType();
  for (auto& condition : join.conditions) {
    auto left =
        dynamic_cast<::duckdb::BoundReferenceExpression*>(condition.left.get());
    auto right = dynamic_cast<::duckdb::BoundReferenceExpression*>(
        condition.right.get());
    VELOX_CHECK(left && right);
    VELOX_CHECK_LT(left->index, numFromDelimGet);
    delimAlias[left->index] = right->index;
  }
  std::vector<std::string> names;
  std::vector<TypedExprPtr> exprs;
  for (auto i = 0; i < numFromDelimGet; ++i) {
    names.push_back(queryContext.nextColumnName("_delim"));
    auto rightIndex = delimAlias[i];
    auto type = rightType->childAt(rightIndex);
    exprs.push_back(std::make_shared<FieldAccessTypedExpr>(
        type, rightType->nameOf(rightIndex)));
  }
  for (auto i = 0; i < rightType->size(); ++i) {
    names.push_back(rightType->nameOf(i));
    auto type = rightType->childAt(i);
    exprs.push_back(std::make_shared<FieldAccessTypedExpr>(type, names.back()));
  }
  return std::make_shared<ProjectNode>(
      queryContext.nextNodeId(), std::move(names), std::move(exprs), right);
}

static void customScalarFunction(
    ::duckdb::DataChunk& args,
    ::duckdb::ExpressionState& state,
    ::duckdb::Vector& result) {
  VELOX_UNREACHABLE();
}

static ::duckdb::idx_t customAggregateState() {
  VELOX_UNREACHABLE();
}

static void customAggregateInitialize(::duckdb::data_ptr_t) {
  VELOX_UNREACHABLE();
}

static void customAggregateUpdate(
    ::duckdb::Vector inputs[],
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::idx_t input_count,
    ::duckdb::Vector& state,
    ::duckdb::idx_t count) {
  VELOX_UNREACHABLE();
}

static void customAggregateCombine(
    ::duckdb::Vector& state,
    ::duckdb::Vector& combined,
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::idx_t count) {
  VELOX_UNREACHABLE();
}

static void customAggregateFinalize(
    ::duckdb::Vector& state,
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::Vector& result,
    ::duckdb::idx_t count,
    ::duckdb::idx_t offset) {
  VELOX_UNREACHABLE();
}

} // namespace

PlanNodePtr parseQuery(
    const std::string& sql,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
        inMemoryTables) {
  DuckDbQueryPlanner planner(pool);

  for (auto& [name, data] : inMemoryTables) {
    planner.registerTable(name, data);
  }

  return planner.plan(sql);
}

void DuckDbQueryPlanner::registerTable(
    const std::string& name,
    const std::vector<RowVectorPtr>& data) {
  VELOX_CHECK_EQ(
      tables_.count(name), 0, "Table is already registered: {}", name);

  auto createTableSql =
      duckdb::makeCreateTableSql(name, *asRowType(data[0]->type()));
  auto res = conn_.Query(createTableSql);
  VELOX_CHECK(
      !res->HasError(), "Failed to create DuckDB table: {}", res->GetError());

  tables_.insert({name, data});
}

void DuckDbQueryPlanner::registerTable(
    const std::string& name,
    const RowTypePtr& type) {
  VELOX_CHECK_EQ(
      tables_.count(name), 0, "Table is already registered: {}", name);

  auto createTableSql = duckdb::makeCreateTableSql(name, *type);
  auto res = conn_.Query(createTableSql);
}

void DuckDbQueryPlanner::registerScalarFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  ::duckdb::vector<::duckdb::LogicalType> argDuckTypes;
  for (auto& type : argTypes) {
    argDuckTypes.push_back(duckdb::fromVeloxType(type));
  }

  conn_.CreateVectorizedFunction(
      name,
      argDuckTypes,
      duckdb::fromVeloxType(returnType),
      customScalarFunction);
}

void DuckDbQueryPlanner::registerAggregateFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  ::duckdb::vector<::duckdb::LogicalType> argDuckTypes;
  for (auto& type : argTypes) {
    argDuckTypes.push_back(duckdb::fromVeloxType(type));
  }

  conn_.CreateAggregateFunction(
      name,
      argDuckTypes,
      duckdb::fromVeloxType(returnType),
      customAggregateState,
      customAggregateInitialize,
      customAggregateUpdate,
      customAggregateCombine,
      customAggregateFinalize);
}

PlanNodePtr DuckDbQueryPlanner::plan(const std::string& sql) {
  // Disable the optimizer. Otherwise, the filter over table scan gets pushdown
  // as a callback that is impossible to recover.
  conn_.Query("PRAGMA disable_optimizer");

  auto plan = conn_.ExtractPlan(sql);

  QueryContext queryContext{tables_};
  queryContext.makeTableScan = makeTableScan_;
  return toVeloxPlan(*plan, pool_, queryContext);
}

} // namespace facebook::velox::core
