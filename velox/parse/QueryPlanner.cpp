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
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/parse/DuckLogicalOperator.h"

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

  const auto& columnIds = logicalGet.column_ids;
  std::vector<std::string> names(columnIds.size());
  std::vector<TypePtr> types(columnIds.size());

  for (auto i = 0; i < columnIds.size(); ++i) {
    names[i] = queryContext.nextColumnName(logicalGet.names[columnIds[i]]);
    types[i] = duckdb::toVeloxType(logicalGet.returned_types[columnIds[i]]);
  }

  auto rowType = ROW(std::move(names), std::move(types));

  auto tableName = logicalGet.function.to_string(logicalGet.bind_data.get());
  auto it = queryContext.inMemoryTables.find(tableName);
  VELOX_CHECK(
      it != queryContext.inMemoryTables.end(),
      "Can't find in-memory table: {}",
      tableName);

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

TypedExprPtr toVeloxExpression(
    ::duckdb::Expression& expression,
    const TypePtr& inputType) {
  switch (expression.type) {
    case ::duckdb::ExpressionType::VALUE_CONSTANT: {
      auto* constant =
          dynamic_cast<::duckdb::BoundConstantExpression*>(&expression);
      return std::make_shared<ConstantTypedExpr>(
          duckdb::toVeloxType(constant->return_type),
          duckdb::duckValueToVariant(
              constant->value, false /*parseDecimalAsDouble*/));
    }
    case ::duckdb::ExpressionType::COMPARE_EQUAL:
      return toVeloxComparisonExpression("eq", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return toVeloxComparisonExpression("gt", expression, inputType);
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

      return std::make_shared<CallTypedExpr>(
          duckdb::toVeloxType(func->function.return_type),
          std::move(children),
          mapScalarFunctionName(func->function.name));
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
  VELOX_CHECK_EQ(1, logicalFilter.expressions.size())
  auto filter = toVeloxExpression(
      *logicalFilter.expressions.front(), sources[0]->outputType());
  return std::make_shared<FilterNode>(
      queryContext.nextNodeId(), std::move(filter), std::move(sources[0]));
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

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalAggregate& logicalAggregate,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<CallTypedExprPtr> aggregates;

  std::vector<std::string> projectNames;
  std::vector<TypedExprPtr> projections;

  bool identityProjection = true;
  for (auto& expression : logicalAggregate.expressions) {
    auto call = std::dynamic_pointer_cast<const CallTypedExpr>(
        toVeloxExpression(*expression, sources[0]->outputType()));
    std::vector<TypedExprPtr> fieldInputs;

    for (auto& input : call->inputs()) {
      projections.push_back(input);

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

    aggregates.push_back(std::make_shared<CallTypedExpr>(
        call->type(), fieldInputs, call->name()));
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
      aggregates,
      std::vector<FieldAccessTypedExprPtr>{}, // aggregateMasks
      false, // ignoreNullKeys
      source);
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

  return std::make_shared<CrossJoinNode>(
      queryContext.nextNodeId(),
      std::move(sources[0]),
      std::move(sources[1]),
      ROW(std::move(names), std::move(types)));
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalOperator& plan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<PlanNodePtr> sources;
  for (auto& child : plan.children) {
    sources.push_back(toVeloxPlan(*child, pool, queryContext));
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
    default:
      VELOX_NYI(
          "Plan node is not supported yet: {}",
          ::duckdb::LogicalOperatorToString(plan.type));
  }
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
      0, tables_.count(name), "Table is already registered: {}", name);

  auto createTableSql =
      duckdb::makeCreateTableSql(name, *asRowType(data[0]->type()));
  auto res = conn_.Query(createTableSql);
  VELOX_CHECK(res->success, "Failed to create DuckDB table: {}", res->error);

  tables_.insert({name, data});
}

void DuckDbQueryPlanner::registerScalarFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  std::vector<::duckdb::LogicalType> argDuckTypes;
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
  std::vector<::duckdb::LogicalType> argDuckTypes;
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
  return toVeloxPlan(*plan, pool_, queryContext);
}

} // namespace facebook::velox::core
