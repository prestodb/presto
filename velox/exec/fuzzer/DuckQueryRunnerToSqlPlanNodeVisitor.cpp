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

#include "velox/exec/fuzzer/DuckQueryRunnerToSqlPlanNodeVisitor.h"

namespace facebook::velox::exec::test {
namespace {

bool containsMap(const TypePtr& type) {
  if (type->isMap()) {
    return true;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (containsMap(type->childAt(i))) {
      return true;
    }
  }

  return false;
}

bool isSupportedType(const TypePtr& type) {
  // DuckDB doesn't support nanosecond precision for timestamps.
  if (type->kind() == TypeKind::TIMESTAMP) {
    return false;
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (!isSupportedType(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

} // namespace

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::AggregationNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(node.step() == core::AggregationNode::Step::kSingle);

  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  for (const auto& agg : node.aggregates()) {
    if (aggregateFunctionNames_.count(agg.call->name()) == 0) {
      visitorContext.sql = std::nullopt;
      return;
    }
  }

  std::vector<std::string> groupingKeys;
  for (const auto& key : node.groupingKeys()) {
    // Aggregations with group by keys that contain maps are buggy.
    if (containsMap(key->type())) {
      visitorContext.sql = std::nullopt;
      return;
    }
    groupingKeys.push_back(key->name());
  }

  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", groupingKeys);

  const auto& aggregates = node.aggregates();
  if (!aggregates.empty()) {
    if (!groupingKeys.empty()) {
      sql << ", ";
    }

    for (auto i = 0; i < aggregates.size(); ++i) {
      appendComma(i, sql);
      const auto& aggregate = aggregates[i];
      sql << toAggregateCallSql(
          aggregate.call,
          aggregate.sortingKeys,
          aggregate.sortingOrders,
          aggregate.distinct);

      if (aggregate.mask != nullptr) {
        sql << " filter (where " << aggregate.mask->name() << ")";
      }
      sql << " as " << node.aggregateNames()[i];
    }
  }

  // AggregationNode should have a single source.
  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  sql << " FROM " << *source;

  if (!groupingKeys.empty()) {
    sql << " GROUP BY " << folly::join(", ", groupingKeys);
  }

  visitorContext.sql = sql.str();
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::HashJoinNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  PrestoSqlPlanNodeVisitor::visit(node, ctx);
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::NestedLoopJoinNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  PrestoSqlPlanNodeVisitor::visit(node, ctx);
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ProjectNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  const auto sourceSql = toSql(node.sources()[0]);
  if (!sourceSql.has_value()) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT ";

  for (auto i = 0; i < node.names().size(); ++i) {
    appendComma(i, sql);
    auto projection = node.projections()[i];
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                projection)) {
      sql << field->name();
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(projection)) {
      sql << toCallSql(call);
    } else {
      VELOX_NYI();
    }

    sql << " as " << node.names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  visitorContext.sql = sql.str();
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::RowNumberNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = node.sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", row_number() OVER (";

  const auto& partitionKeys = node.partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  // RowNumberNode should have a single source.
  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  sql << ") as row_number FROM " << *source;

  visitorContext.sql = sql.str();
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::TableScanNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  PrestoSqlPlanNodeVisitor::visit(node, ctx);
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::TopNRowNumberNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT * FROM (SELECT ";

  const auto& inputType = node.sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", row_number() OVER (";

  const auto& partitionKeys = node.partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  const auto& sortingKeys = node.sortingKeys();
  const auto& sortingOrders = node.sortingOrders();

  if (!sortingKeys.empty()) {
    sql << " ORDER BY ";
    for (auto j = 0; j < sortingKeys.size(); ++j) {
      appendComma(j, sql);
      sql << sortingKeys[j]->name() << " " << sortingOrders[j].toString();
    }
  }

  std::string rowNumberColumnName = node.generateRowNumber()
      ? node.outputType()->nameOf(node.outputType()->children().size() - 1)
      : "row_number";

  // TopNRowNumberNode should have a single source.
  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  sql << ") as " << rowNumberColumnName << " FROM " << *source << ") ";
  sql << " where " << rowNumberColumnName << " <= " << node.limit();

  visitorContext.sql = sql.str();
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ValuesNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  PrestoSqlPlanNodeVisitor::visit(node, ctx);
}

void DuckQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::WindowNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!isSupportedType(node.outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = node.sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", ";

  const auto& functions = node.windowFunctions();
  for (auto i = 0; i < functions.size(); ++i) {
    appendComma(i, sql);
    sql << toCallSql(functions[i].functionCall);
  }
  sql << " OVER (";

  const auto& partitionKeys = node.partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  const auto& sortingKeys = node.sortingKeys();
  const auto& sortingOrders = node.sortingOrders();

  if (!sortingKeys.empty()) {
    sql << " order by ";
    for (auto i = 0; i < sortingKeys.size(); ++i) {
      appendComma(i, sql);
      sql << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
    }
  }

  // WindowNode should have a single source.
  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  sql << ") FROM " << *source;

  visitorContext.sql = sql.str();
}

} // namespace facebook::velox::exec::test
