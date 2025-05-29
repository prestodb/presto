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

#include "velox/exec/fuzzer/PrestoQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {
namespace {

std::string toWindowCallSql(
    const core::CallTypedExprPtr& call,
    bool ignoreNulls = false) {
  std::stringstream sql;
  sql << call->name() << "(";
  toCallInputsSql(call->inputs(), sql);
  sql << ")";
  if (ignoreNulls) {
    sql << " IGNORE NULLS";
  }
  return sql.str();
}

} // namespace

void PrestoQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::AggregationNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(node.step() == core::AggregationNode::Step::kSingle);

  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!PrestoQueryRunner::isSupportedDwrfType(
          node.sources()[0]->outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::vector<std::string> groupingKeys;
  for (const auto& key : node.groupingKeys()) {
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

  sql << " FROM (" << *source << ")";

  if (!groupingKeys.empty()) {
    sql << " GROUP BY " << folly::join(", ", groupingKeys);
  }

  visitorContext.sql = sql.str();
}

void PrestoQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ProjectNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

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
      if (field->isInputColumn()) {
        sql << field->name();
      } else {
        toCallInputsSql(field->inputs(), sql);
        sql << fmt::format(".{}", field->name());
      }
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(projection)) {
      sql << toCallSql(call);
    } else if (
        auto cast =
            std::dynamic_pointer_cast<const core::CastTypedExpr>(projection)) {
      sql << toCastSql(*cast);
    } else if (
        auto concat = std::dynamic_pointer_cast<const core::ConcatTypedExpr>(
            projection)) {
      sql << toConcatSql(*concat);
    } else if (
        auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                projection)) {
      sql << toConstantSql(*constant);
    } else {
      VELOX_NYI();
    }

    sql << " as " << node.names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  visitorContext.sql = sql.str();
}

void PrestoQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::RowNumberNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!PrestoQueryRunner::isSupportedDwrfType(
          node.sources()[0]->outputType())) {
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

void PrestoQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::TableWriteNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  auto insertTableHandle =
      std::dynamic_pointer_cast<connector::hive::HiveInsertTableHandle>(
          node.insertTableHandle()->connectorInsertTableHandle());

  // Returns a CTAS sql with specified table properties from TableWriteNode,
  // example sql:
  // CREATE TABLE tmp_write WITH (
  // PARTITIONED_BY = ARRAY['p0'],
  // BUCKETED_COUNT = 2, BUCKETED_BY = ARRAY['b0', 'b1'],
  // SORTED_BY = ARRAY['s0 ASC', 's1 DESC'],
  // FORMAT = 'ORC'
  // )
  // AS SELECT * FROM t_<id>
  std::stringstream sql;
  sql << "CREATE TABLE tmp_write";
  std::vector<std::string> partitionKeys;
  for (auto i = 0; i < node.columnNames().size(); ++i) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      partitionKeys.push_back(insertTableHandle->inputColumns()[i]->name());
    }
  }
  sql << " WITH (";

  if (insertTableHandle->isPartitioned()) {
    sql << " PARTITIONED_BY = ARRAY[";
    for (int i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << "'" << partitionKeys[i] << "'";
    }
    sql << "], ";

    if (insertTableHandle->bucketProperty() != nullptr) {
      const auto bucketCount =
          insertTableHandle->bucketProperty()->bucketCount();
      const auto& bucketColumns =
          insertTableHandle->bucketProperty()->bucketedBy();
      sql << " BUCKET_COUNT = " << bucketCount << ", BUCKETED_BY = ARRAY[";
      for (int i = 0; i < bucketColumns.size(); ++i) {
        appendComma(i, sql);
        sql << "'" << bucketColumns[i] << "'";
      }
      sql << "], ";

      const auto& sortColumns = insertTableHandle->bucketProperty()->sortedBy();
      if (!sortColumns.empty()) {
        sql << " SORTED_BY = ARRAY[";
        for (int i = 0; i < sortColumns.size(); ++i) {
          appendComma(i, sql);
          sql << "'" << sortColumns[i]->sortColumn() << " "
              << (sortColumns[i]->sortOrder().isAscending() ? "ASC" : "DESC")
              << "'";
        }
        sql << "], ";
      }
    }
  }

  // TableWriteNode should have a single source.
  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  sql << "FORMAT = 'ORC')  AS SELECT * FROM " << *source;

  visitorContext.sql = sql.str();
}

void PrestoQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::TopNRowNumberNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!PrestoQueryRunner::isSupportedDwrfType(
          node.sources()[0]->outputType())) {
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

void PrestoQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::WindowNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!PrestoQueryRunner::isSupportedDwrfType(
          node.sources()[0]->outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = node.inputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", ";

  const auto& functions = node.windowFunctions();
  for (auto i = 0; i < functions.size(); ++i) {
    appendComma(i, sql);
    sql << toWindowCallSql(functions[i].functionCall, functions[i].ignoreNulls);

    sql << " OVER (";

    const auto& partitionKeys = node.partitionKeys();
    if (!partitionKeys.empty()) {
      sql << "PARTITION BY ";
      for (auto j = 0; j < partitionKeys.size(); ++j) {
        appendComma(j, sql);
        sql << partitionKeys[j]->name();
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

    sql << " " << queryRunnerContext_->windowFrames_.at(node.id()).at(i);
    sql << ") as " << node.windowColumnNames()[i];
  }

  // WindowNode should have a single source.
  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }

  sql << " FROM (" << *source << ")";

  visitorContext.sql = sql.str();
}

} // namespace facebook::velox::exec::test
