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

#include "velox/functions/sparksql/fuzzer/SparkQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"

namespace facebook::velox::functions::sparksql::fuzzer {

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::AggregationNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(node.step() == core::AggregationNode::Step::kSingle);

  exec::test::PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);

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
      exec::test::appendComma(i, sql);
      const auto& aggregate = aggregates[i];
      VELOX_CHECK(
          aggregate.sortingKeys.empty(),
          "Sort key is not supported in Spark's aggregation. You may need to disable 'enable_sorted_aggregations' when running the fuzzer test.");
      sql << exec::test::toAggregateCallSql(
          aggregate.call, {}, {}, aggregate.distinct);

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

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ProjectNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  exec::test::PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);

  const auto sourceSql = toSql(node.sources()[0]);
  if (!sourceSql.has_value()) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT ";

  for (auto i = 0; i < node.names().size(); ++i) {
    exec::test::appendComma(i, sql);
    auto projection = node.projections()[i];
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                projection)) {
      sql << field->name();
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(projection)) {
      sql << exec::test::toCallSql(call);
    } else {
      VELOX_NYI(
          "Unsupported projection {} in project node: {}.",
          projection->toString(),
          node.toString());
    }

    sql << " as " << node.names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  visitorContext.sql = sql.str();
}

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ValuesNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  exec::test::PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);
  visitorContext.sql = exec::test::ReferenceQueryRunner::getTableName(node);
}

} // namespace facebook::velox::functions::sparksql::fuzzer
