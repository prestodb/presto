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

#include <optional>
#include <set>
#include <unordered_map>

#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/exec/fuzzer/ToSQLUtil.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {

namespace {

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

std::unordered_set<std::string> getAggregateFunctions() {
  std::string sql =
      "SELECT distinct on(function_name) function_name "
      "FROM duckdb_functions() "
      "WHERE function_type = 'aggregate'";

  DuckDbQueryRunner queryRunner;
  auto result = queryRunner.executeOrdered(sql, ROW({VARCHAR()}));

  std::unordered_set<std::string> names;
  for (const auto& row : result) {
    names.insert(row[0].value<std::string>());
  }

  return names;
}
} // namespace

DuckQueryRunner::DuckQueryRunner(memory::MemoryPool* aggregatePool)
    : ReferenceQueryRunner(aggregatePool),
      aggregateFunctionNames_{getAggregateFunctions()} {}

void DuckQueryRunner::disableAggregateFunctions(
    const std::vector<std::string>& names) {
  for (const auto& name : names) {
    aggregateFunctionNames_.erase(name);
  }
}

const std::vector<TypePtr>& DuckQueryRunner::supportedScalarTypes() const {
  static const std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      DATE(),
  };
  return kScalarTypes;
}

const std::unordered_map<std::string, DataSpec>&
DuckQueryRunner::aggregationFunctionDataSpecs() const {
  // There are some functions for which DuckDB and Velox have inconsistent
  // behavior with Nan and Infinity, so we exclude those.
  static const std::unordered_map<std::string, DataSpec>
      kAggregationFunctionDataSpecs{
          {"covar_pop", DataSpec{true, false}},
          {"covar_samp", DataSpec{true, false}},
          {"histogram", DataSpec{false, false}},
          {"regr_avgx", DataSpec{true, false}},
          {"regr_avgy", DataSpec{true, false}},
          {"regr_intercept", DataSpec{false, false}},
          {"regr_r2", DataSpec{false, false}},
          {"regr_replacement", DataSpec{false, false}},
          {"regr_slope", DataSpec{false, false}},
          {"regr_sxx", DataSpec{false, false}},
          {"regr_sxy", DataSpec{false, false}},
          {"regr_syy", DataSpec{false, false}},
          {"var_pop", DataSpec{false, false}}};

  return kAggregationFunctionDataSpecs;
}

std::pair<
    std::optional<std::multiset<std::vector<velox::variant>>>,
    ReferenceQueryErrorCode>
DuckQueryRunner::execute(const core::PlanNodePtr& plan) {
  if (std::optional<std::string> sql = toSql(plan)) {
    try {
      DuckDbQueryRunner queryRunner;
      std::unordered_map<std::string, std::vector<RowVectorPtr>> inputMap =
          getAllTables(plan);
      for (const auto& [tableName, input] : inputMap) {
        queryRunner.createTable(tableName, input);
      }
      return std::make_pair(
          queryRunner.execute(*sql, plan->outputType()),
          ReferenceQueryErrorCode::kSuccess);
    } catch (...) {
      LOG(WARNING) << "Query failed in DuckDB";
      return std::make_pair(
          std::nullopt, ReferenceQueryErrorCode::kReferenceQueryFail);
    }
  }

  LOG(INFO) << "Query not supported in DuckDB";
  return std::make_pair(
      std::nullopt, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
}

std::optional<std::string> DuckQueryRunner::toSql(
    const core::PlanNodePtr& plan) {
  if (!isSupportedType(plan->outputType())) {
    return std::nullopt;
  }

  for (const auto& source : plan->sources()) {
    if (!isSupportedType(source->outputType())) {
      return std::nullopt;
    }
  }

  if (const auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(plan)) {
    return toSql(projectNode);
  }

  if (const auto windowNode =
          std::dynamic_pointer_cast<const core::WindowNode>(plan)) {
    return toSql(windowNode);
  }

  if (const auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(plan)) {
    return toSql(aggregationNode);
  }

  if (const auto rowNumberNode =
          std::dynamic_pointer_cast<const core::RowNumberNode>(plan)) {
    return toSql(rowNumberNode);
  }

  if (const auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(plan)) {
    return toSql(joinNode);
  }

  if (const auto joinNode =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(plan)) {
    return toSql(joinNode);
  }

  if (const auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(plan)) {
    return toSql(valuesNode);
  }

  if (const auto tableScanNode =
          std::dynamic_pointer_cast<const core::TableScanNode>(plan)) {
    return toSql(tableScanNode);
  }

  VELOX_NYI();
}

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
} // namespace

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::AggregationNode>& aggregationNode) {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(aggregationNode->step() == core::AggregationNode::Step::kSingle);

  for (const auto& agg : aggregationNode->aggregates()) {
    if (aggregateFunctionNames_.count(agg.call->name()) == 0) {
      return std::nullopt;
    }
  }

  std::vector<std::string> groupingKeys;
  for (const auto& key : aggregationNode->groupingKeys()) {
    // Aggregations with group by keys that contain maps are buggy.
    if (containsMap(key->type())) {
      return std::nullopt;
    }
    groupingKeys.push_back(key->name());
  }

  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", groupingKeys);

  const auto& aggregates = aggregationNode->aggregates();
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
      sql << " as " << aggregationNode->aggregateNames()[i];
    }
  }

  // AggregationNode should have a single source.
  std::optional<std::string> source = toSql(aggregationNode->sources()[0]);
  if (!source) {
    return std::nullopt;
  }
  sql << " FROM " << *source;

  if (!groupingKeys.empty()) {
    sql << " GROUP BY " << folly::join(", ", groupingKeys);
  }

  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::ProjectNode>& projectNode) {
  auto sourceSql = toSql(projectNode->sources()[0]);
  if (!sourceSql.has_value()) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT ";

  for (auto i = 0; i < projectNode->names().size(); ++i) {
    appendComma(i, sql);
    auto projection = projectNode->projections()[i];
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

    sql << " as " << projectNode->names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::WindowNode>& windowNode) {
  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = windowNode->sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", ";

  const auto& functions = windowNode->windowFunctions();
  for (auto i = 0; i < functions.size(); ++i) {
    appendComma(i, sql);
    sql << toCallSql(functions[i].functionCall);
  }
  sql << " OVER (";

  const auto& partitionKeys = windowNode->partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  const auto& sortingKeys = windowNode->sortingKeys();
  const auto& sortingOrders = windowNode->sortingOrders();

  if (!sortingKeys.empty()) {
    sql << " order by ";
    for (auto i = 0; i < sortingKeys.size(); ++i) {
      appendComma(i, sql);
      sql << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
    }
  }

  // WindowNode should have a single source.
  std::optional<std::string> source = toSql(windowNode->sources()[0]);
  if (!source) {
    return std::nullopt;
  }
  sql << ") FROM " << *source;

  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::RowNumberNode>& rowNumberNode) {
  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = rowNumberNode->sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", row_number() OVER (";

  const auto& partitionKeys = rowNumberNode->partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  // RowNumberNode should have a single source.
  std::optional<std::string> source = toSql(rowNumberNode->sources()[0]);
  if (!source) {
    return std::nullopt;
  }
  sql << ") as row_number FROM " << *source;

  return sql.str();
}
} // namespace facebook::velox::exec::test
