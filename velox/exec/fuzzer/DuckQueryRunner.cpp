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
#include "velox/exec/fuzzer/DuckQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {

namespace {
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
  PrestoSqlPlanNodeVisitorContext context;
  DuckQueryRunnerToSqlPlanNodeVisitor visitor(aggregateFunctionNames_);
  plan->accept(visitor, context);

  return context.sql;
}

} // namespace facebook::velox::exec::test
