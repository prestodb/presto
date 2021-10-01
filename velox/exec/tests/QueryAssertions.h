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
#pragma once
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/Cursor.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec::test {

using MaterializedRow = std::vector<velox::variant>;

class DuckDbQueryRunner {
 public:
  DuckDbQueryRunner() : db_(nullptr) {}

  void createTable(
      const std::string& name,
      const std::vector<RowVectorPtr>& data);

  std::multiset<MaterializedRow> execute(
      const std::string& sql,
      const std::shared_ptr<const RowType>& resultRowType) {
    std::multiset<MaterializedRow> allRows;
    execute(
        sql,
        resultRowType,
        [&allRows](std::vector<MaterializedRow>& rows) mutable {
          std::copy(
              rows.begin(), rows.end(), std::inserter(allRows, allRows.end()));
        });
    return allRows;
  }

  std::vector<MaterializedRow> executeOrdered(
      const std::string& sql,
      const std::shared_ptr<const RowType>& resultRowType) {
    std::vector<MaterializedRow> allRows;
    execute(
        sql,
        resultRowType,
        [&allRows](std::vector<MaterializedRow>& rows) mutable {
          std::copy(rows.begin(), rows.end(), std::back_inserter(allRows));
        });
    return allRows;
  }

 private:
  ::duckdb::DuckDB db_;

  void execute(
      const std::string& sql,
      const std::shared_ptr<const RowType>& resultRowType,
      std::function<void(std::vector<MaterializedRow>&)> resultCallback);
};

std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> readCursor(
    const CursorParameters& params,
    std::function<void(exec::Task*)> addSplits);

std::shared_ptr<Task> assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

std::shared_ptr<Task> assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    std::function<void(exec::Task*)> addSplits,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

std::shared_ptr<Task> assertQuery(
    const CursorParameters& params,
    std::function<void(exec::Task*)> addSplits,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

std::shared_ptr<Task> assertQueryReturnsEmptyResult(
    const std::shared_ptr<const core::PlanNode>& plan);

void assertResults(
    const std::vector<RowVectorPtr>& results,
    const std::shared_ptr<const RowType>& resultType,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner);

void assertResultsOrdered(
    const std::vector<RowVectorPtr>& results,
    const std::shared_ptr<const RowType>& resultType,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    const std::vector<uint32_t>& sortingKeys);

std::shared_ptr<Task> assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::vector<RowVectorPtr>& expectedResults);

velox::variant readSingleValue(
    const std::shared_ptr<const core::PlanNode>& plan);

void assertEqualResults(
    const std::vector<RowVectorPtr>& expected,
    const std::vector<RowVectorPtr>& actual);

} // namespace facebook::velox::exec::test
