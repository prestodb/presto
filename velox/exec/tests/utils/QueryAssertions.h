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
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/external/duckdb/tpch/include/tpch-extension.hpp"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec::test {

using MaterializedRow = std::vector<velox::variant>;
using DuckDBQueryResult = std::unique_ptr<::duckdb::MaterializedQueryResult>;

// Comparison function used in tests which compares floating point variants
// using 'epsilon' parameter to treat close enough floating point values as
// equal.
struct MaterializedRowComparator {
  bool operator()(const MaterializedRow& lhs, const MaterializedRow& rhs)
      const {
    const auto minSize = std::min(lhs.size(), rhs.size());
    for (size_t i = 0; i < minSize; ++i) {
      if (lhs[i].equalsWithEpsilon(rhs[i])) {
        continue;
      }
      // The 1st non-equal element determines if 'left' is smaller or not.
      return lhs[i].lessThanWithEpsilon(rhs[i]);
    }
    // The shorter vector is smaller.
    return lhs.size() < rhs.size();
  }
};
// Multiset that uses 'epsilon' to compare doubles.
using MaterializedRowMultiset =
    std::multiset<MaterializedRow, MaterializedRowComparator>;

class DuckDbQueryRunner {
 public:
  DuckDbQueryRunner() : db_(nullptr) {}

  void createTable(
      const std::string& name,
      const std::vector<RowVectorPtr>& data);

  DuckDBQueryResult execute(const std::string& sql);

  MaterializedRowMultiset execute(
      const std::string& sql,
      const std::shared_ptr<const RowType>& resultRowType) {
    MaterializedRowMultiset allRows;
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

  // Returns the DuckDB TPC-H Extension Query as string for a given 'queryNo'
  // Example: queryNo = 1 returns the TPC-H Query1 in the TPC-H Extension
  std::string getTpchQuery(int queryNo) {
    auto queryString = ::duckdb::TPCHExtension::GetQuery(queryNo);
    // Output of GetQuery() has a new line and a semi-colon. These need to be
    // removed in order to use the query string in a subquery
    queryString.pop_back(); // remove new line
    queryString.pop_back(); // remove semi-colon
    return queryString;
  }

  void initializeTpch(double scaleFactor);

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

/// The Task can return results before the Driver is finished executing.
/// Wait upto maxWaitMicros for the Task to finish before returning to ensure
/// it's stable e.g. the Driver isn't updating it anymore.
/// Returns true if the task is completed before maxWaitMicros expires.
bool waitForTaskCompletion(
    exec::Task* task,
    uint64_t maxWaitMicros = 1'000'000);

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

std::shared_ptr<Task> assertQuery(
    const CursorParameters& params,
    const std::vector<RowVectorPtr>& expectedResults);

velox::variant readSingleValue(
    const std::shared_ptr<const core::PlanNode>& plan,
    int32_t maxDrivers = 1);

void assertEqualResults(
    const std::vector<RowVectorPtr>& expected,
    const std::vector<RowVectorPtr>& actual);

} // namespace facebook::velox::exec::test
