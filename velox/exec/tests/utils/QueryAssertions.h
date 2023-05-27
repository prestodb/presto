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

// Multiset that compares floating-point values directly.
using MaterializedRowMultiset = std::multiset<MaterializedRow>;

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
    std::function<void(exec::Task*)> addSplits,
    uint64_t maxWaitMicros = 1'000'000);

/// The Task can return results before the Driver is finished executing.
/// Wait upto maxWaitMicros for the Task to finish as 'expectedState' before
/// returning to ensure it's stable e.g. the Driver isn't updating it anymore.
/// Returns true if the task is completed before maxWaitMicros expires.
bool waitForTaskFinish(
    exec::Task* task,
    TaskState expectedState,
    uint64_t maxWaitMicros = 1'000'000);

/// Similar to waitForTaskFinish but wait for the task to succeed.
bool waitForTaskCompletion(
    exec::Task* task,
    uint64_t maxWaitMicros = 1'000'000);

/// Similar to waitForTaskFinish but wait for the task to fail.
bool waitForTaskFailure(exec::Task* task, uint64_t maxWaitMicros = 1'000'000);

/// Similar to waitForTaskFinish but wait for the task to abort.
bool waitForTaskAborted(exec::Task* task, uint64_t maxWaitMicros = 1'000'000);

/// Similar to waitForTaskFinish but wait for the task to cancel.
bool waitForTaskCancelled(exec::Task* task, uint64_t maxWaitMicros = 1'000'000);

/// Wait up to maxWaitMicros for 'task' state changes to 'state'. The function
/// returns true if 'task' has changed to the expected 'state', otherwise false.
bool waitForTaskStateChange(
    exec::Task* task,
    TaskState state,
    uint64_t maxWaitMicros = 1'000'000);

/// Wait up to maxWaitMicros for all the task drivers to finish. The function
/// returns true if all the drivers have finished, otherwise false.
///
/// NOTE: user must call this on a finished or failed task.
bool waitForTaskDriversToFinish(
    exec::Task* task,
    uint64_t maxWaitMicros = 1'000'000);

std::shared_ptr<Task> assertQuery(
    const core::PlanNodePtr& plan,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

std::shared_ptr<Task> assertQuery(
    const core::PlanNodePtr& plan,
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
    const core::PlanNodePtr& plan);

void assertEmptyResults(const std::vector<RowVectorPtr>& results);

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
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& expectedResults);

std::shared_ptr<Task> assertQuery(
    const CursorParameters& params,
    const std::vector<RowVectorPtr>& expectedResults);

velox::variant readSingleValue(
    const core::PlanNodePtr& plan,
    int32_t maxDrivers = 1);

/// assertEqualResults() has limited support for results with floating-point
/// columns.
///   1. When there is one or more floating-point columns, we try to group the
///   rows in each set by the non-floating-point columns. If every group
///   contains only one row in both sets, we compare their rows
///   via the sort-merge algorithm with epsilon comparator.
///   2. If there is no floating-point column, we loop over every row in one set
///   and try to find it in the other via std::multiset::find(). Values are
///   compared directly without epsilon.
/// We made this difference because some operations, such as aggregation,
/// require tolerance to imprecision of floating-point computation in their
/// results, while epsilon comparator doesn't satisfy the strict weak ordering
/// requirement of std::multiset. Since aggregation results typically have one
/// row per group, we use sort-merge algorithm with epsilon comparator to verify
/// their results. This condition may be relaxed if there is only one
/// floating-point column in each set. Verifying arbitrary result sets with
/// epsilon comparison would require more advanced algorithms such as maximum
/// bipartite matching. Hence we leave them as future work. Check out
/// https://github.com/facebookincubator/velox/issues/3493 for more dicsussion.
bool assertEqualResults(
    const std::vector<RowVectorPtr>& expected,
    const std::vector<RowVectorPtr>& actual);

bool assertEqualResults(
    const MaterializedRowMultiset& expected,
    const std::vector<RowVectorPtr>& actual);

/// Ensure both datasets have the same type and number of rows.
void assertEqualTypeAndNumRows(
    const TypePtr& expectedType,
    vector_size_t expectedNumRows,
    const std::vector<RowVectorPtr>& actual);

void printResults(const RowVectorPtr& result, std::ostream& out);

} // namespace facebook::velox::exec::test
