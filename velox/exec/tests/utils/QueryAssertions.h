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
#include <chrono>

#include "velox/common/testutil/TestValue.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/vector/ComplexVector.h"

#include <duckdb.hpp> // @manual

namespace facebook::velox::exec::test {

using MaterializedRow = std::vector<velox::variant>;
using DuckDBQueryResult = std::unique_ptr<::duckdb::MaterializedQueryResult>;

/// Multiset that compares floating-point values directly.
using MaterializedRowMultiset = std::multiset<MaterializedRow>;

/// Converts input 'RowVector' into a list of 'MaterializedRow's.
std::vector<MaterializedRow> materialize(const RowVectorPtr& vector);

/// Converts a list of 'RowVector's into 'MaterializedRowMultiset'.
MaterializedRowMultiset materialize(const std::vector<RowVectorPtr>& vectors);

class DuckDbQueryRunner {
 public:
  DuckDbQueryRunner() : db_(nullptr) {}

  void createTable(
      const std::string& name,
      const std::vector<RowVectorPtr>& data);

  DuckDBQueryResult execute(const std::string& sql);

  MaterializedRowMultiset execute(
      const std::string& sql,
      const RowTypePtr& resultRowType) {
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
      const RowTypePtr& resultRowType) {
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
      const RowTypePtr& resultRowType,
      std::function<void(std::vector<MaterializedRow>&)> resultCallback);
};

/// Scoped abort percentage utility that allows user to trigger abort during the
///  query execution.
/// 'abortPct' specifies the probability of of triggering abort. 100% means
/// abort will always be triggered.
/// 'maxInjections' indicates the max number of actual triggering, e.g. when
/// 'abortPct' is 20 and 'maxInjections' is 10, continuous calls to
/// testingMaybeTriggerAbort() will keep rolling the dice that has a chance of
/// 20% triggering until 10 triggers have been invoked.
class TestScopedAbortInjection {
 public:
  explicit TestScopedAbortInjection(
      int32_t abortPct,
      int32_t maxInjections = std::numeric_limits<int32_t>::max());

  ~TestScopedAbortInjection();
};

// This class leverages TestValue to inject OOMs into query execution.
// Therefore, it can only be used in debug builds, and only one instance
// of this class can be enabled at a time.
class ScopedOOMInjector {
 public:
  ScopedOOMInjector(
      const std::function<bool()>& oomConditionChecker,
      uint64_t oomCheckIntervalMs)
      : oomConditionChecker_(oomConditionChecker),
        oomCheckIntervalMs_(oomCheckIntervalMs) {}

  inline static const std::string kErrorMessage = "Injected OOM";

  void enable() {
    // Since this relies on TestValue to trigger the OOMs, it only supports
    // debug builds.
#ifdef NDEBUG
    VELOX_FAIL("OOM injection can only be used in debug builds");
#endif

    if (enabled_.exchange(true)) {
      VELOX_FAIL("Already enabled");
    }

    // Make sure TestValues are enabled.
    common::testutil::TestValue::enable();

    common::testutil::TestValue::set(
        kInjectionPoint,
        std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool*) {
          const auto currentTime = now();
          if (currentTime - lastOomCheckTime_ >= oomCheckIntervalMs_) {
            lastOomCheckTime_ = currentTime;
            if (oomConditionChecker_()) {
              LOG(INFO) << "<-- Triggering OOM --";
              VELOX_MEM_POOL_CAP_EXCEEDED(kErrorMessage);
            }
          }
        }));
  }

  ~ScopedOOMInjector() {
    common::testutil::TestValue::clear(kInjectionPoint);
    enabled_ = false;
  }

 private:
  inline static const std::string kInjectionPoint =
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe";
  // If more than one instance of this class is enabled, they'll overwrite
  // each other, so make sure no one does this by accident.
  inline static std::atomic_bool enabled_{false};

  static size_t now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  // When this function returns true, an OOM will be triggered.
  const std::function<bool()> oomConditionChecker_;
  // The interval between each check of the condition.
  const uint64_t oomCheckIntervalMs_;

  std::atomic<size_t> lastOomCheckTime_{0};
};

/// Test utility that might trigger task abort. The function returns true if
/// abortion triggers otherwise false.
bool testingMaybeTriggerAbort(exec::Task* task);

std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> readCursor(
    const CursorParameters& params,
    std::function<void(exec::Task*)> addSplits,
    uint64_t maxWaitMicros = 5'000'000);

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

/// Invoked to wait for all the tasks created by the test to be deleted.
///
/// NOTE: it is assumed that there is no more task to be created after or
/// during this wait call. This is for testing purpose for now.
void waitForAllTasksToBeDeleted(uint64_t maxWaitUs = 3'000'000);

/// Similar to above test utility except waiting for a specific number of
/// tasks to be deleted.
void waitForAllTasksToBeDeleted(
    uint64_t expectedDeletedTasks,
    uint64_t maxWaitUs);

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
    const RowTypePtr& resultType,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner);

void assertResultsOrdered(
    const std::vector<RowVectorPtr>& results,
    const RowTypePtr& resultType,
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
    const MaterializedRowMultiset& expectedRows,
    const TypePtr& expectedRowType,
    const std::vector<RowVectorPtr>& actual);

/// Ensure both plans have the same results.
bool assertEqualResults(
    const core::PlanNodePtr& plan1,
    const core::PlanNodePtr& plan2);

/// Ensure both datasets have the same type and number of rows.
void assertEqualTypeAndNumRows(
    const TypePtr& expectedType,
    vector_size_t expectedNumRows,
    const std::vector<RowVectorPtr>& actual);

void printResults(const RowVectorPtr& result, std::ostream& out);

} // namespace facebook::velox::exec::test
