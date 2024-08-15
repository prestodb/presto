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
#include "velox/common/process/ThreadDebugInfo.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/Udf.h"

#include <folly/Unit.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

// Only build if address sanitizer is not enabled since this test initiated a
// segfault which should be captured by the signal handler but gets caught by
// the address sanitizer handler instead which print a different error log.

// For clang
#ifdef __has_feature
#define IS_BUILDING_WITH_ASAN() __has_feature(address_sanitizer)
#else
// For GCC
#if defined(__SANITIZE_ADDRESS__) && __SANITIZE_ADDRESS__
#define IS_BUILDING_WITH_ASAN() 1
#else
#define IS_BUILDING_WITH_ASAN() 0
#endif
#endif

// Ensure the test class name has "DeathTest" as a prefix to ensure this runs in
// a single thread since the ASSERT_DEATH forks the process.
class ThreadDebugInfoDeathTest : public OperatorTestBase {};

namespace {

template <typename T>
struct InduceSegFaultFunction {
  template <typename TResult, typename TInput>
  void call(TResult& out, const TInput& in) {
    int* nullpointer = nullptr;
    *nullpointer = 6;
  }
};
} // namespace

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, withinSeperateDriverThread) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<InduceSegFaultFunction, int64_t, int64_t>({"segFault"});
  auto op = PlanBuilder().values({vector}).project({"segFault(c0)"}).planNode();
#if IS_BUILDING_WITH_ASAN() == 0
  ASSERT_DEATH(
      (assertQuery(op, vector)),
      ".*Fatal signal handler. Query Id= TaskCursorQuery_0 Task Id= test_cursor 1.*");
#endif
}

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, withinQueryCompilation) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<InduceSegFaultFunction, int64_t, int64_t>({"segFault"});
  // Call expression with a constant to trigger the constant folding during
  // compilation.
  auto op = PlanBuilder().values({vector}).project({"segFault(1)"}).planNode();

#if IS_BUILDING_WITH_ASAN() == 0
  ASSERT_DEATH(
      (assertQuery(op, vector)),
      ".*Fatal signal handler. Query Id= TaskCursorQuery_0 Task Id= test_cursor 1.*");
#endif
}

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, withinTheCallingThread) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<InduceSegFaultFunction, int64_t, int64_t>({"segFault"});
  auto plan =
      PlanBuilder().values({vector}).project({"segFault(c0)"}).planFragment();

  auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig({}),
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      cache::AsyncDataCache::getInstance(),
      nullptr,
      nullptr,
      "TaskCursorQuery_0");
  auto task = exec::Task::create(
      "single.execution.task.0",
      std::move(plan),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);

#if IS_BUILDING_WITH_ASAN() == 0
  ASSERT_DEATH(
      (task->next()),
      ".*Fatal signal handler. Query Id= TaskCursorQuery_0 Task Id= single.execution.task.0.*");
#endif
}

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, noThreadContextSet) {
  int* nullpointer = nullptr;
#if IS_BUILDING_WITH_ASAN() == 0
  ASSERT_DEATH((*nullpointer = 6), ".*ThreadDebugInfo object not found.*");
#endif
  folly::compiler_must_not_elide(nullpointer);
}
