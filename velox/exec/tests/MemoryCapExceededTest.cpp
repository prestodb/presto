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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <gmock/gmock.h>

namespace facebook::velox::exec::test {
namespace {

class MemoryCapExceededTest : public OperatorTestBase {};

TEST_F(MemoryCapExceededTest, singleDriver) {
  // Executes a plan with a single driver and query memory limit that forces it
  // to throw MEM_CAP_EXCEEDED exception. Verifies that the error message
  // contains all the details expected.

  vector_size_t size = 1'024;
  // This limit ensures that only the Aggregation Operator fails.
  constexpr int64_t kMaxBytes = 5LL << 20; // 5MB
  // Regex is used to offset the fact that operators with the same usage (in
  // this case the ones with zero usage) are not deterministically sorted and
  // the last one that makes its way to the top 3 can change between runs.
  std::string expectedErrorMsgRegex =
      "Exceeded memory cap of 5.00MB when requesting 2.00MB. Task total: "
      "5.00MB Peak: 5.00MB. Top 3 Operators \\(by aggregate usage across "
      "all drivers\\): Aggregation_#2_x1: 1.77MB Peak: 4.00MB, "
      "FilterProject_#1_x1: 12.00KB Peak: 1.00MB, .+_x1: 0B Peak: "
      "0B. Failed Operator: Aggregation_#2: 1.77MB";
  std::vector<RowVectorPtr> data;
  for (auto i = 0; i < 100; ++i) {
    data.push_back(makeRowVector({
        makeFlatVector<int64_t>(
            size, [&i](auto row) { return row + (i * 1000); }),
        makeFlatVector<int64_t>(size, [](auto row) { return row + 3; }),
    }));
  }

  // Plan created to allow multiple operators to show up in the top 3 memory
  // usage list in the error message.
  auto plan = PlanBuilder()
                  .values(data)
                  .project({"c0", "c0 + c1"})
                  .singleAggregation({"c0"}, {"sum(p1)"})
                  .orderBy({"c0"}, false)
                  .planNode();
  auto queryCtx = core::QueryCtx::createForTest();
  queryCtx->pool()->setMemoryUsageTracker(
      velox::memory::MemoryUsageTracker::create(
          kMaxBytes, kMaxBytes, kMaxBytes));
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  try {
    readCursor(params, [](Task*) {});
    FAIL() << "Expected a MEM_CAP_EXCEEDED RuntimeException.";
  } catch (const VeloxException& e) {
    auto errorMessage = e.message();
    ASSERT_THAT(errorMessage, ::testing::MatchesRegex(expectedErrorMsgRegex));
  }
}

TEST_F(MemoryCapExceededTest, multipleDrivers) {
  // Executes a plan that runs with ten drivers and query memory limit that
  // forces it to throw MEM_CAP_EXCEEDED exception. Verifies that the error
  // message contains information that acknowledges the existence of 10 drivers.
  // Rest of the message is not verified as the contents are non-deterministic
  // with respect to which operators make it to the top 3 and their memory
  // usage.
  vector_size_t size = 1'024;
  const int32_t numSplits = 100;
  constexpr int64_t kMaxBytes = 12LL << 20; // 12MB
  std::vector<RowVectorPtr> data;
  for (auto i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            size, [&i](auto row) { return row + (i * 1000); }),
        makeFlatVector<int32_t>(size, [](auto row) { return row + 3; }),
    });
    data.push_back(rowVector);
  }

  auto plan = PlanBuilder()
                  .values(data, true)
                  .singleAggregation({"c0"}, {"sum(c1)"})
                  .planNode();
  auto queryCtx = core::QueryCtx::createForTest();
  queryCtx->pool()->setMemoryUsageTracker(
      velox::memory::MemoryUsageTracker::create(
          kMaxBytes, kMaxBytes, kMaxBytes));

  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  params.maxDrivers = 10;
  VELOX_ASSERT_THROW(readCursor(params, [](Task*) {}), "x10");
}

} // namespace
} // namespace facebook::velox::exec::test
