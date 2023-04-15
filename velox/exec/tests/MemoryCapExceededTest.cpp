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
  // Executes a plan with a single driver thread and query memory limit that
  // forces it to throw MEM_CAP_EXCEEDED exception. Verifies that the error
  // message contains all the details expected.

  vector_size_t size = 1'024;
  // This limit ensures that only the Aggregation Operator fails.
  constexpr int64_t kMaxBytes = 5LL << 20; // 5MB
  // We look for these lines separately, since their order can change (not sure
  // why).
  std::array<std::string, 14> expectedTexts = {
      "Exceeded memory cap of 5.00MB when requesting 2.00MB.",
      "node.0: 0B in 1 operators, min 0B, max 0B",
      "op.0.0.0.Values: 0B in 1 instances, min 0B, max 0B",
      "node.1: 12.00KB in 1 operators, min 12.00KB, max 12.00KB",
      "op.1.0.0.FilterProject: 12.00KB in 1 instances, min 12.00KB, max 12.00KB",
      "node.2: 3.77MB in 1 operators, min 3.77MB, max 3.77MB",
      "op.2.0.0.Aggregation: 3.77MB in 1 instances, min 3.77MB, max 3.77MB",
      "node.3: 0B in 1 operators, min 0B, max 0B",
      "op.3.0.0.OrderBy: 0B in 1 instances, min 0B, max 0B",
      "node.N/A: 0B in 1 operators, min 0B, max 0B",
      "op.N/A.0.0.CallbackSink: 0B in 1 instances, min 0B, max 0B",
      "Top operator memory usages:",
      "Failed Operator: Aggregation.2: 3.77MB"};

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
  auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  queryCtx->testingOverrideMemoryPool(
      memory::defaultMemoryManager().addRootPool(
          queryCtx->queryId(), kMaxBytes));
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  params.maxDrivers = 1;
  try {
    readCursor(params, [](Task*) {});
    FAIL() << "Expected a MEM_CAP_EXCEEDED RuntimeException.";
  } catch (const VeloxException& e) {
    auto errorMessage = e.message();
    for (const auto& expectedText : expectedTexts) {
      ASSERT_TRUE(errorMessage.find(expectedText) != std::string::npos)
          << "Expected error message to contain '" << expectedText
          << "', but received '" << errorMessage << "'.";
    }
  }
}

TEST_F(MemoryCapExceededTest, multipleDrivers) {
  // Executes a plan that runs with ten drivers and query memory limit that
  // forces it to throw MEM_CAP_EXCEEDED exception. Verifies that the error
  // message contains information that acknowledges the existence of N
  // operators. Rest of the message is not verified as the contents are
  // non-deterministic with respect to which operators make it to the top 3 and
  // their memory usage.
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
  auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  queryCtx->testingOverrideMemoryPool(
      memory::defaultMemoryManager().addRootPool(
          queryCtx->queryId(), kMaxBytes));

  const int32_t numDrivers = 10;
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  params.maxDrivers = numDrivers;
  VELOX_ASSERT_THROW(
      readCursor(params, [](Task*) {}),
      fmt::format("{} operators", numDrivers));
}

} // namespace
} // namespace facebook::velox::exec::test
