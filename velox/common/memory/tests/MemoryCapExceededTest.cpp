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
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <re2/re2.h>

DECLARE_bool(velox_suppress_memory_capacity_exceeding_error_message);

namespace facebook::velox::exec::test {
namespace {

class MemoryCapExceededTest : public OperatorTestBase,
                              public testing::WithParamInterface<bool> {
  void SetUp() override {
    OperatorTestBase::SetUp();
    // NOTE: if 'GetParam()' is true, then suppress the verbose error message in
    // memory capacity exceeded exception.
    FLAGS_velox_suppress_memory_capacity_exceeding_error_message = GetParam();
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
    FLAGS_velox_suppress_memory_capacity_exceeding_error_message = false;
  }
};

namespace {
bool someLineMatches(const std::string& text, const std::string& pattern) {
  std::stringstream in;
  in << text;
  std::string line;
  auto exp = fmt::format(".*{}.*", pattern);
  while (std::getline(in, line)) {
    if (RE2::FullMatch(line, exp)) {
      return true;
    }
  }
  return false;
}
} // namespace

TEST_P(MemoryCapExceededTest, singleDriver) {
  // Executes a plan with a single driver thread and query memory limit that
  // forces it to throw MEM_CAP_EXCEEDED exception. Verifies that the error
  // message contains all the details expected.

  vector_size_t size = 1'024;
  // This limit ensures that only the Aggregation Operator fails.
  constexpr int64_t kMaxBytes = 5LL << 20; // 5MB
  // We look for these lines separately, since their order can change (not sure
  // why).
  std::vector<std::string> expectedTexts = {
      "Exceeded memory pool cap of 5.00MB with max 5.00MB when requesting "
      "2.00MB, memory manager cap is 8.00GB, requestor "
      "'op.2.0.0.Aggregation' with current usage 3.70MB"};
  std::vector<std::string> expectedDetailedTexts = {
      "node.1 usage 1.00MB reserved 1.00MB peak 1.00MB",
      "op.1.0.0.FilterProject usage 12.00KB reserved 1.00MB peak 12.00KB",
      "node.2 usage 4.00MB reserved 4.00MB peak 4.00MB",
      "op.2.0.0.Aggregation usage 3.70MB reserved 4.00MB peak 3.70MB",
      "Top 2 leaf memory pool usages:"};

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
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, exec::MemoryReclaimer::create()));
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  params.maxDrivers = 1;
  try {
    readCursor(params, [](Task*) {});
    FAIL() << "Expected a MEM_CAP_EXCEEDED RuntimeException.";
  } catch (const VeloxException& e) {
    const auto errorMessage = e.message();
    for (const auto& expectedText : expectedTexts) {
      ASSERT_TRUE(errorMessage.find(expectedText) != std::string::npos)
          << "Expected error message to contain '" << expectedText
          << "', but received '" << errorMessage << "'.";
    }
    for (const auto& expectedText : expectedDetailedTexts) {
      LOG(ERROR) << expectedText;
      if (!GetParam()) {
        ASSERT_TRUE(someLineMatches(errorMessage, expectedText))
            << "Expected error message to contain '" << expectedText
            << "', but received '" << errorMessage << "'.";
      } else {
        ASSERT_TRUE(errorMessage.find(expectedText) == std::string::npos)
            << "Unexpected error message to contain '" << expectedText
            << "', but received '" << errorMessage << "'.";
      }
    }
  }
}

TEST_P(MemoryCapExceededTest, multipleDrivers) {
  // Executes a plan that runs with ten drivers and query memory limit that
  // forces it to throw MEM_CAP_EXCEEDED exception. Verifies that the error
  // message contains information that acknowledges the existence of N
  // operator memory pool instances. Rest of the message is not verified as the
  // contents are non-deterministic with respect to which operators make it to
  // the top 3 and their memory usage.
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

  const std::string expectedText("Aggregation usage");

  auto plan = PlanBuilder()
                  .values(data, true)
                  .singleAggregation({"c0"}, {"sum(c1)"})
                  .planNode();
  auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, exec::MemoryReclaimer::create()));

  const int32_t numDrivers = 10;
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  params.maxDrivers = numDrivers;
  try {
    readCursor(params, [](Task*) {});
    FAIL() << "Expected a MEM_CAP_EXCEEDED RuntimeException.";
  } catch (const VeloxException& e) {
    const auto errorMessage = e.message();
    if (!GetParam()) {
      ASSERT_TRUE(someLineMatches(errorMessage, expectedText))
          << "Expected error message to contain '" << expectedText
          << "', but received '" << errorMessage << "'.";
    } else {
      ASSERT_TRUE(errorMessage.find(expectedText) == std::string::npos)
          << "Unexpected error message to contain '" << expectedText
          << "', but received '" << errorMessage << "'.";
    }
  }
}

TEST_P(MemoryCapExceededTest, allocatorCapacityExceededError) {
  // Executes a plan with no memory pool capacity limit but very small memory
  // manager's limit.
  struct {
    int64_t allocatorCapacity;
    bool useMmap;
    std::vector<std::string> expectedErrorMessages;
  } testSettings[] = {
      {64LL << 20,
       false,
       std::vector<std::string>{
           "allocateContiguous failed with .* pages",
           "max capacity 128.00MB unlimited capacity used .* available .*",
           ".* reservation .used .*MB, reserved .*MB, min 0B. counters",
           "allocs .*, frees .*, reserves .*, releases .*, collisions .*"}},
      {64LL << 20,
       true,
       std::vector<std::string>{
           "allocateContiguous failed with .* pages",
           "max capacity 128.00MB unlimited capacity used .* available .*",
           ".* reservation .used .*MB, reserved .*MB, min .*B. counters",
           ".*, frees .*, reserves .*, releases .*, collisions .*"}}};
  for (const auto& testData : testSettings) {
    memory::MemoryManager manager(
        {.allocatorCapacity = (int64_t)testData.allocatorCapacity,
         .useMmapAllocator = testData.useMmap});

    vector_size_t size = 1'024;
    // This limit ensures that only the Aggregation Operator fails.
    constexpr int64_t kMaxBytes = 128LL << 20; // 128MB

    std::vector<RowVectorPtr> data;
    for (auto i = 0; i < 10000; ++i) {
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
        manager.addRootPool(queryCtx->queryId(), kMaxBytes));
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = queryCtx;
    params.maxDrivers = 1;
    try {
      readCursor(params, [](Task*) {});
      FAIL() << "Expected a MEM_CAP_EXCEEDED RuntimeException.";
    } catch (const VeloxException& e) {
      const auto errorMessage = e.message();
      for (const auto& expectedText : testData.expectedErrorMessages) {
        ASSERT_TRUE(someLineMatches(errorMessage, expectedText))
            << "Expected error message to contain '" << expectedText
            << "', but received '" << errorMessage << "'.";
      }
    }
    waitForAllTasksToBeDeleted();
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MemoryCapExceededTest,
    MemoryCapExceededTest,
    testing::ValuesIn({false, true}));

} // namespace
} // namespace facebook::velox::exec::test
