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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class AssignUniqueIdTest : public OperatorTestBase {
 protected:
  void verifyUniqueId(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<RowVectorPtr>& inputRows,
      const int numThreads = 1) {
    CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = numThreads;

    auto result = readCursor(params, [](auto /*task*/) {});
    auto numColumns = result.second[0]->childrenSize();
    ASSERT_EQ(numColumns, inputRows[0]->childrenSize() + 1);

    std::set<int64_t> ids;
    for (int i = 0; i < numColumns; i++) {
      for (auto output : result.second) {
        auto column = output->childAt(i);
        if (i < numColumns - 1) {
          assertEqualVectors(inputRows[0]->childAt(i), column);
        } else {
          auto idValues = column->asFlatVector<int64_t>()->rawValues();
          std::copy(
              idValues,
              idValues + column->size(),
              std::inserter(ids, ids.end()));
        }
      }
    }
    vector_size_t totalInputSize = std::accumulate(
        inputRows.begin(),
        inputRows.end(),
        0,
        [](vector_size_t sum, RowVectorPtr row) { return sum + row->size(); });
    ASSERT_EQ(totalInputSize * numThreads, ids.size());

    auto task = result.first->task();

    // Verify number of memory allocations. There should be exactly one
    // allocation (per thread of execution) for the values buffer of the unique
    // ID vector. Memory should be allocated when producing first batch of
    // output and re-used for subsequent batches.
    ASSERT_EQ(
        numThreads, task->pool()->getMemoryUsageTracker()->getNumAllocs());
  }
};

TEST_F(AssignUniqueIdTest, multiBatch) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> input;
  for (int i = 0; i < 3; ++i) {
    auto column =
        makeFlatVector<int32_t>(batchSize, [](auto row) { return row; });
    input.push_back(makeRowVector({column}));
  }

  auto plan = PlanBuilder().values(input).assignUniqueId().planNode();

  verifyUniqueId(plan, input);
}

TEST_F(AssignUniqueIdTest, exceedRequestLimit) {
  vector_size_t requestLimit = 1 << 20L;
  auto input = {makeRowVector({makeFlatVector<int32_t>(
      requestLimit + 1, [](auto row) { return row; })})};

  auto plan = PlanBuilder().values(input).assignUniqueId().planNode();

  verifyUniqueId(plan, input);
}

TEST_F(AssignUniqueIdTest, multiThread) {
  for (int i = 0; i < 3; i++) {
    vector_size_t batchSize = 1000;
    auto input = {makeRowVector(
        {makeFlatVector<int32_t>(batchSize, [](auto row) { return row; })})};
    auto plan = PlanBuilder().values(input, true).assignUniqueId().planNode();

    verifyUniqueId(plan, input, 8);
  }
}

TEST_F(AssignUniqueIdTest, maxRowIdLimit) {
  auto input = {makeRowVector({makeFlatVector<int32_t>({1, 2, 3})})};

  auto plan = PlanBuilder().values(input).assignUniqueId().planNode();
  // Increase the counter to kMaxRowId.
  std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(plan)
      ->uniqueIdCounter()
      ->fetch_add(1L << 40);

  EXPECT_THROW(verifyUniqueId(plan, input), VeloxRuntimeError);
}

TEST_F(AssignUniqueIdTest, taskUniqueIdLimit) {
  auto input = {makeRowVector({makeFlatVector<int32_t>({1, 2, 3})})};

  auto plan =
      PlanBuilder().values(input).assignUniqueId("unique", 1L << 24).planNode();

  EXPECT_THROW(verifyUniqueId(plan, input), VeloxRuntimeError);
}
