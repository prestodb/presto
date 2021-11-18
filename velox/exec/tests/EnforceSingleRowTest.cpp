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

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class EnforceSingleRowTest : public OperatorTestBase {
 protected:
  void assertQueryFails(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& errorMessage) {
    CursorParameters params;
    params.planNode = plan;
    try {
      readCursor(params, [](auto /*task*/) {});
      FAIL() << "Expected query to fail, but it succeeded";
    } catch (const VeloxException& e) {
      ASSERT_TRUE(e.message().find(errorMessage) != std::string::npos)
          << "Expected query to fail with error message: " << errorMessage
          << ". The query failed with a different error message: "
          << e.message();
    }
  }
};

TEST_F(EnforceSingleRowTest, basic) {
  auto singleRow =
      makeRowVector({makeFlatVector<int32_t>(1, [](auto row) { return row; })});
  auto multipleRows = makeRowVector(
      {makeFlatVector<int32_t>(27, [](auto row) { return row; })});

  createDuckDbTable({singleRow});

  // Single row of input. The query should pass.
  auto plan = PlanBuilder().values({singleRow}).enforceSingleRow().planNode();
  assertQuery(plan, "SELECT * FROM tmp");

  // Two rows of input in two separate single-row batches. The query should
  // fail.
  assertQueryFails(
      PlanBuilder()
          .values({singleRow, singleRow})
          .enforceSingleRow()
          .planNode(),
      "Expected single row of input. Received 1 extra rows.");

  // Multiple rows of input in a single batch. The query should fail.
  assertQueryFails(
      PlanBuilder().values({multipleRows}).enforceSingleRow().planNode(),
      "Expected single row of input. Received 27 rows.");

  // Empty input. The query should pass and return a single row of nulls.
  plan = PlanBuilder()
             .values({singleRow})
             .filter("c0 = 12345")
             .enforceSingleRow()
             .planNode();
  assertQuery(plan, "SELECT null");
}
