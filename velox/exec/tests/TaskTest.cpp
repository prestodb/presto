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
#include "velox/exec/Task.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;

class TaskTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    functions::prestosql::registerAllScalarFunctions();
  }
};

TEST_F(TaskTest, wrongPlanNodeForSplit) {
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      "test",
      "file:/tmp/abc",
      facebook::velox::dwio::common::FileFormat::ORC,
      0,
      100);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
                  .project({"a * a", "b + b"})
                  .planFragment();

  exec::Task task(
      "task-1", std::move(plan), 0, core::QueryCtx::createForTest());

  // Add split for the source node.
  task.addSplit("0", exec::Split(folly::copy(connectorSplit)));

  // Try to add split for a non-source node.
  auto errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 1 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      task.addSplit("1", exec::Split(folly::copy(connectorSplit))),
      errorMessage)

  VELOX_ASSERT_THROW(
      task.addSplitWithSequence(
          "1", exec::Split(folly::copy(connectorSplit)), 3),
      errorMessage)

  VELOX_ASSERT_THROW(task.setMaxSplitSequenceId("1", 9), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplits("1"), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplitsForGroup("1", 5), errorMessage)

  // Try to add split for non-existent node.
  errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 12 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      task.addSplit("12", exec::Split(folly::copy(connectorSplit))),
      errorMessage)

  VELOX_ASSERT_THROW(
      task.addSplitWithSequence(
          "12", exec::Split(folly::copy(connectorSplit)), 3),
      errorMessage)

  VELOX_ASSERT_THROW(task.setMaxSplitSequenceId("12", 9), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplits("12"), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplitsForGroup("12", 5), errorMessage)

  // Try to add split for a Values source node.
  plan =
      exec::test::PlanBuilder()
          .values({makeRowVector(ROW({"a", "b"}, {INTEGER(), DOUBLE()}), 10)})
          .project({"a * a", "b + b"})
          .planFragment();

  exec::Task valuesTask(
      "task-2", std::move(plan), 0, core::QueryCtx::createForTest());
  errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 0 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      valuesTask.addSplit("0", exec::Split(folly::copy(connectorSplit))),
      errorMessage)
}

TEST_F(TaskTest, duplicatePlanNodeIds) {
  auto plan = exec::test::PlanBuilder()
                  .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
                  .hashJoin(
                      {"a"},
                      {"a1"},
                      exec::test::PlanBuilder()
                          .tableScan(ROW({"a1", "b1"}, {INTEGER(), DOUBLE()}))
                          .planNode(),
                      "",
                      {"b", "b1"})
                  .planFragment();

  VELOX_ASSERT_THROW(
      exec::Task("task-1", std::move(plan), 0, core::QueryCtx::createForTest()),
      "Plan node IDs must be unique. Found duplicate ID: 0.")
}
