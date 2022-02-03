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
#include <gtest/gtest.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

#define VELOX_ASSERT_THROW(expression, errorMessage)                  \
  try {                                                               \
    (expression);                                                     \
    VELOX_FAIL("Expected an exception");                              \
  } catch (const VeloxException& e) {                                 \
    ASSERT_TRUE(e.isUserError());                                     \
    ASSERT_TRUE(e.message().find(errorMessage) != std::string::npos); \
  }

using namespace facebook::velox;

class TaskTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    functions::prestosql::registerAllFunctions();
  }

  void useOneSplit(
      exec::Task& task,
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId) {
    exec::Split splitOut;
    exec::ContinueFuture futureOut{true};
    task.getSplitOrFuture(splitGroupId, planNodeId, splitOut, futureOut);
    task.splitFinished(planNodeId, splitGroupId);
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
      "Splits can be associated only with source plan nodes. Plan node ID 1 doesn't refer to a source node.";
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
      "Splits can be associated only with source plan nodes. Plan node ID 12 doesn't refer to a source node.";
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
}

// Test if the Task correctly handles split groups.
TEST_F(TaskTest, splitGroup) {
  // Create single hive connector split and the task.
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      "test",
      "file:/tmp/abc",
      facebook::velox::dwio::common::FileFormat::ORC,
      0,
      100);
  auto plan = exec::test::PlanBuilder()
                  .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
                  .planNode();
  core::PlanNodeId planNodeId{"0"};
  core::PlanFragment planFragment{plan, core::ExecutionStrategy::kGrouped, 3};
  exec::Task task(
      "0", std::move(planFragment), 0, core::QueryCtx::createForTest());

  // This is the set of completed groups we expect.
  std::unordered_set<int32_t> completedSplitGroups;

  // Add and complete 3 splits for group 0.
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 0));
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 0));
  useOneSplit(task, 0, planNodeId);
  useOneSplit(task, 0, planNodeId);
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 0));
  useOneSplit(task, 0, planNodeId);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Declare 'no more splits' for group 0.
  task.noMoreSplitsForGroup(planNodeId, 0);
  completedSplitGroups.insert(0);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Add 3 splits for group 1, declare 'no more splits' for group 1, finish 2
  // splits from group 1.
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 1));
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 1));
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 1));
  task.noMoreSplitsForGroup(planNodeId, 1);
  useOneSplit(task, 1, planNodeId);
  useOneSplit(task, 1, planNodeId);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Add 2 splits for group 2, declare 'no more splits' for group 2.
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 2));
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 2));
  task.noMoreSplitsForGroup(planNodeId, 2);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Finish the last split for group 1
  useOneSplit(task, 1, planNodeId);
  completedSplitGroups.insert(1);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Finish the 2 split for group 2
  useOneSplit(task, 2, planNodeId);
  useOneSplit(task, 2, planNodeId);
  completedSplitGroups.insert(2);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);
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
