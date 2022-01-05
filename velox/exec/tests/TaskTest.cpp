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

using namespace facebook::velox;

class TaskTest : public testing::Test {};

// Test if the Task correctly handles split groups.
TEST_F(TaskTest, splitGroup) {
  // Create single hive connector split and the task.
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      "test",
      "file:/tmp/abc",
      facebook::velox::dwio::common::FileFormat::ORC,
      0,
      100);
  core::PlanNodeId planNodeId{"0"};
  auto queryCtx = core::QueryCtx::createForTest();
  exec::Task task("0", nullptr, 0, queryCtx);

  // This is the set of completed groups we expect.
  std::unordered_set<int32_t> completedSplitGroups;

  // Add and complete 3 splits for group 0.
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 0));
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 0));
  task.splitFinished(planNodeId, 0);
  task.splitFinished(planNodeId, 0);
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 0));
  task.splitFinished(planNodeId, 0);
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
  task.splitFinished(planNodeId, 1);
  task.splitFinished(planNodeId, 1);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Add 2 splits for group 2, declare 'no more splits' for group 2.
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 2));
  task.addSplit(planNodeId, exec::Split(folly::copy(connectorSplit), 2));
  task.noMoreSplitsForGroup(planNodeId, 2);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Finish the last split for group 1
  task.splitFinished(planNodeId, 1);
  completedSplitGroups.insert(1);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);

  // Finish the 2 split for group 2
  task.splitFinished(planNodeId, 2);
  task.splitFinished(planNodeId, 2);
  completedSplitGroups.insert(2);
  EXPECT_EQ(completedSplitGroups, task.taskStats().completedSplitGroups);
}
