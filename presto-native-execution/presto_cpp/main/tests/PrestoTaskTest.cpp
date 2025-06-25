/*
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
#include "presto_cpp/main/PrestoTask.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/PlanNodeIdGenerator.h"
#include "velox/type/Type.h"

DECLARE_bool(velox_memory_leak_check_enabled);

using namespace facebook::velox;
using namespace facebook::presto;

using facebook::velox::exec::test::PlanBuilder;
using facebook::presto::PrestoTaskId;

namespace {
// Create a simple velox task for testing.
std::shared_ptr<exec::Task> createExecTask(
    const std::string& taskId,
    PrestoTask& prestoTask) {
  RowTypePtr rowType = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .tableScan(rowType)
                        .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                        .planNode();
  return exec::Task::create(
      taskId,
      core::PlanFragment{plan},
      prestoTask.id.id(),
      core::QueryCtx::create(nullptr),
      exec::Task::ExecutionMode::kParallel,
      static_cast<exec::Consumer>(nullptr),
      prestoTask.id.stageId());
}

// Add a split to the task.
void addSplitToTask(PrestoTask& prestoTask, long sequenceId) {
  exec::Split split;
  split.connectorSplit =
      std::make_shared<connector::ConnectorSplit>("connector", 0);
  prestoTask.task->addSplitWithSequence("0", std::move(split), sequenceId);
}
} // namespace

class PrestoTaskTest : public testing::Test {
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;
  }
};

TEST_F(PrestoTaskTest, basicTaskId) {
  const std::string taskIdStr("20201107_130540_00011_wrpkw.1.2.3.4");
  PrestoTaskId id(taskIdStr);
  ASSERT_EQ(id.queryId(), "20201107_130540_00011_wrpkw");
  ASSERT_EQ(id.stageId(), 1);
  ASSERT_EQ(id.stageExecutionId(), 2);
  ASSERT_EQ(id.id(), 3);
  ASSERT_EQ(id.attemptNumber(), 4);
  ASSERT_EQ(id.toString(), taskIdStr);
}

TEST_F(PrestoTaskTest, malformedTaskId) {
  VELOX_ASSERT_THROW(PrestoTaskId(""), "Malformed task ID: ");
  VELOX_ASSERT_THROW(
      PrestoTaskId("20201107_130540_00011_wrpkw."),
      "Malformed task ID: 20201107_130540_00011_wrpkw.");
  VELOX_ASSERT_THROW(PrestoTaskId("q.1.2"), "Malformed task ID: q.1.2");
}

TEST_F(PrestoTaskTest, runtimeMetricConversion) {
  RuntimeMetric veloxMetric;
  veloxMetric.unit = RuntimeCounter::Unit::kBytes;
  veloxMetric.sum = 101;
  veloxMetric.count = 17;
  veloxMetric.min = 62;
  veloxMetric.max = 79;

  const std::string metricName{"my_name"};
  const auto prestoMetric = toRuntimeMetric(metricName, veloxMetric);
  EXPECT_EQ(metricName, prestoMetric.name);
  EXPECT_EQ(protocol::RuntimeUnit::BYTE, prestoMetric.unit);
  EXPECT_EQ(veloxMetric.sum, prestoMetric.sum);
  EXPECT_EQ(veloxMetric.count, prestoMetric.count);
  EXPECT_EQ(veloxMetric.max, prestoMetric.max);
  EXPECT_EQ(veloxMetric.min, prestoMetric.min);
}

TEST_F(PrestoTaskTest, basic) {
  PrestoTask task{"20201107_130540_00011_wrpkw.1.2.3.4", "node2", 0};

  // Test coordinator heartbeat.
  EXPECT_EQ(task.timeSinceLastCoordinatorHeartbeatMs(), 0);
  task.updateCoordinatorHeartbeat();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_GE(task.timeSinceLastCoordinatorHeartbeatMs(), 100);
}

TEST_F(PrestoTaskTest, updateStatus) {
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  const std::string taskId{"20201107_130540_00011_wrpkw.1.2.3.4"};
  PrestoTask prestoTask{taskId, "node1", 0};
  long sequenceId{0};

  // No exec task yet (no fragment plan), so in planned state.
  auto status = prestoTask.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::PLANNED);
  EXPECT_EQ(status.queuedPartitionedDrivers, 0);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);

  // Create exec task, but not started yet, so still in planned state.
  prestoTask.task = createExecTask(taskId, prestoTask);
  prestoTask.info.needsPlan = false;
  status = prestoTask.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::PLANNED);
  EXPECT_EQ(status.queuedPartitionedDrivers, 0);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);

  // Add some splits. We should return some splits queued.
  addSplitToTask(prestoTask, sequenceId++);
  addSplitToTask(prestoTask, sequenceId++);
  status = prestoTask.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::PLANNED);
  EXPECT_EQ(status.queuedPartitionedDrivers, 2);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);

  // We 'start' the task, so should be in the running state.
  prestoTask.taskStarted = true;
  status = prestoTask.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::RUNNING);
  EXPECT_EQ(status.queuedPartitionedDrivers, 2);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);

  // Add some splits. We should return more splits queued.
  addSplitToTask(prestoTask, sequenceId++);
  status = prestoTask.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::RUNNING);
  EXPECT_EQ(status.queuedPartitionedDrivers, 3);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);

  // Error the task. Should be no new splits since the last update.
  addSplitToTask(prestoTask, sequenceId++);
  try {
    VELOX_FAIL("Test error");
  } catch (const VeloxException&) {
    prestoTask.error = std::current_exception();
  }
  addSplitToTask(prestoTask, sequenceId++);
  status = prestoTask.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::FAILED);
  EXPECT_EQ(status.queuedPartitionedDrivers, 3);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);

  // Create aborted task and test the state is as expected.
  PrestoTask prestoTask2{taskId, "node2", 0};
  prestoTask2.info.taskStatus.state = protocol::TaskState::ABORTED;
  status = prestoTask2.updateStatus();
  EXPECT_EQ(status.state, protocol::TaskState::ABORTED);
  EXPECT_EQ(status.queuedPartitionedDrivers, 0);
  EXPECT_EQ(status.runningPartitionedDrivers, 0);
}
