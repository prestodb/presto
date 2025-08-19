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

#include <gtest/gtest.h>
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/presto_protocol/core/Duration.h"
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"

using namespace facebook::presto;

class TaskInfoTest : public ::testing::Test {
  protected:
  void SetUp() override {
    registerPrestoToVeloxConnector(
        std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  }

  void TearDown() override {
    unregisterPrestoToVeloxConnector("hive");
  }
};

const std::string BASE_DATA_PATH = "/github/presto-trunk/presto-native-execution/presto_cpp/main/tests/data/";

TEST_F(TaskInfoTest, duration) {
  double thrift = 0;
  thrift::toThrift(protocol::Duration(123, protocol::TimeUnit::MILLISECONDS), thrift);
  ASSERT_EQ(thrift, 123);
}

TEST_F(TaskInfoTest, taskInfo) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "TaskInfo.json"));
  json j = json::parse(str);
  protocol::TaskInfo taskInfo = j;
  thrift::TaskInfo thriftTaskInfo;
  thrift::toThrift(taskInfo, thriftTaskInfo);

  ASSERT_EQ(thriftTaskInfo.needsPlan(), false);
  ASSERT_EQ(thriftTaskInfo.outputBuffers()->buffers()->size(), 2);
  ASSERT_EQ(thriftTaskInfo.outputBuffers()->buffers()[0].bufferId()->id(), 100);
  ASSERT_EQ(thriftTaskInfo.outputBuffers()->buffers()[1].bufferId()->id(), 200);
  ASSERT_EQ(thriftTaskInfo.stats()->blockedReasons()->count(thrift::BlockedReason::WAITING_FOR_MEMORY), 1);
  ASSERT_EQ(thriftTaskInfo.stats()->runtimeStats()->metrics()->size(), 2);
  ASSERT_EQ(thriftTaskInfo.stats()->runtimeStats()->metrics()["test_metric1"].sum(), 123);
  ASSERT_EQ(thriftTaskInfo.stats()->runtimeStats()->metrics()["test_metric2"].name(), "test_metric2");
}

TEST_F(TaskInfoTest, taskId) {
  protocol::TaskId taskId = "queryId.1.2.3.4";
  thrift::TaskId thriftTaskId;
  thrift::toThrift(taskId, thriftTaskId);
  
  ASSERT_EQ(thriftTaskId.stageExecutionId()->stageId()->queryId(), "queryId");
  ASSERT_EQ(thriftTaskId.stageExecutionId()->stageId()->id(), 1);
  ASSERT_EQ(thriftTaskId.stageExecutionId()->id(), 2);
  ASSERT_EQ(thriftTaskId.id(), 3);
  ASSERT_EQ(thriftTaskId.attemptNumber(), 4);
}


TEST_F(TaskInfoTest, operatorStatsEmptyBlockedReason) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "OperatorStatsEmptyBlockedReason.json"));
  json j = json::parse(str);
  protocol::OperatorStats operatorStats = j;
  thrift::OperatorStats thriftOperatorStats;
  thrift::toThrift(operatorStats, thriftOperatorStats);
  
  ASSERT_EQ(thriftOperatorStats.blockedReason().has_value(), false);
  ASSERT_EQ(thriftOperatorStats.blockedWall(), 80);
  ASSERT_EQ(thriftOperatorStats.finishCpu(), 1000);
}

TEST_F(TaskInfoTest, operatorStats) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "OperatorStats.json"));
  json j = json::parse(str);
  protocol::OperatorStats operatorStats = j;
  thrift::OperatorStats thriftOperatorStats;
  thrift::toThrift(operatorStats, thriftOperatorStats);
  
  ASSERT_EQ(thriftOperatorStats.blockedReason(), thrift::BlockedReason::WAITING_FOR_MEMORY);
}
