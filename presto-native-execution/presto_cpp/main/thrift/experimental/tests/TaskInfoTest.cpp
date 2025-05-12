// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "presto_cpp/main/thrift/ThriftUtils.h"
#include "presto_cpp/presto_protocol/core/Duration.h"
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"

using namespace facebook;
using namespace facebook::presto::protocol;

class TaskInfoTest : public ::testing::Test {};

TEST_F(TaskInfoTest, duration) {
  std::unique_ptr<double> thrift = std::make_unique<double>();
  cpp2::toThrift(Duration(123, TimeUnit::MILLISECONDS), *thrift);
  ASSERT_EQ(*thrift, 123);
}

TEST_F(TaskInfoTest, binaryMetadataUpdates) {
  std::string str = slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/main/thrift/tests/data/", "MetadataUpdates.json"));
  json j = json::parse(str);
  registerPrestoToVeloxConnector(std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  MetadataUpdates metadataUpdates = j;
  std::unique_ptr<std::string> thriftMetadataUpdates = std::make_unique<std::string>();
  cpp2::toThrift(metadataUpdates, *thriftMetadataUpdates);

  json thriftJson = json::parse(*thriftMetadataUpdates);
  ASSERT_EQ(j, thriftJson);

  presto::unregisterPrestoToVeloxConnector("hive");
}

TEST_F(TaskInfoTest, taskInfo) {
  std::string str = slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/main/thrift/tests/data/", "TaskInfo.json"));
  json j = json::parse(str);
  registerPrestoToVeloxConnector(std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  TaskInfo taskInfo = j;
  cpp2::TaskInfo thriftTaskInfo;
  toThrift(taskInfo, thriftTaskInfo);
  
  json thriftJson = json::parse(*thriftTaskInfo.metadataUpdates()->metadataUpdates());
  ASSERT_EQ(taskInfo.metadataUpdates, thriftJson);
  ASSERT_EQ(thriftTaskInfo.needsPlan(), false);
  ASSERT_EQ(thriftTaskInfo.outputBuffers()->buffers()->size(), 2);
  ASSERT_EQ(thriftTaskInfo.outputBuffers()->buffers()[0].bufferId()->id(), 100);
  ASSERT_EQ(thriftTaskInfo.outputBuffers()->buffers()[1].bufferId()->id(), 200);
  ASSERT_EQ(thriftTaskInfo.stats()->blockedReasons()->count(cpp2::BlockedReason::WAITING_FOR_MEMORY), 1);
  ASSERT_EQ(thriftTaskInfo.stats()->runtimeStats()->metrics()->size(), 2);
  ASSERT_EQ(thriftTaskInfo.stats()->runtimeStats()->metrics()["test_metric1"].sum(), 123);
  ASSERT_EQ(thriftTaskInfo.stats()->runtimeStats()->metrics()["test_metric2"].name(), "test_metric2");

  presto::unregisterPrestoToVeloxConnector("hive");
}

TEST_F(TaskInfoTest, taskId) {
  TaskId taskId = "queryId.1.2.3.4";
  cpp2::TaskId thriftTaskId;
  toThrift(taskId, thriftTaskId);
  
  ASSERT_EQ(thriftTaskId.stageExecutionId()->stageId()->queryId(), "queryId");
  ASSERT_EQ(thriftTaskId.stageExecutionId()->stageId()->id(), 1);
  ASSERT_EQ(thriftTaskId.stageExecutionId()->id(), 2);
  ASSERT_EQ(thriftTaskId.id(), 3);
  ASSERT_EQ(thriftTaskId.attemptNumber(), 4);
}


TEST_F(TaskInfoTest, operatorStatsEmptyBlockedReason) {
  std::string str = slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/main/thrift/tests/data/", "OperatorStatsEmptyBlockedReason.json"));
  json j = json::parse(str);
  OperatorStats operatorStats = j;
  cpp2::OperatorStats thriftOperatorStats;
  toThrift(operatorStats, thriftOperatorStats);
  
  ASSERT_EQ(thriftOperatorStats.blockedReason().has_value(), false);
  ASSERT_EQ(thriftOperatorStats.blockedWall(), 80);
  ASSERT_EQ(thriftOperatorStats.finishCpu(), 1000);
}

TEST_F(TaskInfoTest, operatorStats) {
  std::string str = slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/main/thrift/tests/data/", "OperatorStats.json"));
  json j = json::parse(str);
  OperatorStats operatorStats = j;
  cpp2::OperatorStats thriftOperatorStats;
  toThrift(operatorStats, thriftOperatorStats);
  
  ASSERT_EQ(thriftOperatorStats.blockedReason(), cpp2::BlockedReason::WAITING_FOR_MEMORY);
}
