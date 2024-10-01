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
#include "presto_cpp/main/ServerOperation.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/PrestoServerOperations.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

DECLARE_bool(velox_memory_leak_check_enabled);

using namespace facebook::velox;

namespace facebook::presto {

class ServerOperationTest : public testing::Test {
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(ServerOperationTest, targetActionLookup) {
  {
    // Targets lookup verification
    EXPECT_EQ(
        ServerOperation::kTargetLookup.size(),
        ServerOperation::kReverseTargetLookup.size());
    const auto endReverseIt = ServerOperation::kReverseTargetLookup.end();
    for (auto lookupIt = ServerOperation::kTargetLookup.begin();
         lookupIt != ServerOperation::kTargetLookup.end();
         lookupIt++) {
      EXPECT_NE(
          ServerOperation::kReverseTargetLookup.find(lookupIt->second),
          endReverseIt);
    }
    const auto endLookupIt = ServerOperation::kTargetLookup.end();
    for (auto reverseIt = ServerOperation::kReverseTargetLookup.begin();
         reverseIt != ServerOperation::kReverseTargetLookup.end();
         reverseIt++) {
      EXPECT_NE(
          ServerOperation::kTargetLookup.find(reverseIt->second), endLookupIt);
    }
  }

  {
    // Actions lookup verification
    EXPECT_EQ(
        ServerOperation::kActionLookup.size(),
        ServerOperation::kReverseActionLookup.size());
    const auto endReverseIt = ServerOperation::kReverseActionLookup.end();
    for (auto lookupIt = ServerOperation::kActionLookup.begin();
         lookupIt != ServerOperation::kActionLookup.end();
         lookupIt++) {
      EXPECT_NE(
          ServerOperation::kReverseActionLookup.find(lookupIt->second),
          endReverseIt);
    }
    const auto endLookupIt = ServerOperation::kActionLookup.end();
    for (auto reverseIt = ServerOperation::kReverseActionLookup.begin();
         reverseIt != ServerOperation::kReverseActionLookup.end();
         reverseIt++) {
      EXPECT_NE(
          ServerOperation::kActionLookup.find(reverseIt->second), endLookupIt);
    }
  }
}

TEST_F(ServerOperationTest, stringEnumConversion) {
  for (auto lookupIt = ServerOperation::kTargetLookup.begin();
       lookupIt != ServerOperation::kTargetLookup.end();
       lookupIt++) {
    EXPECT_EQ(
        ServerOperation::targetFromString(lookupIt->first), lookupIt->second);
    EXPECT_EQ(ServerOperation::targetString(lookupIt->second), lookupIt->first);
  }
  EXPECT_THROW(
      ServerOperation::targetFromString("UNKNOWN_TARGET"),
      velox::VeloxUserError);

  for (auto lookupIt = ServerOperation::kActionLookup.begin();
       lookupIt != ServerOperation::kActionLookup.end();
       lookupIt++) {
    EXPECT_EQ(
        ServerOperation::actionFromString(lookupIt->first), lookupIt->second);
    EXPECT_EQ(ServerOperation::actionString(lookupIt->second), lookupIt->first);
  }
  EXPECT_THROW(
      ServerOperation::actionFromString("UNKNOWN_ACTION"),
      velox::VeloxUserError);
}

TEST_F(ServerOperationTest, buildServerOp) {
  ServerOperation op;
  op = buildServerOpFromHttpMsgPath("/v1/operation/connector/clearCache");
  EXPECT_EQ(ServerOperation::Target::kConnector, op.target);
  EXPECT_EQ(ServerOperation::Action::kClearCache, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/connector/getCacheStats");
  EXPECT_EQ(ServerOperation::Target::kConnector, op.target);
  EXPECT_EQ(ServerOperation::Action::kGetCacheStats, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/systemConfig/setProperty");
  EXPECT_EQ(ServerOperation::Target::kSystemConfig, op.target);
  EXPECT_EQ(ServerOperation::Action::kSetProperty, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/task/getDetail");
  EXPECT_EQ(ServerOperation::Target::kTask, op.target);
  EXPECT_EQ(ServerOperation::Action::kGetDetail, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/task/listAll");
  EXPECT_EQ(ServerOperation::Target::kTask, op.target);
  EXPECT_EQ(ServerOperation::Action::kListAll, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/server/trace");
  EXPECT_EQ(ServerOperation::Target::kServer, op.target);
  EXPECT_EQ(ServerOperation::Action::kTrace, op.action);

  EXPECT_THROW(
      op = buildServerOpFromHttpMsgPath("/v1/operation/whatzit/setProperty"),
      velox::VeloxUserError);
}

TEST_F(ServerOperationTest, taskEndpoint) {
  // Setup environment for TaskManager
  if (!connector::hasConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)) {
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
  }
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              "test-hive",
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);

  const auto driverExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
      4, std::make_shared<folly::NamedThreadFactory>("Driver"));
  const auto httpSrvCpuExecutor =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          4, std::make_shared<folly::NamedThreadFactory>("HTTPSrvCpu"));
  auto taskManager = std::make_unique<TaskManager>(
      driverExecutor.get(), httpSrvCpuExecutor.get(), nullptr);

  const std::function<void(const std::string& taskId)> addDummyTask =
      [taskManager = taskManager.get()](const std::string& taskId) {
        protocol::TaskUpdateRequest updateRequest;
        auto planFragment =
            exec::test::PlanBuilder()
                .tableScan(ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}))
                .partitionedOutput({}, 1, {"c0", "c1"})
                .planFragment();

        taskManager->createOrUpdateTask(
            taskId,
            {},
            planFragment,
            taskManager->getQueryContextManager()->findOrCreateQueryCtx(
                taskId, updateRequest.session),
            0);
      };
  std::vector<std::string> taskIds = {"task_0.0.0.0.0", "task_1.0.0.0.0"};
  for (const auto& taskId : taskIds) {
    addDummyTask(taskId);
  }
  EXPECT_EQ(2, taskManager->tasks().size());

  // Test body
  PrestoServerOperations serverOperation(taskManager.get(), nullptr);

  proxygen::HTTPMessage httpMessage;
  auto listAllResponse = serverOperation.taskOperation(
      {.target = ServerOperation::Target::kTask,
       .action = ServerOperation::Action::kListAll},
      &httpMessage);
  nlohmann::json j = nlohmann::json::parse(listAllResponse);
  EXPECT_EQ(2, j.size());

  httpMessage.setQueryParam("limit", "1");
  listAllResponse = serverOperation.taskOperation(
      {.target = ServerOperation::Target::kTask,
       .action = ServerOperation::Action::kListAll},
      &httpMessage);
  EXPECT_EQ(listAllResponse.substr(0, 19), "Showing 1/2 tasks:\n");
  j = nlohmann::json::parse(listAllResponse.substr(19));
  EXPECT_EQ(1, j.size());

  httpMessage.setQueryParam("limit", "abc");
  VELOX_ASSERT_THROW(
      serverOperation.taskOperation(
          {.target = ServerOperation::Target::kTask,
           .action = ServerOperation::Action::kListAll},
          &httpMessage),
      "Invalid limit provided 'abc'.");

  // Cleanup and shutdown
  for (const auto& taskId : taskIds) {
    taskManager->deleteTask(taskId, true);
  }
  taskManager->shutdown();
  connector::unregisterConnector("test-hive");
  driverExecutor->join();
  httpSrvCpuExecutor->join();
}

TEST_F(ServerOperationTest, systemConfigEndpoint) {
  PrestoServerOperations serverOperation(nullptr, nullptr);
  proxygen::HTTPMessage httpMessage;
  httpMessage.setQueryParam("name", "foo");
  VELOX_ASSERT_THROW(
      serverOperation.systemConfigOperation(
          {.target = ServerOperation::Target::kSystemConfig,
           .action = ServerOperation::Action::kGetProperty},
          &httpMessage),
      "Could not find property 'foo'\n");

  httpMessage.setQueryParam("name", "task.max-drivers-per-task");
  auto getPropertyResponse = serverOperation.systemConfigOperation(
      {.target = ServerOperation::Target::kSystemConfig,
       .action = ServerOperation::Action::kGetProperty},
      &httpMessage);
  EXPECT_EQ(getPropertyResponse, "16\n");
}

} // namespace facebook::presto