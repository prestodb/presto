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
#include "presto_cpp/main/QueryContextManager.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/common/Configs.h"

namespace facebook::presto {

class QueryContextManagerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(velox::memory::MemoryManager::Options{});
  }

  void SetUp() override {
    driverExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        4, std::make_shared<folly::NamedThreadFactory>("Driver"));
    httpSrvCpuExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        4, std::make_shared<folly::NamedThreadFactory>("HTTPSrvCpu"));
    spillerExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        4, std::make_shared<folly::NamedThreadFactory>("Spiller"));
    taskManager_ = std::make_unique<TaskManager>(
        driverExecutor_.get(),
        httpSrvCpuExecutor_.get(),
        spillerExecutor_.get());
  }
  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> httpSrvCpuExecutor_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> spillerExecutor_;
  std::unique_ptr<TaskManager> taskManager_;
};

TEST_F(QueryContextManagerTest, nativeSessionProperties) {
  protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::SessionRepresentation session{
      .systemProperties = {
          {"native_max_spill_level", "2"},
          {"native_spill_compression_codec", "NONE"},
          {"native_join_spill_enabled", "false"},
          {"native_spill_write_buffer_size", "1024"},
          {"native_debug_validate_output_from_operators", "true"},
          {"native_debug_disable_expression_with_peeling", "true"},
          {"native_debug_disable_common_sub_expressions", "true"},
          {"native_debug_disable_expression_with_memoization", "true"},
          {"native_debug_disable_expression_with_lazy_inputs", "true"},
          {"native_selective_nimble_reader_enabled", "true"},
          {"preferred_output_batch_bytes", "10485760"},
          {"preferred_output_batch_rows", "1024"},
          {"row_size_tracking_enabled", "true"},
          {"aggregation_spill_all", "true"},
          {"native_expression_max_array_size_in_reduce", "99999"},
          {"native_expression_max_compiled_regexes", "54321"},
          {"request_data_sizes_max_wait_sec", "20"},
      }};
  protocol::TaskUpdateRequest updateRequest;
  updateRequest.session = session;
  auto queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
      taskId, updateRequest);
  EXPECT_EQ(queryCtx->queryConfig().maxSpillLevel(), 2);
  EXPECT_EQ(queryCtx->queryConfig().spillCompressionKind(), "NONE");
  EXPECT_FALSE(queryCtx->queryConfig().joinSpillEnabled());
  EXPECT_TRUE(queryCtx->queryConfig().validateOutputFromOperators());
  EXPECT_TRUE(queryCtx->queryConfig().debugDisableExpressionsWithPeeling());
  EXPECT_TRUE(queryCtx->queryConfig().debugDisableCommonSubExpressions());
  EXPECT_TRUE(queryCtx->queryConfig().debugDisableExpressionsWithMemoization());
  EXPECT_TRUE(queryCtx->queryConfig().debugDisableExpressionsWithLazyInputs());
  EXPECT_TRUE(queryCtx->queryConfig().selectiveNimbleReaderEnabled());
  EXPECT_TRUE(queryCtx->queryConfig().rowSizeTrackingEnabled());
  EXPECT_EQ(queryCtx->queryConfig().preferredOutputBatchBytes(), 10UL << 20);
  EXPECT_EQ(queryCtx->queryConfig().preferredOutputBatchRows(), 1024);
  EXPECT_EQ(queryCtx->queryConfig().spillWriteBufferSize(), 1024);
  EXPECT_EQ(queryCtx->queryConfig().exprMaxArraySizeInReduce(), 99999);
  EXPECT_EQ(queryCtx->queryConfig().exprMaxCompiledRegexes(), 54321);
  EXPECT_EQ(queryCtx->queryConfig().requestDataSizesMaxWaitSec(), 20);
}

TEST_F(QueryContextManagerTest, nativeConnectorSessionProperties) {
  protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::SessionRepresentation session;
  std::map<std::string, std::string> hiveSessions{
      {"native_stats_based_filter_reorder_disabled", "true"},
      {"orc_max_merge_distance", "512kB"}};
  session.catalogProperties.emplace("hive", hiveSessions);
  protocol::TaskUpdateRequest updateRequest;
  updateRequest.session = session;
  auto queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
      taskId, updateRequest);
  EXPECT_EQ(
      queryCtx->connectorSessionProperties().at("hive")->get<std::string>(
          "orc_max_merge_distance"),
      "512kB");
  EXPECT_EQ(
      queryCtx->connectorSessionProperties().at("hive")->get<std::string>(
          "stats_based_filter_reorder_disabled"),
      "true");
}

TEST_F(QueryContextManagerTest, defaultSessionProperties) {
  const std::unordered_map<std::string, std::string> values;
  auto defaultQC = std::make_shared<velox::core::QueryConfig>(values);

  protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::SessionRepresentation session{.systemProperties = {}};
  protocol::TaskUpdateRequest updateRequest;
  updateRequest.session = session;
  auto queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
      taskId, updateRequest);
  const auto& queryConfig = queryCtx->queryConfig();
  EXPECT_EQ(queryConfig.maxSpillLevel(), defaultQC->maxSpillLevel());
  EXPECT_EQ(
      queryConfig.spillCompressionKind(), defaultQC->spillCompressionKind());
  EXPECT_EQ(queryConfig.spillEnabled(), defaultQC->spillEnabled());
  EXPECT_EQ(queryConfig.aggregationSpillEnabled(), defaultQC->aggregationSpillEnabled());
  EXPECT_EQ(queryConfig.joinSpillEnabled(), defaultQC->joinSpillEnabled());
  EXPECT_EQ(queryConfig.orderBySpillEnabled(), defaultQC->orderBySpillEnabled());
  EXPECT_EQ(
      queryConfig.validateOutputFromOperators(),
      defaultQC->validateOutputFromOperators());
  EXPECT_EQ(
      queryConfig.spillWriteBufferSize(), defaultQC->spillWriteBufferSize());
  EXPECT_EQ(
      queryConfig.requestDataSizesMaxWaitSec(), defaultQC->requestDataSizesMaxWaitSec());
  EXPECT_EQ(
      queryConfig.maxSplitPreloadPerDriver(), defaultQC->maxSplitPreloadPerDriver());
  EXPECT_EQ(
      queryConfig.maxLocalExchangePartitionBufferSize(), defaultQC->maxLocalExchangePartitionBufferSize());
}

TEST_F(QueryContextManagerTest, overridingSessionProperties) {
  protocol::TaskId taskId = "scan.0.0.1.0";
  const auto& systemConfig = SystemConfig::instance();
  {
    protocol::SessionRepresentation session{.systemProperties = {}};
    protocol::TaskUpdateRequest updateRequest;
    updateRequest.session = session;
    auto queryCtx =
        taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
            taskId, updateRequest);
    // When session properties are not explicitly set, they should be set to 
    // system config values.
    EXPECT_EQ(
        queryCtx->queryConfig().queryMaxMemoryPerNode(),
        systemConfig->queryMaxMemoryPerNode());
    EXPECT_EQ(
        queryCtx->queryConfig().spillFileCreateConfig(),
        systemConfig->spillerFileCreateConfig());
    EXPECT_EQ(
        queryCtx->queryConfig().spillEnabled(),
        systemConfig->spillEnabled());
    EXPECT_EQ(
        queryCtx->queryConfig().aggregationSpillEnabled(),
        systemConfig->aggregationSpillEnabled());
    EXPECT_EQ(
        queryCtx->queryConfig().joinSpillEnabled(),
        systemConfig->joinSpillEnabled());
    EXPECT_EQ(
        queryCtx->queryConfig().orderBySpillEnabled(),
        systemConfig->orderBySpillEnabled());
    EXPECT_EQ(
        queryCtx->queryConfig().requestDataSizesMaxWaitSec(),
        systemConfig->requestDataSizesMaxWaitSec());
  }
  {
    protocol::SessionRepresentation session{
        .systemProperties = {
            {"query_max_memory_per_node", "1GB"},
            {"spill_file_create_config", "encoding:replica_2"},
            {"spill_enabled", "true"},
            {"aggregation_spill_enabled", "false"},
            {"join_spill_enabled", "true"},
            {"request_data_sizes_max_wait_sec", "12"}}};
    protocol::TaskUpdateRequest updateRequest;
    updateRequest.session = session;
    auto queryCtx =
        taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
            taskId, updateRequest);
    EXPECT_EQ(
        queryCtx->queryConfig().queryMaxMemoryPerNode(),
        1UL * 1024 * 1024 * 1024);
    EXPECT_EQ(
        queryCtx->queryConfig().spillFileCreateConfig(), "encoding:replica_2");
    // Override with different value
    EXPECT_EQ(
        queryCtx->queryConfig().spillEnabled(), true);
    EXPECT_NE(
        queryCtx->queryConfig().spillEnabled(),
        systemConfig->spillEnabled());
    // Override with different value
    EXPECT_EQ(
        queryCtx->queryConfig().aggregationSpillEnabled(), false);
    EXPECT_NE(
        queryCtx->queryConfig().aggregationSpillEnabled(),
        systemConfig->aggregationSpillEnabled());
    // Override with same value
    EXPECT_EQ(
        queryCtx->queryConfig().joinSpillEnabled(), true);
    EXPECT_EQ(
        queryCtx->queryConfig().joinSpillEnabled(),
        systemConfig->joinSpillEnabled());
    // No override
    EXPECT_EQ(
        queryCtx->queryConfig().orderBySpillEnabled(),
        systemConfig->orderBySpillEnabled());
    EXPECT_EQ(queryCtx->queryConfig().requestDataSizesMaxWaitSec(), 12);
  }
}

TEST_F(QueryContextManagerTest, duplicateQueryRootPoolName) {
  const protocol::TaskId fakeTaskId = "scan.0.0.1.0";
  const protocol::SessionRepresentation fakeSession{.systemProperties = {}};
  protocol::TaskUpdateRequest fakeUpdateRequest;
  fakeUpdateRequest.session = fakeSession;
  auto* queryCtxManager = taskManager_->getQueryContextManager();
  struct {
    bool hasPendingReference;
    bool clearCache;
    bool expectedNewPoolName;

    std::string debugString() const {
      return fmt::format(
          "hasPendingReference: {}, clearCache: {}, expectedNewPoolName: {}",
          hasPendingReference,
          clearCache,
          expectedNewPoolName);
    }
  } testSettings[] = {
      {true, true, true},
      {true, false, false},
      {false, true, true},
      {false, false, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    queryCtxManager->testingClearCache();

    auto queryCtx =
        queryCtxManager->findOrCreateQueryCtx(fakeTaskId, fakeUpdateRequest);
    const auto poolName = queryCtx->pool()->name();
    ASSERT_THAT(poolName, testing::HasSubstr("scan_"));
    if (!testData.hasPendingReference) {
      queryCtx.reset();
    }
    if (testData.clearCache) {
      queryCtxManager->testingClearCache();
    }
    auto newQueryCtx =
        queryCtxManager->findOrCreateQueryCtx(fakeTaskId, fakeUpdateRequest);
    const auto newPoolName = newQueryCtx->pool()->name();
    ASSERT_THAT(newPoolName, testing::HasSubstr("scan_"));
    if (testData.expectedNewPoolName) {
      ASSERT_NE(poolName, newPoolName);
    } else {
      ASSERT_EQ(poolName, newPoolName);
    }
  }
}
} // namespace facebook::presto
