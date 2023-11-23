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

namespace facebook::presto {

class QueryContextManagerTest : public testing::Test {
 protected:
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
          {"native_max_spill_level", "1"},
          {"native_spill_compression_codec", "NONE"},
          {"native_join_spill_enabled", "false"},
          {"native_spill_write_buffer_size", "1024"},
          {"native_debug_validate_output_from_operators", "true"},
          {"aggregation_spill_all", "true"}}};
  auto queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
      taskId, session);
  EXPECT_EQ(queryCtx->queryConfig().maxSpillLevel(), 1);
  EXPECT_EQ(queryCtx->queryConfig().spillCompressionKind(), "NONE");
  EXPECT_FALSE(queryCtx->queryConfig().joinSpillEnabled());
  EXPECT_TRUE(queryCtx->queryConfig().validateOutputFromOperators());
  EXPECT_EQ(queryCtx->queryConfig().spillWriteBufferSize(), 1024);
}

TEST_F(QueryContextManagerTest, defaultSessionProperties) {
  protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::SessionRepresentation session{.systemProperties = {}};
  auto queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
      taskId, session);
  EXPECT_EQ(queryCtx->queryConfig().maxSpillLevel(), 4);
  EXPECT_EQ(queryCtx->queryConfig().spillCompressionKind(), "none");
  EXPECT_TRUE(queryCtx->queryConfig().joinSpillEnabled());
  EXPECT_FALSE(queryCtx->queryConfig().validateOutputFromOperators());
  EXPECT_EQ(queryCtx->queryConfig().spillWriteBufferSize(), 1L << 20);
}

TEST_F(QueryContextManagerTest, duplicateQueryRootPoolName) {
  const protocol::TaskId fakeTaskId = "scan.0.0.1.0";
  const protocol::SessionRepresentation fakeSession{.systemProperties = {}};
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
        queryCtxManager->findOrCreateQueryCtx(fakeTaskId, fakeSession);
    const auto poolName = queryCtx->pool()->name();
    ASSERT_THAT(poolName, testing::HasSubstr("scan_"));
    if (!testData.hasPendingReference) {
      queryCtx.reset();
    }
    if (testData.clearCache) {
      queryCtxManager->testingClearCache();
    }
    auto newQueryCtx =
        queryCtxManager->findOrCreateQueryCtx(fakeTaskId, fakeSession);
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
