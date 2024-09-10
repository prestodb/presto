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
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/EvalCtx.h"

namespace facebook::velox::core::test {

class QueryConfigTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(QueryConfigTest, emptyConfig) {
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{{}});
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_FALSE(config.isLegacyCast());
}

TEST_F(QueryConfigTest, setConfig) {
  std::string path = "/tmp/setConfig";
  std::unordered_map<std::string, std::string> configData(
      {{QueryConfig::kLegacyCast, "true"}});
  auto queryCtx = QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_TRUE(config.isLegacyCast());
}

TEST_F(QueryConfigTest, invalidConfig) {
  std::unordered_map<std::string, std::string> configData(
      {{QueryConfig::kSessionTimezone, "invalid"}});
  VELOX_ASSERT_USER_THROW(
      QueryCtx::create(nullptr, QueryConfig{std::move(configData)}),
      "session 'session_timezone' set with invalid value 'invalid'");
}

TEST_F(QueryConfigTest, taskWriterCountConfig) {
  struct {
    std::optional<int> numWriterCounter;
    std::optional<int> numPartitionedWriterCounter;
    int expectedWriterCounter;
    int expectedPartitionedWriterCounter;

    std::string debugString() const {
      return fmt::format(
          "numWriterCounter[{}] numPartitionedWriterCounter[{}] expectedWriterCounter[{}] expectedPartitionedWriterCounter[{}]",
          numWriterCounter.value_or(0),
          numPartitionedWriterCounter.value_or(0),
          expectedWriterCounter,
          expectedPartitionedWriterCounter);
    }
  } testSettings[] = {
      {std::nullopt, std::nullopt, 4, 4},
      {std::nullopt, 1, 4, 1},
      {std::nullopt, 6, 4, 6},
      {2, 4, 2, 4},
      {4, 2, 4, 2},
      {4, 6, 4, 6},
      {6, 5, 6, 5},
      {6, 4, 6, 4},
      {6, std::nullopt, 6, 6}};
  for (const auto& testConfig : testSettings) {
    SCOPED_TRACE(testConfig.debugString());
    std::unordered_map<std::string, std::string> configData;
    if (testConfig.numWriterCounter.has_value()) {
      configData.emplace(
          QueryConfig::kTaskWriterCount,
          std::to_string(testConfig.numWriterCounter.value()));
    }
    if (testConfig.numPartitionedWriterCounter.has_value()) {
      configData.emplace(
          QueryConfig::kTaskPartitionedWriterCount,
          std::to_string(testConfig.numPartitionedWriterCounter.value()));
    }
    auto queryCtx =
        QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
    const QueryConfig& config = queryCtx->queryConfig();
    ASSERT_EQ(config.taskWriterCount(), testConfig.expectedWriterCounter);
    ASSERT_EQ(
        config.taskPartitionedWriterCount(),
        testConfig.expectedPartitionedWriterCounter);
  }
}

TEST_F(QueryConfigTest, enableExpressionEvaluationCacheConfig) {
  std::shared_ptr<memory::MemoryPool> rootPool{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

  auto testConfig = [&](bool enableExpressionEvaluationCache) {
    std::unordered_map<std::string, std::string> configData(
        {{core::QueryConfig::kEnableExpressionEvaluationCache,
          enableExpressionEvaluationCache ? "true" : "false"}});
    auto queryCtx =
        core::QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
    const core::QueryConfig& config = queryCtx->queryConfig();
    ASSERT_EQ(
        config.isExpressionEvaluationCacheEnabled(),
        enableExpressionEvaluationCache);

    auto execCtx = std::make_shared<core::ExecCtx>(pool.get(), queryCtx.get());
    ASSERT_EQ(
        execCtx->optimizationParams().exprEvalCacheEnabled,
        enableExpressionEvaluationCache);
    ASSERT_EQ(
        execCtx->vectorPool() != nullptr, enableExpressionEvaluationCache);

    auto evalCtx = std::make_shared<exec::EvalCtx>(execCtx.get());
    ASSERT_EQ(
        evalCtx->dictionaryMemoizationEnabled(),
        enableExpressionEvaluationCache);

    // Test ExecCtx::selectivityVectorPool_.
    auto rows = execCtx->getSelectivityVector(100);
    ASSERT_NE(rows, nullptr);
    ASSERT_EQ(
        execCtx->releaseSelectivityVector(std::move(rows)),
        enableExpressionEvaluationCache);

    // Test ExecCtx::decodedVectorPool_.
    auto decoded = execCtx->getDecodedVector();
    ASSERT_NE(decoded, nullptr);
    ASSERT_EQ(
        execCtx->releaseDecodedVector(std::move(decoded)),
        enableExpressionEvaluationCache);
  };

  testConfig(true);
  testConfig(false);
}

TEST_F(QueryConfigTest, expressionEvaluationRelatedConfigs) {
  // Verify that the expression evaluation related configs are porpogated
  // correctly to ExprCtx which is used during expression evaluation. Each
  // config is individually set and verified.
  std::shared_ptr<memory::MemoryPool> rootPool{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

  auto testConfig =
      [&](std::unordered_map<std::string, std::string> configData) {
        auto queryCtx =
            core::QueryCtx::create(nullptr, QueryConfig{std::move(configData)});
        const auto& queryConfig = queryCtx->queryConfig();
        auto execCtx =
            std::make_shared<core::ExecCtx>(pool.get(), queryCtx.get());
        auto evalCtx = std::make_shared<exec::EvalCtx>(execCtx.get());

        ASSERT_EQ(
            evalCtx->peelingEnabled(),
            !queryConfig.debugDisableExpressionsWithPeeling());
        ASSERT_EQ(
            evalCtx->sharedSubExpressionReuseEnabled(),
            !queryConfig.debugDisableCommonSubExpressions());
        ASSERT_EQ(
            evalCtx->dictionaryMemoizationEnabled(),
            !queryConfig.debugDisableExpressionsWithMemoization());
        ASSERT_EQ(
            evalCtx->deferredLazyLoadingEnabled(),
            !queryConfig.debugDisableExpressionsWithLazyInputs());
      };

  auto createConfig = [&](bool debugDisableExpressionsWithPeeling,
                          bool debugDisableCommonSubExpressions,
                          bool debugDisableExpressionsWithMemoization,
                          bool debugDisableExpressionsWithLazyInputs) -> auto {
    std::unordered_map<std::string, std::string> configData(
        {{core::QueryConfig::kDebugDisableExpressionWithPeeling,
          std::to_string(debugDisableExpressionsWithPeeling)},
         {core::QueryConfig::kDebugDisableCommonSubExpressions,
          std::to_string(debugDisableCommonSubExpressions)},
         {core::QueryConfig::kDebugDisableExpressionWithMemoization,
          std::to_string(debugDisableExpressionsWithMemoization)},
         {core::QueryConfig::kDebugDisableExpressionWithLazyInputs,
          std::to_string(debugDisableExpressionsWithLazyInputs)}});
    return configData;
  };

  testConfig({}); // Verify default config.
  testConfig(createConfig(true, false, false, false));
  testConfig(createConfig(false, true, false, false));
  testConfig(createConfig(false, false, true, false));
  testConfig(createConfig(false, false, false, true));
}

} // namespace facebook::velox::core::test
