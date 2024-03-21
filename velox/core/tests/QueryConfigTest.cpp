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
  std::unordered_map<std::string, std::string> configData;
  auto queryCtx = std::make_shared<QueryCtx>(nullptr, std::move(configData));
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_FALSE(config.isLegacyCast());
}

TEST_F(QueryConfigTest, setConfig) {
  std::string path = "/tmp/setConfig";
  std::unordered_map<std::string, std::string> configData(
      {{QueryConfig::kLegacyCast, "true"}});
  auto queryCtx = std::make_shared<QueryCtx>(nullptr, std::move(configData));
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_TRUE(config.isLegacyCast());
}

TEST_F(QueryConfigTest, memConfig) {
  const std::string tz = "timezone1";
  const std::unordered_map<std::string, std::string> configData(
      {{QueryConfig::kSessionTimezone, tz}});

  {
    MemConfig cfg{configData};
    MemConfig cfg2{};
    auto configDataCopy = configData;
    ASSERT_EQ(
        tz,
        cfg.Config::get<std::string>(QueryConfig::kSessionTimezone).value());
    ASSERT_FALSE(cfg.Config::get<std::string>("missing-entry").has_value());
    ASSERT_EQ(configData, cfg.values());
    ASSERT_EQ(configData, cfg.valuesCopy());
  }

  {
    MemConfigMutable cfg{configData};
    MemConfigMutable cfg2{};
    auto configDataCopy = configData;
    MemConfigMutable cfg3{std::move(configDataCopy)};
    ASSERT_EQ(
        tz,
        cfg.Config::get<std::string>(QueryConfig::kSessionTimezone).value());
    ASSERT_FALSE(cfg.Config::get<std::string>("missing-entry").has_value());
    const std::string tz2 = "timezone2";
    ASSERT_NO_THROW(cfg.setValue(QueryConfig::kSessionTimezone, tz2));
    ASSERT_EQ(
        tz2,
        cfg.Config::get<std::string>(QueryConfig::kSessionTimezone).value());
    ASSERT_THROW(cfg.values(), VeloxException);
    ASSERT_EQ(configData, cfg3.valuesCopy());
  }
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
    auto queryCtx = std::make_shared<QueryCtx>(nullptr, std::move(configData));
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
        std::make_shared<core::QueryCtx>(nullptr, std::move(configData));
    const core::QueryConfig& config = queryCtx->queryConfig();
    ASSERT_EQ(
        config.isExpressionEvaluationCacheEnabled(),
        enableExpressionEvaluationCache);

    auto execCtx = std::make_shared<core::ExecCtx>(pool.get(), queryCtx.get());
    ASSERT_EQ(execCtx->exprEvalCacheEnabled(), enableExpressionEvaluationCache);
    ASSERT_EQ(
        execCtx->vectorPool() != nullptr, enableExpressionEvaluationCache);

    auto evalCtx = std::make_shared<exec::EvalCtx>(execCtx.get());
    ASSERT_EQ(evalCtx->cacheEnabled(), enableExpressionEvaluationCache);

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

TEST_F(QueryConfigTest, capacityConversion) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1);

  std::unordered_map<CapacityUnit, std::string> unitStrLookup{
      {CapacityUnit::BYTE, "B"},
      {CapacityUnit::KILOBYTE, "kB"},
      {CapacityUnit::MEGABYTE, "MB"},
      {CapacityUnit::GIGABYTE, "GB"},
      {CapacityUnit::TERABYTE, "TB"},
      {CapacityUnit::PETABYTE, "PB"}};

  std::vector<std::pair<CapacityUnit, double>> units{
      {CapacityUnit::BYTE, 1},
      {CapacityUnit::KILOBYTE, 1024},
      {CapacityUnit::MEGABYTE, 1024 * 1024},
      {CapacityUnit::GIGABYTE, 1024 * 1024 * 1024},
      {CapacityUnit::TERABYTE, 1024ll * 1024 * 1024 * 1024},
      {CapacityUnit::PETABYTE, 1024ll * 1024 * 1024 * 1024 * 1024}};
  for (int32_t i = 0; i < units.size(); i++) {
    for (int32_t j = 0; j < units.size(); j++) {
      // We use this diffRatio to prevent float conversion overflow when
      // converting from one unit to another.
      uint64_t diffRatio = i < j ? units[j].second / units[i].second
                                 : units[i].second / units[j].second;
      uint64_t randNumber = folly::Random::rand64(rng);
      uint64_t testNumber = i > j ? randNumber / diffRatio : randNumber;
      ASSERT_EQ(
          toCapacity(
              std::string(
                  std::to_string(testNumber) + unitStrLookup[units[i].first]),
              units[j].first),
          (uint64_t)(testNumber * (units[i].second / units[j].second)));
    }
  }
}

TEST_F(QueryConfigTest, durationConversion) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1);

  std::vector<std::pair<std::string, uint64_t>> units{
      {"ns", 1},
      {"us", 1000},
      {"ms", 1000 * 1000},
      {"s", 1000ll * 1000 * 1000},
      {"m", 1000ll * 1000 * 1000 * 60},
      {"h", 1000ll * 1000 * 1000 * 60 * 60},
      {"d", 1000ll * 1000 * 1000 * 60 * 60 * 24}};
  for (uint32_t i = 0; i < units.size(); i++) {
    auto testNumber = folly::Random::rand32(rng) % 10000;
    auto duration =
        toDuration(std::string(std::to_string(testNumber) + units[i].first));
    ASSERT_EQ(
        testNumber * units[i].second,
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
  }
}

} // namespace facebook::velox::core::test
