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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <memory>

#include "velox/common/file/FileSystems.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/trace/QueryDataReader.h"
#include "velox/exec/trace/QueryDataWriter.h"
#include "velox/exec/trace/QueryMetadataReader.h"
#include "velox/exec/trace/QueryMetadataWriter.h"
#include "velox/exec/trace/QueryTraceUtil.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
class QueryTracerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    HiveConnectorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  static VectorFuzzer::Options getFuzzerOptions() {
    return VectorFuzzer::Options{
        .vectorSize = 16,
        .nullRatio = 0.2,
        .stringLength = 1024,
        .stringVariableLength = false,
        .allowLazyVector = false,
    };
  }

  QueryTracerTest() : vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
    filesystems::registerLocalFileSystem();
  }

  RowTypePtr generateTypes(size_t numColumns) {
    std::vector<std::string> names;
    names.reserve(numColumns);
    std::vector<TypePtr> types;
    types.reserve(numColumns);
    for (auto i = 0; i < numColumns; ++i) {
      names.push_back(fmt::format("c{}", i));
      types.push_back(vectorFuzzer_.randType((2)));
    }
    return ROW(std::move(names), std::move(types));
    ;
  }

  bool isSamePlan(
      const core::PlanNodePtr& left,
      const core::PlanNodePtr& right) {
    if (left->id() != right->id() || left->name() != right->name()) {
      return false;
    }

    if (left->sources().size() != right->sources().size()) {
      return false;
    }

    for (auto i = 0; i < left->sources().size(); ++i) {
      isSamePlan(left->sources().at(i), right->sources().at(i));
    }
    return true;
  }

  VectorFuzzer vectorFuzzer_;
};

TEST_F(QueryTracerTest, traceData) {
  const auto rowType = generateTypes(5);
  std::vector<RowVectorPtr> inputVectors;
  constexpr auto numBatch = 3;
  inputVectors.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    inputVectors.push_back(vectorFuzzer_.fuzzInputRow(rowType));
  }

  const auto outputDir = TempDirectoryPath::create();
  auto writer = trace::QueryDataWriter(outputDir->getPath(), pool());
  for (auto i = 0; i < numBatch; ++i) {
    writer.write(inputVectors[i]);
  }
  writer.finish();

  const auto reader = trace::QueryDataReader(outputDir->getPath(), pool());
  RowVectorPtr actual;
  size_t numOutputVectors{0};
  while (reader.read(actual)) {
    const auto expected = inputVectors[numOutputVectors];
    const auto size = actual->size();
    ASSERT_EQ(size, expected->size());
    for (auto i = 0; i < size; ++i) {
      actual->compare(expected.get(), i, i, {.nullsFirst = true});
    }
    ++numOutputVectors;
  }
  ASSERT_EQ(numOutputVectors, inputVectors.size());
}

TEST_F(QueryTracerTest, traceMetadata) {
  const auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()});
  std::vector<RowVectorPtr> rows;
  constexpr auto numBatch = 1;
  rows.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    rows.push_back(vectorFuzzer_.fuzzRow(rowType, 2));
  }

  const auto outputDir = TempDirectoryPath::create();
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto planNode =
      PlanBuilder(planNodeIdGenerator)
          .values(rows, false)
          .project({"c0", "c1", "c2"})
          .hashJoin(
              {"c0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator)
                  .values(rows, true)
                  .singleAggregation({"c0", "c1"}, {"min(c2)"})
                  .project({"c0 AS u0", "c1 AS u1", "a0 AS u2"})
                  .planNode(),
              "c0 < 135",
              {"c0", "c1", "c2"},
              core::JoinType::kInner)
          .planNode();
  const auto expectedQueryConfigs =
      std::unordered_map<std::string, std::string>{
          {core::QueryConfig::kSpillEnabled, "true"},
          {core::QueryConfig::kSpillNumPartitionBits, "17"},
          {"key1", "value1"},
      };
  const auto expectedConnectorProperties =
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{
          {"test_trace",
           std::make_shared<config::ConfigBase>(
               std::unordered_map<std::string, std::string>{
                   {"cKey1", "cVal1"}})}};
  const auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig(expectedQueryConfigs),
      expectedConnectorProperties);
  auto writer = trace::QueryMetadataWriter(outputDir->getPath(), pool());
  writer.write(queryCtx, planNode);

  std::unordered_map<std::string, std::string> acutalQueryConfigs;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      actualConnectorProperties;
  core::PlanNodePtr actualQueryPlan;
  auto reader = trace::QueryMetadataReader(outputDir->getPath(), pool());
  reader.read(acutalQueryConfigs, actualConnectorProperties, actualQueryPlan);

  ASSERT_TRUE(isSamePlan(actualQueryPlan, planNode));
  ASSERT_EQ(acutalQueryConfigs.size(), expectedQueryConfigs.size());
  for (const auto& [key, value] : acutalQueryConfigs) {
    ASSERT_EQ(acutalQueryConfigs.at(key), expectedQueryConfigs.at(key));
  }

  ASSERT_EQ(
      actualConnectorProperties.size(), expectedConnectorProperties.size());
  ASSERT_EQ(actualConnectorProperties.count("test_trace"), 1);
  const auto expectedConnectorConfigs =
      expectedConnectorProperties.at("test_trace")->rawConfigsCopy();
  const auto actualConnectorConfigs =
      actualConnectorProperties.at("test_trace");
  for (const auto& [key, value] : actualConnectorConfigs) {
    ASSERT_EQ(actualConnectorConfigs.at(key), expectedConnectorConfigs.at(key));
  }
}

TEST_F(QueryTracerTest, task) {
  const auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()});
  std::vector<RowVectorPtr> rows;
  constexpr auto numBatch = 1;
  rows.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    rows.push_back(vectorFuzzer_.fuzzRow(rowType, 2));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto planNode =
      PlanBuilder(planNodeIdGenerator)
          .values(rows, false)
          .project({"c0", "c1", "c2"})
          .hashJoin(
              {"c0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator)
                  .values(rows, true)
                  .singleAggregation({"c0", "c1"}, {"min(c2)"})
                  .project({"c0 AS u0", "c1 AS u1", "a0 AS u2"})
                  .planNode(),
              "c0 < 135",
              {"c0", "c1", "c2"},
              core::JoinType::kInner)
          .planNode();
  const auto expectedResult =
      AssertQueryBuilder(planNode).maxDrivers(1).copyResults(pool());

  for (const auto& queryTraceNodeIds : {"1,2", ""}) {
    const auto outputDir = TempDirectoryPath::create();
    const auto expectedQueryConfigs =
        std::unordered_map<std::string, std::string>{
            {core::QueryConfig::kSpillEnabled, "true"},
            {core::QueryConfig::kSpillNumPartitionBits, "17"},
            {core::QueryConfig::kQueryTraceEnabled, "true"},
            {core::QueryConfig::kQueryTraceDir, outputDir->getPath()},
            {core::QueryConfig::kQueryTraceEnabled, queryTraceNodeIds},
            {"key1", "value1"},
        };
    const auto expectedConnectorProperties =
        std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{
            {"test_trace",
             std::make_shared<config::ConfigBase>(
                 std::unordered_map<std::string, std::string>{
                     {"cKey1", "cVal1"}})}};
    const auto queryCtx = core::QueryCtx::create(
        executor_.get(),
        core::QueryConfig(expectedQueryConfigs),
        expectedConnectorProperties);

    std::shared_ptr<Task> task;
    const auto result = AssertQueryBuilder(planNode)
                            .queryCtx(queryCtx)
                            .maxDrivers(1)
                            .copyResults(pool(), task);
    assertEqualResults({result}, {expectedResult});

    const auto expectedDir =
        fmt::format("{}/{}", outputDir->getPath(), task->taskId());
    const auto fs = filesystems::getFileSystem(expectedDir, nullptr);
    const auto actaulDirs = fs->list(outputDir->getPath());
    ASSERT_EQ(actaulDirs.size(), 1);
    ASSERT_EQ(actaulDirs.at(0), expectedDir);

    std::unordered_map<std::string, std::string> acutalQueryConfigs;
    std::
        unordered_map<std::string, std::unordered_map<std::string, std::string>>
            actualConnectorProperties;
    core::PlanNodePtr actualQueryPlan;
    auto reader = trace::QueryMetadataReader(expectedDir, pool());
    reader.read(acutalQueryConfigs, actualConnectorProperties, actualQueryPlan);

    ASSERT_TRUE(isSamePlan(actualQueryPlan, planNode));
    ASSERT_EQ(acutalQueryConfigs.size(), expectedQueryConfigs.size());
    for (const auto& [key, value] : acutalQueryConfigs) {
      ASSERT_EQ(acutalQueryConfigs.at(key), expectedQueryConfigs.at(key));
    }

    ASSERT_EQ(
        actualConnectorProperties.size(), expectedConnectorProperties.size());
    ASSERT_EQ(actualConnectorProperties.count("test_trace"), 1);
    const auto expectedConnectorConfigs =
        expectedConnectorProperties.at("test_trace")->rawConfigsCopy();
    const auto actualConnectorConfigs =
        actualConnectorProperties.at("test_trace");
    for (const auto& [key, value] : actualConnectorConfigs) {
      ASSERT_EQ(
          actualConnectorConfigs.at(key), expectedConnectorConfigs.at(key));
    }
  }
}

TEST_F(QueryTracerTest, error) {
  const auto planNode = PlanBuilder().values({}).planNode();
  const auto expectedQueryConfigs =
      std::unordered_map<std::string, std::string>{
          {core::QueryConfig::kSpillEnabled, "true"},
          {core::QueryConfig::kSpillNumPartitionBits, "17"},
          {core::QueryConfig::kQueryTraceEnabled, "true"},
      };
  const auto queryCtx = core::QueryCtx::create(
      executor_.get(), core::QueryConfig(expectedQueryConfigs));
  VELOX_ASSERT_USER_THROW(
      AssertQueryBuilder(planNode).queryCtx(queryCtx).maxDrivers(1).copyResults(
          pool()),
      "Query trace enabled but the trace dir is not set");
}

TEST_F(QueryTracerTest, traceDir) {
  const auto outputDir = TempDirectoryPath::create();
  const auto rootDir = outputDir->getPath();
  const auto fs = filesystems::getFileSystem(rootDir, nullptr);
  auto dir1 = fmt::format("{}/{}", outputDir->getPath(), "t1");
  trace::createTraceDirectory(dir1);
  ASSERT_TRUE(fs->exists(dir1));

  auto dir2 = fmt::format("{}/{}", dir1, "t1_1");
  trace::createTraceDirectory(dir2);
  ASSERT_TRUE(fs->exists(dir2));

  // It will remove the old dir1 along with its subdir when created the dir1
  // again.
  trace::createTraceDirectory(dir1);
  ASSERT_TRUE(fs->exists(dir1));
  ASSERT_FALSE(fs->exists(dir2));

  const auto parentDir = fmt::format("{}/{}", outputDir->getPath(), "p");
  fs->mkdir(parentDir);

  constexpr auto numThreads = 5;
  std::vector<std::thread> traceThreads;
  traceThreads.reserve(numThreads);
  std::mutex mutex;
  std::set<std::string> expectedDirs;
  for (int i = 0; i < numThreads; ++i) {
    traceThreads.emplace_back([&, i]() {
      const auto dir = fmt::format("{}/s{}", parentDir, i);
      trace::createTraceDirectory(dir);
      std::lock_guard<std::mutex> l(mutex);
      expectedDirs.insert(dir);
    });
  }

  for (auto& traceThread : traceThreads) {
    traceThread.join();
  }

  const auto actualDirs = fs->list(parentDir);
  ASSERT_EQ(actualDirs.size(), numThreads);
  ASSERT_EQ(actualDirs.size(), expectedDirs.size());
  for (const auto& dir : actualDirs) {
    ASSERT_EQ(expectedDirs.count(dir), 1);
  }
}
} // namespace facebook::velox::exec::test

// This main is needed for some tests on linux.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // Signal handler required for ThreadDebugInfoTest
  facebook::velox::process::addDefaultFatalSignalHandler();
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
