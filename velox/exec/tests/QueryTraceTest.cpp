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
#include "velox/exec/QueryDataReader.h"
#include "velox/exec/QueryDataWriter.h"
#include "velox/exec/QueryMetadataReader.h"
#include "velox/exec/QueryMetadataWriter.h"
#include "velox/exec/QueryTraceUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::trace::test {
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
  const auto rowType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()});
  std::vector<RowVectorPtr> inputVectors;
  constexpr auto numBatch = 5;
  inputVectors.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    inputVectors.push_back(vectorFuzzer_.fuzzInputFlatRow(rowType));
  }

  struct {
    uint64_t maxTracedBytes;
    uint8_t numTracedBatches;
    bool limitExceeded;

    std::string debugString() const {
      return fmt::format(
          "maxTracedBytes: {}, numTracedBatches: {}, limitExceeded {}",
          maxTracedBytes,
          numTracedBatches,
          limitExceeded);
    }
  } testSettings[]{
      {0, 0, true}, {800, 2, true}, {100UL << 30, numBatch, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto outputDir = TempDirectoryPath::create();
    // Ensure the writer only write one batch.
    uint64_t numTracedBytes{0};
    auto writer = trace::QueryDataWriter(
        outputDir->getPath(), pool(), [&](uint64_t bytes) {
          numTracedBytes += bytes;
          return numTracedBytes >= testData.maxTracedBytes;
        });
    for (auto i = 0; i < numBatch; ++i) {
      writer.write(inputVectors[i]);
    }
    writer.finish();

    const auto fs = filesystems::getFileSystem(outputDir->getPath(), nullptr);
    const auto summaryFile = fs->openFileForRead(fmt::format(
        "{}/{}", outputDir->getPath(), QueryTraceTraits::kDataSummaryFileName));
    const auto summary = summaryFile->pread(0, summaryFile->size());
    ASSERT_FALSE(summary.empty());
    folly::dynamic obj = folly::parseJson(summary);
    ASSERT_EQ(
        obj[QueryTraceTraits::kTraceLimitExceededKey].asBool(),
        testData.limitExceeded);

    if (testData.maxTracedBytes == 0) {
      const auto dataFile = fs->openFileForRead(fmt::format(
          "{}/{}", outputDir->getPath(), QueryTraceTraits::kDataFileName));
      ASSERT_EQ(dataFile->size(), 0);
      continue;
    }

    const auto reader = QueryDataReader(outputDir->getPath(), rowType, pool());
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
    ASSERT_EQ(numOutputVectors, testData.numTracedBatches);
  }
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

  struct {
    std::string taskRegExpr;
    uint8_t expectedNumDirs;

    std::string debugString() const {
      return fmt::format(
          "taskRegExpr: {}, expectedNumDirs: ", taskRegExpr, expectedNumDirs);
    }
  } testSettings[]{{".*", 1}, {"test_cursor .*", 1}, {"xxx_yyy \\d+", 0}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto outputDir = TempDirectoryPath::create();
    const auto expectedQueryConfigs =
        std::unordered_map<std::string, std::string>{
            {core::QueryConfig::kSpillEnabled, "true"},
            {core::QueryConfig::kSpillNumPartitionBits, "17"},
            {core::QueryConfig::kQueryTraceEnabled, "true"},
            {core::QueryConfig::kQueryTraceDir, outputDir->getPath()},
            {core::QueryConfig::kQueryTraceTaskRegExp, testData.taskRegExpr},
            {core::QueryConfig::kQueryTraceNodeIds, "1,2"},
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

    if (testData.taskRegExpr == "xxx_yyy \\d+") {
      ASSERT_EQ(actaulDirs.size(), testData.expectedNumDirs);
      continue;
    }
    ASSERT_EQ(actaulDirs.size(), testData.expectedNumDirs);
    ASSERT_EQ(actaulDirs.at(0), expectedDir);
    const auto taskIds = getTaskIds(outputDir->getPath(), fs);
    ASSERT_EQ(taskIds.size(), testData.expectedNumDirs);
    ASSERT_EQ(taskIds.at(0), task->taskId());

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

TEST_F(QueryTracerTest, traceTableWriter) {
  const auto rowType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()});
  std::vector<RowVectorPtr> inputVectors;
  constexpr auto numBatch = 5;
  inputVectors.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    inputVectors.push_back(vectorFuzzer_.fuzzInputFlatRow(rowType));
  }

  struct {
    std::string taskRegExpr;
    uint64_t maxTracedBytes;
    uint8_t numTracedBatches;
    bool limitExceeded;

    std::string debugString() const {
      return fmt::format(
          "taskRegExpr: {}, maxTracedBytes: {}, numTracedBatches: {}, limitExceeded {}",
          taskRegExpr,
          maxTracedBytes,
          numTracedBatches,
          limitExceeded);
    }
  } testSettings[]{
      {".*", 10UL << 30, numBatch, false},
      {".*", 0, numBatch, false},
      {"wrong id", 10UL << 30, 0, false},
      {"test_cursor \\d+", 10UL << 30, numBatch, false},
      {"test_cursor \\d+", 800, 2, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto outputDir = TempDirectoryPath::create();
    const auto planNode = PlanBuilder()
                              .values(inputVectors)
                              .tableWrite(outputDir->getPath())
                              .planNode();
    const auto testDir = TempDirectoryPath::create();
    const auto traceRoot =
        fmt::format("{}/{}", testDir->getPath(), "traceRoot");
    std::shared_ptr<Task> task;
    AssertQueryBuilder(planNode)
        .maxDrivers(1)
        .config(core::QueryConfig::kQueryTraceEnabled, true)
        .config(core::QueryConfig::kQueryTraceDir, traceRoot)
        .config(core::QueryConfig::kQueryTraceMaxBytes, testData.maxTracedBytes)
        .config(core::QueryConfig::kQueryTraceTaskRegExp, testData.taskRegExpr)
        .config(core::QueryConfig::kQueryTraceNodeIds, "1")
        .copyResults(pool(), task);

    const auto metadataDir = fmt::format("{}/{}", traceRoot, task->taskId());
    const auto fs = filesystems::getFileSystem(metadataDir, nullptr);

    if (testData.taskRegExpr == "wrong id") {
      ASSERT_FALSE(fs->exists(traceRoot));
      continue;
    }

    // Query metadta file should exist.
    const auto traceMetaFile = fmt::format(
        "{}/{}/{}",
        traceRoot,
        task->taskId(),
        trace::QueryTraceTraits::kQueryMetaFileName);
    ASSERT_TRUE(fs->exists(traceMetaFile));

    const auto dataDir =
        fmt::format("{}/{}/{}", traceRoot, task->taskId(), "1/0/0/data");

    // Query data tracing disabled.
    if (testData.maxTracedBytes == 0) {
      ASSERT_FALSE(fs->exists(dataDir));
      continue;
    }

    ASSERT_EQ(fs->list(dataDir).size(), 2);
    // Check data summaries.
    const auto summaryFile = fs->openFileForRead(
        fmt::format("{}/{}", dataDir, QueryTraceTraits::kDataSummaryFileName));
    const auto summary = summaryFile->pread(0, summaryFile->size());
    ASSERT_FALSE(summary.empty());
    folly::dynamic obj = folly::parseJson(summary);
    ASSERT_EQ(
        obj[QueryTraceTraits::kTraceLimitExceededKey].asBool(),
        testData.limitExceeded);

    const auto reader = trace::QueryDataReader(dataDir, rowType, pool());
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
    ASSERT_EQ(numOutputVectors, testData.numTracedBatches);
  }
}
} // namespace facebook::velox::exec::trace::test
