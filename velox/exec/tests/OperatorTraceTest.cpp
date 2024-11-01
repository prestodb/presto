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
#include <memory>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TaskTraceReader.h"
#include "velox/exec/Trace.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::trace::test {
class OperatorTraceTest : public HiveConnectorTestBase {
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

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    dataType_ = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()});
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

  OperatorTraceTest() : vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
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

  std::unique_ptr<DriverCtx> driverCtx() {
    return std::make_unique<DriverCtx>(nullptr, 0, 0, 0, 0);
  }

  RowTypePtr dataType_;
  VectorFuzzer vectorFuzzer_;
};

TEST_F(OperatorTraceTest, emptyTrace) {
  auto input = vectorFuzzer_.fuzzInputRow(dataType_);
  input->childAt(0) =
      makeFlatVector<int64_t>(input->size(), [](auto /*unused*/) { return 0; });
  createDuckDbTable({input});

  std::string planNodeId;
  auto traceDirPath = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .values({input})
                  .filter("a > 0")
                  .singleAggregation({"a"}, {"count(1)"})
                  .capturePlanNodeId(planNodeId)
                  .planNode();

  const auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .plan(plan)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeIds, planNodeId)
          .assertResults("SELECT a, count(1) FROM tmp WHERE a > 0 GROUP BY 1");

  const auto taskTraceDir =
      getTaskTraceDirectory(traceDirPath->getPath(), *task);
  const auto opTraceDir = getOpTraceDirectory(
      taskTraceDir,
      planNodeId,
      /*pipelineId=*/0,
      /*driverId=*/0);
  const auto summary = OperatorTraceSummaryReader(opTraceDir, pool()).read();
  ASSERT_EQ(summary.inputRows, 0);
  ASSERT_EQ(summary.opType, "Aggregation");
  // The hash aggregation operator might allocate memory when prepare output
  // buffer even though there is no output. We could optimize out this later if
  // needs.
  ASSERT_GT(summary.peakMemory, 0);
}

TEST_F(OperatorTraceTest, traceData) {
  std::vector<RowVectorPtr> inputVectors;
  constexpr auto numBatch = 5;
  inputVectors.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    inputVectors.push_back(vectorFuzzer_.fuzzInputFlatRow(dataType_));
  }
  createDuckDbTable(inputVectors);

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

    std::string planNodeId;
    auto traceDirPath = TempDirectoryPath::create();
    auto plan = PlanBuilder()
                    .values(inputVectors)
                    .singleAggregation({"a"}, {"count(1)"})
                    .capturePlanNodeId(planNodeId)
                    .planNode();

    if (testData.limitExceeded) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(duckDbQueryRunner_)
              .plan(plan)
              .config(core::QueryConfig::kQueryTraceEnabled, true)
              .config(
                  core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
              .config(
                  core::QueryConfig::kQueryTraceMaxBytes,
                  testData.maxTracedBytes)
              .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
              .config(core::QueryConfig::kQueryTraceNodeIds, planNodeId)
              .assertResults("SELECT a, count(1) FROM tmp GROUP BY 1"),
          "Query exceeded per-query local trace limit of");
      continue;
    }
    const auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .plan(plan)
            .config(core::QueryConfig::kQueryTraceEnabled, true)
            .config(core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
            .config(
                core::QueryConfig::kQueryTraceMaxBytes, testData.maxTracedBytes)
            .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
            .config(core::QueryConfig::kQueryTraceNodeIds, planNodeId)
            .assertResults("SELECT a, count(1) FROM tmp GROUP BY 1");

    const auto fs =
        filesystems::getFileSystem(traceDirPath->getPath(), nullptr);
    const auto taskTraceDir =
        getTaskTraceDirectory(traceDirPath->getPath(), *task);
    const auto opTraceDir = getOpTraceDirectory(
        taskTraceDir,
        planNodeId,
        /*pipelineId=*/0,
        /*driverId=*/0);
    const auto summaryFilePath = getOpTraceSummaryFilePath(opTraceDir);
    const auto dataFilePath = getOpTraceInputFilePath(opTraceDir);
    ASSERT_TRUE(fs->exists(summaryFilePath));
    ASSERT_TRUE(fs->exists(dataFilePath));

    const auto summary = OperatorTraceSummaryReader(opTraceDir, pool()).read();
    ASSERT_EQ(summary.opType, "Aggregation");
    ASSERT_GT(summary.peakMemory, 0);
    ASSERT_EQ(summary.inputRows, testData.numTracedBatches * 16);

    const auto reader = OperatorTraceInputReader(opTraceDir, dataType_, pool());
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

TEST_F(OperatorTraceTest, traceMetadata) {
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
  auto writer = trace::TaskTraceMetadataWriter(outputDir->getPath(), pool());
  writer.write(queryCtx, planNode);

  std::unordered_map<std::string, std::string> acutalQueryConfigs;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      actualConnectorProperties;
  core::PlanNodePtr actualQueryPlan;
  auto reader = trace::TaskTraceMetadataReader(outputDir->getPath(), pool());
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

TEST_F(OperatorTraceTest, task) {
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
            {core::QueryConfig::kQueryTraceMaxBytes,
             std::to_string(100UL << 30)},
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
    const auto taskIds =
        getTaskIds(outputDir->getPath(), task->queryCtx()->queryId(), fs);
    ASSERT_EQ(taskIds.size(), testData.expectedNumDirs);
    ASSERT_EQ(taskIds.at(0), task->taskId());

    std::unordered_map<std::string, std::string> acutalQueryConfigs;
    std::
        unordered_map<std::string, std::unordered_map<std::string, std::string>>
            actualConnectorProperties;
    core::PlanNodePtr actualQueryPlan;
    auto reader = trace::TaskTraceMetadataReader(expectedDir, pool());
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

TEST_F(OperatorTraceTest, error) {
  const auto planNode = PlanBuilder().values({}).planNode();
  // No trace dir.
  {
    const auto queryConfigs = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kQueryTraceEnabled, "true"}};
    const auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(queryConfigs));
    VELOX_ASSERT_USER_THROW(
        AssertQueryBuilder(planNode)
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .copyResults(pool()),
        "Query trace enabled but the trace dir is not set");
  }
  // Duplicate trace plan node ids.
  {
    const auto queryConfigs = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kQueryTraceEnabled, "true"},
        {core::QueryConfig::kQueryTraceDir, "traceDir"},
    };
    const auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(queryConfigs));
    VELOX_ASSERT_USER_THROW(
        AssertQueryBuilder(planNode)
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .copyResults(pool()),
        "Query trace enabled but the trace task regexp is not set");
  }
  // No trace plan node ids.
  {
    const auto queryConfigs = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kQueryTraceEnabled, "true"},
        {core::QueryConfig::kQueryTraceDir, "traceDir"},
        {core::QueryConfig::kQueryTraceTaskRegExp, ".*"}};
    const auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(queryConfigs));
    VELOX_ASSERT_USER_THROW(
        AssertQueryBuilder(planNode)
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .copyResults(pool()),
        "Query trace nodes are not set");
  }
  // Duplicate trace plan node ids.
  {
    const auto queryConfigs = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kQueryTraceEnabled, "true"},
        {core::QueryConfig::kQueryTraceDir, "traceDir"},
        {core::QueryConfig::kQueryTraceTaskRegExp, ".*"},
        {core::QueryConfig::kQueryTraceNodeIds, "1,1"},
    };
    const auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(queryConfigs));
    VELOX_ASSERT_USER_THROW(
        AssertQueryBuilder(planNode)
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .copyResults(pool()),
        "Duplicate trace nodes found: 1, 1");
  }
  // Nonexist trace plan node id.
  {
    const auto queryConfigs = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kQueryTraceEnabled, "true"},
        {core::QueryConfig::kQueryTraceDir, "traceDir"},
        {core::QueryConfig::kQueryTraceTaskRegExp, ".*"},
        {core::QueryConfig::kQueryTraceNodeIds, "nonexist"},
    };
    const auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(queryConfigs));
    VELOX_ASSERT_USER_THROW(
        AssertQueryBuilder(planNode)
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .copyResults(pool()),
        "Trace plan nodes not found from task");
  }
}

TEST_F(OperatorTraceTest, traceTableWriter) {
  std::vector<RowVectorPtr> inputVectors;
  constexpr auto numBatch = 5;
  inputVectors.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    inputVectors.push_back(vectorFuzzer_.fuzzInputFlatRow(dataType_));
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
      {".*", 0, numBatch, true},
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
    if (testData.limitExceeded) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(planNode)
              .maxDrivers(1)
              .config(core::QueryConfig::kQueryTraceEnabled, true)
              .config(core::QueryConfig::kQueryTraceDir, traceRoot)
              .config(
                  core::QueryConfig::kQueryTraceMaxBytes,
                  testData.maxTracedBytes)
              .config(
                  core::QueryConfig::kQueryTraceTaskRegExp,
                  testData.taskRegExpr)
              .config(core::QueryConfig::kQueryTraceNodeIds, "1")
              .copyResults(pool(), task),
          "Query exceeded per-query local trace limit of");
      continue;
    }
    AssertQueryBuilder(planNode)
        .maxDrivers(1)
        .config(core::QueryConfig::kQueryTraceEnabled, true)
        .config(core::QueryConfig::kQueryTraceDir, traceRoot)
        .config(core::QueryConfig::kQueryTraceMaxBytes, testData.maxTracedBytes)
        .config(core::QueryConfig::kQueryTraceTaskRegExp, testData.taskRegExpr)
        .config(core::QueryConfig::kQueryTraceNodeIds, "1")
        .copyResults(pool(), task);

    const auto taskTraceDir = getTaskTraceDirectory(traceRoot, *task);
    const auto fs = filesystems::getFileSystem(taskTraceDir, nullptr);

    if (testData.taskRegExpr == "wrong id") {
      ASSERT_FALSE(fs->exists(traceRoot));
      continue;
    }

    // Query metadta file should exist.
    const auto traceMetaFilePath = getTaskTraceMetaFilePath(taskTraceDir);
    ASSERT_TRUE(fs->exists(traceMetaFilePath));

    const auto opTraceDir = getOpTraceDirectory(taskTraceDir, "1", 0, 0);

    ASSERT_EQ(fs->list(opTraceDir).size(), 2);

    const auto summary = OperatorTraceSummaryReader(opTraceDir, pool()).read();
    const auto reader =
        trace::OperatorTraceInputReader(opTraceDir, dataType_, pool());
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

TEST_F(OperatorTraceTest, filterProject) {
  std::vector<RowVectorPtr> inputVectors;
  constexpr auto numBatch = 5;
  inputVectors.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    inputVectors.push_back(vectorFuzzer_.fuzzInputFlatRow(dataType_));
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
      {".*", 0, numBatch, true},
      {"wrong id", 10UL << 30, 0, false},
      {"test_cursor \\d+", 10UL << 30, numBatch, false},
      {"test_cursor \\d+", 800, 2, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto outputDir = TempDirectoryPath::create();
    core::PlanNodeId projectNodeId;
    const auto planNode = PlanBuilder()
                              .values(inputVectors)
                              .filter("a % 10 < 9")
                              .project({"a", "b", "a % 100 + c % 50 AS d"})
                              .capturePlanNodeId(projectNodeId)
                              .planNode();
    const auto testDir = TempDirectoryPath::create();
    const auto traceRoot =
        fmt::format("{}/{}", testDir->getPath(), "traceRoot");
    std::shared_ptr<Task> task;
    if (testData.limitExceeded) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(planNode)
              .maxDrivers(1)
              .config(core::QueryConfig::kQueryTraceEnabled, true)
              .config(core::QueryConfig::kQueryTraceDir, traceRoot)
              .config(
                  core::QueryConfig::kQueryTraceMaxBytes,
                  testData.maxTracedBytes)
              .config(
                  core::QueryConfig::kQueryTraceTaskRegExp,
                  testData.taskRegExpr)
              .config(core::QueryConfig::kQueryTraceNodeIds, projectNodeId)
              .copyResults(pool(), task),
          "Query exceeded per-query local trace limit of");
      continue;
    }
    AssertQueryBuilder(planNode)
        .maxDrivers(1)
        .config(core::QueryConfig::kQueryTraceEnabled, true)
        .config(core::QueryConfig::kQueryTraceDir, traceRoot)
        .config(core::QueryConfig::kQueryTraceMaxBytes, testData.maxTracedBytes)
        .config(core::QueryConfig::kQueryTraceTaskRegExp, testData.taskRegExpr)
        .config(core::QueryConfig::kQueryTraceNodeIds, projectNodeId)
        .copyResults(pool(), task);

    const auto taskTraceDir = getTaskTraceDirectory(traceRoot, *task);
    const auto fs = filesystems::getFileSystem(taskTraceDir, nullptr);

    if (testData.taskRegExpr == "wrong id") {
      ASSERT_FALSE(fs->exists(traceRoot));
      continue;
    }

    // Query metadata file should exist.
    const auto traceMetaFilePath = getTaskTraceMetaFilePath(taskTraceDir);
    ASSERT_TRUE(fs->exists(traceMetaFilePath));

    const auto opTraceDir =
        getOpTraceDirectory(taskTraceDir, projectNodeId, 0, 0);

    ASSERT_EQ(fs->list(opTraceDir).size(), 2);

    const auto summary = OperatorTraceSummaryReader(opTraceDir, pool()).read();
    const auto reader =
        trace::OperatorTraceInputReader(opTraceDir, dataType_, pool());
    RowVectorPtr actual;
    size_t numOutputVectors{0};
    while (reader.read(actual)) {
      const auto& expected = inputVectors[numOutputVectors];
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
