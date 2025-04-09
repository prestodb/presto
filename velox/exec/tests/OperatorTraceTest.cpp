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
#include <folly/io/Cursor.h>
#include <gtest/gtest.h>
#include <memory>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/OperatorTraceWriter.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/Split.h"
#include "velox/exec/TaskTraceReader.h"
#include "velox/exec/Trace.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
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
    connector::hive::HiveConnectorSplit::registerSerDe();
    connector::hive::HiveInsertFileNameGenerator::registerSerDe();
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

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const RowTypePtr& rowType = nullptr) {
    auto inputs = rowType ? rowType : dataType_;
    return HiveConnectorTestBase::makeVectors(inputs, count, rowsPerVector);
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
    ASSERT_GT(summary.inputBytes, 0);
    ASSERT_EQ(summary.rawInputRows, 0);
    ASSERT_EQ(summary.rawInputBytes, 0);
    ASSERT_FALSE(summary.numSplits.has_value());

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
  const auto reader =
      trace::TaskTraceMetadataReader(outputDir->getPath(), pool());
  const auto actualQueryConfigs = reader.queryConfigs();
  const auto actualConnectorProperties = reader.connectorProperties();
  const auto actualQueryPlan = reader.queryPlan();

  ASSERT_TRUE(isSamePlan(actualQueryPlan, planNode));
  ASSERT_EQ(actualQueryConfigs.size(), expectedQueryConfigs.size());
  for (const auto& [key, value] : actualQueryConfigs) {
    ASSERT_EQ(actualQueryConfigs.at(key), expectedQueryConfigs.at(key));
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
  core::PlanNodeId hashJoinNodeId;
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
          .capturePlanNodeId(hashJoinNodeId)
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
            {core::QueryConfig::kQueryTraceNodeIds, hashJoinNodeId},
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

    const auto reader = trace::TaskTraceMetadataReader(expectedDir, pool());
    const auto actualQueryConfigs = reader.queryConfigs();
    const auto actualConnectorProperties = reader.connectorProperties();
    const auto actualQueryPlan = reader.queryPlan();

    ASSERT_TRUE(isSamePlan(actualQueryPlan, planNode));
    ASSERT_EQ(actualQueryConfigs.size(), expectedQueryConfigs.size());
    for (const auto& [key, value] : actualQueryConfigs) {
      ASSERT_EQ(actualQueryConfigs.at(key), expectedQueryConfigs.at(key));
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
  // No trace task regexp.
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
    core::PlanNodeId tableWriteNodeId;
    const auto planNode = PlanBuilder()
                              .values(inputVectors)
                              .tableWrite(outputDir->getPath())
                              .capturePlanNodeId(tableWriteNodeId)
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
              .config(core::QueryConfig::kQueryTraceNodeIds, tableWriteNodeId)
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
        .config(core::QueryConfig::kQueryTraceNodeIds, tableWriteNodeId)
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
    const auto taskTraceReader =
        trace::TaskTraceMetadataReader(taskTraceDir, pool());
    ASSERT_EQ("TableWrite", taskTraceReader.nodeName(tableWriteNodeId));

    const auto opTraceDir =
        getOpTraceDirectory(taskTraceDir, tableWriteNodeId, 0, 0);

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
    const auto taskTraceReader =
        trace::TaskTraceMetadataReader(taskTraceDir, pool());
    ASSERT_TRUE(fs->exists(traceMetaFilePath));
    ASSERT_EQ("Project", taskTraceReader.nodeName(projectNodeId));

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

TEST_F(OperatorTraceTest, traceSplitRoundTrip) {
  constexpr auto numSplits = 5;
  const auto vectors = makeVectors(10, 100);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < numSplits; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }
  auto splits = makeHiveConnectorSplits(splitFiles);
  std::sort(splits.begin(), splits.end());

  auto traceDirPath = TempDirectoryPath::create();
  auto plan = PlanBuilder().tableScan(dataType_).planNode();
  std::shared_ptr<Task> task;
  AssertQueryBuilder(plan)
      .maxDrivers(3)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, "0")
      .splits(splits)
      .copyResults(pool(), task);

  const auto taskTraceDir =
      getTaskTraceDirectory(traceDirPath->getPath(), *task);
  std::vector<std::string> traceDirs;
  for (int i = 0; i < 3; ++i) {
    const auto opTraceDir = getOpTraceDirectory(
        taskTraceDir,
        /*planNodeId=*/"0",
        /*pipelineId=*/0,
        /*driverId=*/i);
    const auto summaryFilePath = getOpTraceSummaryFilePath(opTraceDir);
    const auto splitFilePath = getOpTraceSplitFilePath(opTraceDir);
    ASSERT_TRUE(fs->exists(summaryFilePath));
    ASSERT_TRUE(fs->exists(splitFilePath));

    traceDirs.push_back(opTraceDir);
  }

  const auto reader = exec::trace::OperatorTraceSplitReader(
      traceDirs, memory::MemoryManager::getInstance()->tracePool());
  auto actualSplits = reader.read();
  std::unordered_set<std::string> splitStrs;
  std::transform(
      splits.begin(),
      splits.end(),
      std::inserter(splitStrs, splitStrs.begin()),
      [](const auto& s) { return s->toString(); });
  ASSERT_EQ(actualSplits.size(), splits.size());
  for (int i = 0; i < numSplits; ++i) {
    folly::dynamic splitInfoObj = folly::parseJson(actualSplits[i]);
    const auto actualSplit = exec::Split{
        std::const_pointer_cast<connector::hive::HiveConnectorSplit>(
            ISerializable::deserialize<connector::hive::HiveConnectorSplit>(
                splitInfoObj))};
    ASSERT_FALSE(actualSplit.hasGroup());
    ASSERT_TRUE(actualSplit.hasConnectorSplit());
    const auto actualConnectorSplit = actualSplit.connectorSplit;
    ASSERT_EQ(splitStrs.count(actualConnectorSplit->toString()), 1);
  }
}

TEST_F(OperatorTraceTest, traceSplitPartial) {
  constexpr auto numSplits = 3;
  const auto vectors = makeVectors(2, 100);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < numSplits; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }
  auto splits = makeHiveConnectorSplits(splitFiles);

  auto traceDirPath = TempDirectoryPath::create();
  auto plan = PlanBuilder().tableScan(dataType_).planNode();
  std::shared_ptr<Task> task;
  AssertQueryBuilder(plan)
      .maxDrivers(3)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, "0")
      .splits(splits)
      .copyResults(pool(), task);

  const auto taskTraceDir =
      getTaskTraceDirectory(traceDirPath->getPath(), *task);
  std::vector<std::string> traceDirs;
  for (int i = 0; i < 3; ++i) {
    const auto opTraceDir = getOpTraceDirectory(
        taskTraceDir,
        /*planNodeId=*/"0",
        /*pipelineId=*/0,
        /*driverId=*/i);
    const auto summaryFilePath = getOpTraceSummaryFilePath(opTraceDir);
    const auto splitFilePath = getOpTraceSplitFilePath(opTraceDir);
    ASSERT_TRUE(fs->exists(summaryFilePath));
    ASSERT_TRUE(fs->exists(splitFilePath));

    traceDirs.push_back(opTraceDir);
  }

  // Append a partial split to the split info file.
  const std::string split = "split123";
  const uint32_t length = split.length();
  const uint32_t crc32 = folly::crc32(
      reinterpret_cast<const uint8_t*>(split.data()), split.size());
  const auto splitInfoFile = fs->openFileForWrite(
      fmt::format(
          "{}/{}",
          traceDirPath->getPath(),
          OperatorTraceTraits::kSplitFileName),
      filesystems::FileOptions{.shouldThrowOnFileAlreadyExists = false});
  auto ioBuf = folly::IOBuf::create(12 + 16);
  folly::io::Appender appender(ioBuf.get(), 0);
  // Writes an invalid split without crc.
  appender.writeLE(length);
  appender.push(reinterpret_cast<const uint8_t*>(split.data()), length);
  // Writes a valid spilt.
  appender.writeLE(length);
  appender.push(reinterpret_cast<const uint8_t*>(split.data()), length);
  appender.writeLE(crc32);
  splitInfoFile->append(std::move(ioBuf));
  splitInfoFile->close();

  const auto reader = exec::trace::OperatorTraceSplitReader(
      traceDirs, memory::MemoryManager::getInstance()->tracePool());
  auto actualSplits = reader.read();
  std::unordered_set<std::string> splitStrs;
  std::transform(
      splits.begin(),
      splits.end(),
      std::inserter(splitStrs, splitStrs.begin()),
      [](const auto& s) { return s->toString(); });
  ASSERT_EQ(actualSplits.size(), splits.size());
  for (int i = 0; i < numSplits; ++i) {
    folly::dynamic splitInfoObj = folly::parseJson(actualSplits[i]);
    const auto actualSplit = exec::Split{
        std::const_pointer_cast<connector::hive::HiveConnectorSplit>(
            ISerializable::deserialize<connector::hive::HiveConnectorSplit>(
                splitInfoObj))};
    ASSERT_FALSE(actualSplit.hasGroup());
    ASSERT_TRUE(actualSplit.hasConnectorSplit());
    const auto actualConnectorSplit = actualSplit.connectorSplit;
    ASSERT_EQ(splitStrs.count(actualConnectorSplit->toString()), 1);
  }
}

TEST_F(OperatorTraceTest, traceSplitCorrupted) {
  constexpr auto numSplits = 3;
  const auto vectors = makeVectors(2, 100);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < numSplits; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }
  auto splits = makeHiveConnectorSplits(splitFiles);

  auto traceDirPath = TempDirectoryPath::create();
  auto plan = PlanBuilder().tableScan(dataType_).planNode();
  std::shared_ptr<Task> task;
  AssertQueryBuilder(plan)
      .maxDrivers(3)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, "0")
      .splits(splits)
      .copyResults(pool(), task);

  const auto taskTraceDir =
      getTaskTraceDirectory(traceDirPath->getPath(), *task);
  std::vector<std::string> traceDirs;
  for (int i = 0; i < 3; ++i) {
    const auto opTraceDir = getOpTraceDirectory(
        taskTraceDir,
        /*planNodeId=*/"0",
        /*pipelineId=*/0,
        /*driverId=*/i);
    const auto summaryFilePath = getOpTraceSummaryFilePath(opTraceDir);
    const auto splitFilePath = getOpTraceSplitFilePath(opTraceDir);
    ASSERT_TRUE(fs->exists(summaryFilePath));
    ASSERT_TRUE(fs->exists(splitFilePath));

    traceDirs.push_back(opTraceDir);
  }

  // Append a split with wrong checksum to the split info file.
  const std::string split = "split123";
  const uint32_t length = split.length();
  const uint32_t crc32 = folly::crc32(
      reinterpret_cast<const uint8_t*>(split.data()), split.size());
  const auto splitInfoFile = fs->openFileForWrite(
      fmt::format(
          "{}/{}",
          traceDirPath->getPath(),
          OperatorTraceTraits::kSplitFileName),
      filesystems::FileOptions{.shouldThrowOnFileAlreadyExists = false});
  auto ioBuf = folly::IOBuf::create(16 * 2);
  folly::io::Appender appender(ioBuf.get(), 0);
  // Writes an invalid split with a wrong checksum.
  appender.writeLE(length);
  appender.push(reinterpret_cast<const uint8_t*>(split.data()), length);
  appender.writeLE(crc32 - 1);
  // Writes a valid split.
  appender.writeLE(length);
  appender.push(reinterpret_cast<const uint8_t*>(split.data()), length);
  appender.writeLE(crc32);
  splitInfoFile->append(std::move(ioBuf));
  splitInfoFile->close();

  const auto reader = exec::trace::OperatorTraceSplitReader(
      traceDirs, memory::MemoryManager::getInstance()->tracePool());
  auto actualSplits = reader.read();
  std::unordered_set<std::string> splitStrs;
  std::transform(
      splits.begin(),
      splits.end(),
      std::inserter(splitStrs, splitStrs.begin()),
      [](const auto& s) { return s->toString(); });
  ASSERT_EQ(actualSplits.size(), splits.size());
  for (int i = 0; i < numSplits; ++i) {
    folly::dynamic splitInfoObj = folly::parseJson(actualSplits[i]);
    const auto actualSplit = exec::Split{
        std::const_pointer_cast<connector::hive::HiveConnectorSplit>(
            ISerializable::deserialize<connector::hive::HiveConnectorSplit>(
                splitInfoObj))};
    ASSERT_FALSE(actualSplit.hasGroup());
    ASSERT_TRUE(actualSplit.hasConnectorSplit());
    const auto actualConnectorSplit = actualSplit.connectorSplit;
    ASSERT_EQ(splitStrs.count(actualConnectorSplit->toString()), 1);
  }
}

TEST_F(OperatorTraceTest, hashJoin) {
  std::vector<RowVectorPtr> probeInput;
  RowTypePtr probeType =
      ROW({"c0", "c1", "c2"}, {BIGINT(), TINYINT(), VARCHAR()});
  constexpr auto numBatch = 5;
  probeInput.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    probeInput.push_back(vectorFuzzer_.fuzzInputFlatRow(probeType));
  }

  std::vector<RowVectorPtr> buildInput;
  RowTypePtr buildType =
      ROW({"u0", "u1", "u2"}, {BIGINT(), SMALLINT(), BIGINT()});
  buildInput.reserve(numBatch);
  for (auto i = 0; i < numBatch; ++i) {
    buildInput.push_back(vectorFuzzer_.fuzzInputFlatRow(buildType));
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
    const auto planNodeIdGenerator{
        std::make_shared<core::PlanNodeIdGenerator>()};
    core::PlanNodeId hashJoinNodeId;
    const auto planNode = PlanBuilder(planNodeIdGenerator)
                              .values(probeInput, false)
                              .hashJoin(
                                  {"c0"},
                                  {"u0"},
                                  PlanBuilder(planNodeIdGenerator)
                                      .values(buildInput, true)
                                      .planNode(),
                                  "c0 < 135",
                                  {"c0", "c1", "c2"},
                                  core::JoinType::kInner)
                              .capturePlanNodeId(hashJoinNodeId)
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
              .config(core::QueryConfig::kQueryTraceNodeIds, hashJoinNodeId)
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
        .config(core::QueryConfig::kQueryTraceNodeIds, hashJoinNodeId)
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
    const auto taskTraceReader =
        trace::TaskTraceMetadataReader(taskTraceDir, pool());
    ASSERT_EQ("HashJoin", taskTraceReader.nodeName(hashJoinNodeId));

    for (uint32_t pipelineId = 0; pipelineId < 2; ++pipelineId) {
      const auto opTraceProbeDir =
          getOpTraceDirectory(taskTraceDir, hashJoinNodeId, pipelineId, 0);

      ASSERT_EQ(fs->list(opTraceProbeDir).size(), 2);

      const auto summary =
          OperatorTraceSummaryReader(opTraceProbeDir, pool()).read();
      RowTypePtr dataType;
      if (pipelineId == 0) {
        dataType = probeType;
      } else {
        dataType = buildType;
      }
      const auto reader =
          trace::OperatorTraceInputReader(opTraceProbeDir, dataType, pool());
      RowVectorPtr actual;
      size_t numOutputVectors{0};
      RowVectorPtr expected;
      if (pipelineId == 0) {
        expected = probeInput[numOutputVectors];
      } else {
        expected = buildInput[numOutputVectors];
      }
      while (reader.read(actual)) {
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
}

TEST_F(OperatorTraceTest, canTrace) {
  struct {
    const std::string operatorType;
    const bool canTrace;

    std::string debugString() const {
      return fmt::format(
          "operatorType: {}, canTrace: {}", operatorType, canTrace);
    }
  } testSettings[] = {
      {"PartitionedOutput", true},
      {"HashBuild", true},
      {"HashProbe", true},
      {"RowNumber", false},
      {"OrderBy", false},
      {"PartialAggregation", true},
      {"Aggregation", true},
      {"TableWrite", true},
      {"TableScan", true},
      {"FilterProject", true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_EQ(testData.canTrace, trace::canTrace(testData.operatorType));
  }
}

TEST_F(OperatorTraceTest, hiveConnectorId) {
  constexpr auto numSplits = 2;
  const auto vectors = makeVectors(10, 10);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < numSplits; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }
  auto splits = makeHiveConnectorSplits(splitFiles);
  auto traceDirPath = TempDirectoryPath::create();
  core::PlanNodeId scanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(dataType_)
                  .capturePlanNodeId(scanNodeId)
                  .planNode();
  std::shared_ptr<Task> task;
  AssertQueryBuilder(plan)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceDirPath->getPath())
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, "0")
      .splits(splits)
      .runWithoutResults(task);
  const auto taskTraceDir =
      getTaskTraceDirectory(traceDirPath->getPath(), *task);
  const auto reader = trace::TaskTraceMetadataReader(taskTraceDir, pool());
  ASSERT_EQ("test-hive", reader.connectorId("0"));
}
} // namespace facebook::velox::exec::trace::test
