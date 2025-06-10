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

#include "velox/exec/tests/utils/TableScanTestBase.h"

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

void TableScanTestBase::verifyCacheStats(
    const FileHandleCacheStats& cacheStats,
    size_t curSize,
    size_t numHits,
    size_t numLookups) {
  EXPECT_EQ(cacheStats.curSize, curSize);
  EXPECT_EQ(cacheStats.numHits, numHits);
  EXPECT_EQ(cacheStats.numLookups, numLookups);
}

void TableScanTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();
  exec::ExchangeSource::factories().clear();
  exec::ExchangeSource::registerFactory(createLocalExchangeSource);
}

void TableScanTestBase::SetUpTestCase() {
  HiveConnectorTestBase::SetUpTestCase();
}

std::vector<RowVectorPtr> TableScanTestBase::makeVectors(
    int32_t count,
    int32_t rowsPerVector,
    const RowTypePtr& rowType) {
  auto inputs = rowType ? rowType : rowType_;
  return HiveConnectorTestBase::makeVectors(inputs, count, rowsPerVector);
}

exec::Split TableScanTestBase::makeHiveSplit(
    const std::string& path,
    int64_t splitWeight) {
  return exec::Split(makeHiveConnectorSplit(
      path, 0, std::numeric_limits<uint64_t>::max(), splitWeight));
}

std::shared_ptr<Task> TableScanTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::shared_ptr<connector::ConnectorSplit>& hiveSplit,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(plan, {hiveSplit}, duckDbSql);
}

std::shared_ptr<Task> TableScanTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const exec::Split&& split,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(plan, {split}, duckDbSql);
}

std::shared_ptr<Task> TableScanTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return HiveConnectorTestBase::assertQuery(plan, filePaths, duckDbSql);
}

std::shared_ptr<Task> TableScanTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql,
    const int32_t numPrefetchSplit) {
  return HiveConnectorTestBase::assertQuery(
      plan, makeHiveConnectorSplits(filePaths), duckDbSql, numPrefetchSplit);
}

// Run query with spill enabled.
std::shared_ptr<Task> TableScanTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& spillDirectory,
    const std::string& duckDbSql) {
  return AssertQueryBuilder(plan, duckDbQueryRunner_)
      .spillDirectory(spillDirectory)
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kAggregationSpillEnabled, true)
      .splits(makeHiveConnectorSplits(filePaths))
      .assertResults(duckDbSql);
}

core::PlanNodePtr TableScanTestBase::tableScanNode() {
  return tableScanNode(rowType_);
}

core::PlanNodePtr TableScanTestBase::tableScanNode(
    const RowTypePtr& outputType) {
  core::PlanNodePtr tableScanNode;
  const auto plan = PlanBuilder(pool_.get())
                        .tableScan(outputType)
                        .capturePlanNode(tableScanNode)
                        .planNode();
  VELOX_CHECK(tableScanNode->supportsBarrier());
  return plan;
}

PlanNodeStats TableScanTestBase::getTableScanStats(
    const std::shared_ptr<Task>& task) {
  auto planStats = toPlanStats(task->taskStats());
  return std::move(planStats.at("0"));
}

std::unordered_map<std::string, RuntimeMetric>
TableScanTestBase::getTableScanRuntimeStats(const std::shared_ptr<Task>& task) {
  return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
}

int64_t TableScanTestBase::getSkippedStridesStat(
    const std::shared_ptr<Task>& task) {
  return getTableScanRuntimeStats(task)["skippedStrides"].sum;
}

int64_t TableScanTestBase::getSkippedSplitsStat(
    const std::shared_ptr<Task>& task) {
  return getTableScanRuntimeStats(task)["skippedSplits"].sum;
}

void TableScanTestBase::waitForFinishedDrivers(
    const std::shared_ptr<Task>& task,
    uint32_t n) {
  // Limit wait to 10 seconds.
  size_t iteration{0};
  while (task->numFinishedDrivers() < n && iteration < 100) {
    /* sleep override */
    usleep(100'000); // 0.1 second.
    ++iteration;
  }
  ASSERT_EQ(n, task->numFinishedDrivers());
}

void TableScanTestBase::testPartitionedTableImpl(
    const std::string& filePath,
    const TypePtr& partitionType,
    const std::optional<std::string>& partitionValue) {
  auto split = exec::test::HiveConnectorSplitBuilder(filePath)
                   .partitionKey("pkey", partitionValue)
                   .build();
  auto outputType =
      ROW({"pkey", "c0", "c1"}, {partitionType, BIGINT(), DOUBLE()});
  ColumnHandleMap assignments = {
      {"pkey", partitionKey("pkey", partitionType)},
      {"c0", regularColumn("c0", BIGINT())},
      {"c1", regularColumn("c1", DOUBLE())}};

  auto op = PlanBuilder()
                .startTableScan()
                .outputType(outputType)
                .assignments(assignments)
                .endTableScan()
                .planNode();

  std::string partitionValueStr;
  partitionValueStr =
      partitionValue.has_value() ? "'" + *partitionValue + "'" : "null";
  assertQuery(
      op, split, fmt::format("SELECT {}, * FROM tmp", partitionValueStr));

  outputType = ROW({"c0", "pkey", "c1"}, {BIGINT(), partitionType, DOUBLE()});
  op = PlanBuilder()
           .startTableScan()
           .outputType(outputType)
           .assignments(assignments)
           .endTableScan()
           .planNode();
  assertQuery(
      op, split, fmt::format("SELECT c0, {}, c1 FROM tmp", partitionValueStr));
  outputType = ROW({"c0", "c1", "pkey"}, {BIGINT(), DOUBLE(), partitionType});
  op = PlanBuilder()
           .startTableScan()
           .outputType(outputType)
           .assignments(assignments)
           .endTableScan()
           .planNode();
  assertQuery(
      op, split, fmt::format("SELECT c0, c1, {} FROM tmp", partitionValueStr));

  // select only partition key
  assignments = {{"pkey", partitionKey("pkey", partitionType)}};
  outputType = ROW({"pkey"}, {partitionType});
  op = PlanBuilder()
           .startTableScan()
           .outputType(outputType)
           .assignments(assignments)
           .endTableScan()
           .planNode();
  assertQuery(op, split, fmt::format("SELECT {} FROM tmp", partitionValueStr));
}

void TableScanTestBase::testPartitionedTable(
    const std::string& filePath,
    const TypePtr& partitionType,
    const std::optional<std::string>& partitionValue) {
  testPartitionedTableImpl(filePath, partitionType, partitionValue);
  testPartitionedTableImpl(filePath, partitionType, std::nullopt);
}

} // namespace facebook::velox::exec::test
