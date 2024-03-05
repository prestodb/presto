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
#include "velox/exec/TableScan.h"
#include <atomic>
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

namespace {
void verifyCacheStats(
    const FileHandleCacheStats& cacheStats,
    size_t curSize,
    size_t numHits,
    size_t numLookups) {
  EXPECT_EQ(cacheStats.curSize, curSize);
  EXPECT_EQ(cacheStats.numHits, numHits);
  EXPECT_EQ(cacheStats.numLookups, numLookups);
}
} // namespace

class TableScanTest : public virtual HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const RowTypePtr& rowType = nullptr) {
    auto inputs = rowType ? rowType : rowType_;
    return HiveConnectorTestBase::makeVectors(inputs, count, rowsPerVector);
  }

  exec::Split makeHiveSplit(std::string path, int64_t splitWeight = 0) {
    return exec::Split(makeHiveConnectorSplit(
        std::move(path), 0, std::numeric_limits<uint64_t>::max(), splitWeight));
  }

  std::shared_ptr<Task> assertQuery(
      const PlanNodePtr& plan,
      const std::shared_ptr<connector::ConnectorSplit>& hiveSplit,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {hiveSplit}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const PlanNodePtr& plan,
      const exec::Split&& split,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {split}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql) {
    return HiveConnectorTestBase::assertQuery(plan, filePaths, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit) {
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kMaxSplitPreloadPerDriver,
            std::to_string(numPrefetchSplit))
        .splits(makeHiveConnectorSplits(filePaths))
        .assertResults(duckDbSql);
  }

  core::PlanNodePtr tableScanNode() {
    return tableScanNode(rowType_);
  }

  core::PlanNodePtr tableScanNode(const RowTypePtr& outputType) {
    return PlanBuilder(pool_.get()).tableScan(outputType).planNode();
  }

  static PlanNodeStats getTableScanStats(const std::shared_ptr<Task>& task) {
    auto planStats = toPlanStats(task->taskStats());
    return std::move(planStats.at("0"));
  }

  static std::unordered_map<std::string, RuntimeMetric>
  getTableScanRuntimeStats(const std::shared_ptr<Task>& task) {
    return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
  }

  static int64_t getSkippedStridesStat(const std::shared_ptr<Task>& task) {
    return getTableScanRuntimeStats(task)["skippedStrides"].sum;
  }

  static int64_t getSkippedSplitsStat(const std::shared_ptr<Task>& task) {
    return getTableScanRuntimeStats(task)["skippedSplits"].sum;
  }

  static void waitForFinishedDrivers(
      const std::shared_ptr<Task>& task,
      uint32_t n) {
    // Limit wait to 10 seconds.
    size_t iteration{0};
    while (task->numFinishedDrivers() < n and iteration < 100) {
      /* sleep override */
      usleep(100'000); // 0.1 second.
      ++iteration;
    }
    ASSERT_EQ(n, task->numFinishedDrivers());
  }

  void testPartitionedTableImpl(
      const std::string& filePath,
      const TypePtr& partitionType,
      const std::optional<std::string>& partitionValue) {
    auto split = HiveConnectorSplitBuilder(filePath)
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

    std::string partitionValueStr =
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
        op,
        split,
        fmt::format("SELECT c0, {}, c1 FROM tmp", partitionValueStr));
    outputType = ROW({"c0", "c1", "pkey"}, {BIGINT(), DOUBLE(), partitionType});
    op = PlanBuilder()
             .startTableScan()
             .outputType(outputType)
             .assignments(assignments)
             .endTableScan()
             .planNode();
    assertQuery(
        op,
        split,
        fmt::format("SELECT c0, c1, {} FROM tmp", partitionValueStr));

    // select only partition key
    assignments = {{"pkey", partitionKey("pkey", partitionType)}};
    outputType = ROW({"pkey"}, {partitionType});
    op = PlanBuilder()
             .startTableScan()
             .outputType(outputType)
             .assignments(assignments)
             .endTableScan()
             .planNode();
    assertQuery(
        op, split, fmt::format("SELECT {} FROM tmp", partitionValueStr));
  }

  void testPartitionedTable(
      const std::string& filePath,
      const TypePtr& partitionType,
      const std::optional<std::string>& partitionValue) {
    testPartitionedTableImpl(filePath, partitionType, partitionValue);
    testPartitionedTableImpl(filePath, partitionType, std::nullopt);
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           INTEGER(),
           SMALLINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           TINYINT()})};
};

TEST_F(TableScanTest, allColumns) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  auto plan = tableScanNode();
  auto task = assertQuery(plan, {filePath}, "SELECT * FROM tmp");

  // A quick sanity check for memory usage reporting. Check that peak total
  // memory usage for the project node is > 0.
  auto planStats = toPlanStats(task->taskStats());
  auto scanNodeId = plan->id();
  auto it = planStats.find(scanNodeId);
  ASSERT_TRUE(it != planStats.end());
  ASSERT_TRUE(it->second.peakMemoryBytes > 0);
  EXPECT_LT(0, exec::TableScan::ioWaitNanos());
}

TEST_F(TableScanTest, connectorStats) {
  auto hiveConnector =
      std::dynamic_pointer_cast<connector::hive::HiveConnector>(
          connector::getConnector(kHiveConnectorId));
  EXPECT_NE(nullptr, hiveConnector);
  verifyCacheStats(hiveConnector->fileHandleCacheStats(), 0, 0, 0);

  for (size_t i = 0; i < 99; i++) {
    auto vectors = makeVectors(10, 10);
    auto filePath = TempFilePath::create();
    writeToFile(filePath->path, vectors);
    createDuckDbTable(vectors);
    auto plan = tableScanNode();
    assertQuery(plan, {filePath}, "SELECT * FROM tmp");
  }

  verifyCacheStats(hiveConnector->fileHandleCacheStats(), 99, 0, 99);
  verifyCacheStats(hiveConnector->clearFileHandleCache(), 0, 0, 99);
}

TEST_F(TableScanTest, columnAliases) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  std::string tableName = "t";
  std::unordered_map<std::string, std::string> aliases = {{"a", "c0"}};
  auto outputType = ROW({"a"}, {BIGINT()});
  auto op = PlanBuilder(pool_.get())
                .startTableScan()
                .tableName(tableName)
                .outputType(outputType)
                .columnAliases(aliases)
                .endTableScan()
                .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp");

  // Use aliased column in a range filter.
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .tableName(tableName)
           .outputType(outputType)
           .columnAliases(aliases)
           .subfieldFilter("a < 10")
           .endTableScan()
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp WHERE c0 <= 10");

  // Use aliased column in remaining filter.
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .tableName(tableName)
           .outputType(outputType)
           .columnAliases(aliases)
           .remainingFilter("a % 2 = 1")
           .endTableScan()
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp WHERE c0 % 2 = 1");
}

TEST_F(TableScanTest, partitionKeyAlias) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  ColumnHandleMap assignments = {
      {"a", regularColumn("c0", BIGINT())},
      {"ds_alias", partitionKey("ds", VARCHAR())}};

  auto split = HiveConnectorSplitBuilder(filePath->path)
                   .partitionKey("ds", "2021-12-02")
                   .build();

  auto outputType = ROW({"a", "ds_alias"}, {BIGINT(), VARCHAR()});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(outputType)
                .assignments(assignments)
                .endTableScan()
                .planNode();

  assertQuery(op, split, "SELECT c0, '2021-12-02' FROM tmp");
}

TEST_F(TableScanTest, columnPruning) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  auto op = tableScanNode(ROW({"c0"}, {BIGINT()}));
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp");

  op = tableScanNode(ROW({"c1"}, {INTEGER()}));
  assertQuery(op, {filePath}, "SELECT c1 FROM tmp");

  op = tableScanNode(ROW({"c5"}, {VARCHAR()}));
  assertQuery(op, {filePath}, "SELECT c5 FROM tmp");

  op = tableScanNode(ROW({"c0", "c1"}, {BIGINT(), INTEGER()}));
  assertQuery(op, {filePath}, "SELECT c0, c1 FROM tmp");

  op = tableScanNode(ROW({"c0", "c3", "c5"}, {BIGINT(), REAL(), VARCHAR()}));
  assertQuery(op, {filePath}, "SELECT c0, c3, c5 FROM tmp");

  op = tableScanNode(ROW({"c3", "c0"}, {REAL(), BIGINT()}));
  assertQuery(op, {filePath}, "SELECT c3, c0 FROM tmp");
}

TEST_F(TableScanTest, timestamp) {
  vector_size_t size = 10'000;
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<Timestamp>(
           size,
           [](vector_size_t row) {
             return Timestamp(
                 row, (row % 1000) * Timestamp::kNanosecondsInMillisecond);
           },
           [](vector_size_t row) {
             return row % 5 == 0; /* null every 5 rows */
           })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});
  createDuckDbTable({rowVector});

  auto dataColumns = ROW({"c0", "c1"}, {BIGINT(), TIMESTAMP()});
  auto op = tableScanNode(dataColumns);
  assertQuery(op, {filePath}, "SELECT c0, c1 FROM tmp");

  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({"c0", "c1"}, {BIGINT(), TIMESTAMP()}))
           .subfieldFilter("c1 is null")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0, c1 FROM tmp WHERE c1 is null");

  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({"c0", "c1"}, {BIGINT(), TIMESTAMP()}))
           .subfieldFilter("c1 < '1970-01-01 01:30:00'::TIMESTAMP")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();
  assertQuery(
      op,
      {filePath},
      "SELECT c0, c1 FROM tmp WHERE c1 < timestamp '1970-01-01 01:30:00'");

  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({"c0"}, {BIGINT()}))
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp");

  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({"c0"}, {BIGINT()}))
           .subfieldFilter("c1 is null")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp WHERE c1 is null");

  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({"c0"}, {BIGINT()}))
           .subfieldFilter("c1 < timestamp'1970-01-01 01:30:00'")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();
  assertQuery(
      op,
      {filePath},
      "SELECT c0 FROM tmp WHERE c1 < timestamp'1970-01-01 01:30:00'");
}

DEBUG_ONLY_TEST_F(TableScanTest, timeLimitInGetOutput) {
  // Create two different row vectors: with some nulls and with no nulls.
  vector_size_t numRows = 100;
  auto tsFunc = [](vector_size_t row) {
    return Timestamp(row, (row % 10) * Timestamp::kNanosecondsInMillisecond);
  };
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(numRows, [](vector_size_t row) { return row; }),
       makeFlatVector<Timestamp>(numRows, tsFunc, [](vector_size_t row) {
         return row % 5 == 0; /* null every 5 rows */
       })});
  auto rowVectorNoNulls = makeRowVector(
      {makeFlatVector<int64_t>(numRows, [](vector_size_t row) { return row; }),
       makeFlatVector<Timestamp>(numRows, tsFunc)});

  // Prepare the data files and tables with 2/3 of them having no null row
  // vector.
  const size_t numFiles{20};
  std::vector<std::shared_ptr<TempFilePath>> filePaths;
  std::vector<RowVectorPtr> vectorsForDuckDb;
  filePaths.reserve(numFiles);
  vectorsForDuckDb.reserve(numFiles);
  for (auto i = 0; i < numFiles; ++i) {
    filePaths.emplace_back(TempFilePath::create());
    const auto& vec = (i % 3 == 0) ? rowVector : rowVectorNoNulls;
    writeToFile(filePaths.back()->path, vec);
    vectorsForDuckDb.emplace_back(vec);
  }
  createDuckDbTable(vectorsForDuckDb);

  // Scan with filter. The filter ensures we filter ALL rows from the splits
  // with no nulls, thus ending up with an empty result set for such splits.
  auto dataColumns = ROW({"c0", "c1"}, {BIGINT(), TIMESTAMP()});
  const size_t tableScanGetOutputTimeLimitMs{100};
  auto plan = PlanBuilder(pool_.get())
                  .startTableScan()
                  .outputType(ROW({"c0", "c1"}, {BIGINT(), TIMESTAMP()}))
                  .subfieldFilter("c1 is null")
                  .dataColumns(dataColumns)
                  .endTableScan()
                  .planNode();

  // Ensure the getOutput is long enough to trigger the maxGetOutputTimeMs in
  // TableScan, so we can test early exit (bail) from the TableScan::getOutput.
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::TableScan::getOutput",
      std::function<void(const TableScan*)>(
          ([&](const TableScan* /*tableScan*/) {
            /* sleep override */
            std::this_thread::sleep_for(
                std::chrono::milliseconds(tableScanGetOutputTimeLimitMs));
          })));

  // Count how many times we bailed from getOutput.
  size_t numBailed{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::TableScan::getOutput::bail",
      std::function<void(const TableScan*)>(
          ([&](const TableScan* /*tableScan*/) { ++numBailed; })));

  // Ensure query runs correctly with bails.
  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .splits(makeHiveConnectorSplits(filePaths))
      .config(
          QueryConfig::kTableScanGetOutputTimeLimitMs,
          folly::to<std::string>(tableScanGetOutputTimeLimitMs))
      .assertResults("SELECT c0, c1 FROM tmp WHERE c1 is null");

  // We should have at least 12 splits (20/3*2) producing empty results and
  // after each of them we should bail thanks to our 'sleep' injection.
  EXPECT_GE(numBailed, 12);
}

TEST_F(TableScanTest, subfieldPruningRowType) {
  auto innerType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto columnType = ROW({"c", "d"}, {innerType, BIGINT()});
  auto rowType = ROW({"e"}, {columnType});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("e.c");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["e"] = std::make_shared<HiveColumnHandle>(
      "e",
      HiveColumnHandle::ColumnType::kRegular,
      columnType,
      columnType,
      std::move(requiredSubfields));
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
  ASSERT_EQ(result->size(), 10'000);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 1);
  auto e = rows->childAt(0)->as<RowVector>();
  ASSERT_TRUE(e);
  ASSERT_EQ(e->childrenSize(), 2);
  auto c = e->childAt(0)->as<RowVector>();
  ASSERT_EQ(c->childrenSize(), 2);
  int j = 0;
  for (auto& vec : vectors) {
    ASSERT_LE(j + vec->size(), c->size());
    auto ee = vec->childAt(0)->as<RowVector>();
    auto cc = ee->childAt(0);
    for (int i = 0; i < vec->size(); ++i) {
      if (ee->isNullAt(i) || cc->isNullAt(i)) {
        ASSERT_TRUE(e->isNullAt(j) || c->isNullAt(j));
      } else {
        ASSERT_TRUE(cc->equalValueAt(c, i, j));
      }
      ++j;
    }
  }
  ASSERT_EQ(j, c->size());
  auto d = e->childAt(1);
  ASSERT_EQ(d->size(), e->size());
  for (int i = 0; i < d->size(); ++i) {
    ASSERT_TRUE(e->isNullAt(i) || d->isNullAt(i));
  }
}

TEST_F(TableScanTest, subfieldPruningRemainingFilterSubfieldsMissing) {
  auto columnType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()});
  auto rowType = ROW({"e"}, {columnType});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("e.c");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["e"] = std::make_shared<HiveColumnHandle>(
      "e",
      HiveColumnHandle::ColumnType::kRegular,
      columnType,
      columnType,
      std::move(requiredSubfields));

  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .remainingFilter("e.a is null")
                .assignments(assignments)
                .endTableScan()
                .planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());

  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 1);
  auto e = rows->childAt(0)->as<RowVector>();
  ASSERT_TRUE(e);
  ASSERT_EQ(e->childrenSize(), 3);
  auto a = e->childAt(0);
  for (int i = 0; i < a->size(); ++i) {
    ASSERT_TRUE(e->isNullAt(i) || a->isNullAt(i));
  }
}

TEST_F(TableScanTest, subfieldPruningRemainingFilterRootFieldMissing) {
  auto columnType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()});
  auto rowType = ROW({"d", "e"}, {BIGINT(), columnType});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["d"] = std::make_shared<HiveColumnHandle>(
      "d", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT());
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(ROW({{"d", BIGINT()}}))
                .remainingFilter("e.a is null or e.b is null")
                .dataColumns(rowType)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 1);
  auto d = rows->childAt(0)->asFlatVector<int64_t>();
  ASSERT_TRUE(d);
  int expectedSize = 0;
  for (auto& vec : vectors) {
    auto e = vec->as<RowVector>()->childAt(1)->as<RowVector>();
    for (int i = 0; i < e->size(); ++i) {
      expectedSize += e->isNullAt(i) || e->childAt(0)->isNullAt(i) ||
          e->childAt(1)->isNullAt(i);
    }
  }
  ASSERT_EQ(rows->size(), expectedSize);
  ASSERT_EQ(d->size(), expectedSize);
}

TEST_F(TableScanTest, subfieldPruningRemainingFilterStruct) {
  auto structType = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  auto rowType = ROW({"c", "d"}, {structType, BIGINT()});
  auto vectors = makeVectors(3, 10, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  enum { kNoOutput = 0, kWholeColumn = 1, kSubfieldOnly = 2 };
  for (int outputColumn = kNoOutput; outputColumn <= kSubfieldOnly;
       ++outputColumn) {
    for (int filterColumn = kWholeColumn; filterColumn <= kSubfieldOnly;
         ++filterColumn) {
      SCOPED_TRACE(fmt::format("{} {}", outputColumn, filterColumn));
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          assignments;
      assignments["d"] = std::make_shared<HiveColumnHandle>(
          "d", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT());
      if (outputColumn > kNoOutput) {
        std::vector<common::Subfield> subfields;
        if (outputColumn == kSubfieldOnly) {
          subfields.emplace_back("c.b");
        }
        assignments["c"] = std::make_shared<HiveColumnHandle>(
            "c",
            HiveColumnHandle::ColumnType::kRegular,
            structType,
            structType,
            std::move(subfields));
      }
      std::string remainingFilter;
      if (filterColumn == kWholeColumn) {
        remainingFilter =
            "coalesce(c, cast(null AS ROW(a BIGINT, b BIGINT))).a % 2 == 0";
      } else {
        remainingFilter = "c.a % 2 == 0";
      }
      auto op =
          PlanBuilder()
              .startTableScan()
              .outputType(
                  outputColumn == kNoOutput ? ROW({"d"}, {BIGINT()}) : rowType)
              .remainingFilter(remainingFilter)
              .dataColumns(rowType)
              .assignments(assignments)
              .endTableScan()
              .planNode();
      auto split = makeHiveConnectorSplit(filePath->path);
      auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
      int expectedSize = 0;
      std::vector<std::vector<BaseVector::CopyRange>> ranges;
      for (auto& vec : vectors) {
        std::vector<BaseVector::CopyRange> rs;
        auto& c = vec->as<RowVector>()->childAt(0);
        auto* a = c->as<RowVector>()->childAt(0)->asFlatVector<int64_t>();
        for (int i = 0; i < vec->size(); ++i) {
          if (!c->isNullAt(i) && !a->isNullAt(i) && a->valueAt(i) % 2 == 0) {
            rs.push_back({i, expectedSize++, 1});
          }
        }
        ranges.push_back(std::move(rs));
      }
      auto expected = BaseVector::create(rowType, expectedSize, pool());
      auto& d = expected->as<RowVector>()->childAt(1);
      for (int i = 0; i < vectors.size(); ++i) {
        expected->copyRanges(vectors[i].get(), ranges[i]);
      }
      if (outputColumn == kNoOutput) {
        expected = makeRowVector({"d"}, {d});
      }
      auto rows = result->as<RowVector>();
      ASSERT_TRUE(rows);
      ASSERT_EQ(rows->size(), expectedSize);
      for (int i = 0; i < expectedSize; ++i) {
        ASSERT_TRUE(rows->equalValueAt(expected.get(), i, i))
            << "Row " << i << ": " << rows->toString(i) << " vs "
            << expected->toString(i);
      }
    }
  }
}

TEST_F(TableScanTest, subfieldPruningRemainingFilterMap) {
  auto mapVector = makeMapVector<int64_t, int64_t>(
      10,
      [](auto) { return 3; },
      [](auto i) { return i % 3; },
      [](auto i) { return i % 3; });
  auto mapType = mapVector->type();
  auto vector = makeRowVector(
      {"a", "b"}, {makeFlatVector<int64_t>(10, folly::identity), mapVector});
  auto rowType = asRowType(vector->type());
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {vector});
  enum { kNoOutput = 0, kWholeColumn = 1, kSubfieldOnly = 2 };
  for (int outputColumn = kNoOutput; outputColumn <= kSubfieldOnly;
       ++outputColumn) {
    for (int filterColumn = kWholeColumn; filterColumn <= kSubfieldOnly;
         ++filterColumn) {
      SCOPED_TRACE(fmt::format("{} {}", outputColumn, filterColumn));
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          assignments;
      assignments["a"] = std::make_shared<HiveColumnHandle>(
          "a", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT());
      if (outputColumn > kNoOutput) {
        std::vector<common::Subfield> subfields;
        if (outputColumn == kSubfieldOnly) {
          subfields.emplace_back("b[1]");
        }
        assignments["b"] = std::make_shared<HiveColumnHandle>(
            "b",
            HiveColumnHandle::ColumnType::kRegular,
            mapType,
            mapType,
            std::move(subfields));
      }
      std::string remainingFilter;
      if (filterColumn == kWholeColumn) {
        remainingFilter =
            "coalesce(b, cast(null AS MAP(BIGINT, BIGINT)))[0] == 0";
      } else {
        remainingFilter = "b[0] == 0";
      }
      auto op =
          PlanBuilder()
              .startTableScan()
              .outputType(
                  outputColumn == kNoOutput ? ROW({"a"}, {BIGINT()}) : rowType)
              .remainingFilter(remainingFilter)
              .dataColumns(rowType)
              .assignments(assignments)
              .endTableScan()
              .planNode();
      auto split = makeHiveConnectorSplit(filePath->path);
      auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
      auto expected = vector;
      auto a = vector->as<RowVector>()->childAt(0);
      if (outputColumn == kNoOutput) {
        expected = makeRowVector({"a"}, {a});
      } else if (
          outputColumn == kSubfieldOnly && filterColumn == kSubfieldOnly) {
        auto sizes = allocateIndices(10, pool());
        auto* rawSizes = sizes->asMutable<vector_size_t>();
        std::fill(rawSizes, rawSizes + 10, 2);
        auto b = std::make_shared<MapVector>(
            pool(),
            mapType,
            nullptr,
            10,
            mapVector->offsets(),
            sizes,
            mapVector->mapKeys(),
            mapVector->mapValues());
        expected = makeRowVector({"a", "b"}, {a, b});
      }
      auto rows = result->as<RowVector>();
      ASSERT_TRUE(rows);
      ASSERT_EQ(rows->size(), 10);
      for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(rows->equalValueAt(expected.get(), i, i))
            << "Row " << i << ": " << rows->toString(i) << " vs "
            << expected->toString(i);
      }
    }
  }
}

TEST_F(TableScanTest, subfieldPruningMapType) {
  auto valueType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto mapType = MAP(BIGINT(), valueType);
  std::vector<RowVectorPtr> vectors;
  constexpr int kMapSize = 5;
  constexpr int kSize = 200;
  for (int i = 0; i < 3; ++i) {
    auto nulls = makeNulls(
        kSize, [i](auto j) { return j >= i + 1 && j % 17 == (i + 1) % 17; });
    auto offsets = allocateOffsets(kSize, pool());
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto lengths = allocateOffsets(kSize, pool());
    auto* rawLengths = lengths->asMutable<vector_size_t>();
    int mapEntrySize = 0;
    for (int j = 0; j < kSize; ++j) {
      rawOffsets[j] = mapEntrySize;
      rawLengths[j] = bits::isBitNull(nulls->as<uint64_t>(), j) ? 0 : kMapSize;
      mapEntrySize += rawLengths[j];
    }
    auto keys = makeFlatVector<int64_t>(
        mapEntrySize, [](auto row) { return row % kMapSize; });
    auto values = makeVectors(1, mapEntrySize, valueType)[0];
    auto maps = std::make_shared<MapVector>(
        pool(), mapType, nulls, kSize, offsets, lengths, keys, values);
    vectors.push_back(makeRowVector({"c"}, {maps}));
  }
  auto rowType = asRowType(vectors[0]->type());
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("c[0]");
  requiredSubfields.emplace_back("c[2]");
  requiredSubfields.emplace_back("c[4]");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["c"] = std::make_shared<HiveColumnHandle>(
      "c",
      HiveColumnHandle::ColumnType::kRegular,
      mapType,
      mapType,
      std::move(requiredSubfields));
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
  ASSERT_EQ(result->size(), vectors.size() * kSize);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 1);
  auto maps = rows->childAt(0)->as<MapVector>();
  ASSERT_TRUE(maps);
  ASSERT_EQ(maps->size(), result->size());
  for (int i = 0; i < maps->size(); ++i) {
    auto expected =
        vectors[i / kSize]->as<RowVector>()->childAt(0)->as<MapVector>();
    int j = i % kSize;
    if (expected->isNullAt(j)) {
      ASSERT_TRUE(maps->isNullAt(i));
      continue;
    }
    ASSERT_EQ(maps->sizeAt(i), 3);
    for (int k = 0; k < 3; ++k) {
      int ki = maps->offsetAt(i) + k;
      int kj = expected->offsetAt(j) + 2 * k;
      ASSERT_TRUE(
          maps->mapKeys()->equalValueAt(expected->mapKeys().get(), ki, kj));
      ASSERT_TRUE(
          maps->mapValues()->equalValueAt(expected->mapValues().get(), ki, kj));
    }
  }
}

TEST_F(TableScanTest, subfieldPruningArrayType) {
  auto elementType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto arrayType = ARRAY(elementType);
  std::vector<RowVectorPtr> vectors;
  constexpr int kArraySize = 5;
  constexpr int kSize = 200;
  for (int i = 0; i < 3; ++i) {
    auto nulls = makeNulls(
        kSize, [i](auto j) { return j >= i + 1 && j % 17 == (i + 1) % 17; });
    auto offsets = allocateOffsets(kSize, pool());
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto lengths = allocateOffsets(kSize, pool());
    auto* rawLengths = lengths->asMutable<vector_size_t>();
    int arrayElementSize = 0;
    for (int j = 0; j < kSize; ++j) {
      rawOffsets[j] = arrayElementSize;
      rawLengths[j] =
          bits::isBitNull(nulls->as<uint64_t>(), j) ? 0 : kArraySize;
      arrayElementSize += rawLengths[j];
    }
    auto elements = makeVectors(1, arrayElementSize, elementType)[0];
    auto arrays = std::make_shared<ArrayVector>(
        pool(), arrayType, nulls, kSize, offsets, lengths, elements);
    vectors.push_back(makeRowVector({"c"}, {arrays}));
  }
  auto rowType = asRowType(vectors[0]->type());
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("c[3]");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["c"] = std::make_shared<HiveColumnHandle>(
      "c",
      HiveColumnHandle::ColumnType::kRegular,
      arrayType,
      arrayType,
      std::move(requiredSubfields));
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
  ASSERT_EQ(result->size(), vectors.size() * kSize);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 1);
  auto arrays = rows->childAt(0)->as<ArrayVector>();
  ASSERT_TRUE(arrays);
  ASSERT_EQ(arrays->size(), result->size());
  for (int i = 0; i < arrays->size(); ++i) {
    auto expected =
        vectors[i / kSize]->as<RowVector>()->childAt(0)->as<ArrayVector>();
    int j = i % kSize;
    if (expected->isNullAt(j)) {
      ASSERT_TRUE(arrays->isNullAt(i));
      continue;
    }
    ASSERT_EQ(arrays->sizeAt(i), 3);
    for (int k = 0; k < 3; ++k) {
      int ki = arrays->offsetAt(i) + k;
      int kj = expected->offsetAt(j) + k;
      ASSERT_TRUE(
          arrays->elements()->equalValueAt(expected->elements().get(), ki, kj));
    }
  }
}

// Test reading files written before schema change, e.g. missing newly added
// columns.
TEST_F(TableScanTest, missingColumns) {
  // Simulate schema change of adding a new column.
  // Create even files (old) with one column, odd ones (new) with two columns.
  const vector_size_t size = 1'000;
  std::vector<RowVectorPtr> rows;
  const size_t numFiles{10};
  auto filePaths = makeFilePaths(numFiles);
  for (size_t i = 0; i < numFiles; ++i) {
    if (i % 2 == 0) {
      rows.emplace_back(makeRowVector({makeFlatVector<int64_t>(
          size, [&](auto row) { return row + i * size; })}));
    } else {
      rows.emplace_back(makeRowVector({
          makeFlatVector<int64_t>(
              size, [&](auto row) { return -(row + i * size); }),
          makeFlatVector<double>(
              size, [&](auto row) { return row * 0.1 + i * size; }),
      }));
    }
    writeToFile(filePaths[i]->path, {rows.back()});
  }

  // For duckdb ensure we have nulls for the missing column.
  // Overwrite 'rows' and also reuse its 1st column vector.
  auto constNull{BaseVector::createNullConstant(DOUBLE(), size, pool_.get())};
  for (size_t i = 0; i < numFiles; ++i) {
    if (i % 2 == 0) {
      rows[i] = makeRowVector({rows[i]->childAt(0), constNull});
    }
  }
  createDuckDbTable(rows);

  auto dataColumns = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto outputType = dataColumns;
  auto outputTypeC0 = ROW({"c0"}, {BIGINT()});

  auto op = PlanBuilder(pool_.get())
                .startTableScan()
                .outputType(outputType)
                .dataColumns(dataColumns)
                .endTableScan()
                .planNode();
  // Disable preload so that we test one single data source.
  assertQuery(op, filePaths, "SELECT * FROM tmp", 0);

  // Use missing column in a tuple domain filter.
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(outputType)
           .subfieldFilter("c1 <= 100.1")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();
  assertQuery(op, filePaths, "SELECT * FROM tmp WHERE c1 <= 100.1", 0);

  // Use missing column in a tuple domain filter. Select *.
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(outputType)
           .subfieldFilter("c1 <= 2000.1")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();

  assertQuery(op, filePaths, "SELECT * FROM tmp WHERE c1 <= 2000.1", 0);

  // Use missing column in a tuple domain filter. Select c0.
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(outputTypeC0)
           .subfieldFilter("c1 <= 3000.1")
           .dataColumns(dataColumns)
           .endTableScan()
           .planNode();

  assertQuery(op, filePaths, "SELECT c0 FROM tmp WHERE c1 <= 3000.1", 0);

  // Use missing column in a tuple domain filter. Select count(*).
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({}, {}))
           .subfieldFilter("c1 <= 4000.1")
           .dataColumns(dataColumns)
           .endTableScan()
           .singleAggregation({}, {"count(1)"})
           .planNode();

  assertQuery(op, filePaths, "SELECT count(*) FROM tmp WHERE c1 <= 4000.1", 0);

  // Use missing column 'c1' in 'is null' filter, while not selecting 'c1'.
  SubfieldFilters filters;
  filters[common::Subfield("c1")] = lessThanOrEqualDouble(1050.0, true);
  auto tableHandle = std::make_shared<HiveTableHandle>(
      kHiveConnectorId, "tmp", true, std::move(filters), nullptr, dataColumns);
  ColumnHandleMap assignments;
  assignments["c0"] = regularColumn("c0", BIGINT());
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(outputTypeC0)
           .tableHandle(tableHandle)
           .assignments(assignments)
           .endTableScan()
           .planNode();
  assertQuery(
      op, filePaths, "SELECT c0 FROM tmp WHERE c1 is null or c1 <= 1050.0", 0);

  // Use missing column 'c1' in 'is null' filter, while not selecting anything.
  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(ROW({}, {}))
           .subfieldFilter("c1 is null")
           .dataColumns(dataColumns)
           .endTableScan()
           .singleAggregation({}, {"count(1)"})
           .planNode();

  assertQuery(op, filePaths, "SELECT count(*) FROM tmp WHERE c1 is null", 0);

  // Use column aliases.
  outputType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});

  assignments.clear();
  assignments["a"] = regularColumn("c0", BIGINT());
  assignments["b"] = regularColumn("c1", DOUBLE());

  op = PlanBuilder(pool_.get())
           .startTableScan()
           .outputType(outputType)
           .dataColumns(dataColumns)
           .assignments(assignments)
           .endTableScan()
           .planNode();

  assertQuery(op, filePaths, "SELECT * FROM tmp", 0);
}

// Tests queries that use Lazy vectors with multiple layers of wrapping.
TEST_F(TableScanTest, constDictLazy) {
  vector_size_t size = 1'000;
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeMapVector<int64_t, double>(
           size,
           [](auto row) { return row % 3; },
           [](auto row) { return row; },
           [](auto row) { return row * 0.1; })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});

  createDuckDbTable({rowVector});

  auto rowType = asRowType(rowVector->type());

  // Orchestrate a Const(Dict(Lazy)) by using remaining filter that passes on
  // exactly one row.
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .remainingFilter("c0 % 1000 = 5")
                .endTableScan()
                .project({"c1 + 10"})
                .planNode();

  assertQuery(op, {filePath}, "SELECT c1 + 10 FROM tmp WHERE c0 = 5");

  // Orchestrate a Const(Dict(Lazy)) for a complex type (map)
  op = PlanBuilder()
           .startTableScan()
           .outputType(rowType)
           .remainingFilter("c0 = 0")
           .endTableScan()
           .project({"cardinality(c2)"})
           .planNode();

  assertQuery(op, {filePath}, "SELECT 0 FROM tmp WHERE c0 = 5");

  op = PlanBuilder()
           .startTableScan()
           .outputType(rowType)
           .remainingFilter("c0 = 2")
           .endTableScan()
           .project({"cardinality(c2)"})
           .planNode();

  assertQuery(op, {filePath}, "SELECT 2 FROM tmp WHERE c0 = 5");
}

TEST_F(TableScanTest, count) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  auto cursor = TaskCursor::create(params);

  cursor->task()->addSplit("0", makeHiveSplit(filePath->path));
  cursor->task()->noMoreSplits("0");

  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numRead += vector->size();
  }

  EXPECT_EQ(numRead, 10'000);
}

TEST_F(TableScanTest, batchSize) {
  // Make a wide row of many BIGINT columns to ensure that row size is
  // larger than 1KB.
  auto rowSize = 1024; // 1KB
  auto columnSize = sizeof(int64_t);
  auto numColumns = 2 * rowSize / columnSize;
  // Make total input size 2MB, less than 10MB.
  auto totalInputSize = 2048 * 1024;
  auto numRows = totalInputSize / rowSize; // 1024 rows

  std::vector<std::string> names;
  for (int i = 0; i < numColumns; i++) {
    names.push_back(fmt::format("c{}", i));
  }
  auto rowType =
      ROW(std::move(names), std::vector<TypePtr>(numColumns, BIGINT()));
  auto vector = makeVectors(1, numRows, rowType);

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vector);

  createDuckDbTable(vector);

  auto plan = PlanBuilder().tableScan(rowType).planNode();
  // Test kPreferredOutputBatchBytes is set to be very small and less than a
  // single row size. Then each output batch contains 1 and only 1 row, or
  // the number of batches equals to the number of output rows.
  {
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .plan(plan)
                    .splits(makeHiveConnectorSplits({filePath}))
                    .config(
                        QueryConfig::kPreferredOutputBatchBytes,
                        folly::to<std::string>(rowSize - 100))
                    .assertResults("SELECT * FROM tmp");
    const auto opStats = task->taskStats().pipelineStats[0].operatorStats[0];

    EXPECT_EQ(opStats.outputVectors, opStats.outputPositions);
  }
  // Test kPreferredOutputBatchBytes is set to be very large and more than the
  // total input size.Then there would be only 1 output batch containing all
  // output rows.
  {
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .plan(plan)
                    .splits(makeHiveConnectorSplits({filePath}))
                    .config(
                        QueryConfig::kPreferredOutputBatchBytes,
                        folly::to<std::string>(totalInputSize * 5))
                    .assertResults("SELECT * FROM tmp");
    const auto opStats = task->taskStats().pipelineStats[0].operatorStats[0];

    EXPECT_EQ(opStats.outputVectors, 1);
  }
  // Test kPreferredOutputBatchBytes is set to be less than the total input
  // size. Then there would be more than 1 output batch. Each batch contains
  // more than 1 row but fewer than the total output rows.
  {
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .plan(plan)
                    .splits(makeHiveConnectorSplits({filePath}))
                    .config(
                        QueryConfig::kPreferredOutputBatchBytes,
                        folly::to<std::string>(totalInputSize - 1024))
                    .assertResults("SELECT * FROM tmp");
    const auto opStats = task->taskStats().pipelineStats[0].operatorStats[0];

    EXPECT_GT(opStats.outputVectors, 1);
    EXPECT_LT(opStats.outputVectors, opStats.outputPositions);
    EXPECT_GT(opStats.outputPositions / opStats.outputVectors, 1);
    EXPECT_LT(opStats.outputPositions / opStats.outputVectors, numRows);
  }
}

// Test that adding the same split with the same sequence id does not cause
// double read and the 2nd split is ignored.
TEST_F(TableScanTest, sequentialSplitNoDoubleRead) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  auto cursor = TaskCursor::create(params);
  // Add the same split with the same sequence id twice. The second should be
  // ignored.
  EXPECT_TRUE(cursor->task()->addSplitWithSequence(
      "0", makeHiveSplit(filePath->path), 0));
  cursor->task()->setMaxSplitSequenceId("0", 0);
  EXPECT_FALSE(cursor->task()->addSplitWithSequence(
      "0", makeHiveSplit(filePath->path), 0));
  cursor->task()->noMoreSplits("0");

  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numRead += vector->size();
  }

  EXPECT_EQ(10'000, numRead);
}

// Test that adding the splits out of order does not result in splits being
// ignored.
TEST_F(TableScanTest, outOfOrderSplits) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  auto cursor = TaskCursor::create(params);

  // Add splits out of order (1, 0). Both of them should be processed.
  EXPECT_TRUE(cursor->task()->addSplitWithSequence(
      "0", makeHiveSplit(filePath->path), 1));
  EXPECT_TRUE(cursor->task()->addSplitWithSequence(
      "0", makeHiveSplit(filePath->path), 0));
  cursor->task()->setMaxSplitSequenceId("0", 1);
  cursor->task()->noMoreSplits("0");

  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numRead += vector->size();
  }

  EXPECT_EQ(20'000, numRead);
}

// Test that adding the same split, disregarding the sequence id, causes
// double read, as expected.
TEST_F(TableScanTest, splitDoubleRead) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  for (size_t i = 0; i < 2; ++i) {
    auto cursor = TaskCursor::create(params);

    // Add the same split twice - we should read twice the size.
    cursor->task()->addSplit("0", makeHiveSplit(filePath->path));
    cursor->task()->addSplit("0", makeHiveSplit(filePath->path));
    cursor->task()->noMoreSplits("0");

    int32_t numRead = 0;
    while (cursor->moveNext()) {
      auto vector = cursor->current();
      EXPECT_EQ(vector->childrenSize(), 0);
      numRead += vector->size();
    }

    EXPECT_EQ(numRead, 2 * 10'000);
  }
}

TEST_F(TableScanTest, multipleSplits) {
  std::vector<int32_t> numPrefetchSplits = {0, 2};
  for (const auto& numPrefetchSplit : numPrefetchSplits) {
    SCOPED_TRACE(fmt::format("numPrefetchSplit {}", numPrefetchSplit));
    auto filePaths = makeFilePaths(100);
    auto vectors = makeVectors(100, 100);
    for (int32_t i = 0; i < vectors.size(); i++) {
      writeToFile(filePaths[i]->path, vectors[i]);
    }
    createDuckDbTable(vectors);

    auto task = assertQuery(
        tableScanNode(), filePaths, "SELECT * FROM tmp", numPrefetchSplit);
    auto stats = getTableScanRuntimeStats(task);
    if (numPrefetchSplit != 0) {
      ASSERT_GT(stats.at("preloadedSplits").sum, 10);
    } else {
      ASSERT_EQ(stats.count("preloadedSplits"), 0);
    }
  }
}

TEST_F(TableScanTest, waitForSplit) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  createDuckDbTable(vectors);

  int32_t fileIndex = 0;
  ::assertQuery(
      tableScanNode(),
      [&](Task* task) {
        if (fileIndex < filePaths.size()) {
          task->addSplit("0", makeHiveSplit(filePaths[fileIndex++]->path));
        }
        if (fileIndex == filePaths.size()) {
          task->noMoreSplits("0");
        }
      },
      "SELECT * FROM tmp",
      duckDbQueryRunner_);
}

DEBUG_ONLY_TEST_F(TableScanTest, tableScanSplitsAndWeights) {
  // Create 10 data files for 10 splits.
  const size_t numSplits{10};
  const auto filePaths = makeFilePaths(numSplits);
  auto vectors = makeVectors(numSplits, 100);
  for (auto i = 0; i < numSplits; i++) {
    writeToFile(filePaths.at(i)->path, vectors.at(i));
  }

  // Set the table scan operators wait twice:
  // First, before acquiring a split and then after.
  std::atomic_uint32_t numAcquiredSplits{0};
  std::shared_mutex pauseTableScan;
  std::shared_mutex pauseSplitProcessing;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::TableScan::getOutput",
      std::function<void(const TableScan*)>(
          ([&](const TableScan* /*tableScan*/) {
            pauseTableScan.lock_shared();
            pauseTableScan.unlock_shared();
          })));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::TableScan::getOutput::gotSplit",
      std::function<void(const TableScan*)>(
          ([&](const TableScan* /*tableScan*/) {
            ++numAcquiredSplits;
            pauseSplitProcessing.lock_shared();
            pauseSplitProcessing.unlock_shared();
          })));
  // This will stop table scan operators from proceeding reading from the
  // acquired splits.
  pauseTableScan.lock();
  pauseSplitProcessing.lock();

  // Prepare leaf task for the remote exchange node to pull data from.
  auto leafTaskId = "local://leaf-0";
  auto leafPlan = PlanBuilder()
                      .values(vectors)
                      .partitionedOutput({}, 1, {"c0", "c1", "c2"})
                      .planNode();
  std::unordered_map<std::string, std::string> config;
  auto queryCtx = std::make_shared<core::QueryCtx>(
      executor_.get(), core::QueryConfig(std::move(config)));
  core::PlanFragment planFragment{leafPlan};
  Consumer consumer = nullptr;
  auto leafTask = Task::create(
      leafTaskId,
      core::PlanFragment{leafPlan},
      0,
      std::move(queryCtx),
      std::move(consumer));
  leafTask->start(4);

  // Main task plan with table scan and remote exchange.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodeId, exchangeNodeId;
  auto planNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(rowType_)
                      .capturePlanNodeId(scanNodeId)
                      .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                      .hashJoin(
                          {"t0"},
                          {"u0"},
                          PlanBuilder(planNodeIdGenerator, pool_.get())
                              .exchange(leafPlan->outputType())
                              .capturePlanNodeId(exchangeNodeId)
                              // .values(vectors)
                              // .partitionedOutput({}, 1, {"c0", "c1", "c2"})
                              .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                              .planNode(),
                          "",
                          {"t1"},
                          core::JoinType::kAnti)
                      .planNode();

  // Create task, cursor, start the task and supply the table scan splits.
  const int32_t numDrivers = 6;
  CursorParameters params;
  params.planNode = planNode;
  params.maxDrivers = numDrivers;
  auto cursor = TaskCursor::create(params);
  cursor->start();
  auto task = cursor->task();
  int64_t totalSplitWeights{0};
  for (auto fileIndex = 0; fileIndex < numSplits; ++fileIndex) {
    const int64_t splitWeight = fileIndex * 10 + 1;
    totalSplitWeights += splitWeight;
    auto split = makeHiveSplit(filePaths.at(fileIndex)->path, splitWeight);
    task->addSplit(scanNodeId, std::move(split));
  }
  task->noMoreSplits(scanNodeId);
  // Manage remote exchange splits.
  task->addSplit(
      exchangeNodeId,
      exec::Split(std::make_shared<RemoteConnectorSplit>(leafTaskId)));
  task->noMoreSplits(exchangeNodeId);

  // Check the task stats.
  auto stats = task->taskStats();
  EXPECT_EQ(stats.numRunningTableScanSplits, 0);
  EXPECT_EQ(stats.numQueuedTableScanSplits, numSplits);
  EXPECT_EQ(stats.runningTableScanSplitWeights, 0);
  EXPECT_EQ(stats.queuedTableScanSplitWeights, totalSplitWeights);
  EXPECT_EQ(stats.numTotalSplits, numSplits + 1);

  // Let all the operators proceed to acquire splits.
  pauseTableScan.unlock();

  // Wait till 6 out of 10 splits are acquired by the operators in 6 threads
  while (numAcquiredSplits < numDrivers) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Check the task stats.
  int64_t runningSplitWeights{0};
  for (auto i = 0; i < numAcquiredSplits; ++i) {
    runningSplitWeights += i * 10 + 1;
  }
  stats = task->taskStats();
  const auto queuedSplitWeights = totalSplitWeights - runningSplitWeights;
  EXPECT_EQ(stats.numRunningTableScanSplits, numDrivers);
  EXPECT_EQ(stats.numQueuedTableScanSplits, numSplits - numDrivers);
  EXPECT_EQ(stats.runningTableScanSplitWeights, runningSplitWeights);
  EXPECT_EQ(stats.queuedTableScanSplitWeights, queuedSplitWeights);

  // Let all the operators proceed.
  pauseSplitProcessing.unlock();

  // Finish the task.
  std::vector<RowVectorPtr> result;
  while (cursor->moveNext()) {
    result.push_back(cursor->current());
  }
  EXPECT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  EXPECT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();

  // Check task stats again.
  stats = task->taskStats();
  EXPECT_EQ(stats.numRunningTableScanSplits, 0);
  EXPECT_EQ(stats.numQueuedTableScanSplits, 0);
  EXPECT_EQ(stats.runningTableScanSplitWeights, 0);
  EXPECT_EQ(stats.queuedTableScanSplitWeights, 0);
  EXPECT_EQ(numAcquiredSplits, numSplits);
}

TEST_F(TableScanTest, splitOffsetAndLength) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  assertQuery(
      tableScanNode(),
      makeHiveConnectorSplit(
          filePath->path, 0, fs::file_size(filePath->path) / 2),
      "SELECT * FROM tmp");

  assertQuery(
      tableScanNode(),
      makeHiveConnectorSplit(filePath->path, fs::file_size(filePath->path) / 2),
      "SELECT * FROM tmp LIMIT 0");
}

TEST_F(TableScanTest, fileNotFound) {
  auto split = HiveConnectorSplitBuilder("/path/to/nowhere.orc").build();
  auto assertMissingFile = [&](bool ignoreMissingFiles) {
    AssertQueryBuilder(tableScanNode())
        .connectorSessionProperty(
            kHiveConnectorId,
            connector::hive::HiveConfig::kIgnoreMissingFilesSession,
            std::to_string(ignoreMissingFiles))
        .split(split)
        .assertEmptyResults();
  };
  assertMissingFile(true);
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      assertMissingFile(false),
      error_code::kFileNotFound,
      "No such file or directory");
}

// A valid ORC file (containing headers) but no data.
TEST_F(TableScanTest, validFileNoData) {
  auto rowType = ROW({"c0", "c1", "c2"}, {DOUBLE(), VARCHAR(), BIGINT()});

  auto filePath = facebook::velox::test::getDataFilePath(
      "velox/exec/tests", "data/emptyPresto.dwrf");
  auto split = HiveConnectorSplitBuilder(filePath)
                   .start(0)
                   .length(fs::file_size(filePath) / 2)
                   .build();

  auto op = tableScanNode(rowType);
  assertQuery(op, split, "");
}

// An invalid (size = 0) file.
TEST_F(TableScanTest, emptyFile) {
  auto filePath = TempFilePath::create();

  try {
    assertQuery(
        tableScanNode(),
        makeHiveConnectorSplit(filePath->path),
        "SELECT * FROM tmp");
    ASSERT_FALSE(true) << "Function should throw.";
  } catch (const VeloxException& e) {
    EXPECT_EQ("ORC file is empty", e.message());
  }
}

TEST_F(TableScanTest, partitionedTableVarcharKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  testPartitionedTable(filePath->path, VARCHAR(), "2020-11-01");
}

TEST_F(TableScanTest, partitionedTableBigIntKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, BIGINT(), "123456789123456789");
}

TEST_F(TableScanTest, partitionedTableIntegerKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, INTEGER(), "123456789");
}

TEST_F(TableScanTest, partitionedTableSmallIntKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, SMALLINT(), "1");
}

TEST_F(TableScanTest, partitionedTableTinyIntKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, TINYINT(), "1");
}

TEST_F(TableScanTest, partitionedTableBooleanKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, BOOLEAN(), "0");
}

TEST_F(TableScanTest, partitionedTableRealKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, REAL(), "3.5");
}

TEST_F(TableScanTest, partitionedTableDoubleKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, DOUBLE(), "3.5");
}

TEST_F(TableScanTest, partitionedTableDateKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, DATE(), "2023-10-27");
}

std::vector<StringView> toStringViews(const std::vector<std::string>& values) {
  std::vector<StringView> views;
  views.reserve(values.size());
  for (const auto& value : values) {
    views.emplace_back(StringView(value));
  }
  return views;
}

TEST_F(TableScanTest, statsBasedSkippingBool) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), BOOLEAN()});
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       makeFlatVector<bool>(
           size, [](auto row) { return (row / 10'000) % 2 == 0; })});

  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  auto assertQuery = [&](const std::string& filter) {
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get()).tableScan(rowType, {filter}).planNode(),
        filePaths,
        "SELECT c0, c1 FROM tmp WHERE " + filter);
  };
  auto task = assertQuery("c1 = true");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));
  EXPECT_EQ(1, getTableScanStats(task).numSplits);
  EXPECT_EQ(1, getTableScanStats(task).numDrivers);

  task = assertQuery("c1 = false");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));
}

TEST_F(TableScanTest, statsBasedSkippingDouble) {
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector({makeFlatVector<double>(
      size, [](auto row) { return (double)(row + 0.0001); })});

  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1.05 -> whole file should be skipped based on stats
  auto assertQuery = [&](const std::string& filter) {
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get())
            .tableScan(ROW({"c0"}, {DOUBLE()}), {filter})
            .planNode(),
        filePaths,
        "SELECT c0 FROM tmp WHERE " + filter);
  };

  auto task = assertQuery("c0 <= -1.05");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // c0 >= 11,111.06 - first stride should be skipped based on stats
  task = assertQuery("c0 >= 11111.06");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedStridesStat(task));

  // c0 between 10'100.06 and 10'500.08 - all strides but second should be
  // skipped based on stats
  task = assertQuery("c0 between 10100.06 AND 10500.08");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // c0 <= 1,234.005 - all strides but first should be skipped
  task = assertQuery("c0 <= 1234.005");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));
}

TEST_F(TableScanTest, statsBasedSkippingFloat) {
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector({makeFlatVector<float>(
      size, [](auto row) { return (float)(row + 0.0001); })});

  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1.05 -> whole file should be skipped based on stats

  auto assertQuery = [&](const std::string& filter) {
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get())
            .tableScan(ROW({"c0"}, {REAL()}), {filter})
            .planNode(),
        filePaths,
        "SELECT c0 FROM tmp WHERE " + filter);
  };

  auto task = assertQuery("c0 <= '-1.05'::REAL");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // c0 >= 11,111.06 - first stride should be skipped based on stats
  task = assertQuery("c0 >= 11111.06::REAL");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedStridesStat(task));

  // c0 between 10'100.06 and 10'500.08 - all strides but second should be
  // skipped based on stats
  task = assertQuery("c0 between 10100.06::REAL AND 10500.08::REAL");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // c0 <= 1,234.005 - all strides but first should be skipped
  task = assertQuery("c0 <= 1234.005::REAL");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));
}

// Test skipping whole file based on statistics
TEST_F(TableScanTest, statsBasedSkipping) {
  auto filePaths = makeFilePaths(1);
  const vector_size_t size = 31'234;
  std::vector<std::string> fruits = {"apple", "banana", "cherry", "grapes"};
  std::vector<StringView> fruitViews = toStringViews(fruits);

  std::vector<std::string> vegetables = {"potato", "pepper", "peas", "squash"};
  std::vector<StringView> vegetableViews = toStringViews(vegetables);

  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<StringView>(
           size, [&fruitViews, &vegetableViews](vector_size_t row) {
             // even stripes have fruits; odd - vegetables
             if ((row / 10'000) % 2 == 0) {
               // introduce a unique value to trigger creation of a stride
               // dictionary
               if (row == 23) {
                 return StringView("b-23");
               }
               return fruitViews[row % fruitViews.size()];
             } else {
               return vegetableViews[row % vegetableViews.size()];
             }
           })});

  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1 -> whole file should be skipped based on stats
  auto subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqual(-1));

  ColumnHandleMap assignments = {{"c1", regularColumn("c1", INTEGER())}};

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(
        std::move(subfieldFilters),
        nullptr,
        "hive_table",
        asRowType(rowVector->type()));
    return TableScanTest::assertQuery(
        PlanBuilder()
            .startTableScan()
            .outputType(ROW({"c1"}, {INTEGER()}))
            .tableHandle(tableHandle)
            .assignments(assignments)
            .endTableScan()
            .planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT c1 FROM tmp WHERE c0 <= -1");

  const auto stats = getTableScanStats(task);
  EXPECT_EQ(0, stats.rawInputRows);
  EXPECT_EQ(0, stats.inputRows);
  EXPECT_EQ(0, stats.outputRows);

  // c2 = "tomato" -> whole file should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c2", equal("tomato"));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 = 'tomato'");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // c2 in ("x", "y") -> whole file should be skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c2", orFilter(equal("x"), equal("y")));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 IN ('x', 'y')");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // c0 >= 11,111 - first stride should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c0", greaterThanOrEqual(11'111));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedStridesStat(task));

  // c2 = "banana" - odd stripes should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c2", equal("banana"));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 = 'banana'");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));

  // c2 in ("banana", "y") -> same as previous
  subfieldFilters =
      singleSubfieldFilter("c2", orFilter(equal("banana"), equal("y")));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 IN ('banana', 'y')");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));

  // c2 = "squash" - even stripes should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c2", equal("squash"));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 = 'squash'");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));

  // c2 in ("banana", "squash") -> no skipping
  subfieldFilters =
      singleSubfieldFilter("c2", orFilter(equal("banana"), equal("squash")));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 IN ('banana', 'squash')");
  EXPECT_EQ(31'234, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(0, getSkippedStridesStat(task));

  // c0 <= 100 AND c0 >= 20'100 - skip second stride
  subfieldFilters = singleSubfieldFilter(
      "c0", bigintOr(lessThanOrEqual(100), greaterThanOrEqual(20'100)));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 <= 100 OR c0 >= 20100");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedStridesStat(task));

  // c0 between 10'100 and 10'500 - all strides but second should be skipped
  // based on stats
  subfieldFilters = singleSubfieldFilter("c0", between(10'100, 10'500));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 between 10100 AND 10500");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // c0 <= 1,234 - all strides but first should be skipped
  subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqual(1'234));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 <= 1234");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // c0 >= 10234 AND c1 <= 20345 - first and last strides should be skipped
  subfieldFilters = SubfieldFiltersBuilder()
                        .add("c0", greaterThanOrEqual(10234))
                        .add("c1", lessThanOrEqual(20345))
                        .build();
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 >= 10234 AND c1 <= 20345");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));
}

// Test skipping files and row groups containing constant values based on
// statistics
TEST_F(TableScanTest, statsBasedSkippingConstants) {
  auto filePaths = makeFilePaths(1);
  const vector_size_t size = 31'234;
  std::vector<std::string> fruits = {"apple", "banana", "cherry", "grapes"};
  std::vector<StringView> fruitViews = toStringViews(fruits);

  // c0 and c2 are constant, c1 and c3 ar constant within any given rowgroup
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto /*row*/) { return 123; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row / 10'000; }),
       makeFlatVector<StringView>(
           size, [&fruitViews](auto /*row*/) { return fruitViews[1]; }),
       makeFlatVector<StringView>(size, [&fruitViews](auto row) {
         return fruitViews[row / 10'000];
       })});

  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  auto assertQuery = [&](const std::string& filter) {
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get())
            .tableScan(asRowType(rowVector->type()), {filter})
            .planNode(),
        filePaths,
        "SELECT * FROM tmp WHERE " + filter);
  };

  // skip whole file
  auto task = assertQuery("c0 in (0, 10, 100, 1000)");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // skip all but first rowgroup
  task = assertQuery("c1 in (0, 10, 100, 1000)");

  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // skip whole file
  task = assertQuery("c2 in ('apple', 'cherry')");

  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // skip all but second rowgroup
  task = assertQuery("c3 in ('banana', 'grapefruit')");

  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));
}

// Test stats-based skipping for the IS NULL filter.
TEST_F(TableScanTest, statsBasedSkippingNulls) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), INTEGER()});
  auto filePaths = makeFilePaths(1);
  const vector_size_t size = 31'234;

  auto noNulls = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  auto someNulls = makeFlatVector<int32_t>(
      size,
      [](auto row) { return row; },
      [](auto row) { return row >= 11'111; });
  auto rowVector = makeRowVector({noNulls, someNulls});

  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  // c0 IS NULL - whole file should be skipped based on stats
  auto assignments = allRegularColumns(rowType);

  auto assertQuery = [&](const std::string& filter) {
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, {filter}).planNode(),
        filePaths,
        "SELECT * FROM tmp WHERE " + filter);
  };

  auto task = TableScanTest::assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      filePaths,
      "SELECT * FROM tmp");

  auto stats = getTableScanStats(task);
  EXPECT_EQ(31'234, stats.rawInputRows);
  EXPECT_EQ(31'234, stats.inputRows);
  EXPECT_EQ(31'234, stats.outputRows);
  EXPECT_GT(getTableScanRuntimeStats(task)["totalScanTime"].sum, 0);

  task = assertQuery("c0 IS NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(0, stats.rawInputRows);
  EXPECT_EQ(0, stats.inputRows);
  EXPECT_EQ(0, stats.outputRows);

  // c1 IS NULL - first stride should be skipped based on stats
  task = assertQuery("c1 IS NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(size - 10'000, stats.rawInputRows);
  EXPECT_EQ(size - 11'111, stats.inputRows);
  EXPECT_EQ(size - 11'111, stats.outputRows);

  // c1 IS NOT NULL - 3rd and 4th strides should be skipped based on stats
  task = assertQuery("c1 IS NOT NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(20'000, stats.rawInputRows);
  EXPECT_EQ(11'111, stats.inputRows);
  EXPECT_EQ(11'111, stats.outputRows);
}

// Test skipping whole compression blocks without decompressing these.
TEST_F(TableScanTest, statsBasedSkippingWithoutDecompression) {
  const vector_size_t size = 31'234;

  // Use long, non-repeating strings to ensure there will be multiple
  // compression blocks which can be skipped without decompression.
  std::vector<std::string> strings;
  strings.reserve(size);
  for (auto i = 0; i < size; i++) {
    strings.emplace_back(
        fmt::format("com.facebook.presto.orc.stream.{:05}", i));
  }

  auto rowVector = makeRowVector({makeFlatVector(strings)});

  auto filePaths = makeFilePaths(1);
  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  // Skip 1st row group.
  auto assertQuery = [&](const std::string& filter) {
    auto rowType = asRowType(rowVector->type());
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get()).tableScan(rowType, {filter}).planNode(),
        filePaths,
        "SELECT * FROM tmp WHERE " + filter);
  };

  auto task = assertQuery("c0 >= 'com.facebook.presto.orc.stream.11111'");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // Skip 2nd row group.
  task = assertQuery(
      "c0 <= 'com.facebook.presto.orc.stream.01234' or c0 >= 'com.facebook.presto.orc.stream.20123'");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // Skip first 3 row groups.
  task = assertQuery("c0 >= 'com.facebook.presto.orc.stream.30123'");
  EXPECT_EQ(size - 30'000, getTableScanStats(task).rawInputRows);
}

// Test skipping whole compression blocks without decompressing these.
TEST_F(TableScanTest, filterBasedSkippingWithoutDecompression) {
  const vector_size_t size = 31'234;

  // Use long, non-repeating strings to ensure there will be multiple
  // compression blocks which can be skipped without decompression.
  std::vector<std::string> strings;
  strings.reserve(size);
  for (auto i = 0; i < size; i++) {
    strings.emplace_back(
        fmt::format("com.facebook.presto.orc.stream.{:05}", i));
  }

  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector(strings)});

  auto rowType = asRowType(rowVector->type());

  auto filePaths = makeFilePaths(1);
  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  auto assertQuery = [&](const std::string& remainingFilter) {
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, {}, remainingFilter).planNode(),
        filePaths,
        "SELECT * FROM tmp WHERE " + remainingFilter);
  };

  auto task = assertQuery("c0 % 11111 = 7");
  EXPECT_EQ(size, getTableScanStats(task).rawInputRows);
}

// Test stats-based skipping for numeric columns (integers, floats and booleans)
// that don't have filters themselves. Skipping is driven by a single bigint
// column.
TEST_F(TableScanTest, statsBasedSkippingNumerics) {
  const vector_size_t size = 31'234;

  // Make a vector of all possible integer and floating point types.
  // First column is a row number used to drive skipping.
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       // integer, floating point and boolean columns without nulls
       makeFlatVector<int8_t>(size, [](auto row) { return row % 7; }),
       makeFlatVector<int16_t>(size, [](auto row) { return row % 39; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row % 12'345; }),
       makeFlatVector<float>(size, [](auto row) { return row * 0.1; }),
       makeFlatVector<double>(size, [](auto row) { return row * 1.3; }),
       makeFlatVector<bool>(size, [](auto row) { return row % 11 == 0; }),
       // with nulls
       makeFlatVector<int8_t>(
           size, [](auto row) { return row % 7; }, nullEvery(5)),
       makeFlatVector<int16_t>(
           size, [](auto row) { return row % 39; }, nullEvery(7)),
       makeFlatVector<int32_t>(
           size, [](auto row) { return row; }, nullEvery(11)),
       makeFlatVector<int64_t>(
           size, [](auto row) { return row % 12'345; }, nullEvery(13)),
       makeFlatVector<float>(
           size, [](auto row) { return row * 0.1; }, nullEvery(17)),
       makeFlatVector<double>(
           size, [](auto row) { return row * 1.3; }, nullEvery(19)),
       makeFlatVector<bool>(
           size, [](auto row) { return row % 11 == 0; }, nullEvery(23))});

  auto filePaths = makeFilePaths(1);
  writeToFile(filePaths[0]->path, rowVector);
  createDuckDbTable({rowVector});

  // Skip whole file.
  auto assertQuery = [&](const std::string& filter) {
    auto rowType = asRowType(rowVector->type());
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get()).tableScan(rowType, {filter}).planNode(),
        filePaths,
        "SELECT * FROM tmp WHERE " + filter);
  };

  auto task = assertQuery("c0 <= -1");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);

  // Skip 1st rowgroup.
  task = assertQuery("c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // Skip 2nd rowgroup.
  task = assertQuery("c0 <= 1000 OR c0 >= 23456");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // Skip last 2 rowgroups.
  task = assertQuery("c0 >= 20123");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
}

// Test stats-based skipping for list and map columns that don't have
// filters themselves. Skipping is driven by a single bigint column.
TEST_F(TableScanTest, statsBasedSkippingComplexTypes) {
  const vector_size_t size = 31'234;

  // Make a vector of all possible integer and floating point types.
  // First column is a row number used to drive skipping.
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       // array, no nulls
       vectorMaker_.arrayVector<int32_t>(
           size,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return row * 2 + index; }),
       // array, some nulls
       vectorMaker_.arrayVector<int32_t>(
           size,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return row * 2 + index; },
           nullEvery(7)),
       // map, no nulls
       vectorMaker_.mapVector<int64_t, double>(
           size,
           [](auto row) { return row % 5 + 1; },
           [](auto /*row*/, auto index) { return index; },
           [](auto row, auto index) { return row * 2 + index + 0.01; }),
       // map, some nulls
       vectorMaker_.mapVector<int64_t, double>(
           size,
           [](auto row) { return row % 5 + 1; },
           [](auto /*row*/, auto index) { return index; },
           [](auto row, auto index) { return row * 2 + index + 0.01; },
           nullEvery(11))});

  auto filePaths = makeFilePaths(1);
  writeToFile(filePaths[0]->path, rowVector);
  // TODO Figure out how to create DuckDB tables with columns of complex types
  // For now, using 1st element of the array and map element for key zero.
  createDuckDbTable({makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row * 2; }),
       makeFlatVector<int32_t>(
           size, [](auto row) { return row * 2; }, nullEvery(7)),
       makeFlatVector<double>(size, [](auto row) { return row * 2 + 0.01; }),
       makeFlatVector<double>(
           size, [](auto row) { return row * 2 + 0.01; }, nullEvery(11))})});

  // skip whole file
  auto assertQuery = [&](const std::string& filter) {
    auto rowType = asRowType(rowVector->type());
    return TableScanTest::assertQuery(
        PlanBuilder(pool_.get())
            .tableScan(rowType, {filter})
            // Project row-number column, first element of each array and map
            // elements for key zero.
            .project({"c0", "c1[1]", "c2[1]", "c3[0]", "c4[0]"})
            .planNode(),
        filePaths,
        "SELECT * FROM tmp WHERE " + filter);
  };

  auto task = assertQuery("c0 <= -1");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);

  // skip 1st rowgroup
  task = assertQuery("c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip 2nd rowgroup
  task = assertQuery("c0 <= 1000 OR c0 >= 23456");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip last 2 rowgroups
  task = assertQuery("c0 >= 20123");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
}

/// Test the interaction between stats-based and regular skipping for lists and
/// maps.
TEST_F(TableScanTest, statsBasedAndRegularSkippingComplexTypes) {
  const vector_size_t size = 31'234;

  // Orchestrate the case where the nested reader of a list/map gets behind the
  // top-level reader. This happens if top level reader skips a bunch of
  // non-null rows and the remaining rows are all nulls. Seeking to the next row
  // group catches up the nested reader automatically. The top-level reader must
  // account for that and not try to catch up the reader. If it does, it will
  // advance reader too much and cause read corruption.

  // only first few hundreds of rows in a row group are not null
  auto isNullAt = [](auto row) { return row % 10'000 > 500; };
  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      vectorMaker_.arrayVector<int32_t>(
          size,
          [](auto row) { return row % 5 + 1; },
          [](auto row, auto index) { return row * 2 + index; },
          isNullAt),
      vectorMaker_.mapVector<int64_t, double>(
          size,
          [](auto row) { return row % 5 + 1; },
          [](auto /* row */, auto index) { return index; },
          [](auto row, auto index) { return row * 3 + index + 0.1; },
          isNullAt),
  });

  auto filePaths = makeFilePaths(1);
  writeToFile(filePaths[0]->path, rowVector);

  createDuckDbTable({makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row * 2; }, isNullAt),
      makeFlatVector<double>(
          size, [](auto row) { return row * 3 + 0.1; }, isNullAt),
  })});

  auto filters = singleSubfieldFilter(
      "c0",
      bigintOr(
          lessThanOrEqual(10), between(600, 650), greaterThanOrEqual(21'234)));

  auto rowType = asRowType(rowVector->type());

  auto op =
      PlanBuilder(pool_.get())
          .tableScan(
              rowType, {"c0 <= 10 OR c0 between 600 AND 650 OR c0 >= 21234"})
          .project({"c0", "c1[1]", "c2[0]"})
          .planNode();

  assertQuery(
      op,
      filePaths,
      "SELECT * FROM tmp WHERE c0 <= 10 OR c0 between 600 AND 650 OR c0 >= 21234");
}

TEST_F(TableScanTest, filterPushdown) {
  auto rowType =
      ROW({"c0", "c1", "c2", "c3"}, {TINYINT(), BIGINT(), DOUBLE(), BOOLEAN()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000, rowType);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  createDuckDbTable(vectors);

  // c1 >= 0 or null and c3 is true
  SubfieldFilters subfieldFilters =
      SubfieldFiltersBuilder()
          .add("c1", greaterThanOrEqual(0, true))
          .add("c3", std::make_unique<common::BoolValue>(true, false))
          .build();
  auto tableHandle = makeTableHandle(
      std::move(subfieldFilters), nullptr, "hive_table", rowType);

  auto assignments = allRegularColumns(rowType);

  auto task = assertQuery(
      PlanBuilder()
          .startTableScan()
          .outputType(ROW({"c1", "c3", "c0"}, {BIGINT(), BOOLEAN(), TINYINT()}))
          .tableHandle(tableHandle)
          .assignments(assignments)
          .endTableScan()
          .planNode(),
      filePaths,
      "SELECT c1, c3, c0 FROM tmp WHERE (c1 >= 0 OR c1 IS NULL) AND c3");

  auto tableScanStats = getTableScanStats(task);
  EXPECT_EQ(tableScanStats.rawInputRows, 10'000);
  EXPECT_LT(tableScanStats.inputRows, tableScanStats.rawInputRows);
  EXPECT_EQ(tableScanStats.inputRows, tableScanStats.outputRows);

  // Repeat the same but do not project out the filtered columns.
  assignments.clear();
  assignments["c0"] = regularColumn("c0", TINYINT());
  assertQuery(
      PlanBuilder()
          .startTableScan()
          .outputType(ROW({"c0"}, {TINYINT()}))
          .tableHandle(tableHandle)
          .assignments(assignments)
          .endTableScan()
          .planNode(),
      filePaths,
      "SELECT c0 FROM tmp WHERE (c1 >= 0 OR c1 IS NULL) AND c3");

  // Do the same for count, no columns projected out.
  assignments.clear();
  assertQuery(
      PlanBuilder()
          .startTableScan()
          .outputType(ROW({}, {}))
          .tableHandle(tableHandle)
          .assignments(assignments)
          .endTableScan()
          .singleAggregation({}, {"sum(1)"})
          .planNode(),
      filePaths,
      "SELECT count(*) FROM tmp WHERE (c1 >= 0 OR c1 IS NULL) AND c3");

  // Do the same for count, no filter, no projections.
  assignments.clear();
  subfieldFilters.clear(); // Explicitly clear this.
  tableHandle = makeTableHandle(std::move(subfieldFilters));
  assertQuery(
      PlanBuilder()
          .startTableScan()
          .outputType(ROW({}, {}))
          .tableHandle(tableHandle)
          .assignments(assignments)
          .endTableScan()
          .singleAggregation({}, {"sum(1)"})
          .planNode(),
      filePaths,
      "SELECT count(*) FROM tmp");
}

TEST_F(TableScanTest, path) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto filePath = makeFilePaths(1)[0];
  auto vector = makeVectors(1, 1'000, rowType)[0];
  writeToFile(filePath->path, vector);
  createDuckDbTable({vector});

  static const char* kPath = "$path";

  auto assignments = allRegularColumns(rowType);
  assignments[kPath] = synthesizedColumn(kPath, VARCHAR());

  auto pathValue = fmt::format("file:{}", filePath->path);
  auto typeWithPath = ROW({kPath, "a"}, {VARCHAR(), BIGINT()});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(typeWithPath)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  assertQuery(
      op, {filePath}, fmt::format("SELECT '{}', * FROM tmp", pathValue));

  // use $path in a filter, but don't project it out
  auto tableHandle = makeTableHandle(
      SubfieldFilters{},
      parseExpr(fmt::format("\"{}\" = '{}'", kPath, pathValue), typeWithPath));
  op = PlanBuilder()
           .startTableScan()
           .outputType(rowType)
           .tableHandle(tableHandle)
           .assignments(assignments)
           .endTableScan()
           .planNode();
  assertQuery(op, {filePath}, "SELECT * FROM tmp");

  // use $path in a filter and project it out
  op = PlanBuilder()
           .startTableScan()
           .outputType(typeWithPath)
           .assignments(assignments)
           .endTableScan()
           .planNode();
  assertQuery(
      op, {filePath}, fmt::format("SELECT '{}', * FROM tmp", pathValue));
}

TEST_F(TableScanTest, fileSizeAndModifiedTime) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto filePath = makeFilePaths(1)[0];
  auto vector = makeVectors(1, 10, rowType)[0];
  writeToFile(filePath->path, vector);
  createDuckDbTable({vector});

  static const char* kSize = "$file_size";
  static const char* kModifiedTime = "$file_modified_time";

  auto allColumns =
      ROW({"a", kSize, kModifiedTime}, {BIGINT(), BIGINT(), BIGINT()});

  auto assignments = allRegularColumns(rowType);
  assignments[kSize] = synthesizedColumn(kSize, BIGINT());
  assignments[kModifiedTime] = synthesizedColumn(kModifiedTime, BIGINT());

  auto fileSizeValue = fmt::format("{}", filePath->fileSize());
  auto fileTimeValue = fmt::format("{}", filePath->fileModifiedTime());

  // Select and project both '$file_size', '$file_modified_time'.
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(allColumns)
                .dataColumns(allColumns)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  assertQuery(
      op,
      {filePath},
      fmt::format("SELECT *, {}, {} FROM tmp", fileSizeValue, fileTimeValue));

  auto filterTest = [&](const std::string& filter) {
    auto tableHandle = makeTableHandle(
        SubfieldFilters{},
        parseExpr(filter, allColumns),
        "hive_table",
        allColumns);

    // Use synthesized column in a filter but don't project it.
    op = PlanBuilder()
             .startTableScan()
             .outputType(rowType)
             .dataColumns(allColumns)
             .tableHandle(tableHandle)
             .assignments(assignments)
             .endTableScan()
             .planNode();
    assertQuery(op, {filePath}, "SELECT * FROM tmp");

    // Use synthesized column in a filter and project it out.
    op = PlanBuilder()
             .startTableScan()
             .outputType(allColumns)
             .dataColumns(allColumns)
             .tableHandle(tableHandle)
             .assignments(assignments)
             .endTableScan()
             .planNode();
    assertQuery(
        op,
        {filePath},
        fmt::format("SELECT *, {}, {} FROM tmp", fileSizeValue, fileTimeValue));
  };

  filterTest(fmt::format("\"{}\" = {}", kSize, fileSizeValue));
  filterTest(fmt::format("\"{}\" = {}", kModifiedTime, fileTimeValue));
}

TEST_F(TableScanTest, bucket) {
  vector_size_t size = 1'000;
  int numBatches = 5;

  std::vector<RowVectorPtr> rowVectors;
  rowVectors.reserve(numBatches);

  auto filePaths = makeFilePaths(numBatches);

  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(numBatches);

  std::vector<int> buckets = {10, 12, 15, 16, 27};

  for (auto i = 0; i < numBatches; i++) {
    auto bucket = buckets[i];
    auto rowVector = makeRowVector(
        {makeFlatVector<int32_t>(size, [&](auto /*row*/) { return bucket; }),
         makeFlatVector<int64_t>(
             size, [&](auto row) { return bucket + row; })});
    writeToFile(filePaths[i]->path, rowVector);
    rowVectors.emplace_back(rowVector);

    splits.emplace_back(HiveConnectorSplitBuilder(filePaths[i]->path)
                            .tableBucketNumber(bucket)
                            .build());
  }

  createDuckDbTable(rowVectors);

  static const char* kBucket = "$bucket";
  auto rowType = asRowType(rowVectors.front()->type());

  auto assignments = allRegularColumns(rowType);
  assignments[kBucket] = synthesizedColumn(kBucket, INTEGER());

  // Query that spans on all buckets
  auto typeWithBucket =
      ROW({kBucket, "c0", "c1"}, {INTEGER(), INTEGER(), BIGINT()});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(typeWithBucket)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  OperatorTestBase::assertQuery(op, splits, "SELECT c0, * FROM tmp");

  for (int i = 0; i < buckets.size(); ++i) {
    int bucketValue = buckets[i];
    auto hsplit = HiveConnectorSplitBuilder(filePaths[i]->path)
                      .tableBucketNumber(bucketValue)
                      .build();

    // Filter on bucket and filter on first column should produce
    // identical result for each split
    op = PlanBuilder()
             .startTableScan()
             .outputType(typeWithBucket)
             .assignments(assignments)
             .endTableScan()
             .planNode();
    assertQuery(
        op,
        hsplit,
        fmt::format(
            "SELECT {}, * FROM tmp where c0 = {}", bucketValue, bucketValue));

    // Filter on bucket column, but don't project it out
    auto rowTypes = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});
    hsplit = HiveConnectorSplitBuilder(filePaths[i]->path)
                 .tableBucketNumber(bucketValue)
                 .build();
    op = PlanBuilder()
             .startTableScan()
             .outputType(rowTypes)
             .assignments(assignments)
             .endTableScan()
             .planNode();
    assertQuery(
        op,
        hsplit,
        fmt::format("SELECT * FROM tmp where c0 = {}", bucketValue));
  }
}

TEST_F(TableScanTest, integerNotEqualFilter) {
  auto rowType = ROW(
      {"c0", "c1", "c2", "c3"}, {TINYINT(), SMALLINT(), INTEGER(), BIGINT()});

  const vector_size_t size = 1'000;

  // Create four columns of various integer types for testing the != filter
  // first two columns test normal filtering against TINYINT/SMALLINT
  // third column tests negative numbers and INTEGER type
  // fourth column tests nulls and BIGINT type
  auto rowVector = makeRowVector(
      {makeFlatVector<int8_t>(size, [](auto row) { return row % 15; }),
       makeFlatVector<int16_t>(size, [](auto row) { return row % 122; }),
       makeFlatVector<int32_t>(size, [](auto row) { return (row % 97) * -1; }),
       makeFlatVector<int64_t>(
           size, [](auto row) { return row % 210; }, nullEvery(11))});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, rowVector);
  createDuckDbTable({rowVector});

  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c0 != 0::TINYINT"})
          .planNode(),
      {filePath},
      "SELECT * FROM tmp WHERE c0 != 0");

  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c1 != 1::SMALLINT"})
          .planNode(),
      {filePath},
      "SELECT * FROM tmp WHERE c1 != 1");

  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c2 != (-2)::INTEGER"})
          .planNode(),
      {filePath},
      "SELECT * FROM tmp WHERE c2 != -2");

  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c3 != 3::BIGINT"})
          .planNode(),
      {filePath},
      "SELECT * FROM tmp WHERE c3 != 3");
}

TEST_F(TableScanTest, floatingPointNotEqualFilter) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  auto outputType = ROW({"c4"}, {DOUBLE()});
  auto op =
      PlanBuilder(pool_.get()).tableScan(outputType, {"c4 != 0.0"}).planNode();
  assertQuery(op, {filePath}, "SELECT c4 FROM tmp WHERE c4 != 0.0");

  outputType = ROW({"c3"}, {REAL()});
  op = PlanBuilder(pool_.get())
           .tableScan(outputType, {"c3 != cast(0.0 as REAL)"})
           .planNode();
  assertQuery(
      op, {filePath}, "SELECT c3 FROM tmp WHERE c3 != cast(0.0 as REAL)");
}

TEST_F(TableScanTest, stringNotEqualFilter) {
  auto rowType = ROW({"c0", "c1"}, {VARCHAR(), VARCHAR()});

  const vector_size_t size = 1'000;

  std::vector<StringView> fruitViews = {"apple", "banana", "cherry", "grapes"};
  // ensure empty string is handled properly
  std::vector<StringView> colourViews = {"red", "blue", "green", "purple", ""};
  // create two columns of strings to test against, c0 with some nulls and
  // c1 with some empty strings
  auto rowVector = makeRowVector(
      {makeFlatVector<StringView>(
           size,
           [&fruitViews](auto row) {
             return fruitViews[row % fruitViews.size()];
           },
           nullEvery(15)),
       makeFlatVector<StringView>(size, [&colourViews](auto row) {
         return colourViews[row % colourViews.size()];
       })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, rowVector);
  createDuckDbTable({rowVector});

  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c0 != 'banana'"})
          .planNode(),
      {filePath},
      "SELECT * FROM tmp WHERE c0 != 'banana'");

  assertQuery(
      PlanBuilder(pool_.get()).tableScan(rowType, {"c1 != ''"}).planNode(),
      {filePath},
      "SELECT * FROM tmp WHERE c1 != ''");
}

TEST_F(TableScanTest, arrayIsNullFilter) {
  std::vector<RowVectorPtr> vectors(3);
  auto filePaths = makeFilePaths(vectors.size());
  for (int i = 0; i < vectors.size(); ++i) {
    auto isNullAt = [&](vector_size_t j) {
      // Non-nulls for first file, all nulls for second file, half nulls for
      // third file.
      return i == 0 ? false : i == 1 ? true : j % 2 != 0;
    };
    auto c0 = makeArrayVector<int64_t>(
        100,
        [](vector_size_t i) { return 3 + i % 3; },
        [](vector_size_t, vector_size_t j) { return j; },
        isNullAt);
    vectors[i] = makeRowVector({"c0"}, {c0});
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  createDuckDbTable(vectors);
  auto rowType = asRowType(vectors[0]->type());
  auto makePlan = [&](const std::vector<std::string>& filters) {
    return PlanBuilder().tableScan(rowType, filters).planNode();
  };
  assertQuery(
      makePlan({"c0 is not null"}),
      filePaths,
      "SELECT * FROM tmp WHERE c0 is not null");
  assertQuery(
      makePlan({"c0 is null"}),
      filePaths,
      "SELECT * FROM tmp WHERE c0 is null");
}

TEST_F(TableScanTest, mapIsNullFilter) {
  std::vector<RowVectorPtr> vectors(3);
  auto filePaths = makeFilePaths(vectors.size());
  for (int i = 0; i < vectors.size(); ++i) {
    auto isNullAt = [&](vector_size_t j) {
      // Non-nulls for first file, all nulls for second file, half nulls for
      // third file.
      return i == 0 ? false : i == 1 ? true : j % 2 != 0;
    };
    auto c0 = makeMapVector<int64_t, int64_t>(
        100,
        [](vector_size_t i) { return 3 + i % 3; },
        [](vector_size_t j) { return j; },
        [](vector_size_t j) { return 2 * j; },
        isNullAt);
    vectors[i] = makeRowVector({"c0"}, {c0});
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  createDuckDbTable(vectors);
  auto rowType = asRowType(vectors[0]->type());
  auto makePlan = [&](const std::vector<std::string>& filters) {
    return PlanBuilder().tableScan(rowType, filters).planNode();
  };
  assertQuery(
      makePlan({"c0 is not null"}),
      filePaths,
      "SELECT * FROM tmp WHERE c0 is not null");
  assertQuery(
      makePlan({"c0 is null"}),
      filePaths,
      "SELECT * FROM tmp WHERE c0 is null");
}

TEST_F(TableScanTest, remainingFilter) {
  auto rowType = ROW(
      {"c0", "c1", "c2", "c3"}, {INTEGER(), INTEGER(), DOUBLE(), BOOLEAN()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000, rowType);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  createDuckDbTable(vectors);

  assertQuery(
      PlanBuilder(pool_.get())
          .startTableScan()
          .outputType(rowType)
          .remainingFilter("c1 > c0")
          .endTableScan()
          .planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE c1 > c0");

  // filter that never passes
  assertQuery(
      PlanBuilder(pool_.get())
          .startTableScan()
          .outputType(rowType)
          .remainingFilter("c1 % 5 = 6")
          .endTableScan()
          .planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE c1 % 5 = 6");

  // range filter + remaining filter: c0 >= 0 AND c1 > c0
  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c0 >= 0::INTEGER"}, "c1 > c0")
          .planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE c1 > c0 AND c0 >= 0");

  // Remaining filter uses columns that are not used otherwise.
  ColumnHandleMap assignments = {{"c2", regularColumn("c2", DOUBLE())}};

  assertQuery(
      PlanBuilder(pool_.get())
          .startTableScan()
          .outputType(ROW({"c2"}, {DOUBLE()}))
          .remainingFilter("c1 > c0")
          .dataColumns(rowType)
          .assignments(assignments)
          .endTableScan()
          .planNode(),
      filePaths,
      "SELECT c2 FROM tmp WHERE c1 > c0");

  // Remaining filter uses one column that is used elsewhere (is projected out)
  // and another column that is not used anywhere else.
  assignments = {
      {"c1", regularColumn("c1", INTEGER())},
      {"c2", regularColumn("c2", DOUBLE())}};

  assertQuery(
      PlanBuilder(pool_.get())
          .startTableScan()
          .outputType(ROW({"c1", "c2"}, {INTEGER(), DOUBLE()}))
          .remainingFilter("c1 > c0")
          .dataColumns(rowType)
          .assignments(assignments)
          .endTableScan()
          .planNode(),
      filePaths,
      "SELECT c1, c2 FROM tmp WHERE c1 > c0");

  // Remaining filter converted into tuple domain.
  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {}, "not (c0 > 0::INTEGER or c1 > 0::INTEGER)")
          .planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE not (c0 > 0 or c1 > 0)");
  assertQuery(
      PlanBuilder(pool_.get())
          .tableScan(rowType, {}, "not (c0 > 0::INTEGER or c1 > c0)")
          .planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE not (c0 > 0 or c1 > c0)");
}

TEST_F(TableScanTest, remainingFilterSkippedStrides) {
  auto rowType = ROW({{"c0", BIGINT()}, {"c1", BIGINT()}});
  std::vector<RowVectorPtr> vectors(3);
  auto filePaths = makeFilePaths(vectors.size());
  for (int j = 0; j < vectors.size(); ++j) {
    auto c =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), 100, pool_.get());
    for (int i = 0; i < c->size(); ++i) {
      c->set(i, j);
    }
    vectors[j] = std::make_shared<RowVector>(
        pool_.get(),
        rowType,
        nullptr,
        c->size(),
        std::vector<VectorPtr>({c, c}));
    writeToFile(filePaths[j]->path, vectors[j]);
  }
  createDuckDbTable(vectors);
  core::PlanNodeId tableScanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(rowType, {}, "c0 = 0 or c1 = 2")
                  .capturePlanNodeId(tableScanNodeId)
                  .planNode();
  auto task =
      assertQuery(plan, filePaths, "SELECT * FROM tmp WHERE c0 = 0 or c1 = 2");
  auto skippedStrides = toPlanStats(task->taskStats())
                            .at(tableScanNodeId)
                            .customStats.at("skippedStrides");
  EXPECT_EQ(skippedStrides.count, 1);
  EXPECT_EQ(skippedStrides.sum, 1);
}

TEST_F(TableScanTest, skipStridesForParentNulls) {
  auto b = makeFlatVector<int64_t>(10'000, folly::identity);
  auto a = makeRowVector({"b"}, {b}, [](auto i) { return i % 2 == 0; });
  auto vector = makeRowVector({"a"}, {a});
  auto file = TempFilePath::create();
  writeToFile(file->path, {vector});
  auto plan = PlanBuilder()
                  .tableScan(asRowType(vector->type()), {"a.b IS NULL"})
                  .planNode();
  auto split = makeHiveConnectorSplit(file->path);
  auto result = AssertQueryBuilder(plan).split(split).copyResults(pool());
  ASSERT_EQ(result->size(), 5000);
}

TEST_F(TableScanTest, randomSample) {
  random::setSeed(42);
  auto column = makeFlatVector<double>(
      100, [](auto /*i*/) { return folly::Random::randDouble01(); });
  auto rows = makeRowVector({column});
  auto rowType = asRowType(rows->type());
  std::vector<std::shared_ptr<TempFilePath>> files;
  auto writeConfig = std::make_shared<dwrf::Config>();
  writeConfig->set<uint64_t>(
      dwrf::Config::STRIPE_SIZE, rows->size() * sizeof(double));
  int numTotalRows = 0;
  for (int i = 0; i < 10; ++i) {
    auto file = TempFilePath::create();
    if (i % 2 == 0) {
      std::vector<RowVectorPtr> vectors;
      for (int j = 0; j < 100; ++j) {
        vectors.push_back(rows);
      }
      writeToFile(file->path, vectors, writeConfig);
      numTotalRows += rows->size() * vectors.size();
    } else {
      writeToFile(file->path, {rows}, writeConfig);
      numTotalRows += rows->size();
    }
    files.push_back(file);
  }
  CursorParameters params;
  params.planNode =
      PlanBuilder().tableScan(rowType, {}, "rand() < 0.01").planNode();
  auto cursor = TaskCursor::create(params);
  for (auto& file : files) {
    cursor->task()->addSplit("0", makeHiveSplit(file->path));
  }
  cursor->task()->noMoreSplits("0");
  int numRows = 0;
  while (cursor->moveNext()) {
    auto result = cursor->current();
    VELOX_CHECK_GT(result->size(), 0);
    numRows += result->size();
  }
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
  ASSERT_GT(getSkippedStridesStat(cursor->task()), 0);
  double expectedNumRows = 0.01 * numTotalRows;
  ASSERT_LT(abs(numRows - expectedNumRows) / expectedNumRows, 0.1);
}

/// Test the handling of constant remaining filter results which occur when
/// filter input is a dictionary vector with all indices being the same (i.e.
/// DictionaryVector::isConstant() == true).
TEST_F(TableScanTest, remainingFilterConstantResult) {
  /// Make 2 batches of 10K rows each. 10K is the default batch size in
  /// TableScan. Use a pushed down and a remaining filter. Make it so that
  /// pushed down filter passes only for a subset of rows from each batch, e.g.
  /// pass for the first 100 rows in the first batch and for the first 5 rows
  /// in the second batch. Then, use remaining filter that passes for a subset
  /// of rows that passed the pushed down filter in the first batch and all rows
  /// in the second batch. Make sure that remaining filter doesn't pass on the
  /// first 5 rows in the first batch, e.g. passing row numbers for the first
  /// batch start with 11. Also, make sure that remaining filter inputs for the
  /// second batch are dictionary encoded and constant. This makes it so that
  /// first batch is producing results using dictionary encoding with indices
  /// starting at 11 and second batch cannot re-use these indices as they point
  /// past the vector size (5).
  vector_size_t size = 10'000;
  std::vector<RowVectorPtr> data = {
      makeRowVector({
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              size,
              [](auto row) {
                return StringView::makeInline(fmt::format("{}", row % 23));
              }),
      }),
      makeRowVector({
          makeFlatVector<int64_t>(
              size, [](auto row) { return row < 5 ? row : 1000; }),
          makeFlatVector<StringView>(size, [](auto row) { return "15"_sv; }),
      }),
  };

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, data);
  createDuckDbTable(data);

  auto rowType = asRowType(data[0]->type());

  auto plan =
      PlanBuilder(pool_.get())
          .tableScan(rowType, {"c0 < 100"}, "cast(c1 as bigint) % 23 > 10")
          .planNode();

  assertQuery(
      plan,
      {filePath},
      "SELECT * FROM tmp WHERE c0 < 100 AND c1::bigint % 23 > 10");
}

TEST_F(TableScanTest, aggregationPushdown) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  // Get the number of values processed via aggregation pushdown into scan.
  auto loadedToValueHook = [](const std::shared_ptr<Task> task,
                              int operatorIndex = 0) {
    auto stats = task->taskStats()
                     .pipelineStats[0]
                     .operatorStats[operatorIndex]
                     .runtimeStats;
    auto it = stats.find("loadedToValueHook");
    return it != stats.end() ? it->second.sum : 0;
  };

  auto op =
      PlanBuilder()
          .tableScan(rowType_)
          .partialAggregation(
              {"c5"}, {"max(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(c4)"})
          .planNode();

  auto task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0), sum(c1), sum(c2), sum(c3), sum(c4) FROM tmp group by c5");
  // 5 aggregates processing 10K rows each via pushdown.
  EXPECT_EQ(5 * 10'000, loadedToValueHook(task, 1));

  op = PlanBuilder()
           .tableScan(rowType_)
           .singleAggregation(
               {"c5"}, {"max(c0)", "max(c1)", "max(c2)", "max(c3)", "max(c4)"})
           .planNode();

  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0), max(c1), max(c2), max(c3), max(c4) FROM tmp group by c5");
  // 5 aggregates processing 10K rows each via pushdown.
  EXPECT_EQ(5 * 10'000, loadedToValueHook(task, 1));

  op = PlanBuilder()
           .tableScan(rowType_)
           .singleAggregation(
               {"c5"}, {"min(c0)", "min(c1)", "min(c2)", "min(c3)", "min(c4)"})
           .planNode();

  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, min(c0), min(c1), min(c2), min(c3), min(c4) FROM tmp group by c5");
  // 5 aggregates processing 10K rows each via pushdown.
  EXPECT_EQ(5 * 10'000, loadedToValueHook(task, 1));

  // Pushdown should also happen if there is a FilterProject node that doesn't
  // touch columns being aggregated
  op = PlanBuilder()
           .tableScan(rowType_)
           .project({"c0 % 5", "c1"})
           .singleAggregation({"p0"}, {"sum(c1)"})
           .planNode();

  task =
      assertQuery(op, {filePath}, "SELECT c0 % 5, sum(c1) FROM tmp group by 1");
  // LazyVector stats are reported on the closest operator upstream of the
  // aggregation, e.g. project operator.
  EXPECT_EQ(10'000, loadedToValueHook(task, 2));

  // Add remaining filter to scan to expose LazyVectors wrapped in Dictionary to
  // aggregation.
  op = PlanBuilder()
           .startTableScan()
           .outputType(rowType_)
           .remainingFilter("length(c5) % 2 = 0")
           .endTableScan()
           .singleAggregation({"c5"}, {"max(c0)"})
           .planNode();
  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0) FROM tmp WHERE length(c5) % 2 = 0 GROUP BY c5");
  // Values in rows that passed the filter should be aggregated via pushdown.
  EXPECT_GT(loadedToValueHook(task, 1), 0);
  EXPECT_LT(loadedToValueHook(task, 1), 10'000);

  // No pushdown if two aggregates use the same column or a column is not a
  // LazyVector
  op = PlanBuilder()
           .tableScan(rowType_)
           .singleAggregation({"c5"}, {"min(c0)", "max(c0)"})
           .planNode();
  task = assertQuery(
      op, {filePath}, "SELECT c5, min(c0), max(c0) FROM tmp GROUP BY 1");
  EXPECT_EQ(0, loadedToValueHook(task));

  op = PlanBuilder()
           .tableScan(rowType_)
           .project({"c5", "c0", "c0 + c1 AS c0_plus_c1"})
           .singleAggregation({"c5"}, {"min(c0)", "max(c0_plus_c1)"})
           .planNode();
  task = assertQuery(
      op, {filePath}, "SELECT c5, min(c0), max(c0 + c1) FROM tmp GROUP BY 1");
  EXPECT_EQ(0, loadedToValueHook(task));

  op = PlanBuilder()
           .tableScan(rowType_)
           .project({"c5", "c0 + 1 as a", "c1 + 2 as b", "c2 + 3 as c"})
           .singleAggregation({"c5"}, {"min(a)", "max(b)", "sum(c)"})
           .planNode();
  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, min(c0 + 1), max(c1 + 2), sum(c2 + 3) FROM tmp GROUP BY 1");
  EXPECT_EQ(0, loadedToValueHook(task));
}

TEST_F(TableScanTest, bitwiseAggregationPushdown) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .tableScan(rowType_)
                .singleAggregation(
                    {"c5"},
                    {"bitwise_and_agg(c0)",
                     "bitwise_and_agg(c1)",
                     "bitwise_and_agg(c2)",
                     "bitwise_and_agg(c6)"})
                .planNode();

  assertQuery(
      op,
      {filePath},
      "SELECT c5, bit_and(c0), bit_and(c1), bit_and(c2), bit_and(c6) FROM tmp group by c5");

  op = PlanBuilder()
           .tableScan(rowType_)
           .singleAggregation(
               {"c5"},
               {"bitwise_or_agg(c0)",
                "bitwise_or_agg(c1)",
                "bitwise_or_agg(c2)",
                "bitwise_or_agg(c6)"})
           .planNode();

  assertQuery(
      op,
      {filePath},
      "SELECT c5, bit_or(c0), bit_or(c1), bit_or(c2), bit_or(c6) FROM tmp group by c5");
}

TEST_F(TableScanTest, structLazy) {
  vector_size_t size = 1'000;
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeRowVector({makeMapVector<int64_t, double>(
           size,
           [](auto row) { return row % 3; },
           [](auto row) { return row; },
           [](auto row) { return row * 0.1; })})});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});

  // Exclude struct columns as DuckDB doesn't support complex types yet.
  createDuckDbTable(
      {makeRowVector({rowVector->childAt(0), rowVector->childAt(1)})});

  auto rowType = asRowType(rowVector->type());
  auto op = PlanBuilder()
                .tableScan(rowType)
                .project({"cardinality(c2.c0)"})
                .planNode();

  assertQuery(op, {filePath}, "select c0 % 3 from tmp");
}

TEST_F(TableScanTest, interleaveLazyEager) {
  constexpr int kSize = 1000;
  auto column = makeRowVector(
      {makeFlatVector<int64_t>(kSize, folly::identity),
       makeRowVector({makeFlatVector<int64_t>(kSize, folly::identity)})});
  auto rows = makeRowVector({column});
  auto rowType = asRowType(rows->type());
  auto lazyFile = TempFilePath::create();
  writeToFile(lazyFile->path, {rows});
  auto rowsWithNulls = makeVectors(1, kSize, rowType);
  int numNonNull = 0;
  for (int i = 0; i < kSize; ++i) {
    auto* c0 = rowsWithNulls[0]->childAt(0)->asUnchecked<RowVector>();
    if (c0->isNullAt(i)) {
      continue;
    }
    auto& c0c0 = c0->asUnchecked<RowVector>()->childAt(0);
    numNonNull += !c0c0->isNullAt(i);
  }
  auto eagerFile = TempFilePath::create();
  writeToFile(eagerFile->path, rowsWithNulls);

  ColumnHandleMap assignments = {{"c0", regularColumn("c0", column->type())}};
  CursorParameters params;
  params.planNode = PlanBuilder()
                        .startTableScan()
                        .outputType(rowType)
                        .subfieldFilter("c0.c0 is not null")
                        .assignments(assignments)
                        .endTableScan()
                        .planNode();
  auto cursor = TaskCursor::create(params);
  cursor->task()->addSplit("0", makeHiveSplit(lazyFile->path));
  cursor->task()->addSplit("0", makeHiveSplit(eagerFile->path));
  cursor->task()->addSplit("0", makeHiveSplit(lazyFile->path));
  cursor->task()->noMoreSplits("0");
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(cursor->moveNext());
    auto result = cursor->current();
    ASSERT_EQ(result->size(), i % 2 == 0 ? kSize : numNonNull);
  }
  ASSERT_FALSE(cursor->moveNext());
}

TEST_F(TableScanTest, lazyVectorAccessTwiceWithDifferentRows) {
  auto data = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 1, 1, std::nullopt}),
      makeNullableFlatVector<int64_t>({0, 1, 2, 3}),
  });

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {data});
  createDuckDbTable({data});

  auto plan =
      PlanBuilder()
          .tableScan(asRowType(data->type()))
          .filter(
              "element_at(array_constructor(c0 + c1, if(c1 >= 0, c1, 0)), 1) > 0")
          .planNode();
  assertQuery(
      plan,
      {filePath},
      "SELECT c0, c1 from tmp where ([c0 + c1, if(c1 >= 0, c1, 0)])[1] > 0");
}

TEST_F(TableScanTest, structInArrayOrMap) {
  vector_size_t size = 1'000;

  auto rowNumbers = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  auto innerRow = makeRowVector({rowNumbers});
  auto offsets = AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  std::iota(rawOffsets, rawOffsets + size, 0);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(size, pool_.get(), 1);
  auto rowVector = makeRowVector(
      {rowNumbers,
       rowNumbers,
       std::make_shared<MapVector>(
           pool_.get(),
           MAP(BIGINT(), innerRow->type()),
           BufferPtr(nullptr),
           size,
           offsets,
           sizes,
           makeFlatVector<int64_t>(size, [](int32_t /*row*/) { return 1; }),
           innerRow),
       std::make_shared<ArrayVector>(
           pool_.get(),
           ARRAY(innerRow->type()),
           BufferPtr(nullptr),
           size,
           offsets,
           sizes,
           innerRow)});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});

  // Exclude struct columns as DuckDB doesn't support complex types yet.
  createDuckDbTable(
      {makeRowVector({rowVector->childAt(0), rowVector->childAt(1)})});

  auto rowType = asRowType(rowVector->type());
  auto op = PlanBuilder()
                .tableScan(rowType)
                .project({"c2[1].c0", "c3[1].c0"})
                .planNode();

  assertQuery(op, {filePath}, "select c0, c0 from tmp");
}

TEST_F(TableScanTest, addSplitsToFailedTask) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(12'000, [](auto row) { return row % 5; })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {data});

  core::PlanNodeId scanNodeId;
  exec::test::CursorParameters params;
  params.planNode = exec::test::PlanBuilder()
                        .tableScan(ROW({"c0"}, {INTEGER()}))
                        .capturePlanNodeId(scanNodeId)
                        .project({"5 / c0"})
                        .planNode();

  auto cursor = exec::test::TaskCursor::create(params);
  cursor->task()->addSplit(scanNodeId, makeHiveSplit(filePath->path));

  EXPECT_THROW(while (cursor->moveNext()){}, VeloxUserError);

  // Verify that splits can be added to the task ever after task has failed.
  // In this case these splits will be ignored.
  cursor->task()->addSplit(scanNodeId, makeHiveSplit(filePath->path));
  cursor->task()->addSplitWithSequence(
      scanNodeId, makeHiveSplit(filePath->path), 20L);
  cursor->task()->setMaxSplitSequenceId(scanNodeId, 20L);
}

TEST_F(TableScanTest, errorInLoadLazy) {
  auto cache = cache::AsyncDataCache::getInstance();
  VELOX_CHECK_NOT_NULL(cache);
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  std::atomic<int32_t> counter = 0;
  cache->setVerifyHook([&](const cache::AsyncDataCacheEntry&) {
    if (++counter >= 7) {
      VELOX_FAIL("Testing error");
    }
  });

  class HookCleaner {
   public:
    explicit HookCleaner(cache::AsyncDataCache* cache) : cache_(cache) {}
    ~HookCleaner() {
      cache_->setVerifyHook(nullptr);
    }
    cache::AsyncDataCache* cache_;
  } hookCleaner(cache);

  auto planNode = exec::test::PlanBuilder()
                      .tableScan(ROW({"c0"}, {INTEGER()}))
                      .project({"c0"})
                      .planNode();

  try {
    assertQuery(planNode, {filePath}, "");
    FAIL() << "Excepted exception";
  } catch (VeloxException& ex) {
    EXPECT_TRUE(ex.context().find(filePath->path, 0) != std::string::npos)
        << ex.context();
  }
}

TEST_F(TableScanTest, parallelPrepare) {
  constexpr int32_t kNumParallel = 100;
  const char* kLargeRemainingFilter =
      "c0 + 1::BIGINT > 0::BIGINT or 1111 in (1, 2, 3, 4, 5) or array_sort(array_distinct(array[1, 1, 3, 4, 5, 6,7]))[1] = -5";
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(10, [](auto row) { return row % 5; })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {data});
  auto plan =
      exec::test::PlanBuilder(pool_.get())
          .tableScan(ROW({"c0"}, {INTEGER()}), {}, kLargeRemainingFilter)
          .project({"c0"})
          .planNode();

  std::vector<exec::Split> splits;
  for (auto i = 0; i < kNumParallel; ++i) {
    splits.push_back(makeHiveSplit(filePath->path));
  }
  AssertQueryBuilder(plan)
      .config(
          core::QueryConfig::kMaxSplitPreloadPerDriver,
          std::to_string(kNumParallel))
      .splits(splits)
      .copyResults(pool_.get());
}

TEST_F(TableScanTest, dictionaryMemo) {
  constexpr int kSize = 100;
  const char* baseStrings[] = {
      "qwertyuiopasdfghjklzxcvbnm",
      "qazwsxedcrfvtgbyhnujmikolp",
  };
  auto indices = allocateIndices(kSize, pool_.get());
  for (int i = 0; i < kSize; ++i) {
    indices->asMutable<vector_size_t>()[i] = i % 2;
  }
  auto dict = BaseVector::wrapInDictionary(
      nullptr,
      indices,
      kSize,
      makeFlatVector<std::string>({baseStrings[0], baseStrings[1]}));
  auto rows = makeRowVector({"a", "b"}, {dict, makeRowVector({"c"}, {dict})});
  auto rowType = asRowType(rows->type());
  auto file = TempFilePath::create();
  writeToFile(file->path, {rows});
  auto plan = PlanBuilder()
                  .tableScan(rowType, {}, "a like '%m'")
                  .project({"length(b.c)"})
                  .planNode();
#ifndef NDEBUG
  int numPeelEncodings = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Expr::peelEncodings::mayCache",
      std::function<void(bool*)>([&](bool* mayCache) {
        if (numPeelEncodings++ == 0) {
          ASSERT_TRUE(*mayCache) << "Memoize string dictionary base";
        } else {
          ASSERT_FALSE(*mayCache) << "Do not memoize filter result";
        }
      }));
#endif
  auto result = AssertQueryBuilder(plan)
                    .splits({makeHiveSplit(file->path)})
                    .copyResults(pool_.get());
  ASSERT_EQ(result->size(), 50);
#ifndef NDEBUG
  ASSERT_EQ(numPeelEncodings, 2);
#endif
}

TEST_F(TableScanTest, reuseRowVector) {
  auto iota = makeFlatVector<int32_t>(10, folly::identity);
  auto data = makeRowVector({iota, makeRowVector({iota})});
  auto rowType = asRowType(data->type());
  auto file = TempFilePath::create();
  writeToFile(file->path, {data});
  auto plan = PlanBuilder()
                  .tableScan(rowType, {}, "c0 < 5")
                  .project({"c1.c0"})
                  .planNode();
  auto split = HiveConnectorSplitBuilder(file->path).build();
  auto expected = makeRowVector(
      {makeFlatVector<int32_t>(10, [](auto i) { return i % 5; })});
  AssertQueryBuilder(plan).splits({split, split}).assertResults(expected);
}

// Tests queries that read more row fields than exist in the data.
TEST_F(TableScanTest, readMissingFields) {
  vector_size_t size = 10;
  auto iota = makeFlatVector<int64_t>(size, folly::identity);
  auto rowVector = makeRowVector({makeRowVector({iota, iota}), iota});
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});
  // Create a row type with additional fields not present in the file.
  auto rowType = makeRowType(
      {makeRowType({BIGINT(), BIGINT(), BIGINT(), BIGINT()}), BIGINT()});
  auto op = PlanBuilder().tableScan(rowType).planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto nulls = makeNullConstant(TypeKind::BIGINT, size);
  auto expected =
      makeRowVector({makeRowVector({iota, iota, nulls, nulls}), iota});
  AssertQueryBuilder(op).split(split).assertResults(expected);
}

TEST_F(TableScanTest, readExtraFields) {
  vector_size_t size = 10;
  auto iota = makeFlatVector<int64_t>(size, folly::identity);
  auto rowVector = makeRowVector({makeRowVector({iota, iota}), iota});
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});
  auto rowType = makeRowType({makeRowType({BIGINT()}), BIGINT()});
  auto op = PlanBuilder().tableScan(rowType).planNode();
  auto split = makeHiveConnectorSplit(filePath->path);
  auto nulls = makeNullConstant(TypeKind::BIGINT, size);
  auto expected = makeRowVector({makeRowVector({iota}), iota});
  AssertQueryBuilder(op).split(split).assertResults(expected);
}

// Tests queries that use that read more row fields than exist in the data in
// some files, but exist in other files.
TEST_F(TableScanTest, readMissingFieldsFilesVary) {
  vector_size_t size = 1000;
  auto rowVectorMissingFields = makeRowVector({makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
  })});

  auto missingFieldsFilePath = TempFilePath::create();
  writeToFile(missingFieldsFilePath->path, {rowVectorMissingFields});

  auto rowVectorWithAllFields = makeRowVector({makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }),
  })});

  auto allFieldsFilePath = TempFilePath::create();
  writeToFile(allFieldsFilePath->path, {rowVectorWithAllFields});

  auto op = PlanBuilder()
                .tableScan(asRowType(rowVectorWithAllFields->type()))
                .project({"c0.c0", "c0.c1", "c0.c2", "c0.c3"})
                .planNode();

  auto result = AssertQueryBuilder(op)
                    .split(makeHiveConnectorSplit(missingFieldsFilePath->path))
                    .split(makeHiveConnectorSplit(allFieldsFilePath->path))
                    .split(makeHiveConnectorSplit(missingFieldsFilePath->path))
                    .split(makeHiveConnectorSplit(allFieldsFilePath->path))
                    .copyResults(pool());

  ASSERT_EQ(result->size(), size * 4);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 4);
  for (int i = 0; i < 2; i++) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size * 4);
    for (int j = 0; j < size * 4; j++) {
      // These fields always exist.
      ASSERT_FALSE(val->isNullAt(j));
      ASSERT_EQ(val->valueAt(j), j % size);
    }
  }

  // Handle the case where splits may be read out of order.
  int32_t nullCount = 0;
  auto col2 = rows->childAt(2)->as<SimpleVector<int64_t>>();
  auto col3 = rows->childAt(3)->as<SimpleVector<int64_t>>();

  ASSERT_TRUE(col2);
  ASSERT_TRUE(col3);
  ASSERT_EQ(col2->size(), size * 4);
  ASSERT_EQ(col3->size(), size * 4);
  for (int j = 0; j < size * 4; j++) {
    // If a value in this column is null, then it comes from a split without
    // those additional fields, so the other column should be null as well.
    if (col2->isNullAt(j)) {
      ASSERT_TRUE(col3->isNullAt(j));
      nullCount++;
    } else {
      ASSERT_FALSE(col3->isNullAt(j));
      ASSERT_EQ(col2->valueAt(j), (j % size) + 1);
      ASSERT_EQ(col3->valueAt(j), (j % size) + 1);
    }
  }

  // Half the files are missing the additional columns, so we should see half
  // the rows with nulls.
  ASSERT_EQ(nullCount, size * 2);
}

// Tests queries that use that read more row fields than exist in the data in an
// array.
TEST_F(TableScanTest, readMissingFieldsInArray) {
  vector_size_t size = 1'000;
  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(size * 4, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size * 4, [](auto row) { return row; }),
  });
  std::vector<vector_size_t> offsets;
  for (int i = 0; i < size; i++) {
    offsets.push_back(i * 4);
  }
  auto arrayVector = makeArrayVector(offsets, rowVector);

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {makeRowVector({arrayVector})});
  // Create a row type with additional fields not present in the file.
  auto rowType = makeRowType(
      {ARRAY(makeRowType({BIGINT(), BIGINT(), BIGINT(), BIGINT()}))});

  // Query all the fields.
  auto op = PlanBuilder()
                .tableScan(rowType)
                .project({"c0[1].c0", "c0[2].c1", "c0[3].c2", "c0[4].c3"})
                .planNode();

  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());

  ASSERT_EQ(result->size(), size);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 4);
  // The fields that exist in the data should be present and correct.
  for (int i = 0; i < 2; i++) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (int j = 0; j < size; j++) {
      ASSERT_FALSE(val->isNullAt(j));
      ASSERT_EQ(val->valueAt(j), j * 4 + i);
    }
  }
  // The fields that don't exist in the data should be null.
  for (int i = 2; i < 4; i++) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (int j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }
}

// Tests queries that read more row fields than exist in the data in a map and
// array.
TEST_F(TableScanTest, readMissingFieldsInMap) {
  vector_size_t size = 1'000;
  auto valuesVector = makeRowVector({
      makeFlatVector<int64_t>(size * 4, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size * 4, [](auto row) { return row; }),
  });
  auto keysVector =
      makeFlatVector<int64_t>(size * 4, [](auto row) { return row % 4; });
  std::vector<vector_size_t> offsets;
  for (auto i = 0; i < size; i++) {
    offsets.push_back(i * 4);
  }
  auto mapVector = makeMapVector(offsets, keysVector, valuesVector);
  auto arrayVector = makeArrayVector(offsets, valuesVector);

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {makeRowVector({mapVector, arrayVector})});

  // Create a row type with additional fields in the structure not present in
  // the file ('c' and 'd') and with all columns having different names than in
  // the file.
  auto structType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), INTEGER(), DOUBLE(), REAL()});
  auto rowType =
      ROW({"m1", "a2"}, {{MAP(BIGINT(), structType), ARRAY(structType)}});

  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .dataColumns(rowType)
                .endTableScan()
                .project(
                    {"m1[0].a",
                     "m1[1].b",
                     "m1[2].c",
                     "m1[3].d",
                     "a2[1].a",
                     "a2[2].b",
                     "a2[3].c",
                     "a2[4].d"})
                .planNode();

  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());

  ASSERT_EQ(result->size(), size);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 8);
  // The fields that exist in the data should be present and correct.
  for (int i = 0; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_FALSE(val->isNullAt(j));
      ASSERT_EQ(val->valueAt(j), j * 4);
    }
  }
  for (int i = 1; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<int32_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_FALSE(val->isNullAt(j));
      ASSERT_EQ(val->valueAt(j), j * 4 + 1);
    }
  }
  // The fields that don't exist in the data should be null.
  for (int i = 2; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<double>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }
  for (int i = 3; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<float>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }

  // Now run query with column mapping using names - we should not be able to
  // find any names.
  result = AssertQueryBuilder(op)
               .connectorSessionProperty(
                   kHiveConnectorId,
                   connector::hive::HiveConfig::kOrcUseColumnNamesSession,
                   "true")
               .split(split)
               .copyResults(pool());

  ASSERT_EQ(result->size(), size);
  rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 8);

  for (int i = 0; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val != nullptr);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }
  for (int i = 1; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<int32_t>>();
    ASSERT_TRUE(val != nullptr);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }
  for (int i = 2; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<double>>();
    ASSERT_TRUE(val != nullptr);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }
  for (int i = 3; i < 8; i += 4) {
    auto val = rows->childAt(i)->as<SimpleVector<float>>();
    ASSERT_TRUE(val != nullptr);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }

  // Scan with type mismatch in the 1st item (map vs integer). We should throw.
  rowType = ROW({"i1", "a2"}, {{INTEGER(), ARRAY(structType)}});

  op = PlanBuilder()
           .startTableScan()
           .outputType(rowType)
           .dataColumns(rowType)
           .endTableScan()
           .project({"i1"})
           .planNode();

  EXPECT_THROW(
      AssertQueryBuilder(op).split(split).copyResults(pool()), VeloxUserError);
}

// Tests various projections of top level columns using the output type passed
// into TableScan.
TEST_F(TableScanTest, tableScanProjections) {
  vector_size_t size = 1'000;
  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row + 2; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row + 3; }),
  });

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});

  auto testQueryRow = [&](const std::vector<int32_t>& projections) {
    std::vector<std::string> cols;
    for (auto projection : projections) {
      cols.push_back(fmt::format("c{}", projection));
    }
    auto scanRowType = ROW(
        std::move(cols), std::vector<TypePtr>(projections.size(), BIGINT()));
    auto op = PlanBuilder().tableScan(scanRowType).planNode();

    auto split = makeHiveConnectorSplit(filePath->path);
    auto result = AssertQueryBuilder(op).split(split).copyResults(pool());

    ASSERT_EQ(result->size(), size);
    auto rows = result->as<RowVector>();
    ASSERT_TRUE(rows);
    ASSERT_EQ(rows->childrenSize(), projections.size());
    for (int i = 0; i < projections.size(); i++) {
      auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
      ASSERT_TRUE(val);
      ASSERT_EQ(val->size(), size);
      for (int j = 0; j < size; j++) {
        ASSERT_FALSE(val->isNullAt(j));
        ASSERT_EQ(val->valueAt(j), j + projections[i]);
      }
    }
  };

  // Vanilla, query all the fields in order.
  testQueryRow({0, 1, 2, 3});

  // Query all the fields in various orders.
  testQueryRow({3, 2, 1, 0});
  testQueryRow({3, 1, 2, 0});
  testQueryRow({0, 3, 2, 1});
  testQueryRow({1, 3, 0, 2});

  // Query some of the fields in order.
  testQueryRow({0, 1, 2});
  testQueryRow({1, 2, 3});
  testQueryRow({1, 3});
  testQueryRow({0, 2});
  testQueryRow({2});

  // Query some of the fields in various orders.
  testQueryRow({3, 2, 1});
  testQueryRow({1, 2, 0});
  testQueryRow({0, 2, 1});
  testQueryRow({3, 1});
  testQueryRow({2, 0});
  testQueryRow({3, 2});
}

// Tests queries that read more row fields than exist in the data, and
// read additional columns besides just the row.
TEST_F(TableScanTest, readMissingFieldsWithMoreColumns) {
  vector_size_t size = 1'000;
  std::vector<StringView> fruitViews = {"apple", "banana", "cherry", "grapes"};
  auto rowVector = makeRowVector(
      {makeRowVector({
           makeFlatVector<int64_t>(size, [](auto row) { return row; }),
           makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       }),
       makeFlatVector<int32_t>(size, [](auto row) { return -row; }),
       makeFlatVector<double>(size, [](auto row) { return row * 0.1; }),
       makeFlatVector<bool>(size, [](auto row) { return row % 2 == 0; }),
       makeFlatVector<StringView>(size, [&fruitViews](auto row) {
         return fruitViews[row % fruitViews.size()];
       })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {rowVector});

  // Create a row type with additional fields in the structure not present in
  // the file ('c' and 'd') and with all columns having different names than in
  // the file.
  auto structType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto rowType =
      ROW({"st1", "i2", "d3", "b4", "c4"},
          {{structType, INTEGER(), DOUBLE(), BOOLEAN(), VARCHAR()}});

  auto op =
      PlanBuilder()
          .startTableScan()
          .outputType(rowType)
          .dataColumns(rowType)
          .endTableScan()
          .project({"st1.a", "st1.b", "st1.c", "st1.d", "i2", "d3", "b4", "c4"})
          .planNode();

  auto split = makeHiveConnectorSplit(filePath->path);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());

  ASSERT_EQ(result->size(), size);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 8);
  for (int i = 0; i < 2; i++) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (int j = 0; j < size; j++) {
      ASSERT_FALSE(val->isNullAt(j));
      ASSERT_EQ(val->valueAt(j), j);
    }
  }

  for (int i = 2; i < 4; i++) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val);
    ASSERT_EQ(val->size(), size);
    for (int j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }

  auto intCol = rows->childAt(4)->as<SimpleVector<int32_t>>();
  ASSERT_TRUE(intCol);
  ASSERT_EQ(intCol->size(), size);
  for (int j = 0; j < size; j++) {
    ASSERT_FALSE(intCol->isNullAt(j));
    ASSERT_EQ(intCol->valueAt(j), -j);
  }

  auto doubleCol = rows->childAt(5)->as<SimpleVector<double>>();
  ASSERT_TRUE(doubleCol);
  ASSERT_EQ(doubleCol->size(), size);
  for (int j = 0; j < size; j++) {
    ASSERT_FALSE(doubleCol->isNullAt(j));
    ASSERT_EQ(doubleCol->valueAt(j), j * 0.1);
  }

  auto boolCol = rows->childAt(6)->as<SimpleVector<bool>>();
  ASSERT_TRUE(boolCol);
  ASSERT_EQ(boolCol->size(), size);
  for (int j = 0; j < size; j++) {
    ASSERT_FALSE(boolCol->isNullAt(j));
    ASSERT_EQ(boolCol->valueAt(j), j % 2 == 0);
  }

  auto stringCol = rows->childAt(7)->as<SimpleVector<StringView>>();
  ASSERT_TRUE(stringCol);
  ASSERT_EQ(stringCol->size(), size);
  for (int j = 0; j < size; j++) {
    ASSERT_FALSE(stringCol->isNullAt(j));
    ASSERT_EQ(stringCol->valueAt(j), fruitViews[j % fruitViews.size()]);
  }

  // Now run query with column mapping using names - we should not be able to
  // find any names, except for the last string column.
  result = AssertQueryBuilder(op)
               .connectorSessionProperty(
                   kHiveConnectorId,
                   connector::hive::HiveConfig::kOrcUseColumnNamesSession,
                   "true")
               .split(split)
               .copyResults(pool());

  ASSERT_EQ(result->size(), size);
  rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 8);

  for (int i = 0; i < 4; i++) {
    auto val = rows->childAt(i)->as<SimpleVector<int64_t>>();
    ASSERT_TRUE(val != nullptr);
    ASSERT_EQ(val->size(), size);
    for (auto j = 0; j < size; j++) {
      ASSERT_TRUE(val->isNullAt(j));
    }
  }

  intCol = rows->childAt(4)->as<SimpleVector<int32_t>>();
  ASSERT_TRUE(intCol != nullptr);
  ASSERT_EQ(intCol->size(), size);
  for (auto j = 0; j < size; j++) {
    ASSERT_TRUE(intCol->isNullAt(j));
  }

  doubleCol = rows->childAt(5)->as<SimpleVector<double>>();
  ASSERT_TRUE(doubleCol != nullptr);
  ASSERT_EQ(doubleCol->size(), size);
  for (auto j = 0; j < size; j++) {
    ASSERT_TRUE(doubleCol->isNullAt(j));
  }

  boolCol = rows->childAt(6)->as<SimpleVector<bool>>();
  ASSERT_TRUE(boolCol != nullptr);
  ASSERT_EQ(boolCol->size(), size);
  for (auto j = 0; j < size; j++) {
    ASSERT_TRUE(boolCol->isNullAt(j));
  }

  stringCol = rows->childAt(7)->as<SimpleVector<StringView>>();
  ASSERT_TRUE(stringCol != nullptr);
  ASSERT_EQ(stringCol->size(), size);
  for (auto j = 0; j < size; j++) {
    ASSERT_FALSE(stringCol->isNullAt(j));
    ASSERT_EQ(stringCol->valueAt(j), fruitViews[j % fruitViews.size()]);
  }
}

TEST_F(TableScanTest, varbinaryPartitionKey) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  ColumnHandleMap assignments = {
      {"a", regularColumn("c0", BIGINT())},
      {"ds_alias", partitionKey("ds", VARBINARY())}};

  auto split = HiveConnectorSplitBuilder(filePath->path)
                   .partitionKey("ds", "2021-12-02")
                   .build();

  auto outputType = ROW({"a", "ds_alias"}, {BIGINT(), VARBINARY()});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(outputType)
                .assignments(assignments)
                .endTableScan()
                .planNode();

  assertQuery(op, split, "SELECT c0, '2021-12-02' FROM tmp");
}

TEST_F(TableScanTest, timestampPartitionKey) {
  const char* inputs[] = {"2023-10-14 07:00:00.0", "2024-01-06 04:00:00.0"};
  auto expected = makeRowVector(
      {"t"},
      {
          makeFlatVector<Timestamp>(
              std::end(inputs) - std::begin(inputs),
              [&](auto i) {
                auto t = util::fromTimestampString(inputs[i]);
                t.toGMT(Timestamp::defaultTimezone());
                return t;
              }),
      });
  auto vectors = makeVectors(1, 1);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  ColumnHandleMap assignments = {{"t", partitionKey("t", TIMESTAMP())}};
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (auto& t : inputs) {
    splits.push_back(
        HiveConnectorSplitBuilder(filePath->path).partitionKey("t", t).build());
  }
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW({"t"}, {TIMESTAMP()}))
                  .assignments(assignments)
                  .endTableScan()
                  .planNode();
  AssertQueryBuilder(plan).splits(std::move(splits)).assertResults(expected);
}
