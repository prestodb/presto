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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

static const std::string kNodeSelectionStrategy = "node_selection_strategy";
static const std::string kSoftAffinity = "SOFT_AFFINITY";
static const std::string kTableScanTest = "TableScanTest.Writer";

class TableScanTest : public virtual HiveConnectorTestBase,
                      public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useAsyncCache_ = GetParam();
    HiveConnectorTestBase::SetUp();
    rowType_ =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
            {BIGINT(),
             INTEGER(),
             SMALLINT(),
             REAL(),
             DOUBLE(),
             VARCHAR(),
             TINYINT()});
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const std::shared_ptr<const RowType>& rowType = nullptr) {
    auto inputs = rowType ? rowType : rowType_;
    return HiveConnectorTestBase::makeVectors(inputs, count, rowsPerVector);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::shared_ptr<HiveConnectorSplit>& hiveSplit,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {hiveSplit}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const exec::Split&& split,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {split}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql) {
    return HiveConnectorTestBase::assertQuery(plan, filePaths, duckDbSql);
  }

  std::shared_ptr<facebook::velox::core::PlanNode> tableScanNode() {
    return PlanBuilder().tableScan(rowType_).planNode();
  }

  static std::shared_ptr<facebook::velox::core::PlanNode> tableScanNode(
      const std::shared_ptr<const RowType>& outputType) {
    return PlanBuilder().tableScan(outputType).planNode();
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

  static std::unordered_set<int32_t> getCompletedSplitGroups(
      const std::shared_ptr<Task>& task) {
    return task->taskStats().completedSplitGroups;
  }

  static void waitForFinishedDrivers(
      const std::shared_ptr<Task>& task,
      uint32_t n) {
    while (task->numFinishedDrivers() < n) {
      /* sleep override */
      usleep(100'000); // 0.1 second.
    }
    ASSERT_EQ(n, task->numFinishedDrivers());
  }

  void testPartitionedTableImpl(
      const std::string& filePath,
      const TypePtr& partitionType,
      const std::optional<std::string>& partitionValue) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys =
        {{"pkey", partitionValue}};
    auto split = std::make_shared<HiveConnectorSplit>(
        kHiveConnectorId,
        filePath,
        facebook::velox::dwio::common::FileFormat::ORC,
        0,
        fs::file_size(filePath),
        partitionKeys);
    auto outputType =
        ROW({"pkey", "c0", "c1"}, {partitionType, BIGINT(), DOUBLE()});
    auto tableHandle = makeTableHandle(SubfieldFilters{});
    ColumnHandleMap assignments = {
        {"pkey", partitionKey("pkey", partitionType)},
        {"c0", regularColumn("c0", BIGINT())},
        {"c1", regularColumn("c1", DOUBLE())}};

    auto op = PlanBuilder()
                  .tableScan(outputType, tableHandle, assignments)
                  .planNode();

    std::string partitionValueStr =
        partitionValue.has_value() ? "'" + *partitionValue + "'" : "null";
    assertQuery(
        op, split, fmt::format("SELECT {}, * FROM tmp", partitionValueStr));

    outputType = ROW({"c0", "pkey", "c1"}, {BIGINT(), partitionType, DOUBLE()});
    op = PlanBuilder()
             .tableScan(outputType, tableHandle, assignments)
             .planNode();
    assertQuery(
        op,
        split,
        fmt::format("SELECT c0, {}, c1 FROM tmp", partitionValueStr));
    outputType = ROW({"c0", "c1", "pkey"}, {BIGINT(), DOUBLE(), partitionType});
    op = PlanBuilder()
             .tableScan(outputType, tableHandle, assignments)
             .planNode();
    assertQuery(
        op,
        split,
        fmt::format("SELECT c0, c1, {} FROM tmp", partitionValueStr));

    // select only partition key
    assignments = {{"pkey", partitionKey("pkey", partitionType)}};
    outputType = ROW({"pkey"}, {partitionType});
    op = PlanBuilder()
             .tableScan(outputType, tableHandle, assignments)
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

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
};

TEST_P(TableScanTest, allColumns) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
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
}

TEST_P(TableScanTest, columnAliases) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  ColumnHandleMap assignments = {{"a", regularColumn("c0", BIGINT())}};
  auto outputType = ROW({"a"}, {BIGINT()});
  auto op = PlanBuilder()
                .tableScan(
                    outputType, makeTableHandle(SubfieldFilters{}), assignments)
                .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp");

  // Use aliased column in a range filter.
  auto filters = singleSubfieldFilter("c0", lessThanOrEqual(10));

  op = PlanBuilder()
           .tableScan(
               outputType, makeTableHandle(std::move(filters)), assignments)
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp WHERE c0 <= 10");

  // Use aliased column in remaining filter.
  op = PlanBuilder()
           .tableScan(
               outputType,
               makeTableHandle(
                   SubfieldFilters{}, parseExpr("c0 % 2 = 1", rowType_)),
               assignments)
           .planNode();
  assertQuery(op, {filePath}, "SELECT c0 FROM tmp WHERE c0 % 2 = 1");
}

TEST_P(TableScanTest, partitionKeyAlias) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  ColumnHandleMap assignments = {
      {"a", regularColumn("c0", BIGINT())},
      {"ds_alias", partitionKey("ds", VARCHAR())}};

  auto split = makeHiveConnectorSplit(filePath->path, {{"ds", "2021-12-02"}});

  auto outputType = ROW({"a", "ds_alias"}, {BIGINT(), VARCHAR()});
  auto op = PlanBuilder()
                .tableScan(
                    outputType, makeTableHandle(SubfieldFilters{}), assignments)
                .planNode();

  assertQuery(op, split, "SELECT c0, '2021-12-02' FROM tmp");
}

TEST_P(TableScanTest, columnPruning) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
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

// Test reading files written before schema change, e.g. missing newly added
// columns.
TEST_P(TableScanTest, missingColumns) {
  // Simulate schema change of adding a new column.
  // - Create an "old" file with one column.
  // - Create a "new" file with two columns.
  vector_size_t size = 1'000;
  auto oldData = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; })});
  auto newData = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return -row; }),
      makeFlatVector<double>(size, [](auto row) { return row * 0.1; }),
  });

  auto filePaths = makeFilePaths(2);
  writeToFile(filePaths[0]->path, kTableScanTest, {oldData});
  writeToFile(filePaths[1]->path, kTableScanTest, {newData});

  auto oldDataWithNull = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      BaseVector::createNullConstant(DOUBLE(), size, pool_.get()),
  });
  createDuckDbTable({oldDataWithNull, newData});

  auto outputType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto assignments = allRegularColumns(outputType);

  auto tableHandle = makeTableHandle(SubfieldFilters{});

  auto op =
      PlanBuilder().tableScan(outputType, tableHandle, assignments).planNode();
  assertQuery(op, filePaths, "SELECT * FROM tmp");

  // Use missing column in a tuple domain filter.
  auto filters = singleSubfieldFilter("c1", lessThanOrEqualDouble(100.1));
  op = PlanBuilder()
           .tableScan(
               outputType, makeTableHandle(std::move(filters)), assignments)
           .planNode();
  assertQuery(op, filePaths, "SELECT * FROM tmp WHERE c1 <= 100.1");

  // Use column aliases.
  outputType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});

  assignments.clear();
  assignments["a"] = regularColumn("c0", BIGINT());
  assignments["b"] = regularColumn("c1", DOUBLE());

  op = PlanBuilder().tableScan(outputType, tableHandle, assignments).planNode();
  assertQuery(op, filePaths, "SELECT * FROM tmp");
}

// Tests queries that use Lazy vectors with multiple layers of wrapping.
TEST_P(TableScanTest, constDictLazy) {
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
  writeToFile(filePath->path, kTableScanTest, {rowVector});

  createDuckDbTable({rowVector});

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto assignments = allRegularColumns(rowType);

  // Orchestrate a Const(Dict(Lazy)) by using remaining filter that passes on
  // exactly one row.
  auto tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c0 % 1000 = 5", rowType));
  auto op = PlanBuilder()
                .tableScan(rowType, tableHandle, assignments)
                .project({"c1 + 10"})
                .planNode();

  assertQuery(op, {filePath}, "SELECT c1 + 10 FROM tmp WHERE c0 = 5");

  // Orchestrate a Const(Dict(Lazy)) for a complex type (map)
  tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c0 = 0", rowType));
  op = PlanBuilder()
           .tableScan(rowType, tableHandle, assignments)
           .project({"cardinality(c2)"})
           .planNode();

  assertQuery(op, {filePath}, "SELECT 0 FROM tmp WHERE c0 = 5");

  tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c0 = 2", rowType));
  op = PlanBuilder()
           .tableScan(rowType, tableHandle, assignments)
           .project({"cardinality(c2)"})
           .planNode();

  assertQuery(op, {filePath}, "SELECT 2 FROM tmp WHERE c0 = 5");
}

TEST_P(TableScanTest, count) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  auto cursor = std::make_unique<TaskCursor>(params);

  addSplit(cursor->task().get(), "0", makeHiveSplit(filePath->path));
  cursor->task()->noMoreSplits("0");

  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numRead += vector->size();
  }

  EXPECT_EQ(numRead, 10'000);
}

// Test that adding the same split with the same sequence id does not cause
// double read and the 2nd split is ignored.
TEST_P(TableScanTest, sequentialSplitNoDoubleRead) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  auto cursor = std::make_unique<TaskCursor>(params);
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
TEST_P(TableScanTest, outOfOrderSplits) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  auto cursor = std::make_unique<TaskCursor>(params);

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
TEST_P(TableScanTest, splitDoubleRead) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));

  for (size_t i = 0; i < 2; ++i) {
    auto cursor = std::make_unique<TaskCursor>(params);

    // Add the same split twice - we should read twice the size.
    addSplit(cursor->task().get(), "0", makeHiveSplit(filePath->path));
    addSplit(cursor->task().get(), "0", makeHiveSplit(filePath->path));
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

TEST_P(TableScanTest, multipleSplits) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, kTableScanTest, vectors[i]);
  }
  createDuckDbTable(vectors);

  assertQuery(tableScanNode(), filePaths, "SELECT * FROM tmp");
}

TEST_P(TableScanTest, waitForSplit) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, kTableScanTest, vectors[i]);
  }
  createDuckDbTable(vectors);

  int32_t fileIndex = 0;
  ::assertQuery(
      tableScanNode(),
      [&](Task* task) {
        if (fileIndex < filePaths.size()) {
          addSplit(task, "0", makeHiveSplit(filePaths[fileIndex++]->path));
        }
        if (fileIndex == filePaths.size()) {
          task->noMoreSplits("0");
        }
      },
      "SELECT * FROM tmp",
      duckDbQueryRunner_);
}

TEST_P(TableScanTest, splitOffsetAndLength) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  assertQuery(
      tableScanNode(),
      makeHiveSplit(filePath->path, 0, fs::file_size(filePath->path) / 2),
      "SELECT * FROM tmp");

  assertQuery(
      tableScanNode(),
      makeHiveSplit(filePath->path, fs::file_size(filePath->path) / 2),
      "SELECT * FROM tmp LIMIT 0");
}

TEST_P(TableScanTest, fileNotFound) {
  CursorParameters params;
  params.planNode = tableScanNode();

  auto cursor = std::make_unique<TaskCursor>(params);
  cursor->task()->addSplit("0", makeHiveSplit("file:/path/to/nowhere.orc"));
  EXPECT_THROW(cursor->moveNext(), VeloxRuntimeError);
}

// A valid ORC file (containing headers) but no data.
TEST_P(TableScanTest, validFileNoData) {
  auto rowType = ROW({"c0", "c1", "c2"}, {DOUBLE(), VARCHAR(), BIGINT()});

  auto filePath = facebook::velox::test::getDataFilePath(
      "velox/exec/tests", "data/emptyPresto.dwrf");
  auto split = std::make_shared<HiveConnectorSplit>(
      kHiveConnectorId,
      "file:" + filePath,
      facebook::velox::dwio::common::FileFormat::ORC,
      0,
      fs::file_size(filePath) / 2);

  auto op = tableScanNode(rowType);
  assertQuery(op, split, "");
}

// An invalid (size = 0) file.
TEST_P(TableScanTest, emptyFile) {
  auto filePath = TempFilePath::create();

  try {
    assertQuery(
        tableScanNode(), makeHiveSplit(filePath->path), "SELECT * FROM tmp");
    ASSERT_FALSE(true) << "Function should throw.";
  } catch (const VeloxException& e) {
    EXPECT_EQ("ORC file is empty", e.message());
  }
}

TEST_P(TableScanTest, partitionedTableVarcharKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  testPartitionedTable(filePath->path, VARCHAR(), "2020-11-01");
}

TEST_P(TableScanTest, partitionedTableBigIntKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, BIGINT(), "123456789123456789");
}

TEST_P(TableScanTest, partitionedTableIntegerKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, INTEGER(), "123456789");
}

TEST_P(TableScanTest, partitionedTableSmallIntKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, SMALLINT(), "1");
}

TEST_P(TableScanTest, partitionedTableTinyIntKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, TINYINT(), "1");
}

TEST_P(TableScanTest, partitionedTableBooleanKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, BOOLEAN(), "0");
}

TEST_P(TableScanTest, partitionedTableRealKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, REAL(), "3.5");
}

TEST_P(TableScanTest, partitionedTableDoubleKey) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  testPartitionedTable(filePath->path, DOUBLE(), "3.5");
}

std::vector<StringView> toStringViews(const std::vector<std::string>& values) {
  std::vector<StringView> views;
  views.reserve(values.size());
  for (const auto& value : values) {
    views.emplace_back(StringView(value));
  }
  return views;
}

TEST_P(TableScanTest, statsBasedSkippingBool) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), BOOLEAN()});
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       makeFlatVector<bool>(
           size, [](auto row) { return (row / 10'000) % 2 == 0; })});

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  auto subfieldFilters = singleSubfieldFilter("c1", boolEqual(true));
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments = {{"c0", regularColumn("c0", INTEGER())}};
  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(subfieldFilters));
    return TableScanTest::assertQuery(
        PlanBuilder()
            .tableScan(ROW({"c0"}, {INTEGER()}), tableHandle, assignments)
            .planNode(),
        filePaths,
        query);
  };
  auto task = assertQuery("SELECT c0 FROM tmp WHERE c1 = true");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));

  subfieldFilters = singleSubfieldFilter("c1", boolEqual(false));
  task = assertQuery("SELECT c0 FROM tmp WHERE c1 = false");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(2, getSkippedStridesStat(task));
}

TEST_P(TableScanTest, statsBasedSkippingDouble) {
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector({makeFlatVector<double>(
      size, [](auto row) { return (double)(row + 0.0001); })});

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1.05 -> whole file should be skipped based on stats
  auto subfieldFilters =
      singleSubfieldFilter("c0", lessThanOrEqualDouble(-1.05));

  ColumnHandleMap assignments = {{"c0", regularColumn("c0", DOUBLE())}};

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(subfieldFilters));
    return TableScanTest::assertQuery(
        PlanBuilder()
            .tableScan(ROW({"c0"}, {DOUBLE()}), tableHandle, assignments)
            .planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT c0 FROM tmp WHERE c0 <= -1.05");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // c0 >= 11,111.06 - first stride should be skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", greaterThanOrEqualDouble(11'111.06));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 >= 11111.06");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedStridesStat(task));

  // c0 between 10'100.06 and 10'500.08 - all strides but second should be
  // skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", betweenDouble(10'100.06, 10'500.08));
  task =
      assertQuery("SELECT c0 FROM tmp WHERE c0 between 10100.06 AND 10500.08");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // c0 <= 1,234.005 - all strides but first should be skipped
  subfieldFilters =
      singleSubfieldFilter("c0", lessThanOrEqualDouble(1'234.005));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 <= 1234.005");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));
}

TEST_P(TableScanTest, statsBasedSkippingFloat) {
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector({makeFlatVector<float>(
      size, [](auto row) { return (float)(row + 0.0001); })});

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1.05 -> whole file should be skipped based on stats
  auto subfieldFilters =
      singleSubfieldFilter("c0", lessThanOrEqualFloat(-1.05));

  ColumnHandleMap assignments = {{"c0", regularColumn("c0", REAL())}};

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(subfieldFilters));
    return TableScanTest::assertQuery(
        PlanBuilder()
            .tableScan(ROW({"c0"}, {REAL()}), tableHandle, assignments)
            .planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT c0 FROM tmp WHERE c0 <= -1.05");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // c0 >= 11,111.06 - first stride should be skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", greaterThanOrEqualFloat(11'111.06));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 >= 11111.06");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedStridesStat(task));

  // c0 between 10'100.06 and 10'500.08 - all strides but second should be
  // skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", betweenFloat(10'100.06, 10'500.08));
  task =
      assertQuery("SELECT c0 FROM tmp WHERE c0 between 10100.06 AND 10500.08");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // c0 <= 1,234.005 - all strides but first should be skipped
  subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqualFloat(1'234.005));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 <= 1234.005");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));
}

// Test skipping whole file based on statistics
TEST_P(TableScanTest, statsBasedSkipping) {
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

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1 -> whole file should be skipped based on stats
  auto subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqual(-1));

  ColumnHandleMap assignments = {{"c1", regularColumn("c1", INTEGER())}};

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(subfieldFilters));
    return TableScanTest::assertQuery(
        PlanBuilder()
            .tableScan(ROW({"c1"}, {INTEGER()}), tableHandle, assignments)
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
TEST_P(TableScanTest, statsBasedSkippingConstants) {
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

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // skip whole file
  auto filters = singleSubfieldFilter("c0", in({0, 10, 100, 1000}));

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(filters));
    auto rowType = ROW({"c1"}, {INTEGER()});
    auto assignments = allRegularColumns(rowType);
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT c1 FROM tmp WHERE c0 in (0, 10, 100, 1000)");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // skip all but first rowgroup
  filters = singleSubfieldFilter("c1", in({0, 10, 100, 1000}));
  task = assertQuery("SELECT c1 FROM tmp WHERE c1 in (0, 10, 100, 1000)");

  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));

  // skip whole file
  filters = singleSubfieldFilter(
      "c2", in(std::vector<std::string>{"apple", "cherry"}));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 in ('apple', 'cherry')");

  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(1, getSkippedSplitsStat(task));

  // skip all but second rowgroup
  filters = singleSubfieldFilter(
      "c3", in(std::vector<std::string>{"banana", "grapefruit"}));
  task = assertQuery("SELECT c1 FROM tmp WHERE c3 in ('banana', 'grapefruit')");

  EXPECT_EQ(10'000, getTableScanStats(task).rawInputRows);
  EXPECT_EQ(3, getSkippedStridesStat(task));
}

// Test stats-based skipping for the IS NULL filter.
TEST_P(TableScanTest, statsBasedSkippingNulls) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), INTEGER()});
  auto filePaths = makeFilePaths(1);
  const vector_size_t size = 31'234;

  auto noNulls = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  auto someNulls = makeFlatVector<int32_t>(
      size,
      [](auto row) { return row; },
      [](auto row) { return row >= 11'111; });
  auto rowVector = makeRowVector({noNulls, someNulls});

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 IS NULL - whole file should be skipped based on stats
  auto filters = singleSubfieldFilter("c0", isNull());

  auto assignments = allRegularColumns(rowType);

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(filters));
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT * FROM tmp WHERE c0 IS NULL");

  auto stats = getTableScanStats(task);
  EXPECT_EQ(0, stats.rawInputRows);
  EXPECT_EQ(0, stats.inputRows);
  EXPECT_EQ(0, stats.outputRows);

  // c1 IS NULL - first stride should be skipped based on stats
  filters = singleSubfieldFilter("c1", isNull());
  task = assertQuery("SELECT * FROM tmp WHERE c1 IS NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(size - 10'000, stats.rawInputRows);
  EXPECT_EQ(size - 11'111, stats.inputRows);
  EXPECT_EQ(size - 11'111, stats.outputRows);

  // c1 IS NOT NULL - 3rd and 4th strides should be skipped based on stats
  filters = singleSubfieldFilter("c1", isNotNull());
  task = assertQuery("SELECT * FROM tmp WHERE c1 IS NOT NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(20'000, stats.rawInputRows);
  EXPECT_EQ(11'111, stats.inputRows);
  EXPECT_EQ(11'111, stats.outputRows);
}

// Test skipping whole compression blocks without decompressing these.
TEST_P(TableScanTest, statsBasedSkippingWithoutDecompression) {
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
  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // skip 1st row group
  auto filters = singleSubfieldFilter(
      "c0", greaterThanOrEqual("com.facebook.presto.orc.stream.11111"));

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(filters));
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto assignments = allRegularColumns(rowType);
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery(
      "SELECT * FROM tmp WHERE c0 >= 'com.facebook.presto.orc.stream.11111'");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip 2nd row group
  filters = singleSubfieldFilter(
      "c0",
      orFilter(
          lessThanOrEqual("com.facebook.presto.orc.stream.01234"),
          greaterThanOrEqual("com.facebook.presto.orc.stream.20123")));
  task = assertQuery(
      "SELECT * FROM tmp WHERE c0 <= 'com.facebook.presto.orc.stream.01234' or c0 >= 'com.facebook.presto.orc.stream.20123'");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip first 3 row groups
  filters = singleSubfieldFilter(
      "c0", greaterThanOrEqual("com.facebook.presto.orc.stream.30123"));
  task = assertQuery(
      "SELECT * FROM tmp WHERE c0 >= 'com.facebook.presto.orc.stream.30123'");
  EXPECT_EQ(size - 30'000, getTableScanStats(task).rawInputRows);
}

// Test skipping whole compression blocks without decompressing these.
TEST_P(TableScanTest, filterBasedSkippingWithoutDecompression) {
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

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());

  auto filePaths = makeFilePaths(1);
  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  auto tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c0 % 11111 = 7", rowType));

  auto assertQuery = [&](const std::string& query) {
    auto assignments = allRegularColumns(rowType);
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT * FROM tmp WHERE c0 % 11111 = 7");
  EXPECT_EQ(size, getTableScanStats(task).rawInputRows);
}

// Test stats-based skipping for numeric columns (integers, floats and booleans)
// that don't have filters themselves. Skipping is driven by a single bigint
// column.
TEST_P(TableScanTest, statsBasedSkippingNumerics) {
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
  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // skip whole file
  auto filters = singleSubfieldFilter("c0", lessThanOrEqual(-1));

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(filters));
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto assignments = allRegularColumns(rowType);
    return TableScanTest::assertQuery(
        PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT * FROM tmp WHERE c0 <= -1");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);

  // skip 1st rowgroup
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(11'111));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip 2nd rowgroup
  filters = singleSubfieldFilter(
      "c0", bigintOr(lessThanOrEqual(1'000), greaterThanOrEqual(23'456)));
  task = assertQuery("SELECT * FROM tmp WHERE c0 <= 1000 OR c0 >= 23456");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip last 2 rowgroups
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(20'123));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 20123");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
}

// Test stats-based skipping for list and map columns that don't have
// filters themselves. Skipping is driven by a single bigint column.
TEST_P(TableScanTest, statsBasedSkippingComplexTypes) {
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
  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
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
  auto filters = singleSubfieldFilter("c0", lessThanOrEqual(-1));

  auto assertQuery = [&](const std::string& query) {
    auto tableHandle = makeTableHandle(std::move(filters));
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto assignments = allRegularColumns(rowType);
    return TableScanTest::assertQuery(
        PlanBuilder()
            .tableScan(rowType, tableHandle, assignments)
            // Project row-number column, first element of each array and map
            // elements for key zero.
            .project({"c0", "c1[1]", "c2[1]", "c3[0]", "c4[0]"})
            .planNode(),
        filePaths,
        query);
  };

  auto task = assertQuery("SELECT * FROM tmp WHERE c0 <= -1");
  EXPECT_EQ(0, getTableScanStats(task).rawInputRows);

  // skip 1st rowgroup
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(11'111));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip 2nd rowgroup
  filters = singleSubfieldFilter(
      "c0", bigintOr(lessThanOrEqual(1'000), greaterThanOrEqual(23'456)));
  task = assertQuery("SELECT * FROM tmp WHERE c0 <= 1000 OR c0 >= 23456");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputRows);

  // skip last 2 rowgroups
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(20'123));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 20123");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputRows);
}

/// Test the interaction between stats-based and regular skipping for lists and
/// maps.
TEST_P(TableScanTest, statsBasedAndRegularSkippingComplexTypes) {
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
  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);

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

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto tableHandle = makeTableHandle(std::move(filters));

  auto op = PlanBuilder()
                .tableScan(rowType, tableHandle, allRegularColumns(rowType))
                .project({"c0", "c1[1]", "c2[0]"})
                .planNode();

  assertQuery(
      op,
      filePaths,
      "SELECT * FROM tmp WHERE c0 <= 10 OR c0 between 600 AND 650 OR c0 >= 21234");
}

TEST_P(TableScanTest, filterPushdown) {
  auto rowType =
      ROW({"c0", "c1", "c2", "c3"}, {TINYINT(), BIGINT(), DOUBLE(), BOOLEAN()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000, rowType);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, kTableScanTest, vectors[i]);
  }
  createDuckDbTable(vectors);

  // c1 >= 0 or null and c3 is true
  SubfieldFilters subfieldFilters =
      SubfieldFiltersBuilder()
          .add("c1", greaterThanOrEqual(0, true))
          .add("c3", std::make_unique<common::BoolValue>(true, false))
          .build();
  auto tableHandle = makeTableHandle(std::move(subfieldFilters));

  auto assignments = allRegularColumns(rowType);

  auto task = assertQuery(
      PlanBuilder()
          .tableScan(
              ROW({"c1", "c3", "c0"}, {BIGINT(), BOOLEAN(), TINYINT()}),
              tableHandle,
              assignments)
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
          .tableScan(ROW({"c0"}, {TINYINT()}), tableHandle, assignments)
          .planNode(),
      filePaths,
      "SELECT c0 FROM tmp WHERE (c1 >= 0 OR c1 IS NULL) AND c3");

  // Do the same for count, no columns projected out.
  assignments.clear();
  assertQuery(
      PlanBuilder()
          .tableScan(ROW({}, {}), tableHandle, assignments)
          .singleAggregation({}, {"sum(1)"})
          .planNode(),
      filePaths,
      "SELECT count(*) FROM tmp WHERE (c1 >= 0 OR c1 IS NULL) AND c3");

  // Do the same for count, no filter, no projections.
  assignments.clear();
  tableHandle = makeTableHandle(std::move(subfieldFilters));
  assertQuery(
      PlanBuilder()
          .tableScan(ROW({}, {}), tableHandle, assignments)
          .singleAggregation({}, {"sum(1)"})
          .planNode(),
      filePaths,
      "SELECT count(*) FROM tmp");
}

TEST_P(TableScanTest, path) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto filePath = makeFilePaths(1)[0];
  auto vector = makeVectors(1, 1'000, rowType)[0];
  writeToFile(filePath->path, kTableScanTest, vector);
  createDuckDbTable({vector});

  static const char* kPath = "$path";

  auto assignments = allRegularColumns(rowType);
  assignments[kPath] = synthesizedColumn(kPath, VARCHAR());

  auto tableHandle = makeTableHandle(SubfieldFilters{}, nullptr);

  auto pathValue = fmt::format("file:{}", filePath->path);
  auto typeWithPath = ROW({kPath, "a"}, {VARCHAR(), BIGINT()});
  auto op = PlanBuilder()
                .tableScan(typeWithPath, tableHandle, assignments)
                .planNode();
  assertQuery(
      op, {filePath}, fmt::format("SELECT '{}', * FROM tmp", pathValue));

  // use $path in a filter, but don't project it out
  tableHandle = makeTableHandle(
      SubfieldFilters{},
      parseExpr(fmt::format("\"{}\" = '{}'", kPath, pathValue), typeWithPath));

  op = PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode();
  assertQuery(op, {filePath}, "SELECT * FROM tmp");

  // use $path in a filter and project it out
  op = PlanBuilder()
           .tableScan(typeWithPath, tableHandle, assignments)
           .planNode();
  assertQuery(
      op, {filePath}, fmt::format("SELECT '{}', * FROM tmp", pathValue));
}

TEST_P(TableScanTest, bucket) {
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
    writeToFile(filePaths[i]->path, kTableScanTest, rowVector);
    rowVectors.emplace_back(rowVector);

    splits.emplace_back(std::make_shared<HiveConnectorSplit>(
        kHiveConnectorId,
        filePaths[i]->path,
        facebook::velox::dwio::common::FileFormat::ORC,
        0,
        fs::file_size(filePaths[i]->path),
        std::unordered_map<std::string, std::optional<std::string>>(),
        bucket));
  }

  createDuckDbTable(rowVectors);

  static const char* kBucket = "$bucket";
  auto rowType =
      std::dynamic_pointer_cast<const RowType>(rowVectors.front()->type());

  auto assignments = allRegularColumns(rowType);
  assignments[kBucket] = synthesizedColumn(kBucket, INTEGER());

  // Query that spans on all buckets
  auto typeWithBucket =
      ROW({kBucket, "c0", "c1"}, {INTEGER(), INTEGER(), BIGINT()});
  auto tableHandle = makeTableHandle(SubfieldFilters{});
  auto op = PlanBuilder()
                .tableScan(typeWithBucket, tableHandle, assignments)
                .planNode();
  OperatorTestBase::assertQuery(op, splits, "SELECT c0, * FROM tmp");

  for (int i = 0; i < buckets.size(); ++i) {
    int bucketValue = buckets[i];
    auto connectorSplit = splits[i];
    auto hsplit = std::dynamic_pointer_cast<HiveConnectorSplit>(connectorSplit);
    tableHandle = makeTableHandle(SubfieldFilters{});

    // Filter on bucket and filter on first column should produce
    // identical result for each split
    op = PlanBuilder()
             .tableScan(typeWithBucket, tableHandle, assignments)
             .planNode();
    assertQuery(
        op,
        hsplit,
        fmt::format(
            "SELECT {}, * FROM tmp where c0 = {}", bucketValue, bucketValue));

    // Filter on bucket column, but don't project it out
    auto rowTypes = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});
    op = PlanBuilder().tableScan(rowTypes, tableHandle, assignments).planNode();
    assertQuery(
        op,
        hsplit,
        fmt::format("SELECT * FROM tmp where c0 = {}", bucketValue));
  }
}

TEST_P(TableScanTest, remainingFilter) {
  auto rowType = ROW(
      {"c0", "c1", "c2", "c3"}, {INTEGER(), INTEGER(), DOUBLE(), BOOLEAN()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000, rowType);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, kTableScanTest, vectors[i]);
  }
  createDuckDbTable(vectors);

  auto tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c1 > c0", rowType));

  auto assignments = allRegularColumns(rowType);

  assertQuery(
      PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE c1 > c0");

  // filter that never passes
  tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c1 % 5 = 6", rowType));

  assertQuery(
      PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE c1 % 5 = 6");

  // range filter + remaining filter: c0 >= 0 AND c1 > c0
  auto subfieldFilters = singleSubfieldFilter("c0", greaterThanOrEqual(0));

  tableHandle = makeTableHandle(
      std::move(subfieldFilters), parseExpr("c1 > c0", rowType));

  assertQuery(
      PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode(),
      filePaths,
      "SELECT * FROM tmp WHERE c1 > c0 AND c0 >= 0");

  // Remaining filter uses columns that are not used otherwise.
  assignments = {{"c2", regularColumn("c2", DOUBLE())}};
  tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c1 > c0", rowType));
  assertQuery(
      PlanBuilder()
          .tableScan(ROW({"c2"}, {DOUBLE()}), tableHandle, assignments)
          .planNode(),
      filePaths,
      "SELECT c2 FROM tmp WHERE c1 > c0");

  // Remaining filter uses one column that is used elsewhere (is projected out)
  // and another column that is not used anywhere else.
  assignments = {
      {"c1", regularColumn("c1", INTEGER())},
      {"c2", regularColumn("c2", DOUBLE())}};
  tableHandle =
      makeTableHandle(SubfieldFilters{}, parseExpr("c1 > c0", rowType));
  assertQuery(
      PlanBuilder()
          .tableScan(
              ROW({"c1", "c2"}, {INTEGER(), DOUBLE()}),
              tableHandle,
              assignments)
          .planNode(),
      filePaths,
      "SELECT c1, c2 FROM tmp WHERE c1 > c0");
}

TEST_P(TableScanTest, aggregationPushdown) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  auto tableHandle = makeTableHandle(SubfieldFilters());

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

  auto assignments = allRegularColumns(rowType_);

  auto op =
      PlanBuilder()
          .tableScan(rowType_, tableHandle, assignments)
          .partialAggregation(
              {5}, {"max(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(c4)"})
          .planNode();

  auto task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0), sum(c1), sum(c2), sum(c3), sum(c4) FROM tmp group by c5");
  // 5 aggregates processing 10K rows each via pushdown.
  EXPECT_EQ(5 * 10'000, loadedToValueHook(task));

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .singleAggregation(
               {5}, {"max(c0)", "max(c1)", "max(c2)", "max(c3)", "max(c4)"})
           .planNode();

  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0), max(c1), max(c2), max(c3), max(c4) FROM tmp group by c5");
  // 5 aggregates processing 10K rows each via pushdown.
  EXPECT_EQ(5 * 10'000, loadedToValueHook(task));

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .singleAggregation(
               {5}, {"min(c0)", "min(c1)", "min(c2)", "min(c3)", "min(c4)"})
           .planNode();

  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, min(c0), min(c1), min(c2), min(c3), min(c4) FROM tmp group by c5");
  // 5 aggregates processing 10K rows each via pushdown.
  EXPECT_EQ(5 * 10'000, loadedToValueHook(task));

  // Pushdown should also happen if there is a FilterProject node that doesn't
  // touch columns being aggregated
  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .project({"c0 % 5", "c1"})
           .singleAggregation({0}, {"sum(c1)"})
           .planNode();

  task =
      assertQuery(op, {filePath}, "SELECT c0 % 5, sum(c1) FROM tmp group by 1");
  // LazyVector stats are reported on the closest operator upstream of the
  // aggregation, e.g. project operator.
  EXPECT_EQ(10'000, loadedToValueHook(task, 1));

  // Add remaining filter to scan to expose LazyVectors wrapped in Dictionary to
  // aggregation.
  tableHandle = makeTableHandle(
      SubfieldFilters(), parseExpr("length(c5) % 2 = 0", rowType_));
  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .singleAggregation({5}, {"max(c0)"})
           .planNode();
  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0) FROM tmp WHERE length(c5) % 2 = 0 GROUP BY c5");
  // Values in rows that passed the filter should be aggregated via pushdown.
  EXPECT_GT(loadedToValueHook(task), 0);
  EXPECT_LT(loadedToValueHook(task), 10'000);

  // No pushdown if two aggregates use the same column or a column is not a
  // LazyVector
  tableHandle = makeTableHandle(SubfieldFilters(), nullptr);
  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .singleAggregation({5}, {"min(c0)", "max(c0)"})
           .planNode();
  task = assertQuery(
      op, {filePath}, "SELECT c5, min(c0), max(c0) FROM tmp GROUP BY 1");
  EXPECT_EQ(0, loadedToValueHook(task));

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .project({"c5", "c0", "c0 + c1 AS c0_plus_c1"})
           .singleAggregation({0}, {"min(c0)", "max(c0_plus_c1)"})
           .planNode();
  task = assertQuery(
      op, {filePath}, "SELECT c5, min(c0), max(c0 + c1) FROM tmp GROUP BY 1");
  EXPECT_EQ(0, loadedToValueHook(task));

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .project({"c5", "c0 + 1 as a", "c1 + 2 as b", "c2 + 3 as c"})
           .singleAggregation({0}, {"min(a)", "max(b)", "sum(c)"})
           .planNode();
  task = assertQuery(
      op,
      {filePath},
      "SELECT c5, min(c0 + 1), max(c1 + 2), sum(c2 + 3) FROM tmp GROUP BY 1");
  EXPECT_EQ(0, loadedToValueHook(task));
}

TEST_P(TableScanTest, bitwiseAggregationPushdown) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  auto tableHandle = makeTableHandle(SubfieldFilters(), nullptr);

  auto assignments = allRegularColumns(rowType_);

  auto op = PlanBuilder()
                .tableScan(rowType_, tableHandle, assignments)
                .singleAggregation(
                    {5},
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
           .tableScan(rowType_, tableHandle, assignments)
           .singleAggregation(
               {5},
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

TEST_P(TableScanTest, structLazy) {
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
  writeToFile(filePath->path, kTableScanTest, {rowVector});

  // Exclude struct columns as DuckDB doesn't support complex types yet.
  createDuckDbTable(
      {makeRowVector({rowVector->childAt(0), rowVector->childAt(1)})});

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto assignments = allRegularColumns(rowType);

  auto tableHandle = makeTableHandle(SubfieldFilters{});
  auto op = PlanBuilder()
                .tableScan(rowType, tableHandle, assignments)
                .project({"cardinality(c2.c0)"})
                .planNode();

  assertQuery(op, {filePath}, "select c0 % 3 from tmp");
}

TEST_P(TableScanTest, structInArrayOrMap) {
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
  writeToFile(filePath->path, kTableScanTest, {rowVector});

  // Exclude struct columns as DuckDB doesn't support complex types yet.
  createDuckDbTable(
      {makeRowVector({rowVector->childAt(0), rowVector->childAt(1)})});

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto op = PlanBuilder()
                .tableScan(rowType)
                .project({"c2[1].c0", "c3[1].c0"})
                .planNode();

  assertQuery(op, {filePath}, "select c0, c0 from tmp");
}

// Here we test various aspects of grouped/bucketed execution also involving
// output buffer.
TEST_P(TableScanTest, groupedExecutionWithOutputBuffer) {
  // Create source file - we will read from it in 6 splits.
  const size_t numSplits{6};
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  auto planFragment = PlanBuilder()
                          .tableScan(rowType_)
                          .partitionedOutput({}, 1, {0, 1, 2, 3, 4, 5})
                          .planFragment();
  planFragment.numSplitGroups = 10;
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  auto queryCtx = core::QueryCtx::createForTest();
  auto task = std::make_shared<exec::Task>(
      "0", std::move(planFragment), 0, std::move(queryCtx));
  // 3 drivers max and 1 concurrent split group.
  task->start(task, 3, 1);

  // Add one splits before start to ensure we can handle such cases.
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // Only one split group should be in the processing mode, so 3 drivers.
  EXPECT_EQ(3, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Add the rest of splits
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 1));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // One split group should be in the processing mode, so 3 drivers.
  EXPECT_EQ(3, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Finalize one split group (8) and wait until 3 drivers are finished.
  task->noMoreSplitsForGroup("0", 8);
  waitForFinishedDrivers(task, 3);
  // As one split group is finished, another one should kick in, so 3 drivers.
  EXPECT_EQ(3, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({8}), getCompletedSplitGroups(task));

  // Finalize the second split group (1) and wait until 6 drivers are finished.
  task->noMoreSplitsForGroup("0", 1);
  waitForFinishedDrivers(task, 6);

  // As one split group is finished, another one should kick in, so 3 drivers.
  EXPECT_EQ(3, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({1, 8}), getCompletedSplitGroups(task));

  // Finalize the third split group (5) and wait until 9 drivers are finished.
  task->noMoreSplitsForGroup("0", 5);
  waitForFinishedDrivers(task, 9);

  // No split groups should be processed at the moment, so 0 drivers.
  EXPECT_EQ(0, task->numRunningDrivers());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));

  // Flag that we would have no more split groups.
  task->noMoreSplits("0");

  // 'Delete results' from output buffer triggers 'set all output consumed',
  // which should finish the task.
  auto outputBufferManager =
      PartitionedOutputBufferManager::getInstance(task->queryCtx()->host())
          .lock();
  outputBufferManager->deleteResults(task->taskId(), 0);

  // Task must be finished at this stage.
  EXPECT_EQ(TaskState::kFinished, task->state());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));
}

// Here we test various aspects of grouped/bucketed execution.
TEST_P(TableScanTest, groupedExecution) {
  // Create source file - we will read from it in 6 splits.
  const size_t numSplits{6};
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));
  params.maxDrivers = 2;
  params.concurrentSplitGroups = 2;
  // We will have 10 split groups 'in total', but our task will only handle
  // three of them: 1, 5 and 8.
  // Split 0 is from split group 1.
  // Splits 1 and 2 are from split group 5.
  // Splits 3, 4 and 5 are from split group 8.
  params.executionStrategy = core::ExecutionStrategy::kGrouped;
  params.numSplitGroups = 10;
  // We'll only run 3 split groups, 2 drivers each.
  params.numResultDrivers = 3 * 2;

  // Create the cursor with the task underneath. It is not started yet.
  auto cursor = std::make_unique<TaskCursor>(params);
  auto task = cursor->task();

  // Add one splits before start to ensure we can handle such cases.
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // Start task now.
  cursor->start();

  // Only one split group should be in the processing mode, so 2 drivers.
  EXPECT_EQ(2, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Add the rest of splits
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 1));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // Only two split groups should be in the processing mode, so 4 drivers.
  EXPECT_EQ(4, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Finalize one split group (8) and wait until 2 drivers are finished.
  task->noMoreSplitsForGroup("0", 8);
  waitForFinishedDrivers(task, 2);

  // As one split group is finished, another one should kick in, so 4 drivers.
  EXPECT_EQ(4, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({8}), getCompletedSplitGroups(task));

  // Finalize the second split group (5) and wait until 4 drivers are finished.
  task->noMoreSplitsForGroup("0", 5);
  waitForFinishedDrivers(task, 4);

  // As the second split group is finished, only one is left, so 2 drivers.
  EXPECT_EQ(2, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({5, 8}), getCompletedSplitGroups(task));

  // Finalize the third split group (1) and wait until 6 drivers are finished.
  task->noMoreSplitsForGroup("0", 1);
  waitForFinishedDrivers(task, 6);

  // No split groups should be processed at the moment, so 0 drivers.
  EXPECT_EQ(0, task->numRunningDrivers());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));

  // Make sure split groups with no splits are reported as complete.
  task->noMoreSplitsForGroup("0", 3);
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 3, 5, 8}), getCompletedSplitGroups(task));

  // Flag that we would have no more split groups.
  task->noMoreSplits("0");

  // Make sure we've got the right number of rows.
  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numRead += vector->size();
  }

  // Task must be finished at this stage.
  EXPECT_EQ(TaskState::kFinished, task->state());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 3, 5, 8}), getCompletedSplitGroups(task));
  EXPECT_EQ(numRead, numSplits * 10'000);
}

TEST_P(TableScanTest, addSplitsToFailedTask) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(12'000, [](auto row) { return row % 5; })});

  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, {data});

  core::PlanNodeId scanNodeId;
  exec::test::CursorParameters params;
  params.planNode = exec::test::PlanBuilder()
                        .tableScan(ROW({"c0"}, {INTEGER()}))
                        .capturePlanNodeId(scanNodeId)
                        .project({"5 / c0"})
                        .planNode();

  auto cursor = std::make_unique<exec::test::TaskCursor>(params);
  cursor->task()->addSplit(scanNodeId, makeHiveSplit(filePath->path));

  EXPECT_THROW(while (cursor->moveNext()){}, VeloxUserError);

  // Verify that splits can be added to the task ever after task has failed.
  // In this case these splits will be ignored.
  cursor->task()->addSplit(scanNodeId, makeHiveSplit(filePath->path));
  cursor->task()->addSplitWithSequence(
      scanNodeId, makeHiveSplit(filePath->path), 20L);
  cursor->task()->setMaxSplitSequenceId(scanNodeId, 20L);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableScanTests,
    TableScanTest,
    testing::Values(true, false));
