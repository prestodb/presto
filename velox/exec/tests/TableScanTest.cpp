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
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/exec/tests/Cursor.h"
#include "velox/exec/tests/HiveConnectorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

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
using namespace facebook::velox::exec::test;

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

static const std::string kNodeSelectionStrategy = "node_selection_strategy";
static const std::string kSoftAffinity = "SOFT_AFFINITY";
static const std::string kTableScanTest = "TableScanTest.Writer";

class TableScanTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
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

  static OperatorStats getTableScanStats(std::shared_ptr<Task> task) {
    return task->taskStats().pipelineStats[0].operatorStats[0];
  }

  std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandle(
      SubfieldFilters subfieldFilters,
      const std::shared_ptr<const core::ITypedExpr>& remainingFilter =
          nullptr) {
    return std::make_shared<connector::hive::HiveTableHandle>(
        true, std::move(subfieldFilters), remainingFilter);
  }

  void addRegularColumns(
      const std::shared_ptr<const RowType>& rowType,
      ColumnHandleMap& assignments) const {
    for (auto& name : rowType->names()) {
      assignments[name] = regularColumn(name);
    }
  }

  ColumnHandleMap allRegularColumns(
      const std::shared_ptr<const RowType>& rowType) const {
    ColumnHandleMap assignments;
    assignments.reserve(rowType->size());
    for (auto& name : rowType->names()) {
      assignments[name] = regularColumn(name);
    }
    return assignments;
  }

  void testPartitionedTable(const std::string& filePath) {
    auto outputType = ROW({"ds", "c0", "c1"}, {VARCHAR(), BIGINT(), DOUBLE()});

    auto tableHandle = makeTableHandle(SubfieldFilters{});

    ColumnHandleMap assignments = {
        {"ds", partitionKey("ds")},
        {"c0", regularColumn("c0")},
        {"c1", regularColumn("c1")}};

    std::unordered_map<std::string, std::string> partitionKeys = {
        {"ds", "2020-11-01"}};
    auto split = std::make_shared<HiveConnectorSplit>(
        kHiveConnectorId, filePath, 0, fs::file_size(filePath), partitionKeys);

    auto op = PlanBuilder()
                  .tableScan(outputType, tableHandle, assignments)
                  .planNode();
    assertQuery(op, split, "SELECT '2020-11-01', * FROM tmp");

    outputType = ROW({"c0", "ds", "c1"}, {BIGINT(), VARCHAR(), DOUBLE()});
    op = PlanBuilder()
             .tableScan(outputType, tableHandle, assignments)
             .planNode();
    assertQuery(op, split, "SELECT c0, '2020-11-01', c1 FROM tmp");

    outputType = ROW({"c0", "c1", "ds"}, {BIGINT(), DOUBLE(), VARCHAR()});
    op = PlanBuilder()
             .tableScan(outputType, tableHandle, assignments)
             .planNode();
    assertQuery(op, split, "SELECT c0, c1, '2020-11-01' FROM tmp");

    // select only partition key
    assignments = {{"ds", partitionKey("ds")}};
    outputType = ROW({"ds"}, {VARCHAR()});
    op = PlanBuilder()
             .tableScan(outputType, tableHandle, assignments)
             .planNode();
    assertQuery(op, split, "SELECT '2020-11-01' FROM tmp");
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
};

namespace {

std::unique_ptr<common::BigintRange> lessThanOrEqual(
    int64_t max,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      std::numeric_limits<int64_t>::min(), max, nullAllowed);
}

std::unique_ptr<common::BigintRange> greaterThanOrEqual(
    int64_t min,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      min, std::numeric_limits<int64_t>::max(), nullAllowed);
}

std::unique_ptr<common::DoubleRange> lessThanOrEqualDouble(
    double max,
    bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      max,
      false,
      false,
      nullAllowed);
}

std::unique_ptr<common::DoubleRange> greaterThanOrEqualDouble(
    double min,
    bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      min,
      false,
      false,
      std::numeric_limits<double>::max(),
      true,
      true,
      nullAllowed);
}

std::unique_ptr<common::DoubleRange>
betweenDouble(double min, double max, bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      min, false, false, max, false, false, nullAllowed);
}

std::unique_ptr<common::FloatRange> lessThanOrEqualFloat(
    float max,
    bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      max,
      false,
      false,
      nullAllowed);
}

std::unique_ptr<common::FloatRange> greaterThanOrEqualFloat(
    float min,
    bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      min,
      false,
      false,
      std::numeric_limits<float>::max(),
      true,
      true,
      nullAllowed);
}

std::unique_ptr<common::FloatRange>
betweenFloat(float min, float max, bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      min, false, false, max, false, false, nullAllowed);
}

std::unique_ptr<common::BigintRange>
between(int64_t min, int64_t max, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(min, max, nullAllowed);
}

std::unique_ptr<common::BigintMultiRange> bigintOr(
    std::unique_ptr<common::BigintRange> a,
    std::unique_ptr<common::BigintRange> b,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::BigintRange>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  return std::make_unique<common::BigintMultiRange>(
      std::move(filters), nullAllowed);
}

std::unique_ptr<common::BigintMultiRange> bigintOr(
    std::unique_ptr<common::BigintRange> a,
    std::unique_ptr<common::BigintRange> b,
    std::unique_ptr<common::BigintRange> c,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::BigintRange>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  filters.emplace_back(std::move(c));
  return std::make_unique<common::BigintMultiRange>(
      std::move(filters), nullAllowed);
}

std::unique_ptr<common::BytesValues> equal(
    const std::string& value,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesValues>(
      std::vector<std::string>{value}, nullAllowed);
}

std::unique_ptr<common::BigintRange> equal(
    int64_t value,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(value, value, nullAllowed);
}

std::unique_ptr<common::BytesRange> lessThanOrEqual(
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      "", true, true, max, false, false, nullAllowed);
}

std::unique_ptr<common::BytesRange> greaterThanOrEqual(
    const std::string& min,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, false, "", true, true, nullAllowed);
}

std::unique_ptr<common::MultiRange> stringOr(
    std::unique_ptr<common::BytesRange> a,
    std::unique_ptr<common::BytesRange> b,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  return std::make_unique<common::MultiRange>(
      std::move(filters), nullAllowed, false);
}

std::unique_ptr<common::Filter> in(
    const std::vector<int64_t>& values,
    bool nullAllowed = false) {
  return common::createBigintValues(values, nullAllowed);
}

std::unique_ptr<common::BytesValues> in(
    const std::vector<std::string>& values,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesValues>(values, nullAllowed);
}

std::unique_ptr<common::BoolValue> boolEqual(
    bool value,
    bool nullAllowed = false) {
  return std::make_unique<common::BoolValue>(value, nullAllowed);
}

std::unique_ptr<common::IsNull> isNull() {
  return std::make_unique<common::IsNull>();
}

std::unique_ptr<common::IsNotNull> isNotNull() {
  return std::make_unique<common::IsNotNull>();
}

template <typename T>
std::unique_ptr<common::MultiRange>
orFilter(std::unique_ptr<T> a, std::unique_ptr<T> b, bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  return std::make_unique<common::MultiRange>(
      std::move(filters), nullAllowed, false);
}

class SubfieldFiltersBuilder {
 public:
  SubfieldFiltersBuilder& add(
      const std::string& path,
      std::unique_ptr<common::Filter> filter) {
    filters_[common::Subfield(path)] = std::move(filter);
    return *this;
  }

  SubfieldFilters build() {
    return std::move(filters_);
  }

 private:
  SubfieldFilters filters_;
};

SubfieldFilters singleSubfieldFilter(
    const std::string& path,
    std::unique_ptr<common::Filter> filter) {
  return SubfieldFiltersBuilder().add(path, std::move(filter)).build();
}
} // namespace

TEST_F(TableScanTest, allColumns) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  assertQuery(tableScanNode(), {filePath}, "SELECT * FROM tmp");
}

TEST_F(TableScanTest, columnAliases) {
  auto vectors = makeVectors(1, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  ColumnHandleMap assignments = {{"a", regularColumn("c0")}};
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

TEST_F(TableScanTest, columnPruning) {
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
TEST_F(TableScanTest, missingColumns) {
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
  assignments["a"] = regularColumn("c0");
  assignments["b"] = regularColumn("c1");

  op = PlanBuilder().tableScan(outputType, tableHandle, assignments).planNode();
  assertQuery(op, filePaths, "SELECT * FROM tmp");
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
  writeToFile(filePath->path, kTableScanTest, {rowVector});

  // Exclude map columns as DuckDB doesn't support complex types yet.
  createDuckDbTable(
      {makeRowVector({rowVector->childAt(0), rowVector->childAt(1)})});

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

TEST_F(TableScanTest, count) {
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
TEST_F(TableScanTest, sequentialSplitNoDoubleRead) {
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
TEST_F(TableScanTest, outOfOrderSplits) {
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
TEST_F(TableScanTest, splitDoubleRead) {
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

TEST_F(TableScanTest, multipleSplits) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(10, 1'000);
  for (int32_t i = 0; i < vectors.size(); i++) {
    writeToFile(filePaths[i]->path, kTableScanTest, vectors[i]);
  }
  createDuckDbTable(vectors);

  assertQuery(tableScanNode(), filePaths, "SELECT * FROM tmp");
}

TEST_F(TableScanTest, waitForSplit) {
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

TEST_F(TableScanTest, splitOffsetAndLength) {
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

TEST_F(TableScanTest, fileNotFound) {
  CursorParameters params;
  params.planNode = tableScanNode();

  auto cursor = std::make_unique<TaskCursor>(params);
  cursor->task()->addSplit("0", makeHiveSplit("file:/path/to/nowhere.orc"));
  EXPECT_THROW(cursor->moveNext(), std::runtime_error);
}

// A valid ORC file (containing headers) but no data.
TEST_F(TableScanTest, validFileNoData) {
  auto rowType = ROW({"c0", "c1", "c2"}, {DOUBLE(), VARCHAR(), BIGINT()});

  auto filePath = facebook::velox::test::getDataFilePath(
      "velox/exec/tests", "data/emptyPresto.dwrf");
  auto split = std::make_shared<HiveConnectorSplit>(
      kHiveConnectorId, "file:" + filePath, 0, fs::file_size(filePath) / 2);

  auto op = tableScanNode(rowType);
  assertQuery(op, split, "");
}

// An invalid (size = 0) file.
TEST_F(TableScanTest, emptyFile) {
  auto filePath = TempFilePath::create();

  try {
    assertQuery(
        tableScanNode(), makeHiveSplit(filePath->path), "SELECT * FROM tmp");
    ASSERT_FALSE(true) << "Function should throw.";
  } catch (const VeloxException& e) {
    EXPECT_EQ("ORC file is empty", e.message());
  }
}

TEST_F(TableScanTest, cacheEnabled) {
  // DataCache not enabled
  auto rowType = ROW({"c0", "c1", "c2"}, {DOUBLE(), VARCHAR(), BIGINT()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(rowType);

  auto cursor = std::make_unique<TaskCursor>(params);

  addSplit(cursor->task().get(), "0", makeHiveSplit(filePath->path));
  cursor->task()->noMoreSplits("0");

  while (cursor->moveNext()) {
    cursor->current();
  }

  EXPECT_EQ(dataCache->getCount, 0);
  EXPECT_EQ(dataCache->putCount, 0);

  // DataCache enabled
  std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs;
  std::unordered_map<std::string, std::string> hiveConnectorConfigs;
  hiveConnectorConfigs.insert({kNodeSelectionStrategy, kSoftAffinity});
  connectorConfigs.insert(
      {kHiveConnectorId,
       std::make_shared<core::MemConfig>(std::move(hiveConnectorConfigs))});
  params.queryCtx = core::QueryCtx::create(
      std::make_shared<core::MemConfig>(),
      connectorConfigs,
      memory::MappedMemory::getInstance());

  cursor = std::make_unique<TaskCursor>(params);

  addSplit(cursor->task().get(), "0", makeHiveSplit(filePath->path));
  cursor->task()->noMoreSplits("0");

  while (cursor->moveNext()) {
    cursor->current();
  }

  EXPECT_NE(dataCache->getCount, 0);
  EXPECT_NE(dataCache->putCount, 0);
}

TEST_F(TableScanTest, partitionedTable) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), DOUBLE()});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);

  testPartitionedTable(filePath->path);
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

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  auto subfieldFilters = singleSubfieldFilter("c1", boolEqual(true));
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments = {{"c0", regularColumn("c0")}};
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
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputPositions);

  subfieldFilters = singleSubfieldFilter("c1", boolEqual(false));
  task = assertQuery("SELECT c0 FROM tmp WHERE c1 = false");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputPositions);
}

TEST_F(TableScanTest, statsBasedSkippingDouble) {
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector({makeFlatVector<double>(
      size, [](auto row) { return (double)(row + 0.0001); })});

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1.05 -> whole file should be skipped based on stats
  auto subfieldFilters =
      singleSubfieldFilter("c0", lessThanOrEqualDouble(-1.05));

  ColumnHandleMap assignments = {{"c0", regularColumn("c0")}};

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
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // c0 >= 11,111.06 - first stride should be skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", greaterThanOrEqualDouble(11'111.06));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 >= 11111.06");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // c0 between 10'100.06 and 10'500.08 - all strides but second should be
  // skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", betweenDouble(10'100.06, 10'500.08));
  task =
      assertQuery("SELECT c0 FROM tmp WHERE c0 between 10100.06 AND 10500.08");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);

  // c0 <= 1,234.005 - all strides but first should be skipped
  subfieldFilters =
      singleSubfieldFilter("c0", lessThanOrEqualDouble(1'234.005));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 <= 1234.005");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);
}

TEST_F(TableScanTest, statsBasedSkippingFloat) {
  auto filePaths = makeFilePaths(1);
  auto size = 31'234;
  auto rowVector = makeRowVector({makeFlatVector<float>(
      size, [](auto row) { return (float)(row + 0.0001); })});

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1.05 -> whole file should be skipped based on stats
  auto subfieldFilters =
      singleSubfieldFilter("c0", lessThanOrEqualFloat(-1.05));

  ColumnHandleMap assignments = {{"c0", regularColumn("c0")}};

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
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // c0 >= 11,111.06 - first stride should be skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", greaterThanOrEqualFloat(11'111.06));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 >= 11111.06");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // c0 between 10'100.06 and 10'500.08 - all strides but second should be
  // skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c0", betweenFloat(10'100.06, 10'500.08));
  task =
      assertQuery("SELECT c0 FROM tmp WHERE c0 between 10100.06 AND 10500.08");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);

  // c0 <= 1,234.005 - all strides but first should be skipped
  subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqualFloat(1'234.005));
  task = assertQuery("SELECT c0 FROM tmp WHERE c0 <= 1234.005");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);
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

  writeToFile(filePaths[0]->path, kTableScanTest, rowVector);
  createDuckDbTable({rowVector});

  // c0 <= -1 -> whole file should be skipped based on stats
  auto subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqual(-1));

  ColumnHandleMap assignments = {{"c1", regularColumn("c1")}};

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
  EXPECT_EQ(0, stats.rawInputPositions);
  EXPECT_EQ(0, stats.inputPositions);
  EXPECT_EQ(0, stats.outputPositions);

  // c2 = "tomato" -> whole file should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c2", equal("tomato"));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 = 'tomato'");
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // c2 in ("x", "y") -> whole file should be skipped based on stats
  subfieldFilters =
      singleSubfieldFilter("c2", orFilter(equal("x"), equal("y")));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 IN ('x', 'y')");
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // c0 >= 11,111 - first stride should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c0", greaterThanOrEqual(11'111));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // c2 = "banana" - odd stripes should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c2", equal("banana"));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 = 'banana'");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputPositions);

  // c2 in ("banana", "y") -> same as previous
  subfieldFilters =
      singleSubfieldFilter("c2", orFilter(equal("banana"), equal("y")));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 IN ('banana', 'y')");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputPositions);

  // c2 = "squash" - even stripes should be skipped based on stats
  subfieldFilters = singleSubfieldFilter("c2", equal("squash"));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 = 'squash'");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputPositions);

  // c2 in ("banana", "squash") -> no skipping
  subfieldFilters =
      singleSubfieldFilter("c2", orFilter(equal("banana"), equal("squash")));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 IN ('banana', 'squash')");
  EXPECT_EQ(31'234, getTableScanStats(task).rawInputPositions);

  // c0 <= 100 AND c0 >= 20'100 - skip second stride
  subfieldFilters = singleSubfieldFilter(
      "c0", bigintOr(lessThanOrEqual(100), greaterThanOrEqual(20'100)));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 <= 100 OR c0 >= 20100");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // c0 between 10'100 and 10'500 - all strides but second should be skipped
  // based on stats
  subfieldFilters = singleSubfieldFilter("c0", between(10'100, 10'500));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 between 10100 AND 10500");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);

  // c0 <= 1,234 - all strides but first should be skipped
  subfieldFilters = singleSubfieldFilter("c0", lessThanOrEqual(1'234));
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 <= 1234");
  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);

  // c0 >= 10234 AND c1 <= 20345 - first and last strides should be skipped
  subfieldFilters = SubfieldFiltersBuilder()
                        .add("c0", greaterThanOrEqual(10234))
                        .add("c1", lessThanOrEqual(20345))
                        .build();
  task = assertQuery("SELECT c1 FROM tmp WHERE c0 >= 10234 AND c1 <= 20345");
  EXPECT_EQ(20'000, getTableScanStats(task).rawInputPositions);
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
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // skip all but first rowgroup
  filters = singleSubfieldFilter("c1", in({0, 10, 100, 1000}));
  task = assertQuery("SELECT c1 FROM tmp WHERE c1 in (0, 10, 100, 1000)");

  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);

  // skip whole file
  filters = singleSubfieldFilter(
      "c2", in(std::vector<std::string>{"apple", "cherry"}));
  task = assertQuery("SELECT c1 FROM tmp WHERE c2 in ('apple', 'cherry')");

  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // skip all but second rowgroup
  filters = singleSubfieldFilter(
      "c3", in(std::vector<std::string>{"banana", "grapefruit"}));
  task = assertQuery("SELECT c1 FROM tmp WHERE c3 in ('banana', 'grapefruit')");

  EXPECT_EQ(10'000, getTableScanStats(task).rawInputPositions);
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
  EXPECT_EQ(0, stats.rawInputPositions);
  EXPECT_EQ(0, stats.inputPositions);
  EXPECT_EQ(0, stats.outputPositions);

  // c1 IS NULL - first stride should be skipped based on stats
  filters = singleSubfieldFilter("c1", isNull());
  task = assertQuery("SELECT * FROM tmp WHERE c1 IS NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(size - 10'000, stats.rawInputPositions);
  EXPECT_EQ(size - 11'111, stats.inputPositions);
  EXPECT_EQ(size - 11'111, stats.outputPositions);

  // c1 IS NOT NULL - 3rd and 4th strides should be skipped based on stats
  filters = singleSubfieldFilter("c1", isNotNull());
  task = assertQuery("SELECT * FROM tmp WHERE c1 IS NOT NULL");

  stats = getTableScanStats(task);
  EXPECT_EQ(20'000, stats.rawInputPositions);
  EXPECT_EQ(11'111, stats.inputPositions);
  EXPECT_EQ(11'111, stats.outputPositions);
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
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // skip 2nd row group
  filters = singleSubfieldFilter(
      "c0",
      stringOr(
          lessThanOrEqual("com.facebook.presto.orc.stream.01234"),
          greaterThanOrEqual("com.facebook.presto.orc.stream.20123")));
  task = assertQuery(
      "SELECT * FROM tmp WHERE c0 <= 'com.facebook.presto.orc.stream.01234' or c0 >= 'com.facebook.presto.orc.stream.20123'");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // skip first 3 row groups
  filters = singleSubfieldFilter(
      "c0", greaterThanOrEqual("com.facebook.presto.orc.stream.30123"));
  task = assertQuery(
      "SELECT * FROM tmp WHERE c0 >= 'com.facebook.presto.orc.stream.30123'");
  EXPECT_EQ(size - 30'000, getTableScanStats(task).rawInputPositions);
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
  EXPECT_EQ(size, getTableScanStats(task).rawInputPositions);
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
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // skip 1st rowgroup
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(11'111));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // skip 2nd rowgroup
  filters = singleSubfieldFilter(
      "c0", bigintOr(lessThanOrEqual(1'000), greaterThanOrEqual(23'456)));
  task = assertQuery("SELECT * FROM tmp WHERE c0 <= 1000 OR c0 >= 23456");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // skip last 2 rowgroups
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(20'123));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 20123");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputPositions);
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
  EXPECT_EQ(0, getTableScanStats(task).rawInputPositions);

  // skip 1st rowgroup
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(11'111));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 11111");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // skip 2nd rowgroup
  filters = singleSubfieldFilter(
      "c0", bigintOr(lessThanOrEqual(1'000), greaterThanOrEqual(23'456)));
  task = assertQuery("SELECT * FROM tmp WHERE c0 <= 1000 OR c0 >= 23456");
  EXPECT_EQ(size - 10'000, getTableScanStats(task).rawInputPositions);

  // skip last 2 rowgroups
  filters = singleSubfieldFilter("c0", greaterThanOrEqual(20'123));
  task = assertQuery("SELECT * FROM tmp WHERE c0 >= 20123");
  EXPECT_EQ(size - 20'000, getTableScanStats(task).rawInputPositions);
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

TEST_F(TableScanTest, filterPushdown) {
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
  EXPECT_EQ(tableScanStats.rawInputPositions, 10'000);
  EXPECT_LT(tableScanStats.inputPositions, tableScanStats.rawInputPositions);
  EXPECT_EQ(tableScanStats.inputPositions, tableScanStats.outputPositions);

  // Repeat the same but do not project out the filtered columns.
  assignments.clear();
  assignments["c0"] = regularColumn("c0");
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
          .finalAggregation({}, {"sum(1)"})
          .planNode(),
      filePaths,
      "SELECT count(*) FROM tmp WHERE (c1 >= 0 OR c1 IS NULL) AND c3");

  // Do the same for count, no filter, no projections.
  assignments.clear();
  tableHandle = makeTableHandle(std::move(subfieldFilters));
  assertQuery(
      PlanBuilder()
          .tableScan(ROW({}, {}), tableHandle, assignments)
          .finalAggregation({}, {"sum(1)"})
          .planNode(),
      filePaths,
      "SELECT count(*) FROM tmp");
}

TEST_F(TableScanTest, path) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto filePath = makeFilePaths(1)[0];
  auto vector = makeVectors(1, 1'000, rowType)[0];
  writeToFile(filePath->path, kTableScanTest, vector);
  createDuckDbTable({vector});

  static const char* kPath = "$path";

  auto assignments = allRegularColumns(rowType);
  assignments[kPath] = synthesizedColumn(kPath);

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
    writeToFile(filePaths[i]->path, kTableScanTest, rowVector);
    rowVectors.emplace_back(rowVector);

    splits.emplace_back(std::make_shared<HiveConnectorSplit>(
        kHiveConnectorId,
        filePaths[i]->path,
        0,
        fs::file_size(filePaths[i]->path),
        std::unordered_map<std::string, std::string>(),
        bucket));
  }

  createDuckDbTable(rowVectors);

  static const char* kBucket = "$bucket";
  auto rowType =
      std::dynamic_pointer_cast<const RowType>(rowVectors.front()->type());

  auto assignments = allRegularColumns(rowType);
  assignments[kBucket] = synthesizedColumn(kBucket);

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

TEST_F(TableScanTest, remainingFilter) {
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
  assignments = {{"c2", regularColumn("c2")}};
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
  assignments = {{"c1", regularColumn("c1")}, {"c2", regularColumn("c2")}};
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

TEST_F(TableScanTest, aggregationPushdown) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  auto tableHandle = makeTableHandle(SubfieldFilters(), nullptr);
  std::string query;

  auto assignments = allRegularColumns(rowType_);

  auto op =
      PlanBuilder()
          .tableScan(rowType_, tableHandle, assignments)
          .finalAggregation(
              {5}, {"max(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(c4)"})
          .planNode();
  query =
      "SELECT c5, max(c0), sum(c1), sum(c2), sum(c3), sum(c4) "
      "FROM tmp group by c5";
  assertQuery(op, {filePath}, query);

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .finalAggregation(
               {5}, {"max(c0)", "max(c1)", "max(c2)", "max(c3)", "max(c4)"})
           .planNode();
  query =
      "SELECT c5, max(c0), max(c1), max(c2), max(c3), max(c4) "
      "FROM tmp group by c5";
  assertQuery(op, {filePath}, query);

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .finalAggregation(
               {5}, {"min(c0)", "min(c1)", "min(c2)", "min(c3)", "min(c4)"})
           .planNode();
  query =
      "SELECT c5, min(c0), min(c1), min(c2), min(c3), min(c4) "
      "FROM tmp group by c5";
  assertQuery(op, {filePath}, query);

  // Pushdown should also happen if there is a FilterProject node that doesn't
  // touch columns being aggregated
  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .project({"c0 % 5", "c1"}, {"c0_mod_5", "c1"})
           .finalAggregation({0}, {"sum(c1)"})
           .planNode();

  assertQuery(op, {filePath}, "SELECT c0 % 5, sum(c1) FROM tmp group by 1");

  // Add remaining filter to scan to expose LazyVectors wrapped in Dictionary to
  // aggregation.
  tableHandle = makeTableHandle(
      SubfieldFilters(), parseExpr("length(c5) % 2 = 0", rowType_));
  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .finalAggregation({5}, {"max(c0)"})
           .planNode();
  query = "SELECT c5, max(c0) FROM tmp WHERE length(c5) % 2 = 0 GROUP BY c5 ";
  assertQuery(op, {filePath}, query);

  // No pushdown if two aggregates use the same column or a column is not a
  // LazyVector
  tableHandle = makeTableHandle(SubfieldFilters(), nullptr);
  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .finalAggregation({5}, {"min(c0)", "max(c0)"})
           .planNode();
  assertQuery(
      op, {filePath}, "SELECT c5, min(c0), max(c0) FROM tmp GROUP BY 1");

  op = PlanBuilder()
           .tableScan(rowType_, tableHandle, assignments)
           .project({"c5", "c0", "c0 + c1"}, {"c5", "c0", "c0_plus_c1"})
           .finalAggregation({0}, {"min(c0)", "max(c0_plus_c1)"})
           .planNode();
  assertQuery(
      op, {filePath}, "SELECT c5, min(c0), max(c0 + c1) FROM tmp GROUP BY 1");
}

TEST_F(TableScanTest, bitwiseAggregationPushdown) {
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, kTableScanTest, vectors);
  createDuckDbTable(vectors);
  auto tableHandle = makeTableHandle(SubfieldFilters(), nullptr);

  auto assignments = allRegularColumns(rowType_);

  auto op = PlanBuilder()
                .tableScan(rowType_, tableHandle, assignments)
                .finalAggregation(
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
           .finalAggregation(
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
