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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

#include "velox/connectors/hive/HiveConfig.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::parquet;

class ParquetTableScanTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void SetUp() {
    registerParquetReaderFactory();

    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId, std::make_shared<core::MemConfig>());
    connector::registerConnector(hiveConnector);
  }

  void assertSelect(
      std::vector<std::string>&& outputColumnNames,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder().tableScan(rowType).planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithAssignments(
      std::vector<std::string>&& outputColumnNames,
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>&
          assignments,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan = PlanBuilder()
                    .tableScan(rowType, {}, "", nullptr, assignments)
                    .planNode();
    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilter(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& subfieldFilters,
      const std::string& remainingFilter,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;

    auto plan = PlanBuilder(pool_.get())
                    .setParseOptions(options)
                    .tableScan(rowType, subfieldFilters, remainingFilter)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithAgg(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& groupingKeys,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder()
                    .tableScan(rowType)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilterAndAgg(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& filters,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& groupingKeys,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder()
                    .tableScan(rowType, filters)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void
  loadData(const std::string& filePath, RowTypePtr rowType, RowVectorPtr data) {
    splits_ = {makeSplit(filePath)};
    rowType_ = rowType;
    createDuckDbTable({data});
  }

  void loadDataWithRowType(const std::string& filePath, RowVectorPtr data) {
    splits_ = {makeSplit(filePath)};
    auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
    dwio::common::ReaderOptions readerOpts{pool.get()};
    auto reader = std::make_unique<ParquetReader>(
        std::make_unique<facebook::velox::dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(filePath),
            readerOpts.getMemoryPool()),
        readerOpts);
    rowType_ = reader->rowType();
    createDuckDbTable({data});
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return facebook::velox::test::getDataFilePath(
        "velox/dwio/parquet/tests/reader", "../examples/" + fileName);
  }

  std::shared_ptr<connector::hive::HiveConnectorSplit> makeSplit(
      const std::string& filePath) {
    auto split = makeHiveConnectorSplits(
        filePath, 1, dwio::common::FileFormat::PARQUET)[0];

    return split;
  }

 private:
  RowTypePtr getRowType(std::vector<std::string>&& outputColumnNames) const {
    std::vector<TypePtr> types;
    for (auto colName : outputColumnNames) {
      types.push_back(rowType_->findChild(colName));
    }

    return ROW(std::move(outputColumnNames), std::move(types));
  }

  RowTypePtr rowType_;
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits_;
};

TEST_F(ParquetTableScanTest, basic) {
  loadData(
      getExampleFilePath("sample.parquet"),
      ROW({"a", "b"}, {BIGINT(), DOUBLE()}),
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  // Plain select.
  assertSelect({"a"}, "SELECT a FROM tmp");
  assertSelect({"b"}, "SELECT b FROM tmp");
  assertSelect({"a", "b"}, "SELECT a, b FROM tmp");
  assertSelect({"b", "a"}, "SELECT b, a FROM tmp");

  // With filters.
  assertSelectWithFilter({"a"}, {"a < 3"}, "", "SELECT a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"a", "b"}, {"a < 3"}, "", "SELECT a, b FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"b", "a"}, {"a < 3"}, "", "SELECT b, a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"a", "b"}, {"a < 0"}, "", "SELECT a, b FROM tmp WHERE a < 0");

  assertSelectWithFilter(
      {"b"}, {"b < DOUBLE '2.0'"}, "", "SELECT b FROM tmp WHERE b < 2.0");
  assertSelectWithFilter(
      {"a", "b"},
      {"b >= DOUBLE '2.0'"},
      "",
      "SELECT a, b FROM tmp WHERE b >= 2.0");
  assertSelectWithFilter(
      {"b", "a"},
      {"b <= DOUBLE '2.0'"},
      "",
      "SELECT b, a FROM tmp WHERE b <= 2.0");
  assertSelectWithFilter(
      {"a", "b"},
      {"b < DOUBLE '0.0'"},
      "",
      "SELECT a, b FROM tmp WHERE b < 0.0");

  // With aggregations.
  assertSelectWithAgg({"a"}, {"sum(a)"}, {}, "SELECT sum(a) FROM tmp");
  assertSelectWithAgg({"b"}, {"max(b)"}, {}, "SELECT max(b) FROM tmp");
  assertSelectWithAgg(
      {"a", "b"}, {"min(a)", "max(b)"}, {}, "SELECT min(a), max(b) FROM tmp");
  assertSelectWithAgg(
      {"b", "a"}, {"max(b)"}, {"a"}, "SELECT max(b), a FROM tmp GROUP BY a");
  assertSelectWithAgg(
      {"a", "b"}, {"max(a)"}, {"b"}, "SELECT max(a), b FROM tmp GROUP BY b");

  // With filter and aggregation.
  assertSelectWithFilterAndAgg(
      {"a"}, {"a < 3"}, {"sum(a)"}, {}, "SELECT sum(a) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"a", "b"},
      {"a < 3"},
      {"sum(b)"},
      {},
      "SELECT sum(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"a", "b"},
      {"a < 3"},
      {"min(a)", "max(b)"},
      {},
      "SELECT min(a), max(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"b", "a"},
      {"a < 3"},
      {"max(b)"},
      {"a"},
      "SELECT max(b), a FROM tmp WHERE a < 3 GROUP BY a");
}

TEST_F(ParquetTableScanTest, countStar) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows.
  auto filePath = getExampleFilePath("sample.parquet");
  auto split = makeSplit(filePath);

  // Output type does not have any columns.
  auto rowType = ROW({}, {});
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .singleAggregation({}, {"count(0)"})
                  .planNode();

  assertQuery(plan, {split}, "SELECT 20");
}

TEST_F(ParquetTableScanTest, decimalSubfieldFilter) {
  // decimal.parquet holds two columns (a: DECIMAL(5, 2), b: DECIMAL(20, 5)) and
  // 20 rows (10 rows per group). Data is in plain uncompressed format:
  //   a: [100.01 .. 100.20]
  //   b: [100000000000000.00001 .. 100000000000000.00020]
  std::vector<int64_t> unscaledShortValues(20);
  std::iota(unscaledShortValues.begin(), unscaledShortValues.end(), 10001);
  loadData(
      getExampleFilePath("decimal.parquet"),
      ROW({"a"}, {DECIMAL(5, 2)}),
      makeRowVector(
          {"a"},
          {
              makeFlatVector(unscaledShortValues, DECIMAL(5, 2)),
          }));

  assertSelectWithFilter(
      {"a"}, {"a < 100.07"}, "", "SELECT a FROM tmp WHERE a < 100.07");
  assertSelectWithFilter(
      {"a"}, {"a <= 100.07"}, "", "SELECT a FROM tmp WHERE a <= 100.07");
  assertSelectWithFilter(
      {"a"}, {"a > 100.07"}, "", "SELECT a FROM tmp WHERE a > 100.07");
  assertSelectWithFilter(
      {"a"}, {"a >= 100.07"}, "", "SELECT a FROM tmp WHERE a >= 100.07");
  assertSelectWithFilter(
      {"a"}, {"a = 100.07"}, "", "SELECT a FROM tmp WHERE a = 100.07");
  assertSelectWithFilter(
      {"a"},
      {"a BETWEEN 100.07 AND 100.12"},
      "",
      "SELECT a FROM tmp WHERE a BETWEEN 100.07 AND 100.12");

  VELOX_ASSERT_THROW(
      assertSelectWithFilter(
          {"a"}, {"a < 1000.7"}, "", "SELECT a FROM tmp WHERE a < 1000.7"),
      "Scalar function signature is not supported: lt(DECIMAL(5, 2), DECIMAL(5, 1))");
  VELOX_ASSERT_THROW(
      assertSelectWithFilter(
          {"a"}, {"a = 1000.7"}, "", "SELECT a FROM tmp WHERE a = 1000.7"),
      "Scalar function signature is not supported: eq(DECIMAL(5, 2), DECIMAL(5, 1))");
}

// Core dump is fixed.
TEST_F(ParquetTableScanTest, map) {
  auto vector = makeMapVector<StringView, StringView>({{{"name", "gluten"}}});

  loadData(
      getExampleFilePath("types.parquet"),
      ROW({"map"}, {MAP(VARCHAR(), VARCHAR())}),
      makeRowVector(
          {"map"},
          {
              vector,
          }));

  assertSelectWithFilter({"map"}, {}, "", "SELECT map FROM tmp");
}

// Core dump is fixed.
TEST_F(ParquetTableScanTest, singleRowStruct) {
  auto vector = makeArrayVector<int32_t>({{}});
  loadData(
      getExampleFilePath("single_row_struct.parquet"),
      ROW({"s"}, {ROW({"a", "b"}, {BIGINT(), BIGINT()})}),
      makeRowVector(
          {"s"},
          {
              vector,
          }));

  assertSelectWithFilter({"s"}, {}, "", "SELECT (0, 1)");
}

// Core dump and incorrect result are fixed.
TEST_F(ParquetTableScanTest, array) {
  auto vector = makeArrayVector<int32_t>({});
  loadData(
      getExampleFilePath("old_repeated_int.parquet"),
      ROW({"repeatedInt"}, {ARRAY(INTEGER())}),
      makeRowVector(
          {"repeatedInt"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"repeatedInt"}, {}, "", "SELECT UNNEST(array[array[1,2,3]])");
}

// Optional array with required elements.
TEST_F(ParquetTableScanTest, optArrayReqEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_0.parquet"),
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array['c', 'd'], array['e', 'f'], array[], null])");
}

// Required array with required elements.
TEST_F(ParquetTableScanTest, reqArrayReqEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_1.parquet"),
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array['c', 'd'], array[]])");
}

// Required array with optional elements.
TEST_F(ParquetTableScanTest, reqArrayOptEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_2.parquet"),
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', null], array[], array[null, 'b']])");
}

TEST_F(ParquetTableScanTest, arrayOfArrayTest) {
  auto vector = makeArrayVector<StringView>({});

  loadDataWithRowType(
      getExampleFilePath("array_of_array1.parquet"),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[null, array[array['g', 'h'], null]])");
}

// Required array with legacy format.
TEST_F(ParquetTableScanTest, reqArrayLegacy) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_3.parquet"),
      ROW({"element"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"element"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"element"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array[], array['c', 'd']])");
}

TEST_F(ParquetTableScanTest, readAsLowerCase) {
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(ROW({"a"}, {BIGINT()}), {}, "")
                  .planNode();
  CursorParameters params;
  std::shared_ptr<folly::Executor> executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency());
  std::shared_ptr<core::QueryCtx> queryCtx =
      std::make_shared<core::QueryCtx>(executor.get());
  std::unordered_map<std::string, std::string> session = {
      {std::string(
           connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession),
       "true"}};
  queryCtx->setConnectorSessionOverridesUnsafe(
      kHiveConnectorId, std::move(session));
  params.queryCtx = queryCtx;
  params.planNode = plan;
  const int numSplitsPerFile = 1;

  bool noMoreSplits = false;
  auto addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          {getExampleFilePath("upper.parquet")},
          numSplitsPerFile,
          dwio::common::FileFormat::PARQUET);
      for (const auto& split : splits) {
        task->addSplit("0", exec::Split(split));
      }
      task->noMoreSplits("0");
    }
    noMoreSplits = true;
  };
  auto result = readCursor(params, addSplits);
  ASSERT_TRUE(waitForTaskCompletion(result.first->task().get()));
  assertEqualResults(
      result.second, {makeRowVector({"a"}, {makeFlatVector<int64_t>({0, 1})})});
}

TEST_F(ParquetTableScanTest, rowIndex) {
  // case 1: file not have `_tmp_metadata_row_index`, scan generate it for user.
  loadData(
      getExampleFilePath("sample.parquet"),
      ROW({"a", "b", "_tmp_metadata_row_index"},
          {BIGINT(), DOUBLE(), BIGINT()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row; }),
          }));
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["a"] = std::make_shared<connector::hive::HiveColumnHandle>(
      "a",
      connector::hive::HiveColumnHandle::ColumnType::kRegular,
      BIGINT(),
      BIGINT());
  assignments["b"] = std::make_shared<connector::hive::HiveColumnHandle>(
      "b",
      connector::hive::HiveColumnHandle::ColumnType::kRegular,
      DOUBLE(),
      DOUBLE());
  assignments["_tmp_metadata_row_index"] =
      std::make_shared<connector::hive::HiveColumnHandle>(
          "_tmp_metadata_row_index",
          connector::hive::HiveColumnHandle::ColumnType::kRowIndex,
          BIGINT(),
          BIGINT());

  assertSelect({"a"}, "SELECT a FROM tmp");
  assertSelectWithAssignments(
      {"a", "_tmp_metadata_row_index"},
      assignments,
      "SELECT a, _tmp_metadata_row_index FROM tmp");
  // case 2: file has `_tmp_metadata_row_index` column, then use user data
  // insteads of generating it.
  loadData(
      getExampleFilePath("sample_with_rowindex.parquet"),
      ROW({"a", "b", "_tmp_metadata_row_index"},
          {BIGINT(), DOUBLE(), BIGINT()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
          }));

  assertSelect({"a"}, "SELECT a FROM tmp");
  assertSelect(
      {"a", "_tmp_metadata_row_index"},
      "SELECT a, _tmp_metadata_row_index FROM tmp");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
