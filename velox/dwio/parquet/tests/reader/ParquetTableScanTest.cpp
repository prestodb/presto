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
#include "velox/dwio/common/tests/utils/DataFiles.h" // @manual
#include "velox/dwio/parquet/RegisterParquetReader.h" // @manual
#include "velox/dwio/parquet/reader/PageReader.h" // @manual
#include "velox/dwio/parquet/reader/ParquetReader.h" // @manual=//velox/connectors/hive:velox_hive_connector_parquet
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h" // @manual
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/external/date/tz.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"
#include "velox/type/tz/TimeZoneMap.h"

#include "velox/connectors/hive/HiveConfig.h" // @manual=//velox/connectors/hive:velox_hive_connector_parquet
#include "velox/dwio/parquet/writer/Writer.h" // @manual

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::parquet;

class ParquetTableScanTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void SetUp() {
    OperatorTestBase::SetUp();
    registerParquetReaderFactory();

    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
  }

  void assertSelect(
      std::vector<std::string>&& outputColumnNames,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder().tableScan(rowType).planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithDataColumns(
      std::vector<std::string>&& outputColumnNames,
      const RowTypePtr& dataColumns,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan =
        PlanBuilder().tableScan(rowType, {}, "", dataColumns).planNode();
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
      const std::string& sql,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments = {}) {
    auto rowType = getRowType(std::move(outputColumnNames));
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;

    auto plan =
        PlanBuilder(pool_.get())
            .setParseOptions(options)
            .tableScan(
                rowType, subfieldFilters, remainingFilter, nullptr, assignments)
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

  void assertSelectWithTimezone(
      std::vector<std::string>&& outputColumnNames,
      const std::string& sql,
      const std::string& sessionTimezone) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan = PlanBuilder().tableScan(rowType).planNode();
    std::vector<exec::Split> splits;
    splits.reserve(splits_.size());
    for (const auto& connectorSplit : splits_) {
      splits.emplace_back(folly::copy(connectorSplit), -1);
    }

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kSessionTimezone, sessionTimezone)
        .splits(splits)
        .assertResults(sql);
  }

  void loadData(
      const std::string& filePath,
      RowTypePtr rowType,
      RowVectorPtr data,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = std::nullopt,
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = std::nullopt) {
    splits_ = {makeSplit(filePath, partitionKeys, infoColumns)};
    rowType_ = rowType;
    createDuckDbTable({data});
  }

  void loadDataWithRowType(const std::string& filePath, RowVectorPtr data) {
    splits_ = {makeSplit(filePath)};
    auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
    dwio::common::ReaderOptions readerOpts{pool.get()};
    auto reader = std::make_unique<ParquetReader>(
        std::make_unique<facebook::velox::dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(filePath), readerOpts.memoryPool()),
        readerOpts);
    rowType_ = reader->rowType();
    createDuckDbTable({data});
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return facebook::velox::test::getDataFilePath(
        "velox/dwio/parquet/tests/reader", "../examples/" + fileName);
  }

  std::shared_ptr<connector::hive::HiveConnectorSplit> makeSplit(
      const std::string& filePath,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = std::nullopt,
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = std::nullopt) {
    return makeHiveConnectorSplits(
        filePath,
        1,
        dwio::common::FileFormat::PARQUET,
        partitionKeys,
        infoColumns)[0];
  }

  // Write data to a parquet file on specified path.
  // @param writeInt96AsTimestamp Write timestamp as Int96 if enabled.
  void writeToParquetFile(
      const std::string& path,
      const std::vector<RowVectorPtr>& data,
      bool writeInt96AsTimestamp) {
    VELOX_CHECK_GT(data.size(), 0);

    WriterOptions options;
    options.writeInt96AsTimestamp = writeInt96AsTimestamp;

    auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
    auto sink = std::make_unique<dwio::common::WriteFileSink>(
        std::move(writeFile), path);
    auto childPool =
        rootPool_->addAggregateChild("ParquetTableScanTest.Writer");
    options.memoryPool = childPool.get();

    auto writer = std::make_unique<Writer>(
        std::move(sink), options, asRowType(data[0]->type()));

    for (const auto& vector : data) {
      writer->write(vector);
    }
    writer->close();
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

TEST_F(ParquetTableScanTest, lazy) {
  auto filePath = getExampleFilePath("sample.parquet");
  auto schema = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  CursorParameters params;
  params.copyResult = false;
  params.planNode = PlanBuilder().tableScan(schema).planNode();
  auto cursor = TaskCursor::create(params);
  cursor->task()->addSplit("0", exec::Split(makeSplit(filePath)));
  cursor->task()->noMoreSplits("0");
  int rows = 0;
  while (cursor->moveNext()) {
    auto* result = cursor->current()->asUnchecked<RowVector>();
    ASSERT_TRUE(result->childAt(0)->isLazy());
    ASSERT_TRUE(result->childAt(1)->isLazy());
    rows += result->size();
  }
  ASSERT_EQ(rows, 20);
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
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

TEST_F(ParquetTableScanTest, nullMap) {
  auto path = getExampleFilePath("null_map.parquet");
  loadData(
      path,
      ROW({"i", "c"}, {VARCHAR(), MAP(VARCHAR(), VARCHAR())}),
      makeRowVector(
          {"i", "c"},
          {makeConstant<std::string>("1", 1),
           makeNullableMapVector<std::string, std::string>({std::nullopt})}));

  assertSelectWithFilter({"i", "c"}, {}, "", "SELECT i, c FROM tmp");
}

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

TEST_F(ParquetTableScanTest, filterOnNestedArray) {
  loadData(
      getExampleFilePath("struct_of_array.parquet"),
      ROW({"struct"},
          {ROW({"a0", "a1"}, {ARRAY(VARCHAR()), ARRAY(INTEGER())})}),
      makeRowVector(
          {"unused"},
          {
              makeFlatVector<int32_t>({}),
          }));

  assertSelectWithFilter(
      {"struct"}, {}, "struct.a0 is null", "SELECT ROW(NULL, NULL)");
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
      core::QueryCtx::create(executor.get());
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
  static const char* kPath = "file_path";
  // case 1: file not have `_tmp_metadata_row_index`, scan generate it for user.
  auto filePath = getExampleFilePath("sample.parquet");
  loadData(
      filePath,
      ROW({"a", "b", "_tmp_metadata_row_index", kPath},
          {BIGINT(), DOUBLE(), BIGINT(), VARCHAR()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index", kPath},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row; }),
              makeFlatVector<std::string>(
                  20, [filePath](auto row) { return filePath; }),
          }),
      std::nullopt,
      std::unordered_map<std::string, std::string>{{kPath, filePath}});
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
  assignments[kPath] = synthesizedColumn(kPath, VARCHAR());
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
  assertSelectWithAssignments(
      {"_tmp_metadata_row_index", "a"},
      assignments,
      "SELECT _tmp_metadata_row_index, a FROM tmp");
  assertSelectWithAssignments(
      {"_tmp_metadata_row_index"},
      assignments,
      "SELECT _tmp_metadata_row_index FROM tmp");
  assertSelectWithAssignments(
      {kPath, "_tmp_metadata_row_index"},
      assignments,
      fmt::format("SELECT {}, _tmp_metadata_row_index FROM tmp", kPath));

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

// The file icebergNullIcebergPartition.parquet was copied from a null
// partition in an Iceberg table created with the below DDL using Spark:
//
// CREATE TABLE iceberg_tmp_parquet_partitioned
//    ( c0 bigint, c1 bigint )
// USING iceberg
// PARTITIONED BY (c1)
// TBLPROPERTIES ('write.format.default' = 'parquet', 'format-version' = 2,
// 'write.delete.mode' = 'merge-on-read') LOCATION
// 's3a://presto-workload/tmp/iceberg_tmp_parquet_partitioned';
//
// INSERT INTO iceberg_tmp_parquet_partitioned
// VALUES (1, 1), (2, null),(3, null);
TEST_F(ParquetTableScanTest, filterNullIcebergPartition) {
  loadData(
      getExampleFilePath("icebergNullIcebergPartition.parquet"),
      ROW({"c0", "c1"}, {BIGINT(), BIGINT()}),
      makeRowVector(
          {"c0", "c1"},
          {
              makeFlatVector<int64_t>(std::vector<int64_t>{2, 3}),
              makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
          }),
      std::unordered_map<std::string, std::optional<std::string>>{
          {"c1", std::nullopt}});

  std::shared_ptr<connector::ColumnHandle> c0 = makeColumnHandle(
      "c0", BIGINT(), BIGINT(), {}, HiveColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c1 = makeColumnHandle(
      "c1",
      BIGINT(),
      BIGINT(),
      {},
      HiveColumnHandle::ColumnType::kPartitionKey);

  assertSelectWithFilter(
      {"c0", "c1"},
      {"c1 IS NOT NULL"},
      "",
      "SELECT c0, c1 FROM tmp WHERE c1 IS NOT NULL",
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>{
          {"c0", c0}, {"c1", c1}});

  assertSelectWithFilter(
      {"c0", "c1"},
      {"c1 IS NULL"},
      "",
      "SELECT c0, c1 FROM tmp WHERE c1 IS NULL",
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>{
          {"c0", c0}, {"c1", c1}});
}

TEST_F(ParquetTableScanTest, sessionTimezone) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::parquet::PageReader::readPageHeader",
      std::function<void(PageReader*)>(([&](PageReader* reader) {
        VELOX_CHECK_EQ(reader->sessionTimezone()->name(), "Asia/Shanghai");
      })));

  // Read sample.parquet to verify if the sessionTimezone in the PageReader
  // meets expectations.
  loadData(
      getExampleFilePath("sample.parquet"),
      ROW({"a", "b"}, {BIGINT(), DOUBLE()}),
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  assertSelectWithTimezone({"a"}, "SELECT a FROM tmp", "Asia/Shanghai");
}

TEST_F(ParquetTableScanTest, timestampFilter) {
  // Timestamp-int96.parquet holds one column (t: TIMESTAMP) and
  // 10 rows in one row group. Data is in SNAPPY compressed format.
  // The values are:
  // |t                  |
  // +-------------------+
  // |2015-06-01 19:34:56|
  // |2015-06-02 19:34:56|
  // |2001-02-03 03:34:06|
  // |1998-03-01 08:01:06|
  // |2022-12-23 03:56:01|
  // |1980-01-24 00:23:07|
  // |1999-12-08 13:39:26|
  // |2023-04-21 09:09:34|
  // |2000-09-12 22:36:29|
  // |2007-12-12 04:27:56|
  // +-------------------+
  auto vector = makeFlatVector<Timestamp>(
      {Timestamp(1433187296, 0),
       Timestamp(1433273696, 0),
       Timestamp(981171246, 0),
       Timestamp(888739266, 0),
       Timestamp(1671767761, 0),
       Timestamp(317521387, 0),
       Timestamp(944660366, 0),
       Timestamp(1682068174, 0),
       Timestamp(968798189, 0),
       Timestamp(1197433676, 0)});

  loadData(
      getExampleFilePath("timestamp_int96.parquet"),
      ROW({"t"}, {TIMESTAMP()}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));

  assertSelectWithFilter({"t"}, {}, "", "SELECT t from tmp");
  assertSelectWithFilter(
      {"t"},
      {},
      "t < TIMESTAMP '2000-09-12 22:36:29'",
      "SELECT t from tmp where t < TIMESTAMP '2000-09-12 22:36:29'");
  assertSelectWithFilter(
      {"t"},
      {},
      "t <= TIMESTAMP '2000-09-12 22:36:29'",
      "SELECT t from tmp where t <= TIMESTAMP '2000-09-12 22:36:29'");
  assertSelectWithFilter(
      {"t"},
      {},
      "t > TIMESTAMP '1980-01-24 00:23:07'",
      "SELECT t from tmp where t > TIMESTAMP '1980-01-24 00:23:07'");
  assertSelectWithFilter(
      {"t"},
      {},
      "t >= TIMESTAMP '1980-01-24 00:23:07'",
      "SELECT t from tmp where t >= TIMESTAMP '1980-01-24 00:23:07'");
  assertSelectWithFilter(
      {"t"},
      {},
      "t == TIMESTAMP '2022-12-23 03:56:01'",
      "SELECT t from tmp where t == TIMESTAMP '2022-12-23 03:56:01'");
}

TEST_F(ParquetTableScanTest, timestampPrecisionMicrosecond) {
  // Write timestamp data into parquet.
  constexpr int kSize = 10;
  auto vector = makeRowVector({
      makeFlatVector<Timestamp>(
          kSize, [](auto i) { return Timestamp(i, i * 1'001'001); }),
  });
  auto schema = asRowType(vector->type());
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, true);
  auto plan = PlanBuilder().tableScan(schema).planNode();

  // Read timestamp data from parquet with microsecond precision.
  CursorParameters params;
  std::shared_ptr<folly::Executor> executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency());
  std::shared_ptr<core::QueryCtx> queryCtx =
      core::QueryCtx::create(executor.get());
  std::unordered_map<std::string, std::string> session = {
      {std::string(connector::hive::HiveConfig::kReadTimestampUnitSession),
       "6"}};
  queryCtx->setConnectorSessionOverridesUnsafe(
      kHiveConnectorId, std::move(session));
  params.queryCtx = queryCtx;
  params.planNode = plan;
  const int numSplitsPerFile = 1;

  bool noMoreSplits = false;
  auto addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          {file->getPath()},
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
  auto expected = makeRowVector({
      makeFlatVector<Timestamp>(
          kSize, [](auto i) { return Timestamp(i, i * 1'001'000); }),
  });
  assertEqualResults({expected}, result.second);
}

TEST_F(ParquetTableScanTest, testColumnNotExists) {
  auto rowType =
      ROW({"a", "b", "not_exists", "not_exists_array", "not_exists_map"},
          {BIGINT(),
           DOUBLE(),
           BIGINT(),
           ARRAY(VARBINARY()),
           MAP(VARCHAR(), BIGINT())});
  // message schema {
  //  optional int64 a;
  //  optional double b;
  // }
  loadData(
      getExampleFilePath("sample.parquet"),
      rowType,
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  assertSelectWithDataColumns(
      {"a", "b", "not_exists", "not_exists_array", "not_exists_map"},
      rowType,
      "SELECT a, b, NULL, NULL, NULL FROM tmp");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
