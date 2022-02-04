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

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

class ParquetTpchTest : public testing::Test {
 protected:
  // Setup a DuckDB instance for the entire suite and load TPC-H data with scale
  // factor 0.01.
  static void SetUpTestCase() {
    if (duckDb_ == nullptr) {
      duckDb_ = std::make_shared<DuckDbQueryRunner>();
      constexpr double kTpchScaleFactor = 0.01;
      duckDb_->initializeTpch(kTpchScaleFactor);
    }
    functions::prestosql::registerAllScalarFunctions();
  }

  void SetUp() override {
    filesystems::registerLocalFileSystem();
    parquet::registerParquetReaderFactory();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);
    tempDirectory_ = exec::test::TempDirectoryPath::create();
    lineitemType_ =
        ROW({"orderkey",
             "partkey",
             "suppkey",
             "linenumber",
             "quantity",
             "extendedprice",
             "discount",
             "tax",
             "returnflag",
             "linestatus",
             "shipdate",
             "commitdate",
             "receiptdate",
             "shipinstruct",
             "shipmode",
             "comment"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             DOUBLE(),
             DOUBLE(),
             DOUBLE(),
             DOUBLE(),
             VARCHAR(),
             VARCHAR(),
             DATE(),
             DATE(),
             DATE(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR()});
  }

  void TearDown() override {
    connector::unregisterConnector(kHiveConnectorId);
    parquet::unregisterParquetReaderFactory();
  }

  int64_t date(std::string_view stringDate) const {
    Date date;
    parseTo(stringDate, date);
    return date.days();
  }

  // Split file at a given path 'filePath' into 'numSplits' splits.
  std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>> makeSplits(
      const std::string& filePath,
      int64_t numSplits = 10) const {
    const int fileSize = fs::file_size(filePath);
    // Take the upper bound.
    const int splitSize = std::ceil(fileSize / numSplits);
    std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>> splits;

    // Add all the splits.
    for (int i = 0; i < numSplits; i++) {
      auto split = HiveConnectorTestBase::makeHiveConnectorSplit(
          filePath, i * splitSize, splitSize);
      split->fileFormat = dwio::common::FileFormat::PARQUET;
      splits.push_back(std::move(split));
    }
    return splits;
  }

  // Write the DuckDB Lineitem TPC-H table to a Parquet file and return the file
  // location.
  std::string writeLineitemTableToParquet() const {
    constexpr std::string_view tableName("lineitem");
    constexpr int kRowGroupSize = 10'000;
    const auto& filePath =
        fmt::format("{}/{}.parquet", tempDirectory_->path, tableName);
    // Convert decimal columns to double.
    const auto& query = fmt::format(
        "COPY (SELECT l_orderkey as orderkey, l_partkey as partkey, l_suppkey as suppkey, l_linenumber as linenumber, "
        "l_quantity::DOUBLE as quantity, l_extendedprice::DOUBLE as extendedprice, l_discount::DOUBLE as discount, "
        "l_tax::DOUBLE as tax, l_returnflag as returnflag, l_linestatus as linestatus, "
        "l_shipdate as shipdate, l_receiptdate as receiptdate, "
        "l_shipinstruct as shipinstruct, l_shipmode as shipmode, l_comment as comment "
        "FROM {}) TO '{}' (FORMAT 'parquet', ROW_GROUP_SIZE {})",
        tableName,
        filePath,
        kRowGroupSize);
    duckDb_->execute(query);
    return filePath;
  }

  RowTypePtr getLineitemColumns(std::vector<std::string> names) const {
    std::vector<TypePtr> types;
    for (auto colName : names) {
      types.push_back(lineitemType_->findChild(colName));
    }
    return ROW(std::move(names), std::move(types));
  }

  std::shared_ptr<Task> assertQuery(
      const CursorParameters& params,
      const std::string& filePath,
      const core::PlanNodeId& sourcePlanNodeId,
      const std::string& duckQuery) const {
    bool noMoreSplits = false;
    return exec::test::assertQuery(
        params,
        [&](exec::Task* task) {
          if (!noMoreSplits) {
            auto const& splits = makeSplits(filePath);
            for (const auto& split : splits) {
              task->addSplit(sourcePlanNodeId, exec::Split(split));
            }
            task->noMoreSplits(sourcePlanNodeId);
            noMoreSplits = true;
          }
        },
        duckQuery,
        *duckDb_);
  }

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_;
  RowTypePtr lineitemType_;
};

std::shared_ptr<DuckDbQueryRunner> ParquetTpchTest::duckDb_ = nullptr;

TEST_F(ParquetTpchTest, q1) {
  const auto filePath = writeLineitemTableToParquet();

  auto rowType = getLineitemColumns(
      {"returnflag",
       "linestatus",
       "quantity",
       "extendedprice",
       "discount",
       "tax",
       "shipdate"});

  // shipdate <= '1998-09-02'
  auto filters =
      common::test::SubfieldFiltersBuilder()
          .add("shipdate", common::test::lessThanOrEqual(date("1998-09-02")))
          .build();

  CursorParameters params;
  params.maxDrivers = 4;
  params.numResultDrivers = 1;
  static const core::SortOrder kAscNullsLast(true, false);

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId sourcePlanNodeId;

  const auto stage1 =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              rowType,
              HiveConnectorTestBase::makeTableHandle(std::move(filters)),
              HiveConnectorTestBase::allRegularColumns(rowType))
          .capturePlanNodeId(sourcePlanNodeId)
          .project(
              {"returnflag",
               "linestatus",
               "quantity",
               "extendedprice",
               "extendedprice * (1.0 - discount) AS sum_disc_price",
               "extendedprice * (1.0 - discount) * (1.0 + tax) AS sum_charge",
               "discount"})
          .partialAggregation(
              {0, 1},
              {"sum(quantity)",
               "sum(extendedprice)",
               "sum(sum_disc_price)",
               "sum(sum_charge)",
               "avg(quantity)",
               "avg(extendedprice)",
               "avg(discount)",
               "count(0)"})
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .localPartition({}, {stage1})
                  .finalAggregation(
                      {0, 1},
                      {"sum(a0)",
                       "sum(a1)",
                       "sum(a2)",
                       "sum(a3)",
                       "avg(a4)",
                       "avg(a5)",
                       "avg(a6)",
                       "count(a7)"},
                      {DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       BIGINT()})
                  .orderBy({0, 1}, {kAscNullsLast, kAscNullsLast}, false)
                  // Additional step for double type result verification
                  .project(
                      {"returnflag",
                       "linestatus",
                       "round(a0, cast(2 as integer))",
                       "round(a1, cast(2 as integer))",
                       "round(a2, cast(2 as integer))",
                       "round(a3, cast(2 as integer))",
                       "round(a4, cast(2 as integer))",
                       "round(a5, cast(2 as integer))",
                       "round(a6, cast(2 as integer))",
                       "a7"})
                  .planNode();

  params.planNode = std::move(plan);
  auto duckDbSql = duckDb_->getTpchQuery(1);
  // Additional steps for double type result verification.
  const auto& duckDBRounded = fmt::format(
      "select l_returnflag, l_linestatus, round(sum_qty, 2), "
      "round(sum_base_price, 2), round(sum_disc_price, 2), round(sum_charge, 2), "
      "round(avg_qty, 2), round(avg_price, 2), round(avg_disc, 2),"
      "count_order from ({})",
      duckDbSql);
  auto task = assertQuery(params, filePath, sourcePlanNodeId, duckDBRounded);

  const auto& stats = task->taskStats();
  // There should be two pipelines.
  ASSERT_EQ(2, stats.pipelineStats.size());
  // We used the default of 10 splits per file.
  ASSERT_EQ(10, stats.numFinishedSplits);
}

TEST_F(ParquetTpchTest, q6) {
  const auto& filePath = writeLineitemTableToParquet();

  auto rowType =
      getLineitemColumns({"shipdate", "extendedprice", "quantity", "discount"});

  auto filters =
      common::test::SubfieldFiltersBuilder()
          .add(
              "shipdate",
              common::test::between(date("1994-01-01"), date("1994-12-31")))
          .add("discount", common::test::betweenDouble(0.05, 0.07))
          .add("quantity", common::test::lessThanDouble(24.0))
          .build();

  CursorParameters params;
  params.maxDrivers = 4;
  params.numResultDrivers = 1;

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId sourcePlanNodeId;

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       rowType,
                       HiveConnectorTestBase::makeTableHandle(
                           std::move(filters)),
                       HiveConnectorTestBase::allRegularColumns(rowType))
                   .capturePlanNodeId(sourcePlanNodeId)
                   .project({"extendedprice * discount"})
                   .partialAggregation({}, {"sum(p0)"})
                   .planNode()})
          .finalAggregation({}, {"sum(a0)"}, {DOUBLE()})
          // Additional step for double type result verification
          .project({"round(a0, cast(2 as integer))"})
          .planNode();

  params.planNode = std::move(plan);
  auto duckDbSql = duckDb_->getTpchQuery(6);
  // Additional steps for double type result verification.
  const auto& duckDBRounded =
      fmt::format("select round(revenue, 2) from ({})", duckDbSql);

  auto task = assertQuery(params, filePath, sourcePlanNodeId, duckDBRounded);

  const auto& stats = task->taskStats();
  // There should be two pipelines
  ASSERT_EQ(2, stats.pipelineStats.size());
  // We used the default of 10 splits
  ASSERT_EQ(10, stats.numFinishedSplits);
}
