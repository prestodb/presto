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
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

static const int kNumDrivers = 4;

class ParquetTpchTest : public testing::Test {
 protected:
  // Setup a DuckDB instance for the entire suite and load TPC-H data with scale
  // factor 0.01.
  static void SetUpTestSuite() {
    if (duckDb_ == nullptr) {
      duckDb_ = std::make_shared<DuckDbQueryRunner>();
      constexpr double kTpchScaleFactor = 0.01;
      duckDb_->initializeTpch(kTpchScaleFactor);
    }
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    parquet::registerParquetReaderFactory();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);
    tempDirectory_ = exec::test::TempDirectoryPath::create();
    saveTpchTablesAsParquet();
    tpchBuilder_.initialize(tempDirectory_->path);
  }

  /// Write TPC-H tables as a Parquet file to temp directory in hive-style
  /// partition
  static void saveTpchTablesAsParquet() {
    constexpr int kRowGroupSize = 10'000;
    const auto tableNames = tpchBuilder_.getTableNames();
    for (const auto& tableName : tableNames) {
      auto tableDirectory =
          fmt::format("{}/{}", tempDirectory_->path, tableName);
      fs::create_directory(tableDirectory);
      auto filePath = fmt::format("{}/file.parquet", tableDirectory);
      auto query = fmt::format(
          duckDbParquetWriteSQL_.at(tableName),
          tableName,
          filePath,
          kRowGroupSize);
      duckDb_->execute(query);
    }
  }

  static void TearDownTestSuite() {
    connector::unregisterConnector(kHiveConnectorId);
    parquet::unregisterParquetReaderFactory();
  }

  std::shared_ptr<Task> assertQuery(
      const TpchPlan& tpchPlan,
      const std::string& duckQuery) const {
    bool noMoreSplits = false;
    constexpr int kNumSplits = 10;
    auto addSplits = [&](exec::Task* task) {
      if (!noMoreSplits) {
        for (const auto& entry : tpchPlan.dataFiles) {
          for (const auto& path : entry.second) {
            auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                path, kNumSplits, tpchPlan.dataFileFormat);
            for (const auto& split : splits) {
              task->addSplit(entry.first, exec::Split(split));
            }
          }
          task->noMoreSplits(entry.first);
        }
      }
      noMoreSplits = true;
    };
    CursorParameters params;
    params.maxDrivers = kNumDrivers;
    params.numResultDrivers = 1;
    params.planNode = tpchPlan.plan;
    return exec::test::assertQuery(params, addSplits, duckQuery, *duckDb_);
  }

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  static std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_;
  static TpchQueryBuilder tpchBuilder_;
  static std::unordered_map<std::string, std::string> duckDbParquetWriteSQL_;
};

std::shared_ptr<DuckDbQueryRunner> ParquetTpchTest::duckDb_ = nullptr;
std::shared_ptr<exec::test::TempDirectoryPath> ParquetTpchTest::tempDirectory_ =
    nullptr;
TpchQueryBuilder ParquetTpchTest::tpchBuilder_(
    facebook::velox::dwio::common::FileFormat::PARQUET);
std::unordered_map<std::string, std::string>
    ParquetTpchTest::duckDbParquetWriteSQL_ = {std::make_pair(
        "lineitem",
        R"(COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
        l_quantity::DOUBLE as l_quantity, l_extendedprice::DOUBLE as l_extendedprice, l_discount::DOUBLE as l_discount,
        l_tax::DOUBLE as l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
        l_shipinstruct, l_shipmode, l_comment FROM {}) TO '{}' (FORMAT 'parquet', ROW_GROUP_SIZE {}))")};

TEST_F(ParquetTpchTest, q1) {
  auto tpchPlan = tpchBuilder_.getQueryPlan(1);
  auto duckDbSql = duckDb_->getTpchQuery(1);
  auto task = assertQuery(tpchPlan, duckDbSql);

  const auto& stats = task->taskStats();
  // There should be two pipelines.
  ASSERT_EQ(2, stats.pipelineStats.size());
  // We used the default of 10 splits per file.
  ASSERT_EQ(10, stats.numFinishedSplits);
}

TEST_F(ParquetTpchTest, q6) {
  auto tpchPlan = tpchBuilder_.getQueryPlan(6);
  auto duckDbSql = duckDb_->getTpchQuery(6);
  auto task = assertQuery(tpchPlan, duckDbSql);

  const auto& stats = task->taskStats();
  // There should be two pipelines
  ASSERT_EQ(2, stats.pipelineStats.size());
  // We used the default of 10 splits
  ASSERT_EQ(10, stats.numFinishedSplits);
}
