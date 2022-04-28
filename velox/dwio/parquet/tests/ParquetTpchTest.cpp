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
#include "velox/common/base/tests/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

#include <vector>

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
          fmt::runtime(duckDbParquetWriteSQL_.at(tableName)),
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
      const std::string& duckQuery,
      std::optional<std::vector<uint32_t>> sortingKeys) const {
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
    params.planNode = tpchPlan.plan;
    return exec::test::assertQuery(
        params, addSplits, duckQuery, *duckDb_, sortingKeys);
  }

  void assertQuery(
      int queryId,
      const int expectedPipelineCount,
      const int expectedFinishedSplits,
      std::optional<std::vector<uint32_t>> sortingKeys = {}) const {
    auto tpchPlan = tpchBuilder_.getQueryPlan(queryId);
    auto duckDbSql = duckDb_->getTpchQuery(queryId);
    auto task = assertQuery(tpchPlan, duckDbSql, sortingKeys);
    const auto& stats = task->taskStats();
    ASSERT_EQ(expectedPipelineCount, stats.pipelineStats.size());
    ASSERT_EQ(expectedFinishedSplits, stats.numFinishedSplits);
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
    dwio::common::FileFormat::PARQUET);

std::unordered_map<std::string, std::string>
    ParquetTpchTest::duckDbParquetWriteSQL_ = {
        std::make_pair(
            "lineitem",
            R"(COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
        l_quantity::DOUBLE as quantity, l_extendedprice::DOUBLE as extendedprice, l_discount::DOUBLE as discount,
        l_tax::DOUBLE as tax, l_returnflag, l_linestatus, l_shipdate AS shipdate, l_commitdate, l_receiptdate,
        l_shipinstruct, l_shipmode, l_comment FROM {}) TO '{}' (FORMAT 'parquet', ROW_GROUP_SIZE {}))"),
        std::make_pair(
            "orders",
            R"(COPY (SELECT o_orderkey, o_custkey, o_orderstatus,
        o_totalprice::DOUBLE as o_totalprice,
        o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
        FROM {}) TO '{}' (FORMAT 'parquet', ROW_GROUP_SIZE {}))"),
        std::make_pair(
            "customer",
            R"(COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone,
        c_acctbal::DOUBLE as c_acctbal, c_mktsegment, c_comment
        FROM {}) TO '{}' (FORMAT 'parquet', ROW_GROUP_SIZE {}))"),
};

TEST_F(ParquetTpchTest, Q1) {
  assertQuery(1, 2, 10);
}

TEST_F(ParquetTpchTest, Q6) {
  assertQuery(6, 2, 10);
}

TEST_F(ParquetTpchTest, Q13) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(13, 3, 20, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q18) {
  assertQuery(18, 5, 30);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
