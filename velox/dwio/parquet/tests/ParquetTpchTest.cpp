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
#include <vector>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class ParquetTpchTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});

    duckDb_ = std::make_shared<DuckDbQueryRunner>();
    tempDirectory_ = TempDirectoryPath::create();
    tpchBuilder_ =
        std::make_shared<TpchQueryBuilder>(dwio::common::FileFormat::PARQUET);

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();

    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    dwio::common::registerFileSinks();

    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();

    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);

    connector::registerConnectorFactory(
        std::make_shared<connector::tpch::TpchConnectorFactory>());
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(
                kTpchConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpchConnector);

    saveTpchTablesAsParquet();
    tpchBuilder_->initialize(tempDirectory_->getPath());
  }

  static void TearDownTestSuite() {
    connector::unregisterConnectorFactory(
        connector::hive::HiveConnectorFactory::kHiveConnectorName);
    connector::unregisterConnectorFactory(
        connector::tpch::TpchConnectorFactory::kTpchConnectorName);
    connector::unregisterConnector(kHiveConnectorId);
    connector::unregisterConnector(kTpchConnectorId);
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
  }

  static void saveTpchTablesAsParquet() {
    std::shared_ptr<memory::MemoryPool> rootPool{
        memory::memoryManager()->addRootPool()};
    std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

    for (const auto& table : tpch::tables) {
      auto tableName = toTableName(table);
      auto tableDirectory =
          fmt::format("{}/{}", tempDirectory_->getPath(), tableName);
      auto tableSchema = tpch::getTableSchema(table);
      auto columnNames = tableSchema->names();
      auto plan = PlanBuilder()
                      .tpchTableScan(table, std::move(columnNames), 0.01)
                      .planNode();
      auto split =
          exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
              kTpchConnectorId, 1, 0));

      auto rows =
          AssertQueryBuilder(plan).splits({split}).copyResults(pool.get());
      duckDb_->createTable(tableName.data(), {rows});

      plan = PlanBuilder()
                 .values({rows})
                 .tableWrite(tableDirectory, dwio::common::FileFormat::PARQUET)
                 .planNode();

      AssertQueryBuilder(plan).copyResults(pool.get());
    }
  }

  void assertQuery(
      int queryId,
      const std::optional<std::vector<uint32_t>>& sortingKeys = {}) {
    auto tpchPlan = tpchBuilder_->getQueryPlan(queryId);
    auto duckDbSql = tpch::getQuery(queryId);
    assertQuery(tpchPlan, duckDbSql, sortingKeys);
  }

  std::shared_ptr<Task> assertQuery(
      const TpchPlan& tpchPlan,
      const std::string& duckQuery,
      const std::optional<std::vector<uint32_t>>& sortingKeys) const {
    bool noMoreSplits = false;
    constexpr int kNumSplits = 10;
    constexpr int kNumDrivers = 4;
    auto addSplits = [&](Task* task) {
      if (!noMoreSplits) {
        for (const auto& entry : tpchPlan.dataFiles) {
          for (const auto& path : entry.second) {
            auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                path, kNumSplits, tpchPlan.dataFileFormat);
            for (const auto& split : splits) {
              task->addSplit(entry.first, Split(split));
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

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  static std::shared_ptr<TempDirectoryPath> tempDirectory_;
  static std::shared_ptr<TpchQueryBuilder> tpchBuilder_;

  static constexpr char const* kTpchConnectorId{"test-tpch"};
};

std::shared_ptr<DuckDbQueryRunner> ParquetTpchTest::duckDb_ = nullptr;
std::shared_ptr<TempDirectoryPath> ParquetTpchTest::tempDirectory_ = nullptr;
std::shared_ptr<TpchQueryBuilder> ParquetTpchTest::tpchBuilder_ = nullptr;

TEST_F(ParquetTpchTest, Q1) {
  assertQuery(1);
}

TEST_F(ParquetTpchTest, Q2) {
  std::vector<uint32_t> sortingKeys{0, 1, 2, 3};
  assertQuery(2, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q3) {
  std::vector<uint32_t> sortingKeys{1, 2};
  assertQuery(3, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q4) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(4, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q5) {
  std::vector<uint32_t> sortingKeys{1};
  assertQuery(5, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q6) {
  assertQuery(6);
}

TEST_F(ParquetTpchTest, Q7) {
  std::vector<uint32_t> sortingKeys{0, 1, 2};
  assertQuery(7, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q8) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(8, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q9) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(9, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q10) {
  std::vector<uint32_t> sortingKeys{2};
  assertQuery(10, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q11) {
  std::vector<uint32_t> sortingKeys{1};
  assertQuery(11, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q12) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(12, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q13) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(13, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q14) {
  assertQuery(14);
}

TEST_F(ParquetTpchTest, Q15) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(15, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q16) {
  std::vector<uint32_t> sortingKeys{0, 1, 2, 3};
  assertQuery(16, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q17) {
  assertQuery(17);
}

TEST_F(ParquetTpchTest, Q18) {
  assertQuery(18);
}

TEST_F(ParquetTpchTest, Q19) {
  assertQuery(19);
}

TEST_F(ParquetTpchTest, Q20) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(20, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q21) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(21, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q22) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(22, std::move(sortingKeys));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
