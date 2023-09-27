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

#include <folly/Singleton.h>
#include "gtest/gtest.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/tests/HdfsMiniCluster.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::test;

class InsertIntoHdfsTest : public HiveConnectorTestBase {
 public:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    if (miniCluster == nullptr) {
      miniCluster = std::make_shared<filesystems::test::HdfsMiniCluster>();
      miniCluster->start();
    }
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
    miniCluster->stop();
  }

  void setDataTypes(const RowTypePtr& inputType) {
    rowType_ = inputType;
  }

  static std::shared_ptr<filesystems::test::HdfsMiniCluster> miniCluster;
  RowTypePtr rowType_;
};

std::shared_ptr<filesystems::test::HdfsMiniCluster>
    InsertIntoHdfsTest::miniCluster = nullptr;

TEST_F(InsertIntoHdfsTest, insertIntoHdfsTest) {
  folly::SingletonVault::singleton()->registrationComplete();
  const int64_t expectedRows = 1000;
  setDataTypes(ROW(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()}));

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(expectedRows, [](auto row) { return row; }),
       makeFlatVector<int32_t>(expectedRows, [](auto row) { return row; }),
       makeFlatVector<int16_t>(expectedRows, [](auto row) { return row; }),
       makeFlatVector<double>(expectedRows, [](auto row) { return row; })});

  auto outputDirectory = "hdfs://localhost:7878/";
  // INSERT INTO hdfs with one writer
  auto plan = PlanBuilder()
                  .values({input})
                  .tableWrite(outputDirectory, dwio::common::FileFormat::DWRF)
                  .planNode();

  auto results = AssertQueryBuilder(plan).copyResults(pool());

  // First column has number of rows written in the first row and nulls in other
  // rows.
  auto rowCount = results->childAt(TableWriteTraits::kRowCountChannel)
                      ->as<FlatVector<int64_t>>();
  ASSERT_FALSE(rowCount->isNullAt(0));
  ASSERT_EQ(expectedRows, rowCount->valueAt(0));
  ASSERT_TRUE(rowCount->isNullAt(1));

  // Second column contains details about written files.
  auto details = results->childAt(TableWriteTraits::kFragmentChannel)
                     ->as<FlatVector<StringView>>();
  ASSERT_TRUE(details->isNullAt(0));
  ASSERT_FALSE(details->isNullAt(1));
  folly::dynamic obj = folly::parseJson(details->valueAt(1));

  ASSERT_EQ(expectedRows, obj["rowCount"].asInt());
  auto fileWriteInfos = obj["fileWriteInfos"];
  ASSERT_EQ(1, fileWriteInfos.size());

  auto writeFileName = fileWriteInfos[0]["writeFileName"].asString();

  // Read from 'writeFileName' and verify the data matches the original.
  plan = PlanBuilder().tableScan(rowType_).planNode();

  auto splits = HiveConnectorTestBase::makeHiveConnectorSplits(
      fmt::format("{}/{}", outputDirectory, writeFileName),
      1,
      dwio::common::FileFormat::DWRF);
  auto copy = AssertQueryBuilder(plan).split(splits[0]).copyResults(pool());
  assertEqualResults({input}, {copy});
}
